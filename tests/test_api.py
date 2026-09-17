"""端到端测试：原子扣减、幂等、409、并发不超限、滚动窗口恢复、哈希审计链。"""
import hashlib
import json
import os
import sqlite3
import tempfile
import threading
import time
import uuid

# 必须在导入 app 之前设置环境（config 在导入时读取）
_TMP = tempfile.mkdtemp(prefix="quota-test-")
os.environ["QUOTA_DB_PATH"] = os.path.join(_TMP, "test.db")
os.environ["QUOTA_BUCKETS_CONFIG"] = os.path.join(_TMP, "buckets.json")

with open(os.environ["QUOTA_BUCKETS_CONFIG"], "w") as fh:
    json.dump(
        {"buckets": [{"key": "seeded", "limit": 100, "window_seconds": 60}]},
        fh,
    )

from fastapi.testclient import TestClient  # noqa: E402

from app.main import app  # noqa: E402

# 以上下文管理器方式启动，确保 lifespan（建表 + 播种桶配置）执行
_client_ctx = TestClient(app)
client = _client_ctx.__enter__()


def _rid() -> str:
    return uuid.uuid4().hex


def _bucket(limit: int, window: float) -> str:
    """为单个测试创建独立桶，避免测试间窗口互相干扰。"""
    key = f"b-{_rid()[:12]}"
    r = client.put(f"/v1/buckets/{key}", json={"limit": limit, "window_seconds": window})
    assert r.status_code == 200, r.text
    return key


def _consume(rid: str, key: str, amount: int):
    return client.post("/v1/quota/consume", json={
        "request_id": rid, "buckets": [{"key": key, "amount": amount}],
    })


def _release(rel_id: str, rid: str):
    return client.post("/v1/quota/releases", json={
        "release_id": rel_id, "request_id": rid,
    })


def test_seeded_bucket_and_consume():
    # 启动时从 QUOTA_BUCKETS_CONFIG 播种
    keys = [b["key"] for b in client.get("/v1/buckets").json()["buckets"]]
    assert "seeded" in keys

    rid = _rid()
    r = client.post("/v1/quota/consume", json={
        "request_id": rid,
        "buckets": [{"key": "seeded", "amount": 30}],
    })
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["allowed"] is True
    assert body["request_id"] == rid

    usage = client.get("/v1/buckets/seeded/usage").json()
    assert usage["used"] == 30
    assert usage["remaining"] == 70
    assert usage["limit"] == 100
    assert usage["window_seconds"] == 60


def test_atomic_all_or_nothing():
    """多桶请求中任一超限 → 全部不扣减。"""
    k1, k2 = _bucket(10, 60), _bucket(5, 60)
    r = client.post("/v1/quota/consume", json={
        "request_id": _rid(),
        "buckets": [{"key": k1, "amount": 1}, {"key": k2, "amount": 100}],
    })
    assert r.status_code == 429
    body = r.json()
    assert body["allowed"] is False
    assert [b["key"] for b in body["limited_buckets"]] == [k2]
    # 请求量本身超过限额 → 永远不可重试
    assert body["retry_at"] is None
    assert client.get(f"/v1/buckets/{k1}/usage").json()["used"] == 0
    assert client.get(f"/v1/buckets/{k2}/usage").json()["used"] == 0


def test_denied_returns_retry_time():
    key = _bucket(2, 60)
    assert _consume(_rid(), key, 2).status_code == 200

    r = _consume(_rid(), key, 1)
    assert r.status_code == 429
    body = r.json()
    lb = body["limited_buckets"][0]
    assert lb["key"] == key
    assert lb["used"] == 2 and lb["requested"] == 1 and lb["limit"] == 2
    assert 0 < body["retry_after_seconds"] <= 60
    assert body["retry_at"] is not None
    assert "Retry-After" in r.headers


def test_idempotent_replay():
    key = _bucket(10, 60)
    rid = _rid()
    payload = {"request_id": rid, "buckets": [{"key": key, "amount": 3}]}
    r1 = client.post("/v1/quota/consume", json=payload)
    assert r1.status_code == 200

    r2 = client.post("/v1/quota/consume", json=payload)
    assert r2.status_code == 200
    assert r2.json() == r1.json()  # 原样返回固化判定
    assert r2.headers.get("X-Idempotent-Replay") == "true"
    # 不得重复扣减
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 3


def test_idempotent_replay_of_denied_decision():
    key = _bucket(2, 60)
    assert _consume(_rid(), key, 2).status_code == 200

    rid = _rid()
    payload = {"request_id": rid, "buckets": [{"key": key, "amount": 1}]}
    r1 = client.post("/v1/quota/consume", json=payload)
    assert r1.status_code == 429
    r2 = client.post("/v1/quota/consume", json=payload)
    assert r2.status_code == 429
    assert r2.json() == r1.json()
    assert r2.headers.get("X-Idempotent-Replay") == "true"


def test_conflict_on_changed_payload():
    key = _bucket(10, 60)
    rid = _rid()
    assert _consume(rid, key, 1).status_code == 200
    r = _consume(rid, key, 2)
    assert r.status_code == 409


def test_same_amount_different_bucket_key_is_conflict():
    k1, k2 = _bucket(10, 60), _bucket(10, 60)
    rid = _rid()
    assert _consume(rid, k1, 1).status_code == 200
    assert _consume(rid, k2, 1).status_code == 409


def test_unknown_bucket_404():
    r = client.post("/v1/quota/consume", json={
        "request_id": _rid(), "buckets": [{"key": "nope", "amount": 1}],
    })
    assert r.status_code == 404
    assert client.get("/v1/buckets/nope/usage").status_code == 404


def test_rolling_window_recovers():
    key = _bucket(2, 0.4)
    assert _consume(_rid(), key, 2).status_code == 200
    assert _consume(_rid(), key, 1).status_code == 429
    time.sleep(0.5)  # 窗口 0.4s，等待滚动过期
    assert _consume(_rid(), key, 1).status_code == 200


def test_rolling_window_partial_expiry():
    """窗口滚动时按事件过期逐步释放额度。"""
    key = _bucket(3, 0.6)
    assert _consume(_rid(), key, 2).status_code == 200
    time.sleep(0.35)
    assert _consume(_rid(), key, 1).status_code == 200  # 打满 3/3
    assert _consume(_rid(), key, 1).status_code == 429
    time.sleep(0.35)  # 第一笔(2)过期，释放 2 额度
    assert _consume(_rid(), key, 2).status_code == 200


def test_concurrent_requests_never_exceed_limit():
    """50 个并发请求争抢限额 10 的桶：放行数必须恰好为 10。"""
    key = _bucket(10, 60)
    results = []

    def hit():
        results.append(_consume(_rid(), key, 1).status_code)

    threads = [threading.Thread(target=hit) for _ in range(50)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert results.count(200) == 10, f"放行 {results.count(200)} 次，超出限额 10"
    assert results.count(429) == 40
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 10


def test_same_request_id_concurrent_single_deduction():
    """同一 request_id 并发提交 10 次：只应扣减一次。"""
    key = _bucket(100, 60)
    rid = _rid()
    payload = {"request_id": rid, "buckets": [{"key": key, "amount": 1}]}
    codes = []

    def hit():
        codes.append(client.post("/v1/quota/consume", json=payload).status_code)

    threads = [threading.Thread(target=hit) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert all(c == 200 for c in codes)
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 1


def test_bucket_upsert_updates_config():
    key = _bucket(5, 60)
    r = client.put(f"/v1/buckets/{key}", json={"limit": 7, "window_seconds": 30})
    assert r.status_code == 200
    usage = client.get(f"/v1/buckets/{key}/usage").json()
    assert usage["limit"] == 7 and usage["window_seconds"] == 30


def test_validation_errors():
    key = _bucket(5, 60)
    # amount 必须为正
    assert _consume(_rid(), key, 0).status_code == 422
    # buckets 不能为空
    assert client.post("/v1/quota/consume", json={
        "request_id": _rid(), "buckets": [],
    }).status_code == 422


# ---------------------------------------------------------------- 释放接口

def test_release_restores_all_buckets():
    """首次释放撤销对所有桶的用量贡献，额度恢复。"""
    k1, k2 = _bucket(10, 60), _bucket(20, 60)
    rid = _rid()
    r = client.post("/v1/quota/consume", json={
        "request_id": rid,
        "buckets": [{"key": k1, "amount": 4}, {"key": k2, "amount": 7}],
    })
    assert r.status_code == 200
    assert client.get(f"/v1/buckets/{k1}/usage").json()["used"] == 4
    assert client.get(f"/v1/buckets/{k2}/usage").json()["used"] == 7

    rel = _rid()
    rr = _release(rel, rid)
    assert rr.status_code == 200, rr.text
    body = rr.json()
    assert body["released"] is True
    assert body["release_id"] == rel and body["request_id"] == rid
    assert body["events_reverted"] == 2
    # 载荷经 merge_items 规范化后按 key 排序
    assert sorted(body["released_buckets"], key=lambda b: b["key"]) == sorted(
        [{"key": k1, "amount": 4}, {"key": k2, "amount": 7}],
        key=lambda b: b["key"],
    )
    assert "X-Idempotent-Replay" not in rr.headers

    assert client.get(f"/v1/buckets/{k1}/usage").json()["used"] == 0
    assert client.get(f"/v1/buckets/{k2}/usage").json()["used"] == 0


def test_release_frees_capacity_for_subsequent_consume():
    """释放后新的消费可以使用恢复出的额度。"""
    key = _bucket(3, 60)
    rid1 = _rid()
    assert _consume(rid1, key, 3).status_code == 200
    assert _consume(_rid(), key, 1).status_code == 429

    assert _release(_rid(), rid1).status_code == 200
    r = _consume(_rid(), key, 3)
    assert r.status_code == 200
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 3


def test_release_idempotent_replay():
    """相同 release_id + 相同 request_id 重放：原样返回，不再改动用量。"""
    key = _bucket(10, 60)
    rid = _rid()
    assert _consume(rid, key, 5).status_code == 200
    rel = _rid()
    r1 = _release(rel, rid)
    assert r1.status_code == 200

    # 再制造一笔用量，重放不应影响它
    assert _consume(_rid(), key, 3).status_code == 200
    r2 = _release(rel, rid)
    assert r2.status_code == 200
    assert r2.headers.get("X-Idempotent-Replay") == "true"
    assert r2.json() == r1.json()
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 3


def test_release_retarget_conflict():
    """相同 release_id 改指其他 request_id → 409，且不影响任一目标。"""
    key = _bucket(10, 60)
    rid1, rid2 = _rid(), _rid()
    assert _consume(rid1, key, 2).status_code == 200
    assert _consume(rid2, key, 3).status_code == 200
    rel = _rid()
    assert _release(rel, rid1).status_code == 200

    r = _release(rel, rid2)
    assert r.status_code == 409
    # rid1 已释放（-2），rid2 未被释放，用量只剩 3
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 3


def test_double_release_same_consume_conflict():
    """同一消费请求只能由一个 release_id 成功释放，第二个返回 409。"""
    key = _bucket(10, 60)
    rid = _rid()
    assert _consume(rid, key, 4).status_code == 200
    assert _release(_rid(), rid).status_code == 200

    r = _release(_rid(), rid)
    assert r.status_code == 409
    # 用量不会被重复"撤销"
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 0


def test_release_unknown_request_404():
    r = _release(_rid(), f"missing-{_rid()}")
    assert r.status_code == 404


def test_release_denied_request_conflict():
    """原判定为拒绝（429）的消费不可释放 → 409。"""
    key = _bucket(1, 60)
    assert _consume(_rid(), key, 1).status_code == 200
    rid = _rid()
    assert _consume(rid, key, 1).status_code == 429
    assert _release(_rid(), rid).status_code == 409
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 1


def test_consume_replay_after_release_returns_frozen_result():
    """释放后重放原 consume：仍返回已固化的放行结果，且不再次扣减。"""
    key = _bucket(10, 60)
    rid = _rid()
    payload = {"request_id": rid, "buckets": [{"key": key, "amount": 6}]}
    r1 = client.post("/v1/quota/consume", json=payload)
    assert r1.status_code == 200
    assert _release(_rid(), rid).status_code == 200
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 0

    r2 = client.post("/v1/quota/consume", json=payload)
    assert r2.status_code == 200
    assert r2.headers.get("X-Idempotent-Replay") == "true"
    assert r2.json() == r1.json()
    # 重放不得再次扣减
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 0


def test_release_atomic_restores_every_bucket():
    """释放是原子的：多桶用量全部恢复，不存在部分恢复。"""
    keys = [_bucket(5, 60) for _ in range(5)]
    rid = _rid()
    r = client.post("/v1/quota/consume", json={
        "request_id": rid,
        "buckets": [{"key": k, "amount": 3} for k in keys],
    })
    assert r.status_code == 200
    assert _release(_rid(), rid).status_code == 200
    for k in keys:
        assert client.get(f"/v1/buckets/{k}/usage").json()["used"] == 0


def test_concurrent_releases_only_one_wins():
    """不同 release_id 并发释放同一消费：仅一个 200，其余 409。"""
    key = _bucket(10, 60)
    rid = _rid()
    assert _consume(rid, key, 5).status_code == 200

    outcomes: list[tuple[str, int]] = []
    barrier = threading.Barrier(20)

    def hit(rel: str):
        barrier.wait()
        outcomes.append((rel, _release(rel, rid).status_code))

    threads = [threading.Thread(target=hit, args=(_rid(),)) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert [c for _, c in outcomes].count(200) == 1
    assert [c for _, c in outcomes].count(409) == 19
    # 只删除了一次事件，用量为 0 而非负数
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 0


def test_concurrent_same_release_id_single_result():
    """同一 release_id 并发释放同一消费：全部 200，只执行一次撤销。"""
    key = _bucket(10, 60)
    rid = _rid()
    assert _consume(rid, key, 5).status_code == 200

    rel = _rid()
    codes = []
    barrier = threading.Barrier(15)

    def hit():
        barrier.wait()
        codes.append(_release(rel, rid).status_code)

    threads = [threading.Thread(target=hit) for _ in range(15)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert all(c == 200 for c in codes)
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 0


def test_release_then_new_consume_replay_interaction():
    """释放后：旧 consume 重放不扣减，新 request_id 可正常占用恢复额度。"""
    key = _bucket(5, 60)
    rid_old = _rid()
    assert _consume(rid_old, key, 5).status_code == 200
    assert _release(_rid(), rid_old).status_code == 200

    # 旧单重放不占额度
    assert _consume(rid_old, key, 5).status_code == 200
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 0
    # 新单可以打满恢复出的额度
    rid_new = _rid()
    assert _consume(rid_new, key, 5).status_code == 200
    assert client.get(f"/v1/buckets/{key}/usage").json()["used"] == 5
    assert _consume(_rid(), key, 1).status_code == 429


def test_release_validation_errors():
    r = client.post("/v1/quota/releases", json={
        "release_id": "", "request_id": "x",
    })
    assert r.status_code == 422
    r = client.post("/v1/quota/releases", json={"release_id": "r1"})
    assert r.status_code == 422


# ---------------------------------------------------------------- 哈希审计链

GENESIS = "0" * 64


def _all_audit(after_seq: int = 0):
    """分页拉全量审计记录（默认页大小 100，测试累计记录可能超过一页）。"""
    records = []
    while True:
        page = client.get(f"/v1/audit?after_seq={after_seq}&limit=100").json()
        batch = page["records"]
        records.extend(batch)
        after_seq = page["next_after_seq"]
        if len(batch) < 100:
            return records, page["head"]


def _audit_for(records, op: str, biz_id: str):
    return [r for r in records if r["op"] == op and r["biz_id"] == biz_id]


def _recompute(records, up_to: int | None = None):
    """按规则独立重算哈希链，返回 (prev_hash, hash) of the last checked record。"""
    prev = GENESIS
    h = GENESIS
    for r in records[:up_to]:
        canonical = json.dumps(r["event"], sort_keys=True, separators=(",", ":"))
        h = hashlib.sha256(bytes.fromhex(prev) + canonical.encode("utf-8")).hexdigest()
        assert r["prev_hash"] == prev, f"seq {r['seq']} 的 prev_hash 断链"
        prev = h
    return h


def test_audit_records_first_decisions_but_not_replays():
    """首次消费（放行与拒绝）各留一条审计；幂等重放不重复记录。"""
    key_ok = _bucket(10, 60)
    rid_allow = _rid()
    assert _consume(rid_allow, key_ok, 2).status_code == 200
    assert _consume(rid_allow, key_ok, 2).status_code == 200  # 重放
    assert _consume(rid_allow, key_ok, 9).status_code == 409  # 载荷冲突，非首次判定

    key_full = _bucket(1, 60)
    assert _consume(_rid(), key_full, 1).status_code == 200
    rid_deny = _rid()
    assert _consume(rid_deny, key_full, 1).status_code == 429
    assert _consume(rid_deny, key_full, 1).status_code == 429  # 拒绝重放

    records, head = _all_audit()
    allowed = _audit_for(records, "consume", rid_allow)
    denied = _audit_for(records, "consume", rid_deny)
    assert len(allowed) == 1 and allowed[0]["result"] == "allowed"
    assert len(denied) == 1 and denied[0]["result"] == "denied"
    # 事件含规定四要素
    ev = allowed[0]["event"]
    assert ev == {
        "op": "consume", "biz_id": rid_allow, "result": "allowed",
        "ts": allowed[0]["ts"],
    }
    # 链头指向最后一条记录
    assert head == {"seq": records[-1]["seq"], "hash": records[-1]["hash"]}


def test_audit_chain_hashes_and_seqs():
    """seq 全局递增从 1 开始；首条 prev_hash 为 64 个 0；哈希可被独立重算复现。"""
    _consume(_rid(), _bucket(5, 60), 1)
    records, head = _all_audit()
    seqs = [r["seq"] for r in records]
    assert seqs == list(range(1, len(records) + 1))
    assert records[0]["prev_hash"] == GENESIS
    last_hash = _recompute(records)
    verify = client.get("/v1/audit/verify").json()
    assert verify["valid"] is True
    assert verify["first_invalid_seq"] is None
    assert verify["checked_count"] == len(records)
    assert verify["head_hash"] == last_hash == head["hash"]


def test_audit_release_records_and_payload():
    """首次成功释放留一条 release 审计；重放与失败释放不留记录。"""
    key = _bucket(10, 60)
    rid = _rid()
    assert _consume(rid, key, 4).status_code == 200
    rel = _rid()
    assert _release(rel, rid).status_code == 200
    assert _release(rel, rid).status_code == 200  # 重放
    # 已释放消费的二次释放（不同 release_id）失败，不得留审计
    assert _release(_rid(), rid).status_code == 409
    # 不存在的消费释放失败，不得留审计
    assert _release(_rid(), f"missing-{_rid()}").status_code == 404

    records, _ = _all_audit()
    rels = _audit_for(records, "release", rel)
    assert len(rels) == 1
    r = rels[0]
    assert r["result"] == "released"
    assert r["event"] == {
        "op": "release", "biz_id": rel, "result": "released",
        "request_id": rid, "ts": r["ts"],
    }
    # release 与对应 consume 在同一条全局链上前后衔接
    assert r["prev_hash"] in {x["hash"] for x in records}


def test_audit_pagination_after_seq_and_limit():
    """after_seq 游标分页与 limit 约束生效。"""
    rid = _rid()
    assert _consume(rid, _bucket(5, 60), 1).status_code == 200
    records, _ = _all_audit()
    target = _audit_for(records, "consume", rid)[0]
    s = target["seq"]

    page = client.get(f"/v1/audit?after_seq={s - 1}&limit=1").json()
    assert [r["seq"] for r in page["records"]] == [s]
    assert page["next_after_seq"] == s

    page = client.get(f"/v1/audit?after_seq={s}&limit=10").json()
    assert all(r["seq"] > s for r in page["records"])

    assert client.get("/v1/audit?after_seq=-1").status_code == 422
    assert client.get("/v1/audit?limit=0").status_code == 422
    assert client.get("/v1/audit?limit=1001").status_code == 422


def test_audit_head_and_verify_response_shape():
    """链头字段形态固定为 {seq, hash}，hash 恒为 64 位十六进制。"""
    head = client.get("/v1/audit").json()["head"]
    assert set(head.keys()) == {"seq", "hash"}
    assert isinstance(head["seq"], int) and head["seq"] >= 1
    assert len(head["hash"]) == 64
    int(head["hash"], 16)


def test_audit_verify_detects_tampering():
    """篡改一条记录的哈希载荷后，verify 报告首个失效 seq。"""
    key = _bucket(10, 60)
    rid = _rid()
    assert _consume(rid, key, 1).status_code == 200
    records, head = _all_audit()
    target = _audit_for(records, "consume", rid)[0]
    seq, original_json = target["seq"], target["event"]
    head_after = head["hash"]
    db_path = os.environ["QUOTA_DB_PATH"]

    tampered = dict(original_json)
    tampered["result"] = "denied"
    bogus_json = json.dumps(tampered, sort_keys=True, separators=(",", ":"))
    try:
        wc = sqlite3.connect(db_path)
        wc.execute("UPDATE audit_log SET event_json = ? WHERE seq = ?", (bogus_json, seq))
        wc.commit()
        wc.close()

        verify = client.get("/v1/audit/verify").json()
        assert verify["valid"] is False
        assert verify["first_invalid_seq"] == seq
        # GET 返回的事件反映被篡改内容
        fetched = client.get(f"/v1/audit?after_seq={seq - 1}&limit=1").json()["records"][0]
        assert fetched["seq"] == seq
        assert fetched["event"]["result"] == "denied"
    finally:
        wc = sqlite3.connect(db_path)
        wc.execute(
            "UPDATE audit_log SET event_json = ? WHERE seq = ?",
            (json.dumps(original_json, sort_keys=True, separators=(",", ":")), seq),
        )
        wc.commit()
        wc.close()

    verify = client.get("/v1/audit/verify").json()
    assert verify["valid"] is True
    assert verify["head_hash"] == head_after


def test_audit_concurrent_consumes_one_record_each():
    """并发下审计链依然完整：每个首次判定恰好一条，链可整体校验通过。"""
    key = _bucket(20, 60)
    rids = [_rid() for _ in range(20)]
    barrier = threading.Barrier(20)

    def hit(r):
        barrier.wait()
        _consume(r, key, 1)

    threads = [threading.Thread(target=hit, args=(r,)) for r in rids]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    records, head = _all_audit()
    # 每个 request_id 恰好一条审计（桶限额 20，全部放行）
    for r in rids:
        assert len(_audit_for(records, "consume", r)) == 1
    verify = client.get("/v1/audit/verify").json()
    assert verify["valid"] is True
    assert verify["head_hash"] == head["hash"]
