"""可校验哈希审计链（仅追加）。

链规则：
- seq 全局递增，由 SQLite 按提交顺序分配；
- 首条记录 prev_hash 为 64 个 "0"，后续记录引用上一条 hash；
- hash = SHA-256(prev_hash 的原始 32 字节 + UTF-8 canonical JSON 事件)；
- canonical JSON 使用键排序且无多余空白。

所有追加动作必须发生在消费判定 / 释放各自的原写事务内：
事务回滚则审计记录一并回滚；幂等重放走固化结果分支，不追加记录。
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from typing import Any

# 首条记录的前驱哈希：64 个 0（hex 表示的 32 个零字节）
GENESIS_HASH = "0" * 64

OP_CONSUME = "consume"
OP_RELEASE = "release"


def canonical_event_json(event: dict[str, Any]) -> str:
    """事件的 canonical JSON：键排序、无多余空白，ASCII 形式与 dict 顺序无关。"""
    return json.dumps(event, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def compute_hash(prev_hash_hex: str, event_json: str) -> str:
    """SHA-256(prev_hash 原始 32 字节 + UTF-8 canonical JSON 事件)。"""
    return hashlib.sha256(
        bytes.fromhex(prev_hash_hex) + event_json.encode("utf-8")
    ).hexdigest()


def append_event(
    conn: sqlite3.Connection,
    *,
    op: str,
    biz_id: str,
    result: str,
    event_ts: float,
    request_id: str | None = None,
) -> dict[str, Any]:
    """在当前事务尾部追加一条审计记录，返回写入的记录 dict。

    BEGIN IMMEDIATE 已使写事务串行化：取链头与插入之间不会有其他提交插入，
    因此 prev_hash 引用必然连续。
    """
    event: dict[str, Any] = {"op": op, "biz_id": biz_id, "result": result, "ts": event_ts}
    if op == OP_RELEASE:
        # 释放事件同时带上目标消费 ID，保证事件自身可完全说明业务关联
        event["request_id"] = request_id

    event_json = canonical_event_json(event)
    head = conn.execute("SELECT seq, hash FROM audit_log ORDER BY seq DESC LIMIT 1").fetchone()
    seq = 1 if head is None else head["seq"] + 1
    prev_hash = GENESIS_HASH if head is None else head["hash"]
    record_hash = compute_hash(prev_hash, event_json)

    conn.execute(
        """
        INSERT INTO audit_log
            (seq, op, biz_id, request_id, result, event_ts, prev_hash, hash, event_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (seq, op, biz_id, request_id, result, event_ts, prev_hash, record_hash, event_json),
    )
    return {
        "seq": seq,
        "prev_hash": prev_hash,
        "hash": record_hash,
        "event": json.loads(event_json),
    }


def get_head(conn: sqlite3.Connection) -> dict[str, Any]:
    """链头：无记录时 seq=0、hash 为创世 64 个 0。"""
    row = conn.execute("SELECT seq, hash FROM audit_log ORDER BY seq DESC LIMIT 1").fetchone()
    if row is None:
        return {"seq": 0, "hash": GENESIS_HASH}
    return {"seq": row["seq"], "hash": row["hash"]}


def list_records(
    conn: sqlite3.Connection, after_seq: int = 0, limit: int = 100
) -> list[dict[str, Any]]:
    """按 seq 升序返回 after_seq 之后的审计记录（最多 limit 条）。"""
    rows = conn.execute(
        """
        SELECT seq, op, biz_id, request_id, result, event_ts, prev_hash, hash, event_json
        FROM audit_log WHERE seq > ? ORDER BY seq ASC LIMIT ?
        """,
        (after_seq, limit),
    ).fetchall()
    return [_record_to_dict(r) for r in rows]


def _record_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    # 以入哈希的 event_json 原文为准解析事件，避免响应与哈希载荷不一致
    return {
        "seq": row["seq"],
        "op": row["op"],
        "biz_id": row["biz_id"],
        "result": row["result"],
        "ts": row["event_ts"],
        "prev_hash": row["prev_hash"],
        "hash": row["hash"],
        "event": json.loads(row["event_json"]),
    }


def verify_chain(conn: sqlite3.Connection) -> dict[str, Any]:
    """按 seq 升序重算整条链。

    返回：
    - valid：seq 是否从 1 连续、每条 prev_hash/hash 是否吻合；
    - checked_count：参与校验的记录数；
    - head_hash：当前链尾 hash（空链为创世哈希）；
    - first_invalid_seq：首个失效位置的 seq，全部有效时为 None。
    """
    rows = conn.execute(
        "SELECT seq, prev_hash, hash, event_json FROM audit_log ORDER BY seq ASC"
    ).fetchall()

    expected_seq = 1
    prev_hash = GENESIS_HASH
    first_invalid_seq: int | None = None

    for row in rows:
        invalid = (
            row["seq"] != expected_seq
            or row["prev_hash"] != prev_hash
            or row["hash"] != compute_hash(row["prev_hash"], row["event_json"])
        )
        if invalid and first_invalid_seq is None:
            first_invalid_seq = row["seq"]
        # 以库存 hash 继续向后比对引用，确保链上后续断点也被独立覆盖
        prev_hash = row["hash"]
        expected_seq += 1

    return {
        "valid": first_invalid_seq is None,
        "checked_count": len(rows),
        "head_hash": prev_hash,
        "first_invalid_seq": first_invalid_seq,
    }
