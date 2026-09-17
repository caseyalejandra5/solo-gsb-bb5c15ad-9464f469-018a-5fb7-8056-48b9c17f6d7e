# Rolling Quota API（多桶滚动配额判定服务）

基于 **Python + FastAPI + SQLite** 的本地配额判定服务：一次请求可携带多个桶键，
在**同一事务**中完成全部桶的检查与扣减——任一超限则全部不扣减，并返回受限桶与最早重试时间。

## 核心特性

- **滚动窗口**：每个桶独立配置限额与窗口（秒），窗口内用量随事件过期滚动释放
- **原子扣减**：单事务内检查 + 扣减全部桶，任一超限则整体不扣减（all-or-nothing）
- **幂等判定**：`request_id` 首次出现时固化载荷与判定；相同内容重放原样返回
  （响应头 `X-Idempotent-Replay: true`），内容变化返回 **409**
- **原子释放**：`POST /v1/quota/releases` 在单事务内撤销一次已放行消费对**所有桶**
  的用量贡献；同一消费只能由一个 `release_id` 释放，重放原样返回
- **并发安全**：SQLite `BEGIN IMMEDIATE` 串行化写事务，并发请求不会突破限额
- **用量查询**：随时查询桶当前窗口用量
- **哈希审计链**：首次消费判定（放行或拒绝）与首次成功释放在各自原事务内追加
  仅追加（append-only）的可校验记录；`seq` 全局递增，SHA-256 链式哈希防篡改，
  幂等重放不重复记录

## 快速启动

```bash
docker compose up --build
```

单条命令完成构建、依赖安装与初始化（建表 + 从 `buckets.json` 播种桶配置），
服务监听 **8000** 端口，数据持久化在 `quota-data` 卷。

无 Docker 时本地运行：

```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 桶配置

启动时从 `buckets.json`（可用环境变量 `QUOTA_BUCKETS_CONFIG` 替换）播种，同名桶按文件内容更新：

```json
{
  "buckets": [
    {"key": "api-requests", "limit": 100, "window_seconds": 60},
    {"key": "llm-tokens",   "limit": 10000, "window_seconds": 60},
    {"key": "heavy-jobs",   "limit": 5,   "window_seconds": 300}
  ]
}
```

运行时也可动态创建/更新：`PUT /v1/buckets/{key}`，请求体 `{"limit": 100, "window_seconds": 60}`。

## 示例请求

### 1. 配额判定（检查并扣减）

```bash
curl -X POST http://localhost:8000/v1/quota/consume \
  -H 'Content-Type: application/json' \
  -d '{"request_id": "req-001",
       "buckets": [{"key": "api-requests", "amount": 1},
                   {"key": "llm-tokens",  "amount": 500}]}'
```

放行（`200`）：

```json
{"request_id": "req-001", "allowed": true,
 "granted": [{"key": "api-requests", "amount": 1},
             {"key": "llm-tokens", "amount": 500}],
 "decided_at": 1789657657.47}
```

超限（`429`，含 `Retry-After` 响应头；所有桶均未扣减）：

```json
{"request_id": "req-002", "allowed": false,
 "limited_buckets": [{"key": "llm-tokens", "limit": 10000, "window_seconds": 60,
                      "used": 9950, "requested": 500,
                      "retry_at": 1789657660.12, "retry_after_seconds": 2.65}],
 "retry_at": 1789657660.12, "retry_after_seconds": 2.65,
 "decided_at": 1789657657.47}
```

> `retry_at` / `retry_after_seconds` 为 `null` 表示请求量本身超过限额，永远不可能放行。

### 2. 幂等语义

```bash
# 重复提交相同内容 → 原样返回首次判定，不重复扣减
curl -X POST http://localhost:8000/v1/quota/consume \
  -H 'Content-Type: application/json' \
  -d '{"request_id": "req-001",
       "buckets": [{"key": "api-requests", "amount": 1},
                   {"key": "llm-tokens",  "amount": 500}]}'

# 相同 request_id 但内容变化 → 409 Conflict
curl -X POST http://localhost:8000/v1/quota/consume \
  -H 'Content-Type: application/json' \
  -d '{"request_id": "req-001",
       "buckets": [{"key": "api-requests", "amount": 2}]}'
```

### 3. 释放已放行的消费（恢复用量）

```bash
curl -X POST http://localhost:8000/v1/quota/releases \
  -H 'Content-Type: application/json' \
  -d '{"release_id": "release-001", "request_id": "req-001"}'
```

成功（`200`）：在**单个事务**内删除 `req-001` 对所有桶的扣减事件，用量即刻恢复；
不会出现部分桶已恢复、部分未恢复：

```json
{"release_id": "release-001", "request_id": "req-001",
 "released": true,
 "released_buckets": [{"key": "api-requests", "amount": 1},
                      {"key": "llm-tokens", "amount": 500}],
 "events_reverted": 2,
 "released_at": 1789657690.12}
```

释放语义：

| 场景 | 结果 |
| --- | --- |
| 相同 `release_id` + 相同 `request_id` 重放 | `200`，原样返回首次释放结果，响应头 `X-Idempotent-Replay: true`，不重复恢复 |
| 相同 `release_id` 改指其他 `request_id` | `409` |
| 同一消费请求用另一个 `release_id` 再次释放（含并发竞争落败方） | `409` |
| 原 `request_id` 不存在 | `404` |
| 原消费判定为拒绝（`429`，从未扣减） | `409` |

释放后重放原 `consume` 仍返回其已固化的判定（带 `X-Idempotent-Replay: true`），
**不会再次扣减**；恢复出的额度可供新的 `request_id` 使用。

### 4. 查询桶当前窗口用量

```bash
curl http://localhost:8000/v1/buckets/llm-tokens/usage
```

```json
{"key": "llm-tokens", "limit": 10000, "window_seconds": 60,
 "used": 500, "remaining": 9500,
 "window_start": 1789657597.55, "as_of": 1789657657.55,
 "oldest_event_expires_at": 1789657717.47}
```

### 5. 哈希审计链

每次**首次消费判定**（无论放行还是拒绝）与每次**首次成功释放**，都会在其原写事务内
追加一条审计记录：与业务变更同生共死（事务回滚则审计也回滚），幂等重放不重复记录。

链规则：

- `seq` 全局递增（从 1 开始），由写事务串行化按提交顺序分配；
- 首条记录 `prev_hash` 为 64 个 `0`，后续每条引用上一条的 `hash`；
- `hash = SHA-256(prev_hash 的原始 32 字节 + UTF-8 canonical JSON 事件)`；
- 事件 JSON 使用键排序、无多余空白，包含操作类型、业务 ID、结果、发生时间：

| 字段 | 说明 |
| --- | --- |
| `op` | 操作类型：`consume` / `release` |
| `biz_id` | 业务 ID：consume 为 `request_id`，release 为 `release_id` |
| `result` | 结果：consume 为 `allowed` / `denied`，release 为 `released` |
| `ts` | 发生时间，epoch 秒（与接口判定/释放时间一致） |
| `request_id` | 仅 release 事件附带：被释放消费的 `request_id` |

#### 查询审计记录

```bash
curl "http://localhost:8000/v1/audit?after_seq=0&limit=100"
```

按 `seq` 升序游标分页：`after_seq` 只返回 seq 严格大于该值的记录（默认 0），
`limit` 取 1–1000（默认 100）。响应同时携带当前链头 `head` 与下一页游标
`next_after_seq`；空链时 `head = {"seq": 0, "hash": "<64 个 0>"}`：

```json
{"records": [
   {"seq": 1, "op": "consume", "biz_id": "req-001", "result": "allowed",
    "ts": 1789657657.47,
    "prev_hash": "0000…0000",
    "hash": "7f19…afb5",
    "event": {"biz_id": "req-001", "op": "consume",
              "result": "allowed", "ts": 1789657657.47}},
   {"seq": 2, "op": "release", "biz_id": "release-001", "result": "released",
    "ts": 1789657690.12,
    "prev_hash": "7f19…afb5",
    "hash": "5b64…f6de",
    "event": {"biz_id": "release-001", "op": "release",
              "request_id": "req-001", "result": "released",
              "ts": 1789657690.12}}],
 "head": {"seq": 2, "hash": "5b64…f6de"},
 "next_after_seq": 2}
```

> `event` 即入哈希的 canonical JSON 原文（解析后的对象），可直接据此独立复算 `hash`。

#### 校验审计链

```bash
curl http://localhost:8000/v1/audit/verify
```

服务按 seq 升序重算全链（连续性 + `prev_hash` 引用 + 每条 `hash`）：

```json
{"valid": true, "checked_count": 2,
 "head_hash": "5b64ccb721cf0786e709e8abbdc1ac1e500e3dac67bb9a3cfe09e6b72af276de",
 "first_invalid_seq": null}
```

发现篡改/断链时 `valid` 为 `false`，`first_invalid_seq` 给出首个失效位置；
空链返回 `valid: true`、`checked_count: 0`、`head_hash` 为 64 个 0。

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/v1/quota/consume` | 多桶检查 + 扣减；`200` 放行 / `429` 超限 / `409` 幂等冲突 / `404` 桶未配置 |
| `POST` | `/v1/quota/releases` | 释放一次已放行消费；`200` 成功或重放 / `409` 冲突 / `404` 原请求不存在 |
| `GET` | `/v1/buckets/{key}/usage` | 查询桶当前滚动窗口用量 |
| `PUT` | `/v1/buckets/{key}` | 创建或更新桶（`limit`、`window_seconds`） |
| `GET` | `/v1/buckets` | 列出全部桶配置 |
| `GET` | `/v1/audit?after_seq=&limit=` | 按 seq 游标分页返回审计记录及链头 |
| `GET` | `/v1/audit/verify` | 重算审计链，返回 `valid` / `checked_count` / `head_hash` / `first_invalid_seq` |
| `GET` | `/healthz` | 健康检查 |

交互式文档：`http://localhost:8000/docs`

## 环境变量

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `QUOTA_DB_PATH` | `quota.db`（镜像内 `/data/quota.db`） | SQLite 数据库文件路径 |
| `QUOTA_BUCKETS_CONFIG` | `buckets.json`（镜像内 `/app/buckets.json`） | 启动时播种的桶配置文件，置空跳过 |
| `QUOTA_BUSY_TIMEOUT_MS` | `30000` | SQLite 写事务排队等待毫秒数 |

## 实现说明

- **原子性**：`consume` 在单个 `BEGIN IMMEDIATE` 事务内完成"读窗口用量 → 判定 →
  写扣减事件 → 追加审计记录 → 固化判定"，任一环节失败整体回滚。`release` 同理：在单个写事务内
  完成"校验原请求 → 删除其全部 `usage_events` → 标记 `requests.release_id`
  → 追加审计记录 → 写入 `releases` 释放记录"，保证所有桶同时恢复。
  审计追加与业务变更同事务：回滚则不留审计，幂等重放走固化结果分支、不追加记录
  （失败释放如 404/409 本就不产生审计）。
- **并发**：`BEGIN IMMEDIATE` 使写事务在数据库级串行化；WAL 模式保证读（用量查询、
  审计查询/校验）不阻塞。审计链在事务内先读链头再插入，串行化保证 `seq` 连续、
  `prev_hash` 引用不竞争。多个不同 `release_id` 并发释放同一消费时，事务串行化 +
  占用标记保证仅一个成功，其余返回 `409`。
- **幂等**：`requests` 表以 `request_id` 为主键，固化载荷 SHA-256 与判定 JSON；
  重放命中时直接返回，哈希不一致返回 409。`releases` 表以 `release_id` 为主键固化
  目标 `request_id` 与释放结果：同目标重放原样返回并带 `X-Idempotent-Replay: true`，
  改指其他目标返回 409。
- **审计链**：`audit_log` 表仅追加，`seq INTEGER PRIMARY KEY AUTOINCREMENT` 全局递增；
  `hash = SHA-256(bytes.fromhex(prev_hash) + canonical_event_json.encode("utf-8"))`，
  首条前驱为 64 个 0。`GET /v1/audit/verify` 重算 seq 连续性、`prev_hash` 引用与每条哈希，
  任一不符即报告首个失效 seq。
- **最早重试时间**：按窗口内事件过期时刻累计释放量，找到最早可容纳本次请求的过期点；
  多桶受限时取各桶的最大值。

## 测试

```bash
pip install -r requirements.txt pytest httpx
python -m pytest tests/ -q
```

覆盖：原子扣减、幂等重放（含拒绝判定重放）、409 冲突、滚动窗口过期恢复、
50 并发不超限额、同 `request_id` 并发只扣减一次，以及释放接口的多桶原子恢复、
幂等重放、改指冲突、重复释放冲突、404/409 分支、释放后 consume 重放不再次扣减、
20 并发释放仅一个成功，并覆盖哈希审计链的：首次判定（放行/拒绝）留痕而重放不留痕、
seq/prev_hash/hash 可独立复算、首次成功释放留痕而重放/失败释放不留痕、
`after_seq`/`limit` 分页与参数校验、篡改后 `verify` 报告 `first_invalid_seq`、
20 并发下链完整可校验，共 34 个用例。
