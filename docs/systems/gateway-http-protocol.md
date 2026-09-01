# Gateway HTTP 与 `gateway_protocol`

本文描述 Gateway 的 HTTP server adapter 与中立的 `gateway_protocol` wire system。
它以当前代码、测试和[架构总览](../architecture.md)为事实来源。

## 负责范围与边界

HTTP 层负责路由组合、认证依赖、HTTP 状态/头部、流式响应生命周期，以及把 service
错误转成统一 envelope。`gateway_protocol` 负责 DTO、HTTP client/transport 对
JSON response/error 的有限读取、游标、SSE 编解码和 Task-event 公共投影。

Task effect、路由选择/恢复、command ledger、Task/event 持久化、channel session/
receipt/delivery、Agent execution、checkpoint 和 backend runtime 分别属于 Gateway
service、runtime ledger、channel、AgentControl 和 backend；HTTP 只调用 Gateway
facade。要区分两种 projection：

- [`gateway_protocol/projection.py`](../../src/ruyi_agent/gateway_protocol/projection.py)
  是 Task-event 的 wire projection/codec，生成 lifecycle、snapshot、artifact 和
  受限 delta；
- [`gateway/application.py::GatewayProjection`](../../src/ruyi_agent/gateway/application.py)
  是 Agent、Task、Review、Artifact 的普通 HTTP response projection，不编码 SSE event
  或替代 event ledger。

## 入口与路由

bootstrap 在 FastAPI lifespan 中创建服务并写入 `app.state.gateway_service`，
[`channels/http/routes.py`](../../src/ruyi_agent/channels/http/routes.py) 挂载统一
adapter；`create_gateway_app` 是直接创建同一路由的嵌入/测试入口。

| 组合 | 公开入口 | 认证/返回形态 |
| --- | --- | --- |
| probes | `GET /health`、`GET /ready` | 无 bearer、`no-store` JSON；启动/关闭期间 `/ready` not ready |
| discovery/tasks | `/agents`、`/tasks`、`/agents/{agent_name}/tasks` 及 task command/read | business bearer；JSON DTO；创建 201，异步 command 202 |
| history/events | `/tasks/{task_id}/messages`、`/tasks/{task_id}/events` | bearer；分页 JSON 或 SSE 长连接 |
| reviews/webhook | review read/decision、`POST /webhooks/tasks` | bearer；decision/webhook 202 |
| artifacts | `POST /artifacts/download`、task-scoped download | bearer；成功为 binary，错误仍为 JSON envelope |
| Team Console | `/debug/team`、login/logout 和同源静态资源 | 独立 browser session；不属于普通 API surface |

业务路由统一经过 [`GatewayHttpContext.require_bearer`](../../src/ruyi_agent/channels/http/context.py)。
`/health`/`/ready` 只表示进程探针，不代表 bearer 有效。调用关系为 HTTP/channel/A2A
client → HTTP adapter → `GatewayTaskModule` → local runtime 或 remote A2A Gateway；
调用方不绕过 facade 访问内部 stores。

## HTTP contract

### 请求、DTO 和 limits

`gateway_protocol.dto` 定义 Pydantic wire DTO，public response 使用 frozen model 和
mapping payload；具体 shape 以 [`dto.py`](../../src/ruyi_agent/gateway_protocol/dto.py)
为准。常见请求是 agent discovery、task create/input/cancel、list/detail、message
page、review、webhook 和 artifact download。

当前没有统一 request-body 或 request-string cap：`TaskInput.content`、
`AttachmentInput.data_base64`、`ArtifactDownloadRequest.path` 不受统一字符数限制。
decoded attachment（Gateway 默认 20 MiB）和 artifact bytes（server 默认 50 MiB）的
大小由 Gateway service 限制，不能误称为 JSON transport cap。channel artifact client
只有调用方显式设置 `max_download_bytes` 才有更小的 raw-media 读取上限。

典型请求头为 `Authorization: Bearer <token>`、可选的 `Idempotency-Key`、JSON 的
`Accept: application/json`，以及 SSE 的 `Accept: text/event-stream`、
`Accept-Encoding: identity`、opaque `Last-Event-ID`。`run_count` 绑定一次 Task run。
message history 的 `cursor` 与 SSE cursor 独立，不能互换。Team Console browser API
使用同源 cookie 与 `X-Ruyi-Team-Console: 1`，不把 cookie 当作一般 API bearer。

### 成功、错误和状态

`GatewayProjection` 生成 Agent/Task/Review/Artifact DTO；列表/消息带分页结果和
`next_cursor`。创建返回 201 和 `Location: /tasks/{public_task_id}`；input、cancel、
review decision、webhook 接受 effect 后返回 202；普通读取 200；artifact 成功返回
原始 bytes。幂等 replay 追加 `Idempotency-Replayed: true`。

[`error_handlers.py`](../../src/ruyi_agent/channels/http/error_handlers.py) 把代表性
错误映射为：

| HTTP | 代表语义 |
| --- | --- |
| 400 | malformed/invalid request、attachment、delegation context |
| 401 | `unauthorized`，附 `WWW-Authenticate: Bearer realm="ruyi-agent-gateway"` |
| 403 | private agent、delegation policy、workspace path forbidden |
| 404 | agent/task/review/artifact 不存在 |
| 409 | review/task mismatch、route conflict、idempotency conflict/in-progress/uncertain |
| 413 | attachment/artifact 超限 |
| 422 | 已识别但不可执行的 remote executor 等输入 |
| 429 | delegation budget exhausted |
| 502 | upstream Gateway/failure |
| 503 | agent/runtime/route/history/events/idempotency/durability unavailable |
| 500 | 未映射的 task error 或意外异常；后者只暴露 `internal_error` |

错误 body 是 `{"error":{"code":"...","message":"...","details":...}}`；details
可省略且不得泄露 traceback、凭据或远端私有字段。`idempotency_in_progress` 和
not-ready probe 带 `Retry-After: 1`。SSE 使用 `Content-Type: text/event-stream`、
`Cache-Control: no-cache, no-transform`、`X-Accel-Buffering: no`；artifact response
提供安全的 `Content-Disposition`、受限 `X-Artifact-Path` 和 task-scoped
`X-Artifact-Id`。

### Effect 与 idempotency

[`GatewayEffectDisposition`](../../src/ruyi_agent/gateway/errors.py) 区分
`NOT_DISPATCHED`、`AUTHORITATIVE_REJECTION`、`OUTCOME_UNKNOWN`。transport 还区分
连接/地址等可判定未 dispatch 与 timeout/response 丢失的 `possibly_dispatched`；
HTTP 状态本身不能证明 effect 已撤销或成功。

带 key 的 create/input 由 command store 按 principal、operation/target 和 request
hash claim；同 key/hash 的 terminal outcome 可 replay，冲突为
`idempotency_key_reused`，并发为带重试提示的 `idempotency_in_progress`。没有 key 的
create 不自动去重。remote create 转发显式 inbound key；若 remote reference 已验证
`ruyi_gateway_v1`/create capability 且 caller 没有 key，workflow 可生成稳定的
downstream key，但这不把无 key 的 Gateway call 变成 caller 可自动 replay 的命令。
响应丢失或 route activation/persistence 不确定时保留可查询的 public Task identity，
按 non-retryable/uncertain 语义处理，不能盲目重新创建。

`GatewayProtocolClient` 对成功 JSON 的通用保证只有“解码结果是 object”；Task identity/
state 由 DTO 或具体 consumer 再校验。channel-facing client 将 payload 校验成
channel DTO 并映射 transport failure；client/transport 不拥有 command effect 或
route recovery。

## 有界 JSON transport

[`GatewayHTTPTransport`](../../src/ruyi_agent/gateway_protocol/contracts.py) 消费 JSON
response/error 时读取 `max+1` bytes 后拒绝超限：成功 body 8 MiB、错误 body 64 KiB、
JSON read timeout 5 秒、nesting 100、error text 4096 字符、error details 64 KiB。
它严格解码 UTF-8，拒绝非法 JSON、NaN/Infinity、过深对象和不满足 object payload 的
成功响应，并在成功、错误、异常路径关闭 response。压缩 JSON 不绕过这些边界；SSE
handshake 有单独的 bounded timeout，建立后由 stream lifecycle 管理长连接。

这些是 client response/error 边界，不是 server request cap。event data、lifecycle、
review、artifact metadata 和 delta 另有 projection/ledger/server budgets（例如单段
`assistant.delta` 32 KiB、transient queue 默认 256 项）。二进制 artifact 不走 JSON
decoder；decoded/download bytes 的限制属于 server/service 或显式的 channel media
policy。

## Task-event SSE

SSE 由 [`sse.py`](../../src/ruyi_agent/gateway_protocol/sse.py)、
[`cursor.py`](../../src/ruyi_agent/gateway_protocol/cursor.py) 和
[`task_event_ledger.py`](../../src/ruyi_agent/runtime/task_event_ledger.py) 共同实现。
`Last-Event-ID` 是不透明、版本化、有界的 cursor，local 值绑定 public task、run 和
正整数 durable event id；技术上可构造，服务端仍校验格式和绑定。remote proxy 透传
下游 cursor，只校验相应 event-id shape，不在本地解码/重编码为 public identity。

一次 stream 会认证并校验 task/run，订阅 ledger；fresh stream 首先发送带 anchor cursor
的 `task.snapshot`，之后按 durable 顺序发送 lifecycle/artifact event，并尽力加入
transient delta。resume 不重复 snapshot。无 event 时发无 id/name/replay 语义的
`: heartbeat`；正常结束发无 id 的 `stream.end`，读取/编码/下游错误先发
`stream.error` 再紧邻 `stream.end(reason=error)`。

local resume 从 durable ledger 继续；格式、task/run mismatch 拒绝。remote resume
把 opaque cursor 交给下游；event data 的 upstream task id 验证后才重写为 public task
id。旧 local run 可 replay 历史 durable backlog 并以 `superseded` 结束；新 stream 的
run mismatch 报错。调用方断开只关闭 subscription/response，不取消 Agent run。

| event | durability/用途 |
| --- | --- |
| lifecycle 与 `task.artifact_published` | ledger 有序保存，可 replay，构成 Task/artifact 事实 |
| `task.snapshot` | fresh stream 从 durable anchor/current projection 合成，不作为 resume record |
| `assistant.delta` | transient、无 event id、进程内 best-effort；慢 subscriber 可丢弃 |
| `stream.error`/`stream.end` | 本次连接控制帧，无 durable cursor |

SSE codec 负责 UTF-8、BOM、CRLF、多行 data、comments、JSON、line/event bounds 和
`stream.end` 收束；event projection 负责 status/review/artifact/text budgets 与远端
error sanitization。所有 response-start、未开始迭代、body-send disconnect、异常和
取消路径都要释放 response/subscription。

## Artifact、A2A 与 Team Console

artifact metadata 由 GatewayProjection 投影，下载由 Gateway artifact service 做
workspace/path 检查并从 backend 取 bytes；path 请求和 task-scoped `artifact_id` 请求
都回到统一 error envelope。HTTP 不把 binary 内容包装成 DTO，也不实现 A2A 下游
artifact download protocol。Telegram/Feishu 通过 Gateway client 使用同一公开面，
remote A2A client 复用 JSON/SSE contract；A2A credential 只从 remote reference 指定
的 environment variable 读取。

Team Console 是窄用途 browser trust boundary，而非账号、role、permission 或 session
database。login 只接受允许 transport 下唯一的 form token 字段，拒绝 query token、重复/
未知字段、非法编码和超过 8 KiB body；校验 bearer 后 303 到 `/debug/team`，设置 TTL 8
小时、HttpOnly、SameSite=Strict、版本化 HMAC signed cookie（HTTPS 时 Secure）。页面
和 browser API 只在有效 cookie、marker、允许 transport 与严格 same-origin signals 下
提供；带 Authorization 时错误 bearer 不 fallback 到 cookie。cookie 注销即清除。

允许的 transport 是 HTTPS，或仅 loopback hostname/IP 的明文 HTTP；不信任
`X-Forwarded-Proto`。console/static 和 marker-authenticated business response 使用
`no-store`；响应设置 same-origin CORP/Referrer-Policy、`nosniff`、DENY frame 与限制
脚本/样式/connect/img/font 的 CSP。该边界不会升级为通用用户认证。

## 测试证据入口

行为证据的 canonical 入口为：

- [HTTP core/probes](../../tests/unit/test_gateway_http_core.py)、[probes](../../tests/unit/test_gateway_probes.py)：认证、Team Console、public command、状态码、幂等、attachment/artifact boundary。
- [HTTP events](../../tests/unit/test_gateway_http_events.py)、[task events](../../tests/unit/test_task_events.py)：SSE lifecycle、snapshot/resume、cursor、disconnect、ledger 与 transient delta。
- [protocol boundaries/client](../../tests/unit/test_gateway_protocol_boundaries.py)、[client](../../tests/unit/test_gateway_client.py)：bounded JSON/error、UTF-8/depth、DTO/SSE/raw artifact response 与 close。
- [remote HTTP/A2A](../../tests/unit/test_gateway_http_remote.py)、[A2A client](../../tests/unit/test_a2a_client.py)：remote identity/cursor、capability/idempotency、error/effect sanitization 与 recovery。
- [Gateway SSE flow](../../tests/integration/test_gateway_sse_flow.py)、[message history flow](../../tests/integration/test_gateway_message_history_flow.py)、[Team Console flow](../../tests/integration/test_gateway_team_console_flow.py)：跨 Gateway 的 replay、proxy cursor/public id 和 browser session flow。

## 何时同步本文

仅在以下边界变化时同步：

- 路由、认证/probe、DTO、status/error envelope、公开 header 或 client/server limits；
- SSE/event 类型、durability、snapshot/resume/cursor、transient backpressure、remote
  identity sanitization 或连接清理；
- command idempotency/effect disposition、artifact path/download contract 或
  `GatewayProjection` 与 event projection 的 ownership；
- Team Console cookie、marker、same-origin/transport、CSP/no-store 等浏览器信任边界。

协议文档保持 public/upstream identity、message/SSE cursor、durable/transient event 和
browser/API trust boundary 的区分，不扩展成逐路由字段表。
