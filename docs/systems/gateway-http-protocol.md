# Gateway HTTP 与 `gateway_protocol`

本文描述 Gateway 的 HTTP server adapter 与中立的 `gateway_protocol` wire
system。它以当前代码、测试和 [架构总览](../architecture.md) 为准；这里记录的是
现有边界和行为，不是未来版本方案。

## 负责什么

Gateway HTTP 层把 `GatewayTaskModule` facade 暴露为可被本地进程、channel adapter
和远端 A2A Gateway 调用的 HTTP 协议。server adapter 负责路由组合、认证依赖、HTTP
状态/头部、流式响应生命周期，以及把 service 的错误转成统一 envelope。
`gateway_protocol` 负责跨进程 wire contract：DTO、HTTP client/transport 对 JSON
response/error 的严格有限读取、游标、SSE 编解码和 Task-event 的公共投影。

它不拥有：

- Task effect、路由选择/恢复或 command ledger；这些由
  [`gateway/`](../../src/ruyi_agent/gateway/) 的 service、`TaskRouter`、route
  workflow 和 command store 负责。
- Task/event 的持久化；durable lifecycle 由
  [`runtime/task_event_ledger.py`](../../src/ruyi_agent/runtime/task_event_ledger.py)
  原子写入并读取。
- channel session、receipt、delivery 状态；这些属于各 channel 自己的 store。
- Agent execution、checkpoint 或 backend runtime；HTTP 只调用 Gateway facade。

特别要区分两种 projection：

- [`gateway_protocol/projection.py`](../../src/ruyi_agent/gateway_protocol/projection.py)
  是 Task-event 的 wire projection/codec 边界：生成 lifecycle、snapshot、artifact
  和受限的 `assistant.delta`，编码游标，并清理远端事件。
- [`gateway/application.py::GatewayProjection`](../../src/ruyi_agent/gateway/application.py)
  是普通 HTTP response projection：把 runtime record 映射成 Agent、Task、Review、
  Artifact 的稳定 DTO。它不是 SSE event projection，也不替代 event ledger。

## 入口与路由组合

应用启动时，
[`runtime/bootstrap.py`](../../src/ruyi_agent/runtime/bootstrap.py) 在 FastAPI
lifespan 中构造 backend、ledger、route/command stores、Agent control 和
`GatewayTaskModule`，写入 `app.state.gateway_service`，再由
[`channels/http/routes.py`](../../src/ruyi_agent/channels/http/routes.py) 统一挂载
adapter。teardown 先把 readiness 置为 false，再关闭资源。`create_gateway_app`
则用于直接创建同一组路由的测试/嵌入入口。

路由按功能组合，而不是让每个 adapter 自己实现协议：

| 组合 | 公开入口 | 认证/返回形态 |
| --- | --- | --- |
| probes | `GET /health`、`GET /ready` | 无 bearer；小型 JSON，均 `no-store` |
| discovery/tasks | `/agents`、`/tasks`、`/agents/{agent_name}/tasks` 及 task command/read 入口 | business bearer；JSON DTO；创建为 201，异步 command 为 202 |
| history/events | `/tasks/{task_id}/messages`、`/tasks/{task_id}/events` | bearer；分页 JSON 或长连接 SSE |
| reviews/webhook | review read/decision 与 `POST /webhooks/tasks` | bearer；decision/webhook 为异步 202 |
| artifacts | `POST /artifacts/download`、task-scoped artifact download | bearer；二进制成功响应，错误仍为 JSON envelope |
| Team Console | `/debug/team`、login/logout 及同源静态资源 | 独立浏览器 session 边界；不出现在普通 OpenAPI surface |

所有 business route 通过
[`GatewayHttpContext.require_bearer`](../../src/ruyi_agent/channels/http/context.py)
进入 service。`/health` 与 `/ready` 是 liveness/readiness probe，不能用来推断
business bearer 是否有效；`/ready` 在 startup/teardown 期间返回 not ready。

调用关系可以概括为：

```text
HTTP client / channel / A2A client
             │  neutral JSON, SSE, binary artifact
             ▼
      channels/http/routes.py
        ├─ auth + errors + probes + Team Console
        └─ task/event/review/artifact adapters
             ▼
      GatewayTaskModule facade
        ├─ GatewayProjection (ordinary responses)
        ├─ TaskRouter (local or remote route)
        └─ command store + route state + event ledger
             ▼
      local Agent runtime  或  remote A2A Gateway
```

## HTTP contract

### 请求、DTO 和头部

Business request 的主要类别是 agent discovery、task create/input/cancel、task
list/detail、message page、review list/decision、task webhook 和 artifact download。
`gateway_protocol.dto` 定义 Pydantic wire DTO；其中 public response 的
`GatewayDTO` 使用 frozen model、mapping payload。DTO 在被相应 server/client consumer
解析时校验 shape，但这不等于统一的 request-body 或所有字段字符上限。当前没有
统一 request-body cap；例如
`TaskInput.content`、`AttachmentInput.data_base64` 和
`ArtifactDownloadRequest.path` 没有统一的字符数 cap。attachment decoded bytes 和
artifact downloaded bytes 的大小限制属于 Gateway server/service（当前 `GatewayTaskModule` 默认 attachment
20 MiB、artifact 50 MiB，分别由 attachment/artifact service 的配置执行），不是
JSON transport 的 request cap。文档只约定这些 payload 的语义
分组，不复制每条 route 的字段表；具体 shape 以
[`dto.py`](../../src/ruyi_agent/gateway_protocol/dto.py) 为准。

典型请求头和参数如下：

- business bearer 是 `Authorization: Bearer <token>`。创建、输入等可重试 command
  可带 `Idempotency-Key`；server 按 principal、operation/target 和 request hash
  处理，而不是把它当作 Task ID。
- JSON client 发送 `Accept: application/json`；event client 发送
  `Accept: text/event-stream`、`Accept-Encoding: identity`，恢复时转发不透明的
  `Last-Event-ID`，并以 `run_count` 绑定一次 task run。
- message history 的 `cursor` 是独立于 SSE 的分页游标；不能把
  `Last-Event-ID` 当作 message cursor，或反过来。
- Team Console browser API 另带 `X-Ruyi-Team-Console: 1` 和 same-origin cookie；
  这不是把 cookie 当作一般 API bearer。

### 成功响应、状态和头部

普通成功响应由 `GatewayProjection` 生成 DTO：Task response 带 public task identity、
state/run 信息以及可公开的 pending review/artifact 摘要；agent、review 和 artifact
也有对应的 stable response。列表/消息响应带有分页结果和 `next_cursor`。创建返回
201 与 `Location: /tasks/{public_task_id}`；send input、cancel、review decision
和 webhook 表示 effect 已接受，返回 202。幂等 replay 会在 HTTP response 增加
`Idempotency-Replayed: true`。普通读取为 200；artifact download 成功时是原始
bytes，不包装为 JSON。

状态映射集中在
[`error_handlers.py`](../../src/ruyi_agent/channels/http/error_handlers.py)：

| HTTP | 语义（代表性错误组） |
| --- | --- |
| 400 | malformed/invalid request、attachment、delegation context |
| 401 | `unauthorized`；附 `WWW-Authenticate: Bearer realm="ruyi-agent-gateway"` |
| 403 | `agent_not_public`、delegation policy、workspace path 等禁止操作 |
| 404 | agent/task/review/artifact 不存在 |
| 409 | review/task mismatch、active/routing conflict、idempotency conflict/in-progress/uncertain |
| 413 | attachment/artifact 超过限制 |
| 422 | 未实现的 remote executor 等已识别但不可执行的输入 |
| 429 | delegation budget exhausted |
| 502 | `upstream_gateway_error` 或 upstream failure |
| 503 | `agent_unavailable`、runtime/attachment、route persistence、durability、history/events、idempotency 不可用 |
| 500 | 未映射的 `GatewayTaskError` 或意外异常；后者只暴露 `internal_error` |

错误 body 是统一 envelope：
`{"error":{"code":"...","message":"...","details":...}}`，其中 details
可省略。`GatewayTaskError` 在 service/transport 内可携带
effect disposition；HTTP envelope 只暴露适合 wire 的 code/message/details，不泄露
内部 traceback、凭据或远端私有字段。`idempotency_in_progress`
额外带 `Retry-After: 1`；not-ready probe 也带 `Retry-After: 1`。

SSE response 使用 `Content-Type: text/event-stream`、`Cache-Control: no-cache,
no-transform` 和 `X-Accel-Buffering: no`。artifact response 以 content type 和
安全的 `Content-Disposition` 返回，并附 task-scoped `X-Artifact-Id` 与受限的
`X-Artifact-Path`。探针和 Team Console surface 使用 `Cache-Control: no-store`。

### Effect、幂等和错误边界

[`GatewayEffectDisposition`](../../src/ruyi_agent/gateway/errors.py) 明确区分
`NOT_DISPATCHED`、`AUTHORITATIVE_REJECTION` 和 `OUTCOME_UNKNOWN`。HTTP transport
和 A2A client 还会区分连接/地址等可判定未 dispatch 的失败，以及读写 timeout 或
response 丢失导致的 `possibly_dispatched`。这只描述调用者能安全采取的动作，不
声称底层 effect 已经被撤销。

带 `Idempotency-Key` 的 create/input 先由 command store claim；同一 key/hash 的
terminal outcome 可精确 replay，冲突为 `idempotency_key_reused`，并发占用为带
重试提示的 `idempotency_in_progress`。没有 key 时 create 不自动去重。远程 create
会转发显式传入的 inbound `Idempotency-Key`；若没有 external key 而 remote reference
声明并验证 `ruyi_gateway_v1`/create capability，create workflow 会生成稳定的
downstream key。这个 capability 决定响应丢失后能否安全 replay，不决定是否允许
发送 idempotency header。响应丢失或 route activation/persistence 不确定时保留可
查询的 public task identity，并返回 non-retryable/uncertain 语义，不能盲目重新创建。
`TaskRouter` 对 pending、failed、uncertain route 返回 `task_route_unavailable`
等 409，而不是让 transport 层猜测 route state。

`GatewayProtocolClient` 是中立 client facade，提供 JSON、raw、SSE 和 artifact
操作。它对成功 JSON response 的通用保证只有“解码后是 object”；Task identity/state
等语义由 Gateway 或具体 consumer 进一步以 DTO 校验，SSE 则由 codec/router 校验。
channel-facing 的
[`channels/gateway_client.py`](../../src/ruyi_agent/channels/gateway_client.py)
在其上把 payload 校验成 channel 使用的 DTO，并把 transport/protocol failure 转
为 `GatewayClientError`。因此 client/transport 不拥有 command effect 或 route
recovery。

## 严格且有界的 JSON response transport

[`gateway_protocol/contracts.py`](../../src/ruyi_agent/gateway_protocol/contracts.py)
中的若干常量被 protocol、server/service 和 ledger 复用，但这里的主要边界是
`GatewayHTTPTransport` 消费 JSON response/error 时的边界：成功 JSON body 最大
8 MiB，错误 body 最大 64 KiB，JSON read timeout 5 秒，nesting 最大 100，gateway
error text 最大 4096 字符，error details 最大 64 KiB。它们不是统一的 request-body
cap；当前也没有把 `TaskInput.content`、`AttachmentInput.data_base64`
或 `ArtifactDownloadRequest.path` 限成统一字符数。各类 event data、lifecycle、
review、artifact 列表和 delta 还有各自由 projection/ledger/server 执行的 budgets；
`assistant.delta` 单段最大 32 KiB，pending transient delta 队列默认最多 256 项。

`GatewayHTTPTransport` 以 streaming read 为基础，在消费 JSON response/error 时读取
`max+1` bytes 后拒绝超限；会检查 sane `Content-Length`，严格解码 UTF-8，拒绝非法
JSON、NaN/Infinity、过深对象和未满足 object payload contract 的成功响应。超时、
连接失败、非 JSON 错误和错误状态分别映射到 transport exception，且 response 在
成功、错误和异常路径都会关闭。错误 details 的 bounded normalization（替换
surrogate、截断文字、只保留有限 JSON primitive/list/dict）是客户端消费边界；它
不构成 server 的统一 request validation。HTTPX 的压缩 JSON response 不绕过这些
边界；SSE handshake 有单独的有界 timeout，建立后由 stream lifecycle 管理长连接。

这套限制同时适用于 A2A 下游，因为
[`integrations/a2a/client.py`](../../src/ruyi_agent/integrations/a2a/client.py)
复用 `GatewayProtocolClient`；channel artifact client 只有在调用方配置
`max_download_bytes` 时才按该媒体预算拒绝过大的 raw download，未配置时没有默认
读取上限。二进制 artifact 本身不走 JSON decoder；其 decoded/下载大小限制属于
server/service 或显式的 channel media policy，路径请求和 JSON error response 仍按
各自的 DTO/response 边界处理。

## Task-event SSE：snapshot、resume 与生命周期

事件协议由
[`gateway_protocol/sse.py`](../../src/ruyi_agent/gateway_protocol/sse.py)、
[`cursor.py`](../../src/ruyi_agent/gateway_protocol/cursor.py) 和
[`runtime/task_event_ledger.py`](../../src/ruyi_agent/runtime/task_event_ledger.py)
共同实现。SSE `Last-Event-ID` 是不透明、版本化且有界的 cursor，内部绑定 public
task、run 和正整数 durable event id（在 local ledger 上）。调用方必须将 cursor 当作
opaque，不应解释、依赖编码或自行拼接；但当前 versioned/base64 表示在技术上并非
不可构造，服务端仍会执行格式和绑定校验。remote proxy 透传下游 cursor，只按相应
的 event-id shape 边界验证；event data 的 task/run identity 另行校验，不重编码
cursor 为 public identity。

一次 stream 的流程是：

1. bearer/API session 通过认证，校验 `run_count` 和可选的 `Last-Event-ID`。
2. service/router 验证 task、route 和 run binding，打开 ledger subscription。
3. 新 stream 从 durable anchor/current Task projection 合成并发送带 anchor cursor 的
   `task.snapshot`，随后按 durable 顺序发 lifecycle/artifact event，并尽力插入
   transient delta；snapshot 不作为 resume 时重复发送的记录。
4. 无 event 时发送 `: heartbeat` comment；heartbeat 没有 event name、event id 或
   replay 语义。
5. 正常终止发送无 id 的 `stream.end`；终止原因与最新 lifecycle state 一致。读取、
   编码或下游 stream 出错时先发一个 `stream.error`，再紧邻发送
   `stream.end(reason=error)`。

local resume 使用 `Last-Event-ID` 从 durable ledger 继续，不重新发送 snapshot；local
cursor 绑定错误、run mismatch、格式/大小非法时拒绝。remote proxy 不在本地重解码或
重编码 downstream cursor，而是把它透传给下游；若 local client 恢复的是已经结束且
当前 run 已推进的旧 run，ledger 可重放该历史 durable backlog，最后以 `superseded`
结束；新开 stream 若 run 不匹配则直接报告错误。远端 A2A stream 也必须遵守这一 fresh/resume
规则：fresh 首个 event 必须是 snapshot，resume 不得包含 snapshot；下游 event data
中的 task id 经验证后才重写为 public task id。下游 event id/cursor 仅按 event-id
shape validation 后作为
不透明值透传以支持下游 resume，不在 proxy 中解码或重编码成 public identity；它不能
被调用者当作 task id，也不能让普通 event data 泄露 upstream identity。

durability 和 backpressure 的分界如下：

| event | durable / cursor | 用途 |
| --- | --- | --- |
| lifecycle（created/running/review requested/completed/failed/cancelled/interrupted）与 `task.artifact_published` | ledger 中有序保存并可 replay | Task 状态事实和 artifact 变更 |
| `task.snapshot` | fresh stream 从 durable anchor/current projection 合成，携带 anchor cursor；不在 resume replay，也不是单独的 ledger replay record | 新连接的当前投影 |
| `assistant.delta` | transient、无 event id、进程内 best-effort | 实时 UI 增量；慢 subscriber 满队列时可丢弃 |
| `stream.error`、`stream.end` | 控制帧、无 durable cursor | 本次连接的错误/生命周期收束 |

ledger 把 Task row 与 lifecycle event 原子写入；durable subscriber 按 SQLite event id
读取，transient delta 只在进程内 fan-out。慢订阅者只丢 delta，不影响 durable
lifecycle；delta 的 provider metadata、tool/reasoning 等内部内容不会进入 public
wire event。客户端断开只关闭 subscription/response，不取消 Agent run；response
在 response-start、未开始迭代和 body-send disconnect 等 ASGI 路径都必须释放
subscription。

SSE codec 严格处理 UTF-8、BOM、CRLF、多行 data、comments 和 JSON；拒绝未知字段、
非法 event/id、NaN、超大 line/event、未闭合 stream，且要求 `stream.end` 收束。
event data 由 protocol projection 进行 status/review/artifact/text budgets 校验，
远端 error 文本会清理后再越过 public boundary。

## Artifact transport

artifact metadata 在普通 Task/Review response 中由 `GatewayProjection` 投影；下载
由 [`gateway/artifacts.py`](../../src/ruyi_agent/gateway/artifacts.py) 做 workspace
边界检查并从 backend 取 bytes。路径下载可由受限 path body 发起，task-scoped 下载
先按 public task 与 artifact id 查找，再下载已登记路径。默认 server artifact 上限
为 50 MiB；channel client 可另外设更小的 `max_download_bytes`。

下载成功是 raw binary，不把文件内容塞进 DTO；content type、经过清理的 filename、
`X-Artifact-Path` 和 task-scoped `X-Artifact-Id` 帮助调用者传递媒体。绝不信任用户
提供的 `..`、斜杠、反斜杠、控制字符或未授权 workspace path。任何 path、artifact
not found、too large 或 backend failure 都回到统一 JSON error envelope；client
transport 消费该 error response 时才应用 bounded body/normalization 边界。

## Channel 与 A2A callers

Telegram/Feishu runner 分别在
[`channels/telegram/runner.py`](../../src/ruyi_agent/channels/telegram/runner.py) 和
[`channels/feishu/runner.py`](../../src/ruyi_agent/channels/feishu/runner.py) 中配置
Gateway base URL/bearer 并构造 `GatewayHTTPClient`；CLI/entrypoint 只负责启动这些
runner（入口见 [`entrypoints/main.py`](../../src/ruyi_agent/entrypoints/main.py)）。
Telegram/Feishu adapter 通过
[`ChannelTurnHandler`](../../src/ruyi_agent/channels/turn.py) 消费
`GatewayTaskClient`，接收由 client/consumer 校验的 Gateway DTO，不直接操作
Gateway/runtime store，也不直接使用 httpx transport 或 SSE codec internals。adapter
可以访问自己的 channel session、receipt 和 delivery store；这些状态由 channel
自己持久化，Gateway task idempotency 不能替代外部消息发送的 receipt/dedup 语义。

远程 route 的 A2A client 使用同一 HTTP route contract（通常是下游挂载的 `/a2a`）
调用 create/get/messages/events/input/cancel/review，bearer 从
remote reference 指定的环境变量取得。transport 对成功 JSON 只保证解码结果是 object；
Task identity/state 由 runtime/Gateway consumer 进一步验证，SSE 则由 codec/router
约束 shape 与 public projection。它可转发显式传入的 inbound `Idempotency-Key`；声明
并验证 `ruyi_gateway_v1`/create capability 时，create workflow 在无 external key 时
生成稳定 downstream key，并据此决定 lost-response 后能否安全 replay。当前 A2A client
没有远端 artifact download 操作；artifact binary transport 属于各 Gateway 自己的
HTTP artifact endpoint。内部 worker
delegation 是另一条 runtime path，不应被描述成 channel 或远端普通 API caller。

## Team Console：独立浏览器信任边界

Team Console 是 debug/team UI 的窄用途 browser boundary，不是普通 API auth，也不
是一般用户系统、账号目录、role/permission 或 session database。它的目的只是把一
次已知 bearer credential 的登录结果短暂地委托给同源 Team Console 页面。

流程是：浏览器以允许的 transport `POST /debug/team/login`，以严格的
`application/x-www-form-urlencoded` body 提交唯一 token 字段；请求需通过 same-origin
校验，拒绝 query token、重复/未知字段、非法 percent/UTF-8 和超过 8 KiB 的 body。
server 用 constant-time compare 验证该 Gateway bearer，成功后 303 到 `/debug/team`
并设置短期、无状态、版本化 HMAC signed cookie `ruyi_team_console_session`（TTL
8 小时、HttpOnly、SameSite=Strict、HTTPS 时 Secure、Path `/`）。token 不回显。

之后页面和静态 CSS/JS 只在有效 cookie 下提供；浏览器调用 business API 时携带
`X-Ruyi-Team-Console: 1`、`credentials: "same-origin"`，而不是把 bearer 放在
localStorage。`TeamConsoleAuthenticator` 在没有 Authorization header 时要求 marker、
唯一有效 cookie、允许的 transport 和严格 same-origin signals；若带 Authorization
则必须是唯一正确的 bearer，错误 bearer 不会 fallback 到 cookie。页面注销会清掉该
cookie。

允许的 transport 是 HTTPS，或仅限 loopback hostname/IP 的明文 HTTP；不信任
`X-Forwarded-Proto`，Secure 属性按 ASGI 实际 scheme 设置。console/static response
带 `no-store`、same-origin CORP、same-origin Referrer-Policy、`nosniff`、`DENY` frame
保护；页面 CSP 限制脚本/样式/connect/img/font 为 self，并禁止 base/form/frame
外流。marker 成功认证的 business response 也会由
[`team_console_auth.py`](../../src/ruyi_agent/channels/http/team_console_auth.py) 中的
middleware（由 [`routes.py`](../../src/ruyi_agent/channels/http/routes.py) 挂载）标成
`Cache-Control: no-store`。这些约束防止 bearer、debug 数据和
console session 被浏览器缓存、跨源嵌入或通过错误页面传播，但不把 Team Console
升级为通用用户认证体系。

## 安全与传输状态

- 默认 `dev-token` 仅允许 loopback；外部部署必须显式配置 bearer。A2A credential
  只从配置的 environment variable 取，不进入 DTO、SSE data、日志错误 envelope 或
  upstream public id。
- public task id、upstream task id、run_count 和 cursor 各自有边界；router/codec
  在跨 Gateway 时验证并仅重写允许的 event-data 顶层 identity，remote cursor 保持
  opaque 以便下游恢复。local SSE cursor 按 task/run/event 绑定；local message cursor
  按本地 task/checkpoint/offset 绑定。proxied remote message cursor 保留为 downstream
  opaque 值，不在本地解码或重新绑定 public task；local cursor 不能跨任务挪用。
- JSON response/error、SSE line/event、projection text/delta/review/artifact metadata
  等各有相应上限；这些不构成统一 request-body 或 raw-media 上限。decoded attachment/
  artifact 由 server/service policy 限制，raw artifact client 只有显式配置
  `max_download_bytes` 才有客户端媒体上限。所有长连接在结束、异常、取消和断开
  路径关闭 response/subscription。
- route state 是 `pending`、`active`、`failed` 或 `uncertain`；只有 active route
  接受依赖 task 的操作。Task state 是 pending/running/waiting_for_human/
  completed/failed/cancelled/interrupted；terminal lifecycle 进入 durable ledger。
  route/effect 状态不能由 HTTP status 猜测，必须通过 query/recovery path 观察。

## 测试证据

行为证据与实现一起维护，主要覆盖：

- [HTTP core](../../tests/unit/test_gateway_http_core.py)、[probes](../../tests/unit/test_gateway_probes.py)：bearer、Team Console、public agent/task command、幂等 replay/conflict、attachments、artifact headers/path security 和状态码。
- [SSE HTTP adapter](../../tests/unit/test_gateway_http_events.py)：响应各 ASGI
  生命周期的 close、snapshot/resume/cursor、heartbeat/header、local/remote public
  id rewrite、fresh snapshot、error/end pairing 与 durable-store failure。
- [protocol boundaries](../../tests/unit/test_gateway_protocol_boundaries.py)、
  [client](../../tests/unit/test_gateway_client.py)：neutral import boundary、严格
  JSON、bounded body/error/depth/UTF-8、content encoding、SSE close/handshake、raw
  artifact 与 DTO rejection。
- [event ledger](../../tests/unit/test_task_events.py) 与
  [event projection/SSE](../../tests/unit/test_task_event_projection_sse.py)：atomic
  lifecycle、snapshot/reconnect/superseded、delta drop、budget、sanitization、codec
  schema 和 stream end。
- [A2A client](../../tests/unit/test_a2a_client.py) 与
  [remote HTTP](../../tests/unit/test_gateway_http_remote.py)：下游 route contract、
  bearer/idempotency capability、错误/effect boundary、stream close/cancel、remote
  cursor/task identity 和 route recovery。
- [socket SSE flow](../../tests/integration/test_gateway_sse_flow.py)、[message history](../../tests/integration/test_gateway_message_history_flow.py) 与 [Team Console flow](../../tests/integration/test_gateway_team_console_flow.py)：真实 HTTP disconnect/replay、两层 Gateway public-id 代理、message cursor preservation、登录 cookie/API/logout 及 no-store。

协议边界回归还确保 adapters 不直接使用 HTTP client/stream 或 codec internals；任何
新增调用方应复用 neutral client/protocol，并补充其 effect、close 和错误映射测试。

## 同步触发

下列修改必须与本文和相关测试同步审阅：

- 路由挂载、bearer/probe、status/error envelope、公共 header、DTO 或 JSON limits；
- Task-event 类型、durability、snapshot/resume/cursor、heartbeat、delta backpressure、
  remote sanitizer 或 stream cleanup；
- command idempotency、effect disposition、route state/recovery 或 A2A capability
  只有在影响 wire-visible retry/error/effect semantics、public query identity 或
  `GatewayTaskClient` contract 时才触发同步；纯内部状态/算法变化归未来的 Gateway
  control-plane 文档；
- artifact workspace/path、download headers、content limits；
- Team Console 登录、signed cookie、marker、same-origin/transport、CSP、no-store 或
  静态资源边界；
- `gateway_protocol` event projection 与 `GatewayProjection` 的职责边界。

同步时以当前代码和测试为事实，保持 public/upstream identity、message/SSE cursor、
durable/transient event 和 browser/API trust boundary 的区分；不要把本页扩展成逐路由
字段清单或另拆 DTO/SSE 规范。
