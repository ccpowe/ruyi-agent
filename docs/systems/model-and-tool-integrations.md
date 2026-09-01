# Model Provider 与 MCP 工具集成

本文记录当前 Ruyi Agent 接入外部模型和工具能力的两个独立边界。事实以当前
实现和行为测试为准；[架构总览](../architecture.md)只提供进程级上下文。本文
不逐一介绍 Provider SDK 或 MCP 协议细节，而是说明配置如何进入运行时、错误和
凭据在哪里止步，以及 Agent 可以看到哪些已解析的能力。

## 两个边界

| 边界 | 拥有的接入行为 | 运行时产物 | 不共享的内容 |
| --- | --- | --- | --- |
| A. Model Providers / OpenAI Codex | Provider 声明解析、Provider 工厂、命名 API key 环境变量、OpenAI Codex auth 与 Responses 适配 | `LLMProviderSpec`、`LocalWorkerSpec.model` 和 Codex 的模型对象 | 不读取 MCP registry，不持有 MCP tool inventory 或 MCP 凭据 |
| B. MCP Registry | raw connection dict、server refresh、tool inventory、名称解析、Agent tool scope 和工具调用入口 | `ToolInfo`、已选 tool 对象，或带 scope 的 `tool_search`/`call_tool` | 不读取 Provider key/auth，不持有模型对象或 Provider 状态 |

两组只因为都把外部能力接入本地 Agent 而放在同一篇文档中。两组的配置、缓存、
刷新、错误和凭据机制是分开的；bootstrap 只是把两条分支在构造 local worker 时
组合起来，并不把它们变成共享状态。

本文明确不负责以下行为：

- Agent worker graph、Agent 的完整 middleware 行为和 LangGraph 执行语义；这里只
  记录模型/工具如何作为显式依赖传入。相关构造入口是
  [`agent_factory.py`](../../src/ruyi_agent/runtime/agent_factory.py) 和
  [`stack.py`](../../src/ruyi_agent/runtime/middleware/stack.py)。
- Task 的创建、状态机、run、事件、checkpoint 和恢复生命周期；这些属于
  [`task-execution.md`](task-execution.md)。
- A2A remote task、remote route 和上游任务协议；相关 client 是
  [`a2a/client.py`](../../src/ruyi_agent/integrations/a2a/client.py)。
- backend、workspace 文件执行和 sandbox；相关 runtime 是
  [`backend/runtime.py`](../../src/ruyi_agent/integrations/backend/runtime.py)。
- tool permission、审批和命令策略；它们属于
  [`permissions.py`](../../src/ruyi_agent/control_plane/permissions.py)。
- MCP server 自身的进程、会话、部署或生命周期。Registry 不拥有独立的长期或周期
  server lifecycle；connection/session 及 stdio 子进程由底层 client 在
  `get_tools()`/session 范围按 adapter 约定启动和清理。

## A：Model Providers 与 OpenAI Codex

### 配置模型与 Agent 引用

[`loader.py`](../../src/ruyi_agent/config/loader.py) 从选定 Ruyi home 的
`config/llm_providers.toml` 读取 `[providers]`，交给
[`provider_parser.py`](../../src/ruyi_agent/config/provider_parser.py) 校验，并
返回 `dict[str, LLMProviderSpec]`。`LLMProviderSpec` 定义在
[`provider_models.py`](../../src/ruyi_agent/config/provider_models.py)，字段是：

| 字段 | 语义 |
| --- | --- |
| `name` | Provider 配置键 |
| `kind` | 已支持的 Provider 类型标识，由 parser 校验 |
| `base_url` | 可选的 HTTP endpoint；有值时经过 URL 校验 |
| `api_key_env` | 可选的环境变量名；除 `openai_codex` 外必须是非空字符串 |
| `init_kwargs` | 传给对应构造器的递归不可变参数映射 |

parser 只接受声明的字段，要求 `init_kwargs` 是 table，并拒绝由工厂统一拥有的
`api_key`、`base_url`、`model`、`model_provider` 保留键。`LLMProviderSpec` 及其
`init_kwargs` 不把环境变量值或 SDK 可变对象保存进配置模型；传给 SDK 前由
`mutable_provider_init_kwargs()` 生成防御性副本。

local Agent 在 [`agent_models.py`](../../src/ruyi_agent/config/agent_models.py)
中只通过 `provider` 和 `model` 字段引用 Provider 名称与模型名。它们由
[`agent_parser.py`](../../src/ruyi_agent/config/agent_parser.py) 校验为非空字符串；
Agent 表不重复定义 API key，也不把 Provider 的 init 参数展开到 Agent 表中。

### Provider factory、环境变量与单 Agent 可用性

[`model_providers.py`](../../src/ruyi_agent/integrations/model_providers.py) 的
`build_chat_model()` 是统一构造入口：

```text
LocalAgentConfig.provider/model
             + LLMProviderSpec
                    │
                    ▼
          build_chat_model()
          ├─ defensive init_kwargs copy
          ├─ getenv(api_key_env) when declared
          ├─ add base_url when declared
          └─ dispatch to the provider constructor
```

它先拒绝缺少 `provider`/`model` 或未知 Provider，再由 `_build_provider_kwargs()`
读取 `api_key_env` 指向的实际值，并把 `base_url` 作为统一字段加入构造参数。
普通 Provider 经过通用 `init_chat_model` 或已有的兼容构造分支；本文不规定各个
SDK 的额外行为。`init_kwargs` 只作为已经校验过的构造参数透传，不能覆盖工厂
统一生成的保留键。环境变量名不是凭据本身，缺少命名环境变量会使该次 model
构造失败。

bootstrap 先分别加载 Agent、Provider 和 permission 配置，再调用
[`agent_runtime.py`](../../src/ruyi_agent/config/agent_runtime.py) 的
`build_all_local_worker_specs()`。每个 local Agent 都单独执行
`build_local_worker_spec()`：成功时产生包含 `model` 的 `LocalWorkerSpec`，失败时
在传入 `unavailable_errors` 的生产路径中记录该 Agent 的错误并跳过该 spec，其他
Agent 仍可继续构造。解析阶段的配置错误（例如 Provider table 或 Agent 字段非法）
仍会在 bootstrap 前置阶段失败；“单 Agent unavailable”是已解析配置进入构造后
对单个构造失败的隔离，不是对错误配置的静默接受。Gateway 随后把该记录投影为
Agent 的 `available`/`unavailable_reason`，但这不是 Provider 边界的状态机。

### OpenAI Codex 的认证边界

`kind = "openai_codex"` 使用同一个 Provider factory，但 `api_key_env` 可以省略。
[`openai_codex.py`](../../src/ruyi_agent/integrations/openai_codex.py) 的
`CodexChatModel` 支持在 `init_kwargs` 中传入 `auth_json`，例如 starter 中的
[`llm_providers.toml`](../../src/ruyi_agent/templates/ruyi_home/config/llm_providers.toml)
所示。若没有显式 `api_key` 而有 `auth_json`，构造器调用
`resolve_codex_credentials()`；路径先 `expanduser`，文件不存在或结构不含可用
access token 时报告明确的 `ValueError`。

若 `providers` table 中存在可识别的 `openai-codex` state table，凭据解析先选择
其中的 `tokens`；只有该 state 不存在时才选择顶层 `tokens`。对所选位置若没有非空
access token，才回退到 `credential_pool.openai-codex` 中第一个有 access token
且不在 `last_error_reset_at` 冷却期的条目；不会在 provider state 的 tokens 无效
时再尝试顶层 `tokens`。

`account_id` 优先取 token 数据中的值，也可从 JWT claim 得到，用于构造
`ChatGPT-Account-ID`；access token 作为模型的 API key 使用。直接传入 API key 时
不会因为 Provider 的 `api_key_env` 而改写 auth JSON；auth JSON 是 Codex 专用的
本地凭据输入，不是通用 Provider secret store。

### Access refresh 与 writeback

若 access token 的 JWT `exp` 已进入当前时间加 120 秒的窗口，并且有 refresh token，
`resolve_codex_credentials()` 会向 OpenAI OAuth token endpoint 请求新 token。成功
后：

- 返回新的 access/refresh token 给当前 `CodexChatModel`；
- 将 `providers.openai-codex.tokens` 写回同一个 `auth_json`；
- 对 `source = "device_code"` 的 credential-pool 条目同步 token、id token 和
  account id，并清空旧的 status/error/cooldown 字段；
- 新建文件使用用户可读写的权限（当前 mode 为 `0600`），已有文件按同一路径截断
  写回，不把 token 写入
  Agent 配置、Task projection 或 Provider spec。

没有 refresh token 时不会凭空刷新；refresh endpoint 的非 200 或缺少 access token
会使该次凭据解析失败。刷新是凭据解析时的同步动作，不是独立的 Task 生命周期或
MCP 刷新动作。

### Responses streaming、retry 与错误清理

`CodexChatModel` 将 Codex 作为 Responses-only 的流式模型适配：默认 base URL 是
Codex endpoint，`use_responses_api=True`、`streaming=True`、`store=False`；系统
消息被合并到 `instructions`，其余消息放入 `input`。请求通过 `/responses` 发送，
带 Bearer、`Accept: text/event-stream`、Codex client headers、`session_id` 和
`x-client-request-id`。SSE 事件中的文本 delta 转为 `AIMessageChunk`，function/custom
tool 事件转为 tool-call chunks，完成事件产生最后的 chunk；failed/incomplete 或
带 error 的事件转为流错误。

同步 `_stream()` 和异步 `_astream()` 都按 `max_retries + 1` 尝试。仅在
`httpx.TransportError`、HTTP 429 或 HTTP 5xx 且尚未向调用方产生文本或 tool-call
效果时重试，退避上限为 4 秒；已经产生效果后不重放请求，以免把可能的外部工具
副作用重复提交。非重试的流错误直接结束本次调用。

HTTP 非成功响应的错误诊断有独立的清理边界：读取的响应体有 64 KiB 上限，只从
JSON error object 取 `code`、`message`、`param`、`type`，逐字段限制为 512 字节，
并在生成异常消息前清理 Authorization、API key 和 ChatGPT account id。响应体过大、
格式错误或读取失败时只保留 HTTP status，不回显原始 body。这个清理约束针对
HTTP 错误诊断；它不把外部服务返回的任意流 payload 变成公开的错误协议。

### 探针不是生产入口

[`scripts/probe_openai_codex.py`](../../scripts/probe_openai_codex.py) 是诊断脚本：
可以读取/检查 token、执行 device login、手动 refresh、列出模型、发起一次 live
Responses probe，或用 `--save-auth-json` 写入 Ruyi auth JSON。它的环境变量回退、
输出摘要和手动选项服务于探测，**不由 `ruyi` CLI、bootstrap、Gateway 或 local
Agent run 调用，也不是生产模型构造入口**。生产路径是 Provider factory 加
[`openai_codex.py`](../../src/ruyi_agent/integrations/openai_codex.py)；探针的行为
只能作为诊断证据，不能替代该路径的配置或错误契约。

## B：MCP Registry

### Raw connection dict 与 description

[`load_mcp_server_configs()`](../../src/ruyi_agent/config/loader.py) 从
`config/mcp_servers.toml` 读取 `[mcp_servers]`，只要求顶层是 table，并返回
`dict[str, dict[str, Any]]` raw mapping。它不把 MCP connection 伪装成与 Provider
相同的 typed schema，也不把连接字段提前解释成 secret。

[`registry.py`](../../src/ruyi_agent/integrations/mcp/registry.py) 识别每个 server
dict 中的 `description` 元数据：发送给 `MultiServerMCPClient` 的 connection
dict 会去掉这个字段；刷新后 description 用于 server/tool source 的轻量目录摘要。
其余连接键值保持 raw 形状交给底层 client。因而 `description` 是本地目录元数据，
不是 MCP server capability 或 authorization 声明。

此边界没有由 Registry 提供的通用 `${ENV}` secret resolver、凭据轮换或对外 secret
投影。Registry 在进程内保留完整 raw connection config，并将去掉本地
`description` 元数据后的连接字典传给底层 `MultiServerMCPClient`；底层 transport/
adapter 仍可按自己的约定解释特定字段，例如 stdio adapter 的 `env` 可展开
`${VAR}`。这种解释不是 Registry 的通用规则，也不能把 raw 字符串永不展开或凭据
绝不保留当作本边界保证；Provider 的 `api_key_env` 规则同样不会自动套用到 MCP。

### Bootstrap refresh、并发与 inventory

[`bootstrap_application()`](../../src/ruyi_agent/runtime/bootstrap.py) 创建
`MCPRegistry` 后立即 `await registry.refresh()`，然后才构造 local worker specs。
刷新每次创建一个新的底层 MCP client，并对配置中的每个 server 单独调用
`get_tools(server_name=...)`：

- 默认最大并发为 4，可通过 `max_refresh_concurrency` 调整，但必须至少为 1；
- semaphore 只限制远端 server load，结果整理仍按配置顺序进行；
- 每个 server 都生成 `ServerLoadStatus`，包含 `ok`、tool count、错误和
  `refreshed_at`；单个 server 加载失败时该 server 的 inventory 为空，其他 server
  仍可成功；
- `RefreshResult` 汇总 server 总数、成功/失败数、tool 总数和按配置顺序排列的
  status 列表；空配置也是合法的空 inventory。

`ServerLoadStatus.error` 保存底层异常的原始 `str(exc)` 文本，bootstrap 当前也会把
它直接输出为 `[mcp] ... error=...`；这两个边界都没有统一 redaction。MCP/tool/provider
底层异常不得含凭据，调用方应把 status/bootstrap 输出当作不可信错误文本处理。

刷新完成后，Registry 一次性替换 process-local 的 server status、按 server 分组的
tool inventory、qualified-name 索引和可执行 tool 索引。`list_tools()`、`pick_tools()`、
`search_tools()`、`get_tool()` 等读路径在 refresh 前会拒绝访问，并要求调用方先
`await refresh()`。当前 bootstrap 只有显式的启动刷新；Registry 不拥有独立的长期
或周期性 server lifecycle。connection/session 及 stdio 子进程由底层 client 在
`get_tools()`/session 范围按 adapter 约定启动和清理；本文只定义 Registry 的
refresh、inventory 和调用边界。

### ToolInfo、qualified name 与重名

每个已发现工具被归一化成 `ToolInfo`：`server_name`、底层 raw `name`、稳定的
`qualified_name`、description 和可选 `args_schema`。qualified name 的当前形式是
`<server_name>.<tool_name>`，用于跨 server 聚合时的唯一索引。

Registry 的名称规则是：

- `pick_tools()`、`get_tool()` 和 `resolve_tools()` 可接受 raw name 或 qualified
  name；
- raw name 只在全局唯一时自动映射；跨 server 重复时抛出
  `Ambiguous tool name: ...`，要求调用方使用 qualified name；未知 server/tool
  也会尽早报告错误；
- 由多个 server 组合注入时，即使内部 qualified name 不冲突，只要底层 tool 的
  raw `name` 重复，`resolve_tools()`/`pick_tools()` 仍拒绝这次 Agent injection，
  因为 Agent 看到的是 raw tool name，无法可靠区分两个 callable。

### Agent scope、eager injection 与 tool search

local Agent 的 `server_names` 和 `tool_names` 来自
[`agent_models.py`](../../src/ruyi_agent/config/agent_models.py)，在构造
`LocalWorkerSpec` 时传入 Registry。scope 的含义是“这个 Agent 的工具目录和调用
候选范围”，不是 authorization：

- `server_names` 选择指定 server 的全部已发现工具；`tool_names` 可指定 raw 或
  qualified name；两者合并并去重；
- Registry API 没有传 scope 参数时，`None` 表示完整目录；显式传入空列表则是
  空 allowlist，不能退化为全量；
- scope 会约束 eager 选择、`tool_search` 结果和 `call_tool` 执行目标，但不绕过
  独立的 permission/approval policy，也不授予 MCP server 侧权限。

默认的 eager 路径是 `tool_search = false`：
`build_local_worker_spec()` 直接调用 `registry.resolve_tools(server_names, tool_names)`，
把实际 tool 对象放进 `LocalWorkerSpec.tools`。因此未知名称、加载后的重名冲突等
会在该 Agent 的构造阶段暴露，并按 A 节的单 Agent unavailable 规则处理。

启用 `tool_search` 后，`build_local_worker_spec()` 不把整个 MCP inventory 的实际
tool 对象注入 Agent，而是把 Registry 和同一组 scope 带入 runtime middleware。有效
system-tool 集合由 [`system_tools.py`](../../src/ruyi_agent/config/system_tools.py)
解析；`tool_search = true` 会自动声明 `tool_search` 与 `call_tool`，显式或 disabled
system tools 仍可进一步筛选它们。具体 middleware 组合不属于本文 ownership，接入
契约如下：

1. [`tool_search.py`](../../src/ruyi_agent/runtime/middleware/tool_search.py) 暴露
   稳定的 `tool_search` 和 `call_tool` 两个入口；search 返回 JSON，包含匹配工具的
   `qualified_name`、server/raw name、description、`args_schema` 以及 scope 提示。
2. middleware 注入的 system prompt **推荐**模型在需要时先调用 `tool_search`，再
   使用返回的 qualified name 调用 `call_tool`；这是模型侧提示，不是 runtime 的
   search-state 强制条件。runtime 不记录“已经搜索过”的状态，也不要求每次
   `call_tool` 之前先调用 `tool_search`。
3. `call_tool` 的 runtime 契约只强制精确 qualified name、该 Agent scope 和
   arguments 的 JSON Schema；通过后取得真实 tool，调用其 async/sync invoke 入口，
   并将结果序列化为文本或 JSON。模型不能直接把 raw name 当成 callable，也不能臆造
   qualified name。

### JSON Schema arguments

Registry 从底层 tool 的 `args_schema` 读取参数契约：已有 dict 直接使用，Pydantic
式对象优先调用 `model_json_schema()`，再兼容 `schema()`；没有可识别 schema 时
保留 `None`。`validate_tool_arguments()` 首先要求 arguments 是 object/dict，随后
用 `jsonschema` 的 validator 检查 schema 本身和实际 arguments：schema 非法与参数
不符合分别报告为 `ValueError`。没有 schema 只跳过 schema 校验，不改变 arguments
必须是 dict 的约束。

因此 `args_schema` 是 search metadata 与 call-time validation 共用的同一份目录
信息；它不是 Provider 的 function-call schema，也不是 permission policy。底层 tool
实际返回值的语义和 MCP server 的错误协议仍停留在工具调用边界。

### MCP 错误与 secret 边界

MCP 的配置 table 错误或 refresh 外层无法建立 client 时会阻断 bootstrap；单个
server 的 `get_tools()` 异常则收敛到该 server 的 failed status 和空 inventory。
读路径还会报告 refresh 前访问、未知 server/tool、raw name 歧义、Agent injection
重名、越界 qualified name、非法 schema/arguments，以及底层 tool invoke 错误。
这些错误分别属于配置、目录、scope 或调用边界，不会被伪装成 Provider 错误。

scope 只限制 registry 可见/可调用的工具集合，**不是 authorization**；是否需要
审批、允许什么系统 tool 或 shell 命令由独立 permission 边界决定。MCP raw dict
仍由 Registry 保留并交给底层 client；调用 `call_tool` 不会改变 Registry 没有通用
secret resolver、轮换或投影的边界。

## 入口与独立生命周期

生产组合路径由 [`entrypoints/main.py`](../../src/ruyi_agent/entrypoints/main.py)
启动并在 FastAPI lifespan 中进入 [`bootstrap.py`](../../src/ruyi_agent/runtime/bootstrap.py)：

```text
ruyi CLI / app lifespan
        │
        ▼
bootstrap_application
   ├─ load_agent_configs
   ├─ load_llm_provider_configs ──> build_all_local_worker_specs
   │                                  └─> Provider factory / Codex model
   └─ load_mcp_server_configs ──> MCPRegistry.refresh
                                      └─> eager tools or registry + scope
```

两条分支最终都由 local worker 的显式依赖消费：模型进入 `LocalWorkerSpec.model`，
工具进入 `LocalWorkerSpec.tools`，或 Registry/scope 进入 tool-search middleware。
这只是构造时的依赖汇合，不是共享缓存或凭据通道。Provider 配置、环境变量值和
Codex auth JSON 不进入 MCP registry；MCP raw connection dict、description、status
和 inventory 也不进入 Provider spec 或 Codex auth。

各自的同步触发是：

- Provider/Agent 配置在 bootstrap 的 loader 和 worker-spec 构造时读取；修改
  `llm_providers.toml`、`agents.toml` 或命名 API key 环境变量，需要重新走该构造
  路径才会影响新的 local Agent。Codex access-refresh 只在凭据解析发现 token
  接近过期且存在 refresh token 时触发，并把结果写回 auth JSON。
- MCP 配置在 bootstrap 中读取并显式执行一次 `await registry.refresh()`；要重新
  建立 server status/inventory，调用方必须再次显式 refresh，随后再按 Agent scope
  解析工具。它不会因 Provider token refresh 自动刷新，也不会共享刷新触发器。

## 测试证据

以下是当前边界的少量 canonical 行为证据入口：

- Provider parser/factory、Agent provider/model 引用、Codex 配置映射和单 Agent
  unavailable：[test_config_loader.py](../../tests/unit/test_config_loader.py)。
- Codex auth JSON、refresh/writeback、Responses streaming、retry 和 HTTP 错误处理：
  [test_openai_codex_model.py](../../tests/unit/test_openai_codex_model.py)。
- MCP refresh/status/inventory，以及 bootstrap 的刷新与生命周期：
  [test_mcp_registry.py](../../tests/unit/test_mcp_registry.py) 和
  [test_runtime_bootstrap_shutdown.py](../../tests/unit/test_runtime_bootstrap_shutdown.py)。
- `tool_search`/`call_tool`、精确 qualified name、scope、schema 和 system-tool pair：
  [test_tool_search_middleware.py](../../tests/unit/test_tool_search_middleware.py) 和
  [test_system_tools.py](../../tests/unit/test_system_tools.py)。

## 文档同步触发

仅在以下 ownership 或长期契约变化时同步本文、相关模板和 canonical 证据：

- ownership：Provider、MCP Registry 或 bootstrap 的责任边界变化；
- public contract：Provider/MCP 配置字段、qualified name、arguments schema、
  `tool_search`/`call_tool` 或 Responses 适配契约变化；
- trust：凭据注入/可见性、错误文本 redaction、Agent scope 或 permission 边界变化；
- recovery：Codex refresh/writeback/retry，MCP refresh status，或启动/关闭失败清理变化。

本文只描述已经存在的入口和边界；探针、scope 或 `description` 都不能被解释成
另一套共享凭据、授权或生命周期机制。
