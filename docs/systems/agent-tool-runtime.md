# Agent graph 与 tool runtime

本文说明一个已解析的 local Agent 如何被编译为 LangGraph agent，以及一次模型
调用如何经过 project middleware、权限闸门和工具执行边界。事实以当前实现和行为
测试为准；runtime 总览见[架构总览](../architecture.md)，Task 的调度、状态和事件
见[Task execution runtime](task-execution.md)，backend 的路径与执行信任边界见
[execution backends and workspace](execution-backends-and-workspace.md)。

## 负责范围与边界

本文拥有以下 runtime tool-plane 行为：

- 由 [`create_runtime_agent`](../../src/ruyi_agent/runtime/agent_factory.py) 解析
  model、组合 state schema、middleware 和工具，并返回可调用的已编译 graph；
- 给 graph 注入共享 checkpointer 以及每次 run 的 `configurable` context；
- 组合 task hydration、mailbox hook、tool-call protocol repair、MCP search/call、
  artifact publishing、skills exposure、filesystem、summarization、patch、worker
  delegation、human approval、prompt caching 和 memory middleware；
- 在 model boundary 处理工具 schema/调用，分类工具异常并按有限预算重试；在权限
  命中时生成 LangGraph interrupt，并按 resume payload 继续 graph。

以下 ownership 明确不在本文：

- Task 的创建、排队、run admission、`TaskRecord` 状态/`run_count`、完成和关闭；
  这些由 [`TaskRuntime`](../../src/ruyi_agent/runtime/delegation/task_runtime.py)、
  `RunSupervisor` 和 `TaskManager` 负责；
- Gateway Review 资源、认证、HTTP 和 public response projection；Gateway 只消费
  runtime 的稳定入口；
- 除权限策略以外的 backend 进程/文件系统隔离、host ACL 和 shell sandbox；local
  shell 的实际信任边界以 [backend 文档](execution-backends-and-workspace.md) 为准；
- MCP Registry 的启动、连接和 refresh；本文只使用已经准备好的 registry；
- delegation tree、深度/数量预算和 remote route；本文只描述 worker tool 如何接到
  runtime command port；
- mailbox 的 durability、settled outbox、claim/recovery 和跨崩溃投递；本文只描述
  mailbox 如何在 model boundary 被消费；
- skill catalog、选择解析和 view materialization；本文只消费任务已准备的 skill
  view；
- artifact bytes 的 HTTP 下载、Gateway artifact projection 和对象存储；本文只
  校验 backend path 并登记 Task artifact manifest。

## 入口调用方与依赖方向

外部调用路径是 `GatewayTaskModule/GatewayTaskService -> AgentControl ->
TaskRuntime -> LocalTaskExecutor`。入口和 facade 见
[`async_runtime.py`](../../src/ruyi_agent/runtime/delegation/async_runtime.py)；
Gateway 调用面见 [`gateway/tasks.py`](../../src/ruyi_agent/gateway/tasks.py) 和
[`gateway/task_service.py`](../../src/ruyi_agent/gateway/task_service.py)。runtime
内部 worker delegation tools 通过同一 `TaskCommandPort` 发起子任务，不绕过
`AgentControl`。Telegram/Feishu adapter 只通过 Gateway HTTP，不直接调用本页的
graph 或 middleware。

进程级依赖由 [`bootstrap_application`](../../src/ruyi_agent/runtime/bootstrap.py)
装配：backend、已经 refresh 的 MCP registry、permission policy、共享
`AsyncSqliteSaver`、Task/Mailbox/review-audit stores 和 `AgentControl`。backend、
checkpointer、store connection 的打开和关闭是 bootstrap 生命周期责任；graph 只
持有传入对象。

## 编译 graph 与 run context

`LocalTaskExecutor.compile_agent` 按 Agent 名称取已校验的
[`LocalWorkerSpec`](../../src/ruyi_agent/config/agent_runtime.py)，把 model、direct
tools、声明允许的 local/remote targets、backend、skills callback、mailbox、权限、
artifact callback、review audit store 和 MCP scope 传给 `create_runtime_agent`。
每个 local Agent 的 compiled graph 在 `TaskRuntime._compiled_agents` 中按
`agent_name` 缓存；不是每个 Task 重编译。runtime 关闭时清空该 cache。

`create_runtime_agent` 的关键步骤如下：

1. `resolve_model(model)` 只解析传入的字符串或 `BaseChatModel`；它不读取
   deepagents profile 来隐式改写 prompt、工具或 middleware。
2. 自定义 `system_prompt` 与项目固定的 `PROJECT_BASE_AGENT_PROMPT` 合并；没有
   自定义 prompt 时仍使用固定 base prompt。该固定提示规定 workspace、工具和
   artifact 的 backend path 约定。
3. `RuyiAgentState` 扩展 LangChain `AgentState`，要求 `messages` 使用
   `DeltaChannel(_add_messages_delta)`，同时保留 middleware state（例如 todos、
   files、skills metadata）。
4. 调用底层 `langchain.agents.create_agent`，传入 direct tools、runtime
   middleware、state schema 和可选 checkpointer；不额外插入 deepagents 的通用
   `task/general-purpose` subagent middleware。返回 graph 再设置高 recursion limit
   和 runtime metadata。

bootstrap 传入同一个 `AsyncSqliteSaver` 给所有 local Agent graph；它也把同一个
checkpointer 交给 [`TaskMessageStateReader`](../../src/ruyi_agent/runtime/message_history.py)。
每次 run 的 `RunnableConfig` 由
[`LocalTaskExecutor.build_run_config`](../../src/ruyi_agent/runtime/delegation/local_executor.py)
生成；当前项目写入的 `configurable` 字段是 `thread_id`、`task_id`、
`parent_task_id`、`root_task_id`、`delegation_depth`、`agent_name`、
`permission_profile`、`effective_skill_names`、`skill_view_path` 和
`skill_view_hash`、`mailbox_run_id`。`mailbox_run_id` 是每次 executor invocation
新建的 process-local identity：只有启用 mailbox 的 invocation 用它把 claim token
绑定到精确 Run，并 fence 该 Run 的 ack/release；它不是持久 Task identity，也不构成
对外的 Gateway context。middleware 通过 `get_config()` 或完整的 `ToolRuntime.config`
读取这些字段（artifact helper 也接受 `metadata.task_id`）；后续输入和 review
resume 复用同一 `thread_id`，因此是同一 graph 会话。`RunnableConfig` 还可能带有
框架的其它键，不能把它描述成只含 run context。

有 `astream` 时 executor 以 `stream_mode=["messages", "values"]`、`version="v2"`
运行 graph，保存最新 values 和 interrupts，并只把带公开 provenance 的 assistant
delta 交给 Task event fan-out；没有 stream 时退回 `ainvoke`。这一步负责调用 graph
并返回 `GraphOutput`，不决定 Task 最终状态；Task 状态解释仍归
[Task execution runtime](task-execution.md)。

## Middleware 组合与顺序

[`build_runtime_middleware`](../../src/ruyi_agent/runtime/middleware/stack.py) 返回
一个按下列顺序组装的列表。列表是当前组合契约：其中多项按 callback、policy、
registry、`system_tools` 或输入值条件加入，并非每次都存在；LangChain 对各 hook
phase 的 before/after 调度仍由其 middleware API 决定。即使条件组合不同，列表位置
仍会影响同一 phase 的 wrapper、prompt 合并和 tool-call 观察时机。

| 顺序 | middleware | 加入条件与作用 |
| --- | --- | --- |
| 1 | `TodoListMiddleware`、`ToolErrorMiddleware` | 总是加入；第二者位于 tool wrapper 列表前部，能把后续工具执行异常收口为 `ToolMessage`。 |
| 2 | `TaskHydrationMiddleware` | 仅有 `load_tasks_for_thread` callback 时加入，在 agent 开始时按当前 thread 懒加载可见 delegated tasks。 |
| 3 | `MailboxMiddleware` | 仅有 mailbox 时加入，在 model 前 claim 当前 task/thread 的消息并注入 `HumanMessage`。 |
| 4 | `ToolCallProtocolMiddleware` | 总是加入；紧跟 mailbox，修复 mailbox 输入可能造成的 tool-call/result 不相邻。 |
| 5 | `ToolSearchMiddleware` | 有 registry 且 `system_tools` 未限制，或显式启用 `tool_search`/`call_tool` 时加入；工具列表还会按 enabled set 过滤。 |
| 6 | `ArtifactPublishingMiddleware` | 有 `register_artifact` callback，且 `system_tools` 未限制或启用了 `publish_artifact` 时加入。工具可以被暴露但在没有 tracked `task_id` 的调用中返回 `no_active_task`。 |
| 7 | `RuyiSkillsMiddleware` | 总是加入；根据本次 run 的 skill view context 暴露 metadata。 |
| 8 | `FilesystemMiddleware` | `system_tools` 未限制，或启用了至少一个 filesystem tool 时加入，并按名字过滤。 |
| 9 | summarization、`PatchToolCallsMiddleware` | 总是加入；前者使用 model/backend 的 deepagents summarization，后者提供第三方 tool-call compatibility patch。 |
| 10 | `WorkerDelegationMiddleware` | `worker_tools is not None` 时加入，注入当前可访问的 local worker/remote-ref 描述和 delegation guidance。 |
| 11 | `HumanApprovalMiddleware` | 有 `PermissionPolicy` 时加入，在 model 产生 tool calls 后做权限判定和 interrupt。 |
| 12 | `AnthropicPromptCachingMiddleware` | 总是加入，`unsupported_model_behavior="ignore"`；不适用的 model 不因该 middleware 失败。 |
| 13 | `MemoryMiddleware` | `memory` 为非空列表时加入，从 backend source paths 提供 memory context。 |

这几个相邻关系是行为上的重要点：`ToolErrorMiddleware` 必须能观察后续 tool
wrapper；Mailbox 注入发生后，`ToolCallProtocolMiddleware` 在下一次 model request
前修复 history；summarization 后接 patch；delegation tools 已加入后才由
HumanApproval 统一检查；prompt caching 和 memory 是最后的 request-level additions。
这描述的是当前列表和 hook 影响，不应推断为每个 middleware 都硬编码成单一路径或
每项都一定运行。

`system_tools` 的 automatic/explicit/disabled 合并由
[`system_tools.py`](../../src/ruyi_agent/config/system_tools.py) 完成。它决定工具
是否出现在 graph 的工具集合，不等同于权限结果；即使某 tool 被暴露，仍可能在
HumanApproval 中被 allow、require approval 或 deny。

## Model boundary 与工具执行

model 每次收到的是 graph 当前 state、固定/Agent prompt、middleware 追加的 system
说明以及当次已暴露的工具 schema。模型返回普通 assistant text，或带稳定 `id`、
`name`、`args` 的 `AIMessage.tool_calls`。工具不会因为名字出现在 prompt 中就自动
执行：model output 先经过 middleware 的 after-model 观察，再进入标准 LangChain/
LangGraph tool-call 执行边界；执行结果作为对应 `ToolMessage` 回到下一次 model
request。

因此有三层协议保护：

- `ToolCallProtocolMiddleware` 保证严格 OpenAI-compatible provider 看到每个
  assistant tool call 后紧邻一个结果；
- `HumanApprovalMiddleware` 在执行前处理策略和人工决定；
- `ToolErrorMiddleware` 在执行后把异常转换成可供模型重新规划的结构化错误。

直接工具来自 Agent spec 的 `tools`；system tools 来自条件 middleware（filesystem、
artifact、MCP search/call、delegation）。`ToolRuntime.config` 是完整的
`RunnableConfig`，不是只携带当前 run context 的专用对象；`call_tool` 会把整个 config
传给底层 MCP tool 的 `ainvoke`/`invoke`，artifact 则从 `configurable` 或 `metadata`
读取 `task_id`。runtime 不应把它当作 secret boundary，也不应依赖它自动过滤
public Gateway DTO。

### Task hydration、mailbox 与 tool-call repair

[`TaskHydrationMiddleware`](../../src/ruyi_agent/runtime/middleware/task_hydration.py)
在 `before_agent`/`abefore_agent` 读取当前 LangGraph `thread_id`，调用注入的
`load_tasks_for_thread`；没有 runtime config 或 thread id 时跳过。它只恢复当前
parent thread 的可见任务，不拥有 Task 状态。

[`MailboxMiddleware`](../../src/ruyi_agent/runtime/middleware/mailbox.py) 在
`before_model`/`abefore_model` 读取 `thread_id` 和可选 `task_id`，调用 mailbox 的
`claim`，把结果用 `render_mailbox_messages` 组成一条带 source/message ids 的
`HumanMessage`。一次 graph invocation 正常返回时，`LocalTaskExecutor` 只确认
绑定到该 Run 的 claim token；异常或取消则只释放这些 token。claim、ack、recovery
的 durability 不属于本文。

[`ToolCallProtocolMiddleware`](../../src/ruyi_agent/runtime/middleware/tool_call_protocol.py)
在每个 model boundary 做 repair：把 history 中已有的 `ToolMessage` 移到所属
assistant call 的紧邻位置，删除 orphan result；缺少结果时合成 status=`error` 的
取消说明；对 `invalid_tool_calls` 的非法 JSON 保留一条 content note 并移除不可
执行的 call。变更以 LangGraph `Overwrite` 写回，避免 mailbox 或其它 runtime
message 插入 tool-call/result 中间。

### MCP tool search 与 call

[`ToolSearchMiddleware`](../../src/ruyi_agent/runtime/middleware/tool_search.py) 不把
整个 MCP catalog 作为 direct tools 注入，而是提供稳定的 `tool_search` 和
`call_tool` 两个工具。Agent 的 MCP scope 由 `server_names`/`tool_names` 决定；显式
空 allowlist 仍为空，不退化成全量 catalog。

- `tool_search(query, limit)` 调 registry 的 scoped search，返回 JSON metadata：
  `qualified_name`、server/raw name、description 和 `args_schema`，并返回 scope 与
  “必须用 `call_tool`”的指示；system prompt 只追加轻量 source summary。
- `call_tool(qualified_name, arguments)` 要求带 server 前缀的精确名称，校验名称在
  scope 内，再让 registry 用 JSON Schema 校验 arguments，取回真实 MCP tool，优先
  `ainvoke`（否则在线程中调用 `invoke`），最后序列化结果。
- 不允许模型直接猜 raw tool name，也不允许越过 scope；registry 未被 bootstrap
  refresh 时会报告未加载。refresh 时机、单 server 失败隔离和连接生命周期由
  [`MCPRegistry`](../../src/ruyi_agent/integrations/mcp/registry.py) 与 bootstrap
  拥有，本文不重新定义。

### Filesystem、artifact 与 backend path

Filesystem middleware 由 deepagents adapter 提供 `ls`、`read_file`、`write_file`、
`edit_file`、`glob`、`grep`、`execute`，并使用传入的 `BackendProtocol`。stack 会
根据 `system_tools` 过滤单个工具；它不把 backend path allowlist 变成 host OS 的
全面隔离。尤其 local backend 的 shell 仍以宿主用户权限执行；请参阅
[backend 文档的 shell 与 virtual root 边界](execution-backends-and-workspace.md)。

有 `register_artifact` callback 且 `system_tools` 未限制或启用了 `publish_artifact` 时，
就暴露
[`publish_artifact`](../../src/ruyi_agent/runtime/middleware/artifact_publishing.py)：

1. 从完整 `ToolRuntime.config` 的 `configurable`/`metadata` 读取非空 `task_id`；
   没有 tracked Task 时工具仍可见，但调用返回 `no_active_task`。
2. 要求 POSIX backend workspace absolute path；拒绝空值、反斜杠、Windows drive
   path、相对路径和任意 `..`。除根为 `/` 的情况外，还要求规范 path 位于传入的
   normalized `workspace_root` 下。`name` 只取安全 basename，caption/content type
   也在登记前规范化。
3. 通过 backend `download_files([path])` 读取 bytes；download response 的 `error`
   字段、空响应或非 bytes 统一为 `file_not_found`，超过默认 50 MiB（或构造时的
   `max_bytes`）返回 `file_too_large`。如果 `download_files` 本身抛异常，middleware
   不在这里捕获；异常进入外层 `ToolErrorMiddleware` 的分类、有限重试和
   `ToolMessage` 路径。
4. 调用 `LocalTaskExecutor.register_artifact` callback 生成带 `artifact_id`、path、
   name、caption、content type、size 和当前 `run_count` 的 manifest，并交给
   `TaskManager.add_artifact`。callback 失败时返回 `artifact_registration_failed`
   error `ToolMessage`，其 hint 先使用有界安全异常摘要；成功 manifest/bytes 仍留在
   backend namespace，且不做输出扫描。

middleware path check 与 Gateway artifact download 的 check 是两个边界；本文不
负责后者的 HTTP auth、公共 DTO 或 bytes 下载。

### Skills、summarization、patch、prompt caching 与 memory

skill 的选择、catalog 和 materialization 由 runtime skills/Task 侧完成。spawn 时
已确定的 `skill_view_path`/`skill_view_hash` 放入 run config 后，
[`RuyiSkillsMiddleware`](../../src/ruyi_agent/runtime/middleware/ruyi_skills.py) 在
`before_agent` 读取 view 下的各 `SKILL.md` frontmatter，存入 `skills_metadata`；
hash 未变化时复用 state。每次 model request 它追加 skill name/description/path，
并提醒模型先阅读完整 `SKILL.md`。middleware 暴露 metadata，不负责 catalog、上传
view 或强制 `allowed_tools`。

[`deepagents_adapters.py`](../../src/ruyi_agent/runtime/middleware/deepagents_adapters.py)
集中导入第三方的 Todo、Filesystem、Memory、summarization、PatchToolCalls 和
Anthropic prompt-caching middleware。runtime 只规定它们在 stack 中的相对位置和
输入依赖：summarization 负责按其库策略压缩上下文，patch 负责第三方 tool-call
兼容修补，prompt caching 对不支持的 model 忽略，Memory 从已解析的 backend source
paths 提供记忆；这些第三方组件的内部阈值/缓存格式不是本项目 tool contract。

### Worker delegation

`TaskRuntime._get_or_create_agent` 只有在 Agent 的 declared targets 或启用的
delegation system tools 允许时，才构造 `worker_tools`；stack 随后加入
[`WorkerDelegationMiddleware`](../../src/ruyi_agent/runtime/middleware/worker_delegation.py)。
它向 model 说明 `spawn_agent`、`wait_agent`、`check_agent`、`send_input`、
`cancel_agent`、`list_agents` 的用途，并列出当前被 registry 选出的 local worker
与 remote ref。若没有 `spawn_agent`，它只追加 parent communication guidance。

middleware 不创建 delegation tree，也不执行深度/数量预算；工具调用最终回到
`TaskCommandPort`/`TaskRuntime`，该 ownership 见 [Task execution runtime](task-execution.md)。
这些工具的普通错误返回，以及 failed/interrupted `TaskRecord` 的 agent-facing 文本
projection，都只在 error path 使用同一有界安全摘要；不会改变 target scope、retry、
cancel 或 completed result 的语义。

## 权限、审批与 LangGraph interrupt

### PermissionPolicy

[`PermissionPolicy`](../../src/ruyi_agent/control_plane/permissions.py) 从 run
context 的 agent/profile 选择 profile；未知显式 profile 回退 default profile。普通
tool 先取 tool-specific policy，再取 `default` policy，最后默认
`require_approval`。结果是：

- `allow`：不修改 AI message，工具进入正常执行；
- `deny`：不执行工具，HumanApproval 添加 status=`error` 的人工合成
  `ToolMessage`，把 reason 给模型；
- `require_approval`：构造 action request/review config，暂停 graph 等待决定。

`execute` 有额外的确定顺序：先用 `shlex` 分析 command；先匹配 deny prefix（完整
token 前缀，含 shell control 时也检查各命令段），再识别风险，最后才匹配 allow/
approval prefix 和 execute 默认策略。识别的风险包括 command parse error、shell
control operator、empty command、`sudo` privilege escalation、git destructive、
destructive filesystem、recursive permission change 和 dependency install。profile
的 `execute_review_risks=None` 表示所有已识别风险升级审批；显式集合只升级集合内
风险。deny 前缀始终优先，不能被宽松 allow prefix 绕过。

### HumanApprovalMiddleware 的执行边界

[`HumanApprovalMiddleware`](../../src/ruyi_agent/runtime/middleware/human_approval.py)
在 model 产生 `AIMessage.tool_calls` 后逐个评估，context 包含 agent、profile、
backend kind、workspace、thread/task id。多个 call 中 allow 的保留，deny 的立即
产生人工 `ToolMessage`，require approval 的合并到同一个 review payload：
`review_id`、`action_requests`、`review_configs`。默认可选 decision 是
`approve`、`edit`、`reject`，实际列表以 policy result 为准。

它调用 LangGraph `interrupt(review_payload)`；graph 的 checkpoint 保存当时 state，
不会执行尚未批准的 call。runtime 将唯一 review payload 归入 pending review，外部
决定通过同一 task/thread 以 `Command(resume={"decisions": decisions})` 恢复 graph。
恢复 payload 必须是 decision dict 列表，数量与被中断的 calls 相同，且类型在
`allowed_decisions` 内。`edit` 只能改 args 不能改 tool name；改完后重新进入评估
循环，所以编辑成 deny/risk command 会被再次阻止。

TaskStore/TaskManager 中的 pending review identity 是权威审批资源；
[`ReviewAuditStore`](../../src/ruyi_agent/storage/review_audit.py) 只记录
`permission_evaluated`、`review_requested`、`review_decision`、`tool_denied` 等审计
事件，不是状态机权威。HumanApprovalMiddleware 内联调用这些 audit append 时不捕获
异常：写入失败可以在 interrupt 或 decision 应用前中止 graph，最终使该 run 失败。
另有 LocalTaskExecutor 在权威 Task/review transition 提交后的 task audit；这类
post-commit audit failure 会被 catch/log，不反转已提交状态。具体 Task transition
仍见 [Task execution runtime](task-execution.md)。

## ToolError 分类、重试与错误状态

[`ToolErrorMiddleware`](../../src/ruyi_agent/runtime/middleware/tool_error.py) 将工具
执行边界的异常按 exception group leaf 展开并分类：

| 类别 | 判定示例 | 是否可重试 |
| --- | --- | --- |
| `auth` | HTTP 401/403 | 否 |
| `unavailable` | HTTP 5xx | 是 |
| `invalid_input` | HTTP 4xx、`ValueError`、`TypeError` | 否 |
| `source_unavailable` | 含 `SOURCE_NOT_AVAILABLE` | 否 |
| `timeout` | exception class 名含 timeout | 是 |
| `network` | broken resource/connect/network/remote protocol error | 是 |
| `unexpected` | 其它错误 | 否 |

一次 tool call 最多两次尝试（首次加一次 retry）。首次失败只有在该 call 启动后 15
秒内才允许触发这一次 retry；这不是单次工具执行 timeout，也不是整个 run 的总
timeout。retry 一旦获准，第二次尝试可以在 15 秒之后结束；它失败时因 attempt
预算耗尽而直接生成错误。`KeyboardInterrupt`、`SystemExit` 和纯
`CancelledError` group 继续抛出；混合了取消与 MCP transport error 的 exception
group 视为 transport tool failure，可转为 ToolMessage。

最终错误返回 `ToolMessage(status="error")`，保留 tool name 和 tool_call id（缺失
id 时使用 `unknown`），content 包含 `category`、`retriable`、有界单行的 leaf 异常
摘要和 suggestion。摘要保留异常 class 与非敏感原因，并在写入 ToolMessage 或 Ruyi
日志前应用窄范围的高置信凭据替换。这样一个并行 tool call 的失败不会吞掉其它成功
结果，模型能看到结构化错误并重新规划；正常成功结果则原样保留为 success ToolMessage。

## 状态、恢复与安全边界

graph state/checkpoint 与 Task durable state 是两条边界：本页 graph 使用的
checkpointer 保存 messages、middleware state 和 interrupt；TaskRuntime/TaskManager
才把 run、pending review、artifact manifest 和最终状态写入 Task store。graph
checkpoint 写入不等于 Task 状态提交，反之亦然。启动恢复、run admission、取消、
review resource projection 和 public event 由 [Task execution runtime](task-execution.md)
拥有。

安全上，tool search 只允许 registry scope 内的 qualified name，MCP args 先做 schema
validation；permission gate 在 tool execution 前做 policy decision；artifact 只接收
backend workspace path。上述是 runtime guardrail，不是 host-level sandbox：local
backend 的 `execute`、网络、绝对 host path 和进程权限仍按 backend 实际边界运行。
Provider/A2A 命名环境变量由 `integrations` 解析；MCP 配置保留 raw connection dict
并交给底层 client。`ToolError`、artifact registration、delegation error render，以及
selected runtime failure logs/checkpoint-facing notification errors，在异常文本离开这些
边界时只做有界、单行的摘要，处理 header/key-value、URL 敏感参数、JWT、常见 key
前缀和 private-key PEM 等高置信形状。它不扫描环境、历史 checkpoint 或成功 tool
output，也不是“所有异常/日志都已脱敏”的保证。`ToolRuntime.config` 同样不是 secret
boundary，runtime 不应把它当作凭据过滤器。

## 依赖

主要代码依赖及其边界如下：

| 依赖 | runtime 使用方式 |
| --- | --- |
| LangChain/LangGraph | `create_agent`、middleware hooks、`AIMessage`/`ToolMessage`、`interrupt`/`Command(resume=...)`、state channels 和 stream；graph 生命周期由 runtime/bootstrap 组合。 |
| deepagents adapters | Filesystem、Memory、summarization、patch、Todo、Anthropic prompt caching；通过 [`deepagents_adapters.py`](../../src/ruyi_agent/runtime/middleware/deepagents_adapters.py) 集中接入。 |
| BackendProtocol | 文件、搜索、shell、upload/download 和 artifact/skill view namespace；隔离语义见 backend 文档。 |
| MCPRegistry | scoped catalog/search、qualified name、args schema validation 和真实 MCP tool invocation；registry refresh 不属于本页。 |
| PermissionPolicy/ReviewAuditStore | tool policy、execute risk 和 audit；Task pending review 仍由 TaskManager/TaskStore 持有。 |
| TaskManager/AgentMailbox/SkillSyncer | 通过 callback/context 接入 Task、mailbox、预物化 skill view；各自的 durable/catalog/materialization ownership 不在本页。 |

## 测试证据

以下行为证据按 tool-plane ownership 汇总：

- graph state、DeltaChannel、checkpointer continuation、middleware stack 与 system-tool
  exposure：[`test_agent_factory.py`](../../tests/unit/test_agent_factory.py)、
  [`test_runtime_middleware_stack.py`](../../tests/unit/test_runtime_middleware_stack.py)。
- MCP scope、qualified name、schema validation 与 call path：
  [`test_tool_search_middleware.py`](../../tests/unit/test_tool_search_middleware.py)。
- permission profiles、execute risk、approval interrupt/resume 与 review audit：
  [`test_human_approval_middleware.py`](../../tests/unit/test_human_approval_middleware.py)。
- ToolError 分类、retry、取消传播、并行 success/error 与安全摘要：
  [`test_tool_error_middleware.py`](../../tests/unit/test_tool_error_middleware.py)、
  [`test_safe_errors.py`](../../tests/unit/test_safe_errors.py)。

## 同步触发

以下变化应同步更新本文：

- graph、middleware、permission、tool execution 与相邻组件的 ownership 边界改变；
- graph/run context、middleware stack、tool schema/result、MCP/artifact/skill metadata
  或其他 model-facing 稳定输入输出改变；
- checkpoint、mailbox consumption、approval interrupt/resume、tool retry/error 或
  runtime close 的状态、事务或恢复语义改变；
- permission、MCP scope、artifact/backend path、credential/error handling 或其他
  runtime trust/security boundary 改变。

只改变其他子系统的内部实现时更新其所属文档；跨越上述边界时再同步受影响的文档。
