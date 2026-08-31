# AGENTS.md

本规则适用于仓库全部源代码、测试、配置、脚本和文档。Ruyi Agent 是以 Gateway 为任务控制面、以 runtime 为执行面、以 SQLite stores 保存任务与投影状态、以 HTTP/Telegram/Feishu 作为入口的 Python Agent Runtime：`ruyi` CLI 从当前工作目录的 `.ruyi_agent/ruyi.toml`（或显式 `RUYI_HOME`）读取 runtime TOML，只接受代码列明的有限 env alias，并产出不可变 `RuntimeSettings`；bootstrap 随后分别加载 Agent、Provider、permission 配置，MCP 保留 raw connection dict；Provider 和 A2A remote-ref 的命名环境变量由 integrations 层解析，Telegram/Feishu 适配器通过 Gateway 协议提交和跟踪任务。修改关键区域前，应先读对应实现和测试；当前正式系统文档尚未建立，空的 `docs/` 骨架不是权威事实来源。

## 修改前的阅读顺序

- CLI、路径或配置：先读 `src/ruyi_agent/entrypoints/main.py`、`src/ruyi_agent/config/paths.py`、`src/ruyi_agent/config/runtime_settings.py` 及 `src/ruyi_agent/templates/ruyi_home/`，再看 `tests/unit/test_entrypoint_cli.py`、`test_ruyi_paths.py`、`test_runtime_settings.py`。
- Gateway、任务、HTTP 协议或持久化：先读 `src/ruyi_agent/runtime/bootstrap.py`、`src/ruyi_agent/gateway/`、`src/ruyi_agent/gateway_protocol/` 和 `src/ruyi_agent/storage/`，再按行为选择 Gateway、事件、存储测试。
- Telegram/Feishu 或用户会话：先读 `src/ruyi_agent/channels/turn.py` 以及对应 adapter、identity、receipts、delivery 实现，再看 channel unit/integration 测试。
- runtime、委派、MCP、backend 或 skills：先读 `src/ruyi_agent/runtime/` 和对应 `src/ruyi_agent/integrations/` 边界；不要从已删除的历史说明推断当前行为。

## 仓库目录地图

目录条目说明其拥有的行为边界；不列缓存、构建产物、egg-info 或临时目录。

- 根文件：`pyproject.toml` 负责依赖、`ruyi` script、pytest/Ruff/coverage 和打包；`uv.lock` 锁定环境；`.python-version` 固定 Python 3.13；`paseo.json` 提供工作台启动/测试脚本；`main.py` 提供根级 ASGI/嵌入包装；`LICENSE` 是许可文本。
- `.github/workflows/ci.yml`：CI 的安装、compileall、Ruff、带 coverage 的完整测试、跨平台路径/打包 smoke；workflow 是 gate 的事实来源。
- `README.md` 和 `AGENTS.md` 是当前 active docs/instructions；`README.copy.md` 和 `AGENTS.copy.md` 是按迁移前原文保留的 archival snapshots，不是当前规范权威，其历史链接不受 active-link maintenance rule 约束。
- `.impeccable/`：tracked workbench generated artifacts，含方向/构建选择、答案、日志和易失 state；它记录工作台输入，不拥有 runtime、测试或用户契约。
- `.ruyi_agent/`：仓库内跟踪的 starter/local `ruyi.toml`、`config/*.toml`、`config/*.toml.example` 和 `data/.gitkeep` 目录标记；运行数据库、runtime、skills 和认证文件属于本地运行数据，不应当成为源代码行为的替代品。
- `docs/systems/`、`docs/decisions/`、`docs/guides/`：第一阶段只保留用途骨架和 `.gitkeep`；在有实际内容前不得把它们当作系统、决策或开发规则的权威。
- `scripts/`：Provider/reasoning 探针及其脚本测试；它们用于诊断特定集成，不是默认 Gateway 启动入口。
- `src/ruyi_agent/`：可安装的 Python 包；根部 `task_models.py` 保存跨边界任务模型，`__main__.py`/`entrypoints/` 承载 CLI 启动。
  - `config/`：路径发现、runtime TOML 加载、不可变 `RuntimeSettings`、Agent/Provider/permission 模型和校验；它不包揽 bootstrap 的全部配置。bootstrap 分别装配 Agent/Provider/permission，MCP loader 保留 raw connection dict，Provider/A2A remote-ref 的命名环境变量由 `integrations/` 解析。
  - `channels/`：入口层的 turn、identity、receipt、delivery、presentation 和 Gateway client；`http/` 拥有 probes、bearer/session auth、task/review/artifact/event/team-console 路由，`telegram/` 和 `feishu/` 各自拥有外部 API、poll/websocket、身份、投递和收据适配。
  - `gateway/`：任务服务、创建/输入 command、route reservation、local/remote routing、review、artifact、attachment 和对外错误投影；拥有 Gateway 的效果编排。
  - `gateway_protocol/`：HTTP client/transport、DTO、opaque cursor、SSE 和 Task event public projection；稳定的 Agent/Task/Review/Artifact response projection 由 `gateway/application.py::GatewayProjection` 拥有，不应由 channel 直接重造。
  - `runtime/`：bootstrap、Agent turn/history、task event ledger/projection；`delegation/` 拥有本地/远端委派、reconciliation、mailbox 投递和 supervisor，`mailbox/` 拥有 mailbox service，`middleware/` 拥有任务 hydration、工具调用、审批、artifact、skills 和 worker delegation 链，`skills/` 拥有 catalog、解析和 backend view 同步。
  - `control_plane/`：运行时权限控制策略；`storage/`：Ruyi SQLite schemas、stores、repositories 和 UoW，并在 bootstrap 中参与 LangGraph checkpoint 生命周期；projection/lifecycle 业务语义属于 `runtime/` 与 `gateway/`，不能泛称为 storage 拥有全部 durable state。
  - `integrations/`：Provider 工厂、命名环境变量解析和 OpenAI Codex；`a2a/` 拥有远端任务 client 及 remote-ref 认证环境变量解析，`backend/` 拥有 local/Daytona backend runtime，`mcp/` 拥有 MCP registry 并保留 raw connection dict。
  - `web/team_console/`：随包分发的 team console 静态资源；服务路由和其 session auth 仍由 `channels/http/` 拥有。
  - `templates/ruyi_home/`：随包分发的初始化模板，包括 `ruyi.toml`、`config/*.toml` 和 `config/*.toml.example` 的 Agent/Provider/MCP/permission 配置；修改模板要同时核对 parser 和 packaging 测试。
- `tests/`：行为证据；`unit/` 覆盖模块和边界，`integration/` 覆盖跨 Gateway/HTTP boundary 的 channel 流程（有些使用同进程 ASGITransport，有些使用真实 socket），`support/` 仅提供测试运行时辅助，不是生产 API；`conftest.py` 负责共享 fixtures。

## 从源码运行与常用命令

以下所有初始化和运行示例都显式指定隔离的绝对 `RUYI_HOME`；请把 `/path/to/ruyi-home` 和 `/path/to/workspace` 换成自己拥有的目录，不要让示例隐式写入用户默认目录。

### 从源码运行

1. 安装开发依赖。修改依赖或首次进入仓库时运行：

   ```bash
   uv sync --dev  # 按 pyproject.toml/uv.lock 安装开发环境
   ```

2. 确认 CLI 入口。只检查参数时运行，不会启动服务：

   ```bash
   RUYI_HOME=/path/to/ruyi-home uv run ruyi --help  # 查看 CLI 选项和入口模式
   ```

3. 初始化隔离配置。目标目录是 disposable 或确认可覆盖时才使用 `--force`：

   ```bash
   RUYI_HOME=/path/to/ruyi-home uv run ruyi --init  # 生成 ruyi.toml、config/*.toml 和 config/*.toml.example
   RUYI_HOME=/path/to/ruyi-home uv run ruyi --init --force  # 仅覆盖固定模板；会丢失模板文件的自定义改动，不清理其他文件
   ```

4. 在同一 `RUYI_HOME` 下编辑 `config/agents.toml` 和 `config/llm_providers.toml`。starter 的 `main` Agent 默认使用 `openrouter`；普通 API-key Provider 按条目的 `api_key_env` 在进程环境中提供密钥。`openai_codex` 可用 `init_kwargs.auth_json`，starter 默认的 `~/.ruyi_agent/openai_codex_auth.json` 会经 `expanduser` 解析，且不随隔离 `RUYI_HOME` 改变。Provider/Agent/MCP/permission 配置解析错误可阻断 bootstrap；已解析后，单个 Agent 的 provider/model/credential 构建失败通常只标记该 Agent unavailable，Gateway 不一定停止。starter 还包含远端 MCP URL，启动时会刷新已配置的 MCP；无网络或不使用时应调整该配置。

5. 安全启动本机 Gateway。它是长运行进程；需要调用的 Agent 的 Provider 配置应可用，配置的 MCP 也可能在 bootstrap 时访问网络：

   ```bash
   RUYI_HOME=/path/to/ruyi-home uv run ruyi --gateway  # 仅 Gateway；默认监听 127.0.0.1:8000
   RUYI_HOME=/path/to/ruyi-home uv run ruyi --workspace /path/to/workspace --gateway  # 显式指定 Agent 工作区
   ```

6. 需要 channel 时选择以下任一命令。每个命令都会在同一进程启动 Gateway 和对应 adapter，替代上一步的独立 Gateway 启动；以下都是长运行命令，禁止再另起一个 Gateway 绑定相同默认端口。Telegram 需要 bot token，Feishu 需要 app id/app secret：

   ```bash
   RUYI_HOME=/path/to/ruyi-home uv run ruyi --telegram  # Gateway + Telegram adapter
   RUYI_HOME=/path/to/ruyi-home uv run ruyi --feishu  # Gateway + Feishu adapter
   RUYI_HOME=/path/to/ruyi-home uv run ruyi --all  # Gateway + 已配置的 channel；未配置的 channel 会被过滤
   ```

### 证据匹配的检查

```bash
uv run pytest tests/unit/test_ruyi_paths.py tests/unit/test_runtime_settings.py tests/unit/test_entrypoint_cli.py tests/unit/test_packaging.py -q  # 路径、配置、CLI、打包元数据与模板的定向证据；wheel build/install/help smoke 由 CI 后续步骤执行
uv run pytest tests/unit/test_gateway_http_core.py tests/unit/test_gateway_http_events.py tests/unit/test_gateway_probes.py -q  # Gateway 路由、认证和事件 HTTP 证据
uv run pytest tests/unit/test_channel_turn.py tests/unit/test_telegram_identity_network.py tests/unit/test_feishu_client_identity.py -q  # channel turn 与 Telegram/Feishu identity 证据
uv run pytest tests/unit/test_channel_event_receipts.py tests/unit/test_telegram_receipts.py tests/unit/test_feishu_receipts.py -q  # channel event receipt 证据
uv run pytest tests/unit/test_channel_delivery.py tests/unit/test_settled_outbox.py tests/unit/test_settled_outbox_recovery.py -q  # durable channel delivery/outbox 证据
uv run pytest tests/unit  # 单元层证据；适合 unit 或跨模块但不需真实服务的改动
uv run pytest tests/integration  # 跨 Gateway/HTTP boundary 的集成证据；部分同进程 ASGITransport，部分真实 socket
uv run pytest --cov=ruyi_agent -W error::ResourceWarning -W error::pytest.PytestUnraisableExceptionWarning  # CI 完整 coverage gate；pyproject.toml 的 fail_under 为 78，跨域或最终集成前运行
uv run ruff check src tests scripts  # CI 使用的 Ruff 规则
uv run python -m compileall -q src scripts tests  # CI 使用的 Python 编译检查
git diff --check  # 提交前检查空白和补丁格式
uv build --wheel --out-dir dist-ci --clear  # CI 使用的 wheel 构建；打包入口/包数据变化时运行
```

不要把 full suite 或 coverage 当作每次小改动的默认动作；先运行能证明变化层的定向测试，跨域行为或提交前再运行对应 full gate。

## 安全、凭据与运行数据

- `RUYI_HOME` 有最高优先级；未设置时，代码只在当前工作目录发现 `.ruyi_agent/ruyi.toml` 或 `config/` 才使用项目目录，否则回退到用户 home，不向上搜索仓库根。个人运行应显式使用仓库外的 `RUYI_HOME`，避免把运行配置和工作区混在一起。
- 仓库跟踪的 `.ruyi_agent/ruyi.toml`、`.ruyi_agent/config/*.toml`、`.ruyi_agent/config/*.toml.example` 是无秘密的 starter/local 配置；`.ruyi_agent/data/.gitkeep` 只是目录标记。实际 `*.sqlite*`、`.ruyi_agent/runtime/`、`.ruyi_agent/skills/`、日志、`.env` 和 `.ruyi_agent/openai_codex_auth.json` 都是本地运行数据或凭据边界，禁止提交。
- Provider API key、Telegram bot token、Feishu app secret、Daytona/A2A token、Gateway bearer token、LangSmith key 和 Codex auth 必须通过受支持的环境变量或本地未跟踪配置提供；禁止把真实值写进 starter、测试 fixture、日志或错误响应。
- Gateway 默认 `dev-token` 只适合 loopback 调试；bootstrap 会拒绝用默认 token 在非 loopback 地址启动。对外监听前必须使用非默认 token，并由部署环境负责相应的传输和网络访问控制。
- canonical workspace path 来自 `config/paths.py` 和 runtime settings，backend 再把它映射到 backend 路径；`backend.kind = "local"` 的文件工具映射到 typed workspace，但 shell 仍以当前用户权限直接执行，没有 Daytona 的进程隔离。`gateway/attachments.py` 管理 attachment inbox，`gateway/artifacts.py` 和 `runtime/middleware/artifact_publishing.py` 也会验证 artifact 路径；处理不可信任务时应显式选择并验证 Daytona backend，不要把整个 workspace/file 边界归给单一模块。
- `--init --force` 只覆盖固定生成模板；只有在确认该隔离目录可覆盖时使用，不能用它清除含有自定义设置或凭据的其他文件。

## Ruyi 约定

- CLI 只在 `src/ruyi_agent/config/runtime_settings.py` 的 runtime TOML/有限 env alias 边界生成 immutable `RuntimeSettings`；bootstrap 分别加载 Agent/Provider/permission，MCP 保留 raw connection dict，Provider/A2A remote-ref 的命名环境变量在 `src/ruyi_agent/integrations/` 解析。消费者接收对应 typed 模型；新增字段必须同时核对 starter 和 `tests/unit/test_runtime_settings.py`、`tests/unit/test_config_loader.py`，未知字段和无效 Provider 配置应继续在加载边界失败。
- `/health` 和 `/ready` 是无 bearer 的探针，业务 Gateway 路由必须经过 `src/ruyi_agent/channels/http/context.py::require_bearer`；team console 使用独立的 browser session auth。新增或移动路由要保持这一区分，并用 `tests/unit/test_gateway_probes.py`、`tests/unit/test_gateway_http_core.py`、`tests/unit/test_team_console_auth.py` 证明。
- Gateway 的 `TaskRecord.task_id` 是对外任务身份，`src/ruyi_agent/storage/gateway_route_store.py` 的 `TaskRouteRecord` 把它绑定到 local 或 remote route；`src/ruyi_agent/gateway/routing.py::TaskRouter` 负责查询、恢复和错误投影。remote upstream id 不能取代 Gateway task id，改变路由状态机要覆盖 `tests/unit/test_gateway_task_router.py` 和相关 integration 流程。
- 带 `Idempotency-Key` 的 Gateway create/input 由 `GatewayCommandStore` 按 principal、key 和 request hash 持久化 claim；持久化成功和终态错误都可 exact replay，已开始但不可安全重放的 effect 必须保留可查询身份并进入相应 terminal/uncertain 处理。没有 key 的旧路径不能被文档宣称为自动幂等。对应证据在 `tests/unit/test_gateway_command_store.py`、`tests/unit/test_gateway_create_effect_disposition.py`、`tests/integration/test_gateway_idempotency_flow.py`。
- Task event SSE 的 cursor 绑定 task/run；新 stream 先给 snapshot，durable lifecycle event 由 ledger 保存，`assistant.delta` 只是可丢弃的 transient fanout，重连使用 opaque `Last-Event-ID`。客户端不能把 transient delta 当作恢复依据；变化应覆盖 `src/ruyi_agent/runtime/task_event_ledger.py`、`src/ruyi_agent/gateway_protocol/cursor.py`、`tests/unit/test_task_events.py` 或 `tests/integration/test_gateway_sse_flow.py`。
- `/tasks/{task_id}/messages` 是 `src/ruyi_agent/runtime/message_history.py` 提供的文本投影，cursor 绑定 task/checkpoint；remote message projection 必须校验 downstream task identity，重写 top-level `task_id` 并保留 `items`/`cursor`，不存在 source relabel。不要把任意底层 transcript 当成 public message contract，相关证据是 `tests/unit/test_message_history.py` 和 `tests/integration/test_gateway_message_history_flow.py`。
- Channel session key 的 DM 维度是 platform+agent+chat，群聊再加入 user，有 thread/topic 时再加入该维度；identity 实现位于 `src/ruyi_agent/channels/telegram/identity.py`、`src/ruyi_agent/channels/feishu/identity.py`，对应证据是 `tests/unit/test_telegram_identity_network.py`、`tests/unit/test_feishu_client_identity.py` 和 `tests/unit/test_channel_turn.py`。
- Channel event receipt 只确认 inbound event 的处理占有；durable delivery 只能跳过已确认的 step，外部发送结果不确定时重试仍可能重复，不能承诺绝对去重。实现位于 `src/ruyi_agent/channels/event_receipts.py`、`src/ruyi_agent/storage/channel_delivery_store.py`、`src/ruyi_agent/channels/presentation.py` 和 `src/ruyi_agent/storage/settled_outbox.py`，证据分开看 `tests/unit/test_channel_event_receipts.py`、`tests/unit/test_telegram_receipts.py`、`tests/unit/test_feishu_receipts.py`、`tests/unit/test_channel_delivery.py`、`tests/unit/test_settled_outbox.py`、`tests/unit/test_settled_outbox_recovery.py`。

## 测试规则

必须有足够证据覆盖本次变更的 Ruyi 行为，但不机械要求每个修改都新增测试。配置/CLI 改动优先跑路径、settings、entrypoint 定向测试；Gateway route/protocol 改动覆盖 auth、DTO、route 或 event 测试；channel identity、receipt、durable delivery 改动分别覆盖对应测试；跨 Gateway、runtime、storage 或真实入口的改动再运行 integration。这里的 integration 是跨 Gateway/HTTP boundary，部分使用同进程 ASGITransport，部分使用真实 socket，不要一概称为跨进程。完整测试和 coverage gate 由跨域变更、最终集成或 CI 需要触发，不是每次局部编辑的默认步骤。

## 文档同步

- 只有新根 `README.md`、新根 `AGENTS.md` 和未来写入的 docs 正文属于 active docs；`README.copy.md`、`AGENTS.copy.md` 必须按迁移前原文保留为 archival snapshots，不是当前规范权威，其历史链接不纳入 active-link maintenance rule。
- 代码改变用户可见行为、CLI、配置格式、HTTP/channel 协议或其他稳定契约时，必须同步受影响的当前文档；至少核对 `README.md`、starter 配置和实现/测试中的示例。
- 当前没有 Ruyi 的 `doc-sync` 命令；禁止发明或把它写成现有 gate。未来建立文档 gate 后，再把已验证命令登记到这里和 CI。`docs/systems/`、`docs/decisions/`、`docs/guides/` 目前只是骨架，不预先规定空文档内容。
- 文档命令、路径和本地链接在提交前必须以当前文件、`pyproject.toml`、CI 和轻量命令核实；失效引用应随产生它的代码或文档变更一起修正。

## 维护这些指令

- 根 `AGENTS.md` 只收录已经由 Ruyi 代码、测试或 CI 验证、长期有效、会重复遇到或错误后果严重的规则；一次性任务、未来设想、未研究完成的契约和模糊提醒不得写入。
- 以后需要局部约束时，把它放在最近的局部 `AGENTS.md`，不要把目录专属细节堆入根文件；局部规则不能削弱这里的凭据和安全边界。
- 代码、测试或 CI 证明规则失效时，必须删除或更新规则，并重新核对相关命令和路径；修改本文件本身也必须通过 `git diff --check` 和本文件中仍然有效的轻量验证。

## 相关入口

- 用户快速开始、能力和最小配置：[README.md](README.md)
- 依赖、CLI script、pytest、Ruff、coverage 和打包：[pyproject.toml](pyproject.toml)
- CI 安装与 gate：[.github/workflows/ci.yml](.github/workflows/ci.yml)
- CLI 启动：[src/ruyi_agent/entrypoints/main.py](src/ruyi_agent/entrypoints/main.py)
- 配置边界：[src/ruyi_agent/config/runtime_settings.py](src/ruyi_agent/config/runtime_settings.py)、[src/ruyi_agent/config/provider_parser.py](src/ruyi_agent/config/provider_parser.py)
- bootstrap 与 backend：[src/ruyi_agent/runtime/bootstrap.py](src/ruyi_agent/runtime/bootstrap.py)、[src/ruyi_agent/integrations/backend/runtime.py](src/ruyi_agent/integrations/backend/runtime.py)
- Gateway HTTP、协议与 response projection：[src/ruyi_agent/channels/http/routes.py](src/ruyi_agent/channels/http/routes.py)、[src/ruyi_agent/gateway_protocol/contracts.py](src/ruyi_agent/gateway_protocol/contracts.py)、[src/ruyi_agent/gateway/application.py](src/ruyi_agent/gateway/application.py)
- 任务路由、事件和 durable commands：[src/ruyi_agent/gateway/routing.py](src/ruyi_agent/gateway/routing.py)、[src/ruyi_agent/runtime/task_event_ledger.py](src/ruyi_agent/runtime/task_event_ledger.py)、[src/ruyi_agent/storage/gateway_command_store.py](src/ruyi_agent/storage/gateway_command_store.py)
- 行为测试：[tests/unit/test_gateway_http_core.py](tests/unit/test_gateway_http_core.py)、[tests/unit/test_task_events.py](tests/unit/test_task_events.py)、[tests/integration/test_gateway_sse_flow.py](tests/integration/test_gateway_sse_flow.py)
