# 运行时配置与 Bootstrap

## 范围与所有权

本文描述当前一个 `ruyi` 进程从配置发现到 Gateway runtime 可用的装配边界。
事实以已批准的[架构总览](../architecture.md)、当前代码和行为测试为准；本文不
把历史副本或旧决策说明当作实现依据。

配置层拥有 Ruyi home/workspace 路径发现、runtime TOML 的严格解析、有限环境变量
兼容面，以及不可变的 `RuntimeSettings`。它还通过独立 loader/parser 读取 Agent、
Provider 和 permission 配置，并把 MCP 配置保留为 raw connection dict。bootstrap
是进程级 composition/lifecycle root：它消费 typed settings，装配 backend、
配置投影、MCP registry、checkpoint、SQLite stores、AgentControl 和 Gateway service，
再负责启动恢复与有序关闭。

下列行为不属于本文的 ownership：

- 配置层不启动 HTTP、channel 或 Agent turn，也不让下游重新解析同一份原始
  runtime TOML。
- bootstrap 不定义 Agent/Provider/permission 的文件 schema；schema 和解析由各自
  loader/parser 负责，也不拥有 channel 自己的 session、receipt、delivery store。
- 本文只说明 MCP 配置如何进入 bootstrap 以及单 server 失败的隔离，不展开运行期
  工具内容或其他协议、任务状态细节。

## 入口、调用方与依赖方向

主路径是：

```text
ruyi CLI
  -> parse_cli_options
  -> configure_runtime_environment
  -> immutable RuntimeSettings
  -> create_app
  -> create_bootstrapped_gateway_app
  -> FastAPI lifespan
  -> bootstrap_application
  -> Gateway service + AgentControl + shared resources
```

实现入口是 [`entrypoints/main.py`](../../src/ruyi_agent/entrypoints/main.py)、
[`runtime_settings.py`](../../src/ruyi_agent/config/runtime_settings.py) 和
[`runtime/bootstrap.py`](../../src/ruyi_agent/runtime/bootstrap.py)。CLI 只把
`RuntimeSettings` 交给 app/bootstrap；backend factory、配置 loader、runtime 和
HTTP 路由消费对应的 typed 对象。`--telegram`、`--feishu` 和 `--all` 由同一个
CLI `TaskGroup` 与 Gateway 一起运行；adapter 只使用 settings 中的 Gateway HTTP
endpoint，不接收 `AppRuntime` 或内部 Gateway service。

`create_app()` 在没有传入 settings 时配置一次环境；CLI 的正常调用顺序因此是先
完成配置，再创建 app。显式传入 settings 的调用方则直接使用该对象，不复制配置
发现逻辑。

## Ruyi home、workspace 与初始化

### 路径发现

[`resolve_ruyi_paths`](../../src/ruyi_agent/config/paths.py) 产生五个规范路径：
`ruyi_home`、`config_dir`、`data_dir`、`skills_dir` 和 `workspace`。

Ruyi home 的选择顺序是：

1. 非空的 `RUYI_HOME`（先 `expanduser`，再解析为绝对路径）；
2. 当前目录下已有配置的 `.ruyi_agent`，其中存在 `ruyi.toml` 或 `config/` 目录
   即视为项目配置；
3. 用户目录下的 `~/.ruyi_agent`。

发现只检查当前目录，不向父目录搜索。没有显式 workspace 时，初始 workspace 是
当前目录；runtime 配置的 workspace 优先级是 CLI `--workspace`，其次是
`[backend].workspace`，再其次是 `RUYI_WORKSPACE`，最后回退到当前目录。相对
storage 路径以 `RUYI_HOME` 为基准解析，workspace 则解析为规范绝对路径。POSIX
上会拒绝 Windows 风格的 `RUYI_HOME`、`RUYI_WORKSPACE` 和显式 workspace 参数。

### `--init` 与 force 边界

`ruyi --init` 进入 `configure_runtime_environment(init_templates=True)`，创建
Ruyi home 下的 `config/`、`data/`、`skills/` 目录，并从随包模板复制：

- `ruyi.toml`；
- `config/agents.toml` 与 `config/agents.toml.example`；
- `config/llm_providers.toml`、`config/mcp_servers.toml` 和
  `config/permissions.toml`。

已存在且非空的固定模板默认保留；不存在或空文件会被补齐。`--force` 只能和
`--init` 一起使用，且只覆盖上述固定模板。它不会删除其他文件、数据库、runtime
数据、skills、日志、环境文件或凭据；因此只有确认目标 Ruyi home 可覆盖时才使用
`ruyi --init --force`。初始化完成后 CLI 直接返回，不启动 Gateway 或 adapter。

不带 `--init` 时，必须已有选定 home 的 `ruyi.toml`；缺少它会要求先运行
`ruyi --init`，不会隐式创建用户 home。初始化目录和 starter 的事实入口是
[`templates/ruyi_home`](../../src/ruyi_agent/templates/ruyi_home/)，用户可见的
最小示例见 [`README.md`](../../README.md)。

## Runtime TOML 到不可变 `RuntimeSettings`

`configure_runtime_environment()` 先发现路径，初始化或检查 `ruyi.toml`，再由
[`load_runtime_settings`](../../src/ruyi_agent/config/runtime_settings.py) 一次读取
runtime TOML。它校验顶层和嵌套 table 的允许字段、类型、范围、host/URL 和路径，
并组合以下 frozen typed sections：

`paths`、`credentials`、`backend`、`gateway`、`storage`、`runtime`、`channels` 和
`langsmith`。嵌套 dataclass 也保持 frozen；列表式配置投影为 tuple，消费者得到
的是不可变对象而不是原始 TOML mapping。

兼容环境变量面是代码中列明的、按 table 限定的有限 alias 集合
[`TABLE_SCOPED_TOML_ALIASES`](../../src/ruyi_agent/config/runtime_settings.py)。
当前共 64 个名称，分布为：`model_credentials` 6 个、`backend.local` 3 个、
`backend.daytona` 4 个、`runtime` 6 个、`channels.telegram` 12 个、
`channels.feishu` 24 个、`langsmith` 4 个、`storage` 5 个。对允许环境回退的字段，
选择顺序是：规范 TOML 字段 > 同 table 的 alias 字段 > 对应进程环境 alias >
默认值；空字符串/空列表通常表示未设置。`gateway.*`、`backend.kind` 等没有把
同名大写 projection 当作 TOML 或环境输入；storage alias 也由字段级规则限制，
不能据此推导出任意环境变量覆盖能力。未知 runtime 字段或 alias 在配置边界失败。

加载成功后，[`apply_runtime_settings_to_env`](../../src/ruyi_agent/config/runtime_settings.py)
把 typed 值投影给兼容消费者，包括 `BACKEND_KIND`、`LOCAL_BACKEND_ROOT`、
`GATEWAY_*`、受支持的 alias 和规范路径变量，并设置 `RUYI_RUNTIME_CONFIGURED=1`。
这些 projection 是输出兼容面，不是下一轮 runtime TOML 的权威来源；后续 runtime
消费者使用 settings 或其 typed 子模型。

有一个刻意的 re-entry 例外：当再次调用 configure 时环境已有
`RUYI_RUNTIME_CONFIGURED=1` 且没有新的 workspace 参数，loader 会回读 projection
出的 `RUYI_WORKSPACE` 作为 workspace override，以保留首次显式选择。这只服务于
configured re-entry，不把一般 projection 变成配置输入。另有
`GatewayLaunchOverrides` 这一 typed launch 输入：它在严格 TOML 校验之后只覆盖
Gateway 的 `host`、`port`、`base_url`，保留 TOML 中的 bearer token；[`paseo.json`](../../paseo.json)
的 Gateway service script 使用该入口。

## 独立配置 loader/parser 与动态 secret

bootstrap 从所选 Ruyi home 的 `config/` 读取四类声明。公共读取入口在
[`config/loader.py`](../../src/ruyi_agent/config/loader.py)，解析职责保持分离：

| 声明 | loader/parser | 输出与边界 |
| --- | --- | --- |
| Agent | `load_agent_configs` + [`agent_parser.py`](../../src/ruyi_agent/config/agent_parser.py) | `(main_agent_name, AgentConfigs)`；每个 local/remote declaration 是已校验的 typed model，Agent key/name、必填字段、worker 图和 remote auth/URL 在这里检查 |
| Provider | `load_llm_provider_configs` + [`provider_parser.py`](../../src/ruyi_agent/config/provider_parser.py) | `dict[str, LLMProviderSpec]`；Provider 类型、地址、`api_key_env` 和 init kwargs 在这里校验，spec 及其递归 init kwargs 是 immutable |
| permission | `load_permission_config` + [`permission_parser.py`](../../src/ruyi_agent/config/permission_parser.py) | `PermissionConfig`；default profile、tool policy 和 execute rule 在独立边界解析 |
| MCP | `load_mcp_server_configs` | `dict[str, dict[str, Any]]` raw mapping；bootstrap 原样交给 `MCPRegistry`，不在 config 层伪造另一套 typed MCP schema |

Provider secret 和 remote-ref secret 在 integrations 边界按需解析，而不是把秘密
扩散到公开 task/config projection：

- Provider declaration 的 `api_key_env` 由
  [`integrations/model_providers.py`](../../src/ruyi_agent/integrations/model_providers.py)
  在构建模型时读取；缺少命名变量会使使用该 Provider 的 local Agent 构建失败。
- Agent remote-ref 只保留 `auth.token_env`。请求时由
  [`integrations/a2a/client.py`](../../src/ruyi_agent/integrations/a2a/client.py)
  读取该环境变量；缺少 token 在发送前报告 unavailable，不把 secret 写入 remote
  ref、事件或错误详情。

starter 和已跟踪配置只提供空 credential 字段或环境变量名；真实 Provider、channel、
remote 或 Gateway secret 应由运行环境或未跟踪的本地配置提供。

## CLI 到 bootstrap 的启动流程

`parse_cli_options()` 要求非初始化调用明确选择 entrypoint：`--gateway`（或旧的
位置参数 `gateway`）、`--telegram`、`--feishu` 或 `--all`。`--force` 没有 `--init`
时由 CLI parser 拒绝。`main()` 随后调用 `configure_runtime_environment`；配置阶段
的 `ValueError`/`ConfigError` 打印到 stderr 并以 status 2 退出。

配置成功后：

1. `--gateway` 通过 Uvicorn 调用 `create_app(settings)`。
2. `--telegram` 或 `--feishu` 选择 Gateway 加对应 adapter。
3. `--all` 仍先包含 Gateway，然后只保留有必要凭据的 adapter：Telegram 需要
   bot token，Feishu 需要 app id 和 app secret；两个都未配置时仍启动 Gateway。
4. 多入口由 `run_channels()` 的 `asyncio.TaskGroup` 持有；每个参与者共享同一份
   immutable settings，但 adapter 通过 Gateway HTTP endpoint 使用公开边界。

`create_bootstrapped_gateway_app()` 创建 FastAPI app，初始
`app.state.gateway_ready` 为 false，并把 HTTP route 的 service getter 指向
lifespan 内产生的 `AppRuntime.gateway_service`。真正的资源只在 FastAPI lifespan
内存在。

## Bootstrap 资源、恢复与生命周期

### 资源装配

[`bootstrap_application`](../../src/ruyi_agent/runtime/bootstrap.py) 的顺序是：

1. 校验传入的是 `RuntimeSettings`，按 typed backend settings 创建 backend runtime。
   backend 提供执行根目录和 skills 视图根目录；随后扫描 host workspace 的
   `SkillCatalog` 并创建 `SkillSyncer`。
2. 读取 settings 中的 storage、delegation、webhook 和节点参数。
3. 独立加载 Agent、Provider、permission 配置并建立 `PermissionPolicy`；加载 MCP
   raw dict、创建 `MCPRegistry`，调用一次 `refresh()`。
4. 为 checkpoint DB 确保父目录，打开 `AsyncSqliteSaver`；再创建
   `GatewayRouteStore`、`GatewayCommandStore`、`TaskStore`、`MailboxStore` 和
   `ReviewAuditStore`。`GatewayCommandStore`、`TaskStore` 和 `MailboxStore` 使用
   settings 的 task DB；channel 自己的 store 仍由 channel runner 管理。
5. 用共享 backend、checkpointer、MCP registry、typed Agent/Provider 输出和
   permission policy 构建 local worker specs 与 remote refs；构建失败的 local
   Agent 记入 `unavailable_agents`，而不是让所有 specs 消失。
6. 创建 `AgentControl` 和 `GatewayTaskModule`，最后 yield `AppRuntime`。只有
   bootstrap 成功 yield 后，FastAPI lifespan 才把 `gateway_ready` 设为 true，
   `/ready` 才报告 ready。

### 启动恢复

在构建 Gateway service 前，bootstrap 先调用
`worker_control.wake_pending_mailbox_tasks()` 恢复持久化 mailbox/task 工作，再
调用 `start_mailbox_recovery()` 开启受 runtime supervisor 管理的周期性恢复任务。
因此恢复和 maintenance 使用与正常 AgentControl 相同的 stores、checkpoint 和
backend；HTTP readiness 不会在这些步骤完成前提前打开。

### 关闭顺序

FastAPI lifespan 离开时先把 `gateway_ready` 置为 false，再离开 bootstrap context。
在 control、stores、checkpointer 和 backend 已成功创建的正常退出或晚期启动失败
路径上，关闭顺序是：

```text
gateway_ready = false
  -> AgentControl.close()
  -> ReviewAuditStore.close()
  -> MailboxStore.close()
  -> TaskStore.close()
  -> GatewayCommandStore.close()
  -> GatewayRouteStore.close()
  -> AsyncSqliteSaver.__aexit__
  -> BackendRuntime.close()
```

store 顺序来自 `ExitStack` 的反向释放顺序。`AgentControl.close()` 先停止
supervisor 管理的 recovery/maintenance，等待活动 run 的关闭宽限期，取消仍在运行
的工作并持久化中断收尾，然后才允许 stores 关闭。bootstrap 使用 `finally` 保证
backend 在已经创建的早期失败路径只关闭一次；尚未成功创建的后续资源不会被假定
存在。

## 错误隔离

| 故障范围 | 当前处理 | 对进程可用性的影响 |
| --- | --- | --- |
| 一个 MCP server 在 refresh 时失败 | `MCPRegistry` 按 server 捕获并记录该 server 的失败状态；其他 server 继续 refresh，bootstrap 可继续 | 不是系统性启动失败。失败 server 对应的配置不能提供给 Agent；若某个 Agent 随后因显式工具选择等自身构建错误失败，才按 Agent 粒度隔离 |
| 一个 local Agent 构建失败 | `build_all_local_worker_specs(..., unavailable_errors=...)` 捕获该 Agent 的异常并登记原因；健康 Agent、remote refs 和 Gateway service 仍可组装 | 该 Agent 对外标记 unavailable，指向它的任务返回 `agent_unavailable`；不拖垮其他 Agent |
| Agent/Provider/permission TOML 语法或 schema 失败 | 独立 loader/parser 的异常不在 bootstrap 中吞掉；缺失 runtime `ruyi.toml`、缺失必需 config 文件或 MCP raw 根结构错误同样停在边界 | bootstrap 不 yield，app 保持 not ready；runtime 配置阶段的错误由 CLI 报 status 2，bootstrap 阶段的 loader/基础设施异常沿 lifespan 传播；已创建的 backend 按 finally 清理 |
| 系统性 bootstrap/基础设施失败 | backend、skills、MCP registry/refresh 的非 server 级异常，checkpoint/store、AgentControl 或 Gateway assembly 异常向上传播 | 不产生可用 `AppRuntime`，不会打开 readiness；已经打开的资源按创建层级 unwind |

MCP 的单 server 失败和 Agent 的单项 unavailable 都依赖上表的显式边界；不能把
“某个 server 暂时失败”写成“整个 bootstrap 必然失败”，也不能把单个 Provider
credential 缺失写成全局配置文件损坏。

## 安全边界

- 默认 Gateway bearer token `dev-token` 只用于 loopback 调试。
  `create_bootstrapped_gateway_app` 在非 loopback host 发现该默认值时拒绝启动；对外
  监听必须设置非默认 token，并由部署环境负责 HTTPS 与网络访问控制。
- `RUYI_HOME`、workspace 和 storage 路径在配置边界解析、规范化并做平台检查；个人
  运行宜显式指定仓库外的绝对 `RUYI_HOME`，避免把运行数据混入源码 checkout。
- secret 的稳定边界是命名环境变量或未跟踪本地凭据；不要把真实值写入 starter、
  测试 fixture、日志、公开 DTO 或错误响应。动态读取失败应暴露变量名/不可用原因，
  不应暴露变量值。
- `backend.kind = "local"` 的 shell 以当前用户权限执行，文件工具虽受 typed
  workspace 映射约束但不提供 Daytona 的进程隔离；需要隔离执行时由 backend 配置
 选择 Daytona。

## 测试证据入口

这些是当前行为证据入口；本文变更本身只需做文档和轻量引用校验，不把完整测试当
作文档命令：

| 行为 | 测试 |
| --- | --- |
| Ruyi home/workspace 发现和 POSIX 路径 guard | [`test_ruyi_paths.py`](../../tests/unit/test_ruyi_paths.py) |
| runtime TOML 映射、优先级、64 alias、immutable settings、初始化和严格值校验 | [`test_runtime_settings.py`](../../tests/unit/test_runtime_settings.py) |
| CLI entrypoint、`--all` 过滤、`--init`/`--force` 和配置错误出口 | [`test_entrypoint_cli.py`](../../tests/unit/test_entrypoint_cli.py) |
| Agent/Provider/permission/MCP loader/parser，以及单 local Agent unavailable | [`test_config_loader.py`](../../tests/unit/test_config_loader.py) |
| MCP refresh 的单 server 失败隔离 | [`test_mcp_registry.py`](../../tests/unit/test_mcp_registry.py) |
| bootstrap 早期失败清理、control/store/checkpointer/backend 关闭顺序 | [`test_runtime_bootstrap_shutdown.py`](../../tests/unit/test_runtime_bootstrap_shutdown.py) |
| app readiness 在 lifespan 前、内、后的状态 | [`test_gateway_probes.py`](../../tests/unit/test_gateway_probes.py) |
| settings 传入 app，以及 loopback 判定 | [`test_app_runtime.py`](../../tests/unit/test_app_runtime.py) |
| unavailable Agent 仍让 Gateway 启动并返回稳定错误 | [`test_gateway_http_core.py`](../../tests/unit/test_gateway_http_core.py) |
| remote-ref token 缺失时在 transport dispatch 前失败 | [`test_a2a_client.py`](../../tests/unit/test_a2a_client.py) |

## 何时同步本文

下列用户可见或生命周期稳定行为改变时，应同步本文，并按影响核对
[`README.md`](../../README.md)、[`AGENTS.md`](../../AGENTS.md)、starter 模板、实现
和对应测试：

- Ruyi home/workspace 发现顺序、路径规范化、`--init` 模板集合或 `--force` 覆盖边界；
- `GatewayLaunchOverrides`/Paseo 的 Gateway 覆盖范围，或 `RUYI_RUNTIME_CONFIGURED=1`
  configured re-entry 对 `RUYI_WORKSPACE` 的回读规则；
- runtime TOML 字段、类型/范围/URL 约束、env alias 集合、优先级、projection 或
  `RuntimeSettings` 可变性；
- Agent/Provider/permission loader/parser 的输入文件、MCP raw boundary、动态
  secret 读取位置或错误隔离；
- CLI entrypoint 选择、channel 过滤、FastAPI readiness、bootstrap 资源装配、恢复
  时机、关闭顺序或单项 unavailable/systemic failure 的行为；
- bearer/path/凭据安全约束，或上述测试所证明的稳定契约。

当前仓库的同步方式是随这些行为变更人工核对 active docs、starter 和测试；本文不
引入额外的命令或不存在的自动 gate。提交前只需确认相对链接仍指向存在的文件并
通过 `git diff --check`。
