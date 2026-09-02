# 运行时配置与 Bootstrap

## 范围与所有权

本文说明一个 `ruyi` 进程从配置发现到 Gateway runtime 可用的装配边界。配置层
拥有 Ruyi home/workspace 路径发现、runtime TOML 的严格解析、有限环境变量兼容面、
不可变 `RuntimeSettings`，以及 Agent、Provider、permission 配置的独立
loader/parser；MCP 配置保留为 raw connection dict。bootstrap 是进程级
composition/lifecycle root：它消费 typed settings，装配 backend、配置投影、MCP
registry、checkpoint、SQLite stores、AgentControl 和 Gateway service，并负责启动
恢复与有序关闭。

配置层不启动 HTTP、channel 或 Agent turn，也不让下游重新解析原始 runtime TOML。
bootstrap 不定义 Agent/Provider/permission 的文件 schema，也不拥有 channel 自己的
session、receipt、delivery store；MCP 的运行期工具内容和其他任务协议在本文之外。

## 入口与依赖方向

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
[`runtime/bootstrap.py`](../../src/ruyi_agent/runtime/bootstrap.py)。CLI 把
`RuntimeSettings` 交给 app/bootstrap；backend factory、loader、runtime 和 HTTP
路由消费各自的 typed 对象。`--telegram`、`--feishu`、`--all` 与 Gateway 共用
CLI `TaskGroup`，adapter 仍只通过 Gateway HTTP endpoint 工作。

`create_app()` 在没有传入 settings 时配置一次环境；显式传入 settings 的调用方直接
使用该对象，不复制配置发现逻辑。

## Ruyi home、workspace 与初始化

[`resolve_ruyi_paths`](../../src/ruyi_agent/config/paths.py) 产生规范的
`ruyi_home`、`config_dir`、`data_dir`、`skills_dir` 和 `workspace`。Ruyi home 的
选择顺序为：非空 `RUYI_HOME`（先 `expanduser` 再绝对化）、当前目录中已有
`ruyi.toml` 或 `config/` 的 `.ruyi_agent`、用户目录的 `~/.ruyi_agent`。只检查
当前目录，不向父目录搜索。workspace 的优先级为 CLI `--workspace`、
`[backend].workspace`、`RUYI_WORKSPACE`，最后是当前目录；相对 storage 路径以
`RUYI_HOME` 为基准。POSIX 会拒绝 Windows 风格的 `RUYI_HOME`、`RUYI_WORKSPACE`
和显式 workspace 参数；runtime TOML 的 `[backend].workspace` 不在这个 guard
范围内。

`ruyi --init` 在选定 home 创建 `config/`、`data/`、`skills/`，复制随包模板
`ruyi.toml`、`agents.toml(.example)`、`llm_providers.toml`、`mcp_servers.toml`
和 `permissions.toml`。已有且非空的固定模板保留，不存在或空文件才补齐；
`--force` 只能与 `--init` 一起使用，且只覆盖这些固定模板，不删除数据库、runtime
数据、skills、日志、环境文件或凭据。初始化结束即返回，不启动 Gateway 或 adapter。
非初始化调用必须已有选定 home 的 `ruyi.toml`，不会隐式创建用户 home。模板事实
入口是 [`templates/ruyi_home`](../../src/ruyi_agent/templates/ruyi_home/)，用户示例
见 [`README.md`](../../README.md)。

## Runtime TOML 与 typed 配置

`configure_runtime_environment()` 发现路径、初始化或检查 `ruyi.toml`，再由
[`load_runtime_settings`](../../src/ruyi_agent/config/runtime_settings.py) 一次读取
并严格校验顶层/嵌套 table 的字段、类型、范围、host/URL 和路径。结果是 frozen
typed sections：`paths`、`credentials`、`backend`、`gateway`、`storage`、`runtime`、
`channels`、`langsmith`；嵌套 dataclass 也 frozen，列表投影为 tuple。

环境兼容面是代码列明的 table-scoped alias 集合
[`TABLE_SCOPED_TOML_ALIASES`](../../src/ruyi_agent/config/runtime_settings.py)，
当前 64 个名称分布在 model credentials、local/Daytona backend、runtime、Telegram、
Feishu、LangSmith、storage 各字段。允许回退的字段按
TOML 字段 > 同 table alias 字段 > 对应进程环境 alias > 默认值选择；空字符串/空列表
通常表示未设置。`gateway.*`、`backend.kind` 等没有同名大写 projection 作为输入，
storage alias 也受字段级限制。未知字段或 alias 在配置边界失败。

[`apply_runtime_settings_to_env`](../../src/ruyi_agent/config/runtime_settings.py) 会
把 typed 值投影给兼容消费者（例如 `BACKEND_KIND`、`LOCAL_BACKEND_ROOT`、
`GATEWAY_*`、受支持的 alias 和规范路径），并设置 `RUYI_RUNTIME_CONFIGURED=1`。
这些 projection 是输出兼容面，不是下一轮 TOML 的权威来源。唯一的 re-entry 例外
是：再次 configure 时若已有该标志且没有新的 workspace 参数，会回读 projection
出的 `RUYI_WORKSPACE` 以保留首次显式选择。`GatewayLaunchOverrides` 只在严格
TOML 校验后覆盖 Gateway `host`、`port`、`base_url`，保留 TOML bearer token；
[`paseo.json`](../../paseo.json) 的 Gateway service script 使用此入口。

四类声明由独立边界解析：

| 声明 | loader/parser | 输出与边界 |
| --- | --- | --- |
| Agent | `load_agent_configs` + `agent_parser.py` | 已校验的 local/remote typed declaration，检查 key/name、必填字段、worker 图和 remote auth/URL |
| Provider | `load_llm_provider_configs` + `provider_parser.py` | immutable `dict[str, LLMProviderSpec]`，检查类型、地址、`api_key_env` 和 init kwargs |
| permission | `load_permission_config` + `permission_parser.py` | `PermissionConfig`，检查 profile、tool policy 和 execute rule |
| MCP | `load_mcp_server_configs` | `dict[str, dict[str, Any]]` raw mapping，原样交给 `MCPRegistry` |

Provider 的 `api_key_env` 在构建模型时由 integrations 读取；remote-ref 只保留
`auth.token_env`，请求发送前才读取。缺少变量只使对应 Provider/remote Agent
unavailable，不把 secret 写入 task/config projection、事件或错误详情。local Agent
构造失败进入 `unavailable_agents` 前会生成有界单行摘要：保留 exception class 与
非敏感原因，并只精确替换该 Agent 已配置 Provider key，不扫描整个环境。starter 和
跟踪配置只提供空 credential 字段或变量名。

## CLI、bootstrap、恢复与生命周期

非初始化 CLI 必须选择 `--gateway`（或旧位置参数 `gateway`）、`--telegram`、
`--feishu` 或 `--all`；`--force` 没有 `--init` 时被 parser 拒绝。配置阶段错误
以 status 2 退出。`--all` 先包含 Gateway，再过滤没有必要凭据的 adapter（Telegram
需 bot token，Feishu 需 app id/app secret；都未配置时仍启动 Gateway）。各参与者
共享 immutable settings，但 adapter 通过 Gateway endpoint 使用公开边界。

FastAPI app 初始 `app.state.gateway_ready=false`，route 的 service getter 指向
lifespan 内创建的 `AppRuntime.gateway_service`；只有 bootstrap 成功 yield 后才设为
true，`/ready` 才报告 ready。

bootstrap 的资源顺序是：

1. 按 typed backend settings 创建 backend，扫描 host workspace 的 `SkillCatalog`
   并建立 backend skill view；
2. 独立加载 Agent/Provider/permission，创建 policy，加载 MCP raw dict 并执行一次
   `refresh()`；单个 MCP server 刷新失败按 server 隔离，failed status（以及 bootstrap
   stdout）只使用 registry 已生成的安全异常摘要；
3. 打开 checkpoint DB，创建 route、command、Task、mailbox、review-audit stores；
4. 组装 local worker specs 和 remote refs。单个 local Agent 构建失败记录在
   `unavailable_agents`，不会使其他 specs 消失；
5. 创建 `AgentControl`、`GatewayTaskModule`，唤醒 pending mailbox task，启动
   supervisor 管理的 recovery，再 yield `AppRuntime`。

启动恢复与正常 AgentControl 共用 stores、checkpoint 和 backend；readiness 不会在
恢复/maintenance 设定前提前打开。关闭先撤销 readiness，再按依赖逆序释放：

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

`AgentControl.close()` 先停止 recovery/maintenance，等待活动 run 的宽限期，取消
剩余工作并持久化中断收尾；store 顺序来自 `ExitStack` 反向释放。早期失败只 unwind
已创建的资源，backend 在 `finally` 中最多关闭一次。

故障隔离保持以下边界：MCP 单 server 失败不等于 bootstrap 整体失败；单 local
Agent 构建失败只使该 Agent unavailable，指向它的任务得到 `agent_unavailable`；
Agent/Provider/permission schema、缺少必需文件、MCP raw 根结构或基础设施失败会
阻止 bootstrap yield，app 保持 not ready。非 server 级 backend、stores、AgentControl
或 Gateway assembly 异常向上传播并按已创建层级清理。

## 安全边界

- 默认 Gateway bearer `dev-token` 只用于 loopback；非 loopback 启动必须使用非默认
  token，并由部署环境负责 HTTPS 与网络访问控制。
- `RUYI_HOME`、workspace 和 storage 在配置边界规范化并做平台检查；个人运行宜用
  仓库外的绝对 `RUYI_HOME`。真实 Provider、channel、remote、Gateway、Daytona
  secret 只能来自命名环境变量或未跟踪凭据，不能进入 starter、DTO 或这里定义的 public
  error summaries。安全摘要只处理进入该边界的异常文本，不扫描环境、历史数据或成功输出。
- `backend.kind=local` 的文件工具受 typed workspace 映射约束，但 shell 仍以当前
  用户权限运行；需要进程/文件系统隔离时选择 Daytona。

## 测试证据入口

行为证据的 canonical 入口为：

- [路径发现](../../tests/unit/test_ruyi_paths.py)、[runtime settings](../../tests/unit/test_runtime_settings.py)：home/workspace precedence、Windows-style guard、strict TOML、alias、immutable settings 与初始化边界。
- [CLI 与配置 loader](../../tests/unit/test_entrypoint_cli.py)、[config loader](../../tests/unit/test_config_loader.py)：entrypoint/`--all`/`--init` 及 Agent、Provider、permission、MCP 和 unavailable Agent。
- [MCP registry 与 A2A client](../../tests/unit/test_mcp_registry.py)、[A2A client](../../tests/unit/test_a2a_client.py)：单 server refresh 隔离和 remote-ref token boundary。
- [bootstrap shutdown](../../tests/unit/test_runtime_bootstrap_shutdown.py)、[app runtime](../../tests/unit/test_app_runtime.py)：资源 unwind、关闭顺序、settings 传递和 early failure。
- [Gateway probes](../../tests/unit/test_gateway_probes.py)、[Gateway HTTP core](../../tests/unit/test_gateway_http_core.py)：readiness 生命周期与 unavailable Agent 的公开错误。

## 何时同步本文

仅在以下稳定边界变化时同步：

- home/workspace 发现、初始化模板、force 覆盖、runtime 字段/alias/优先级、projection、
  `RuntimeSettings` 可变性或 Gateway launch override；
- Agent/Provider/permission/MCP loader 的 schema、secret 读取位置或单项/系统性错误隔离；
- CLI entrypoint/channel filtering、bootstrap 资源 wiring、readiness、恢复、关闭顺序
  或跨 store 的 lifecycle/recovery 保证；
- bearer、路径、凭据或 local/Daytona trust boundary。

文档链接和命令以当前实现、starter、README、CI 与 AGENTS 为准。
