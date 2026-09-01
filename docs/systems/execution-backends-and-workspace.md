# Execution backend 与 workspace 边界

本文记录 execution backend、workspace、attachment ingress 和 artifact 文件流之间的
当前边界。`host workspace`、`backend path` 和 `Task` 是不同层次：前者是配置选出的
绝对主机 `Path`，后者是 backend 命名空间中的 POSIX 绝对路径，Task 是 runtime/Gateway
跟踪的身份；artifact bytes 由 backend 持有，durable metadata 附着在 Task 上。

## 负责范围

| 边界 | 拥有的行为 |
| --- | --- |
| config paths/runtime settings | 发现 `RUYI_HOME`、解析 host workspace、规范化路径、校验 backend kind、产出 immutable settings |
| `BackendRuntime` | 创建 local/Daytona backend，暴露 backend/home/skills 路径，关闭持有的 Daytona sandbox |
| `BackendProtocol`/`CompositeBackend` | 文件读写、搜索、批量 upload/download 和 default backend 的 shell execute contract |
| bootstrap | 组合 backend、Agent、MCP、checkpoint、stores、AgentControl/Gateway，并管理生命周期 |
| Gateway attachment/artifact service | attachment 解码/命名/inbox upload；manifest/path 校验与按需 bytes download |
| artifact middleware/local executor/TaskStore | tracked Task 的 path/bytes 检查、manifest 生成、登记和 durable metadata |

这些边界不互相替代：本文不把 HTTP auth、Task route/state、tool permission、channel
外部媒体下载、对象存储或 host ACL 归给 backend；local shell 也能越过文件工具的
virtual root。

## 入口与数据流

```text
CLI -> RuntimeSettings -> bootstrap -> BackendRuntime/AgentControl/Gateway
                                      ├─ Agent middleware -> backend files/execute
HTTP task route -> Gateway attachment service -> AgentControl -> backend upload
HTTP artifact route -> Gateway artifact service -> Task manifest -> backend download
```

bootstrap 把同一 backend 传给 `AgentControl` 和 Agent middleware；Telegram/Feishu 只
通过 Gateway HTTP。local create/input 的 attachment 先走本地 inbox，remote-ref route
把 attachment DTO 转发给上游 A2A，不使用本地 upload。`LocalTaskExecutor.compile_agent`
把 backend、backend workspace root 和 `register_artifact` callback 传给 runtime agent。

## Backend contract

`BackendProtocol` 的当前文件面包括 `ls`、`read`、`grep`、`glob`、`write`、`edit`、
`upload_files`、`download_files` 及 async wrapper；批量结果按输入顺序返回。已翻译的
文件失败可用 `FileUploadResponse`/`FileDownloadResponse.error` 表示
`file_not_found`、`permission_denied`、`is_directory`、`invalid_path` 等，成功 bytes
在 `content`；path validation、普通文件方法和部分 SDK/I/O 错误仍可能抛异常。

实现 shell 的 backend 另满足 `SandboxBackendProtocol`：`id`、
`execute`/`aexecute` 和 `ExecuteResponse(output, exit_code, truncated)`。这是可执行
接口，不表示实现拥有相同隔离级别。

`BackendRuntime.backend` 是 `CompositeBackend`：`default` 为 local 的
`RuyiLocalShellBackend` 或 Daytona 的 `AutoStartDaytonaSandbox`，当前 `routes={}`，
`artifacts_root=home_dir`。文件操作按最长 path prefix 路由，未命中落到 default；
批量操作按 backend 分组后恢复输入顺序。`execute` 永远只委托 default，不按 command
或文件路径选路，因此 CompositeBackend 是 façade，不是 permission/approval/sandbox。

`BackendRuntime` 暴露 `kind`（`local`/`daytona`）、`backend`、`home_dir`、`skills_root`
和可选 `_sandbox`；上层只调用 `create_backend_runtime(settings)`，不自行创建 SDK
或 local backend。

## Host 与 backend namespace

host workspace 的选择顺序为 CLI override、`[backend].workspace`、`RUYI_WORKSPACE`、
路径发现得到的当前目录；解析为绝对 `settings.backend.workspace`。兼容投影
`LOCAL_BACKEND_ROOT` 不会反向覆盖 TOML。POSIX 的 Windows-style guard 覆盖
`RUYI_HOME`、`RUYI_WORKSPACE` 和 CLI/调用方 workspace 参数，不覆盖 TOML 的
`[backend].workspace`。

bootstrap 用 host workspace 扫描 `SkillCatalog`，再由 `SkillSyncer` upload 到 backend
skill view；Agent 只看 view，不直接暴露整个 host workspace。

| backend | backend path 与根 |
| --- | --- |
| local | `/report.txt` 在 virtual root 下映射为 `<host workspace>/report.txt`；`home_dir=/`，skill view 为 `/.ruyi_agent/runtime/skill-views` |
| Daytona | `/home/.../report.txt` 由 sandbox filesystem 处理；当前不把 host workspace 挂载到 sandbox；`home_dir=sandbox.get_user_home_dir()`，skill view 在其下 |

path 检查分层进行：config 负责来源/绝对化/平台风格；local filesystem virtual mode
负责 `..`/`~`/越界；Gateway attachment/artifact service 及 artifact middleware 各自
负责 backend path 的 absolute/no-`..`/workspace confinement；backend 最终执行操作。
这些不是完整 host ACL，local shell 不受 virtual file path 检查约束。

## Local backend

local factory 从 typed settings 读取 workspace、timeout、`max_output_bytes`、
`inherit_env`，固定 `root_dir=host workspace`、`virtual_mode=True`，作为 Composite
default，`home_dir=/`。

文件工具的 `/nested/example.txt` 映射到 host workspace 内对应文件，traversal 和
越界被拒绝；这只定义 path semantics，不改变进程 OS 权限。`execute` 使用
`subprocess.run(..., shell=True)`，以 root 为 child `cwd`，所以绝对路径、`..`、网络、
进程创建等不由 virtual root 拦截，命令继承宿主用户权限。`inherit_env=true`（默认）
复制当前 `os.environ`；false 时不额外注入 env，不能把 env 继承当作 secret 隔离。
默认 timeout 为 120 秒、输出上限 100,000 bytes；超时 exit code 124，过长标记
`truncated`，输出按 bytes 读取并尝试 UTF-8/locale 解码。

选择 local 表示信任 Agent/command 输入并接受宿主权限边界；tool permission/approval
由其他边界负责。

## Daytona backend

### 创建、复用与 enum/start 风险

`_create_sandbox` 用 typed `api_key`、`api_url`、`target`、`sandbox_name` 创建
Daytona client：先 `get(sandbox_name)`；找到但 `str(sandbox.state) != "STARTED"` 就
调用 `start()`，所以 SDK 返回 enum 时，即使语义上已 STARTED，字符串比较也可能再次
start。这是当前 reuse/start 风险，不是可靠的 started skip 保证。只有
`DaytonaNotFoundError` 才走 `create`（`name` 固定、`language="python"`）；get 的
其它错误继续向上，不静默创建另一个 sandbox。创建后取得 user home，建立
`AutoStartDaytonaSandbox`/CompositeBackend；不挂载 host workspace，也不注入 host env。

### auto-start 与隔离

`AutoStartDaytonaSandbox` 只包装 `execute`、`upload_files`、`download_files`，普通
`ls/read/grep/glob/write/edit` 沿继承实现。上述三类操作在前置 `_ensure_ready` 中
每 2 秒最多 refresh 一次：`STARTED` 继续，`STARTING` wait，`STOPPED` start，
recoverable `ERROR` recover；动作后再 refresh。其它 state 或准备异常变为可读
unavailable：execute 返回 exit code 1，upload 返回 `invalid_path`，download 返回
`file_not_found`；wrapper 未覆盖的方法和未翻译 SDK 错误仍可能抛异常。`close()` 的
stop refresh 不受 2 秒 TTL 约束。

Daytona shell、文件和 backend 读取的 artifact bytes 在 sandbox 中，与 Gateway host
隔离；Daytona 自身的 network/org/resource policy 不由本文定义。

`BackendRuntime.close()` 先 refresh；state 不在 `STOPPED`、`STOPPING`、`ARCHIVED`、
`DESTROYED` 时调用 stop。特定 “Sandbox is not started” race 可忽略，其它
`DaytonaError` 继续抛出；local close 是 no-op，不停止 host process。

## Bootstrap 生命周期

bootstrap 先创建 backend，再扫描 host skills、加载 Agent/Provider/permission/MCP 并
refresh registry，打开 checkpoint 和 route/command/Task/mailbox/review stores，创建
AgentControl、恢复 pending mailbox task、启动 recovery，最后创建 Gateway 并开启
readiness。关闭先把 readiness 置 false，再按依赖逆序停止 control、关闭 stores、
checkpointer，最后关闭 backend/Daytona；yield 前失败只清理已创建资源，backend 最多
关闭一次。

## Attachment ingress

`AttachmentInput` 要求非空 `name`（最多 255 字符）和 `data_base64`，`content_type` 可选
（最多 255），`kind` 为 image/document/audio/video/file；一个 TaskInput 最多 10 个
attachment，且必须有 text 或 attachment。local service：

1. 将 `\\` 当 `/`，取 basename、trim，清理为字母/数字/`.`/`-`/`_`；空名/`.`/`..` 或
   清理后为空回退 `attachment`。
2. `base64.b64decode(validate=True)`，按 decoded bytes 执行默认 20 MiB limit。
3. 写入 `<backend root>/inbox/gateway/<batch_id>/<index:02d>-<sanitized-name>`；create
   的 batch 是 Gateway Task id，有 key 的 input 是 command id，无 key input 是 UUID，
   且 service 再校验 normalized workspace/inbox 边界。
4. `AgentControl.upload_files` 返回数量必须匹配且每项无 error；不完整/失败为
   `attachment_upload_failed`。没有成功 bytes 的 rollback/delete 契约，失败后不能假定
   inbox 已清空。
5. 将 name/path/content_type/kind 写入 Agent 输入和 route metadata，不保存 base64。

remote-ref create/input 不走本地 upload，而把 DTO 形式 attachment 转发给上游。

## Artifact publishing 与 download

只有 tracked Task 且 `LocalTaskExecutor.compile_agent` 提供 callback 时才启用
`ArtifactPublishingMiddleware`。`publish_artifact` 从 runtime config/metadata 取得
非空 task id，要求 backend namespace 的绝对 POSIX path：拒绝反斜杠、Windows drive、
相对路径和 `..`，并要求位于 middleware workspace root。middleware 下载当前 bytes，
backend error/空结果视为 `file_not_found`，默认超过 50 MiB 为 `file_too_large`；构造
安全 basename、caption、MIME、实际 size 后，由 local executor 生成 `art_<uuid>`，
带当前 run_count 写入 `PublishedArtifact` manifest。登记失败为
`artifact_registration_failed`。

bytes 与 metadata 分离：bytes 留在 backend，manifest（artifact id/path/name/caption/
content_type/size/run_count）在 `TaskRecord.artifacts`、生产 TaskStore 的
`artifacts_json` 中持久化；Task event 可记录登记。重启先恢复 manifest，能否再次下载
取决于 backend 和文件是否仍在。manifest size 不是 content hash，Gateway 不保证当前
文件与发布时 bytes 强一致。

`GatewayArtifactService` 的 path download 先做 workspace check；task-scoped download
先按 public Task route 和 manifest 查 artifact，再读 path。两者在 backend read 前都
重新验证 absolute normalized path；缺失/空/error 为 `artifact_not_found`，当前 bytes
超过 server limit 为 `artifact_too_large`。下载不改写 manifest；HTTP framing/auth 属于
HTTP 层。

## 状态、错误与安全

backend 生命周期是 typed settings → runtime created → backend ready → file/execute/
upload/download →（Daytona 按需 refresh/start/wait/recover）→ bootstrap shutdown。
配置错误在 settings 边界失败；local bulk errors 可返回已翻译 response，普通文件和
部分 SDK 错误可抛出；Daytona 非可恢复状态需外部修复/重建。attachment 的 invalid
base64/size/path/upload error、artifact publish/download 的 tracked Task/path/file/size/
manifest error 各自在其 service boundary 处理，不把 rollback 或绝对去重写成保证。

local 是宿主信任模型：virtual root 只约束文件工具，shell 仍有宿主权限和可选 env
继承；不应用于不信任代码执行。Daytona 的隔离对象是 sandbox，host workspace 不自动
挂载，Daytona API key/Provider/Gateway secret 只能在 config/integration 边界读取，不
进入 Task metadata、manifest、wire DTO 或普通错误。HTTP bearer、Team Console、tool
permission、任意 host ACL 和对象存储由其他文档/边界负责。

## 测试证据入口

行为证据的 canonical 入口为：

- [backend runtime](../../tests/unit/test_backend_runtime.py)、[runtime settings](../../tests/unit/test_runtime_settings.py)、[paths](../../tests/unit/test_ruyi_paths.py)：local/Daytona factory、Composite/home/skill roots、workspace precedence、Windows-style guard 与 path mapping。
- [Gateway HTTP core](../../tests/unit/test_gateway_http_core.py)：attachment name/base64/inbox/size/upload boundary，以及 direct/task artifact download/path security。
- [artifact middleware/local executor](../../tests/unit/test_artifact_publishing_middleware.py)、[local executor](../../tests/unit/test_async_subagent_local_executor.py)、[TaskStore](../../tests/unit/test_task_store.py)：tracked Task、backend path/bytes/MIME metadata、run manifest 和 metadata persistence。
- [bootstrap shutdown](../../tests/unit/test_runtime_bootstrap_shutdown.py)：bootstrap wiring、early failure cleanup、control/stores/checkpointer/backend close order。
## 何时同步本文

仅在以下稳定边界变化时同步：

- backend kind、`BackendRuntime`/`CompositeBackend` 的 file/execute routing、local virtual
  root/shell trust 或 Daytona get/create/start/auto-start/recover/stop lifecycle；
- host/backend namespace、workspace precedence/guard、skill source-to-view 或 path
  validation ownership；
- AttachmentInput contract、name/base64/decoded-size/inbox/upload/remote-ref boundary；
- artifact tracked Task/path/bytes/manifest persistence、Gateway re-check/download 或
  bootstrap readiness/close/recovery guarantee；
- backend secret and isolation trust boundary。

不要把未实现的对象存储、通用 host ACL 或 rollback/exactly-once 设计写入本文。
