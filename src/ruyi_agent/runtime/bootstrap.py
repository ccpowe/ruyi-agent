from __future__ import annotations

import os
from contextlib import ExitStack, asynccontextmanager
from dataclasses import dataclass
from ipaddress import ip_address
from pathlib import Path

from fastapi import FastAPI
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

from ruyi_agent.config.runtime_settings import (
    RuntimeSettings,
    configure_runtime_environment,
)
from ruyi_agent.config.agent_models import AgentConfigs
from ruyi_agent.runtime.mailbox.service import AgentMailbox
from ruyi_agent.runtime.delegation.async_runtime import AgentControl
from ruyi_agent.integrations.backend.runtime import create_backend_runtime
from ruyi_agent.config.loader import (
    LocalWorkerSpec,
    build_all_local_worker_specs,
    build_all_remote_refs,
    load_agent_configs,
    load_llm_provider_configs,
    load_mcp_server_configs,
    load_permission_config,
)
from ruyi_agent.channels.http.routes import attach_gateway_routes
from ruyi_agent.gateway.tasks import GatewayTaskModule
from ruyi_agent.storage.gateway_command_store import GatewayCommandStore
from ruyi_agent.storage.gateway_route_store import GatewayRouteStore
from ruyi_agent.integrations.mcp.registry import MCPRegistry
from ruyi_agent.storage.task_store import TaskStore
from ruyi_agent.storage.mailbox_store import MailboxStore
from ruyi_agent.control_plane.permissions import PermissionPolicy
from ruyi_agent.storage.review_audit import ReviewAuditStore
from ruyi_agent.runtime.skills.catalog import SkillCatalog
from ruyi_agent.runtime.skills.sync import SkillSyncer
from ruyi_agent.runtime.skills.types import SkillEntry

# Legacy constants remain import-compatible for scripts; production bootstrap
# reads the typed RuntimeSettings instance.
DEFAULT_AGENT_NODE_ID = "local-dev"
DEFAULT_GATEWAY_TOKEN = "dev-token"
DEFAULT_GATEWAY_HOST = "127.0.0.1"
DEFAULT_GATEWAY_PORT = 8000
DEFAULT_CHECKPOINT_DB = "data/checkpoints.sqlite"
DEFAULT_GATEWAY_ROUTE_DB = "data/gateway_routes.sqlite"
DEFAULT_TASK_DB = "data/tasks.sqlite"
DEFAULT_REVIEW_AUDIT_DB = "data/review_audit.sqlite"
DEFAULT_MAX_DELEGATION_DEPTH = 3
DEFAULT_MAX_TASKS_PER_ROOT = 20


def _ensure_sqlite_parent_dir(path: str) -> None:
    if not path or path == ":memory:" or path.startswith("file:"):
        return
    parent = Path(path).expanduser().parent
    if parent != Path("."):
        parent.mkdir(parents=True, exist_ok=True)


def _is_loopback_gateway_host(host: str) -> bool:
    normalized = host.strip().strip("[]").lower()
    if normalized == "localhost":
        return True
    try:
        return ip_address(normalized).is_loopback
    except ValueError:
        return False


@dataclass(slots=True)
class AppRuntime:
    """保存 Gateway 和 Channel Adapter 共同依赖的长生命周期运行对象。"""

    main_agent_name: str
    agent_configs: AgentConfigs
    local_agent_specs: dict[str, LocalWorkerSpec]
    gateway_service: GatewayTaskModule
    worker_control: AgentControl
    checkpoint_db: str
    route_db: str
    task_db: str
    review_audit_store: ReviewAuditStore | None = None
    review_audit_db: str = ""
    permission_default_profile: str = ""
    skill_catalog: dict[str, SkillEntry] | None = None
    skill_syncer: SkillSyncer | None = None


@asynccontextmanager
async def bootstrap_application(settings: RuntimeSettings | None = None):
    """装配并持有当前进程内共享的应用运行时。

    Gateway 和 Channel Adapter 需要同一套运行对象：backend、
    MCP registry、checkpointer、worker 控制面、主 agent 和 Gateway service。
    这个上下文管理器把启动契约集中在一个地方，并负责在退出时关闭需要释放的资源。
    """

    configured_settings = settings
    if configured_settings is None:
        configured_settings = configure_runtime_environment()

    # A few external embedders historically replaced the configure hook with a
    # side-effect-only function. Keep that narrow hook compatibility without
    # bringing back the old raw/env settings path; production configuration
    # still produces one immutable typed instance and passes it through.
    using_legacy_configure_hook = configured_settings is None
    active_settings = configured_settings or RuntimeSettings.defaults()

    node_id = active_settings.runtime.agent_node_id

    # backend 决定 skills、memory 和执行状态所在的位置，所以要先创建 backend，
    # 再把声明式配置翻译成真正可运行的 agent spec。
    backend_runtime = (
        create_backend_runtime()
        if using_legacy_configure_hook
        else create_backend_runtime(active_settings)
    )
    try:
        home_dir = backend_runtime.home_dir
        skills_root = backend_runtime.skills_root
        agent_backend = backend_runtime.backend
        host_workspace_root = active_settings.backend.workspace
        skill_catalog = SkillCatalog(workspace_root=host_workspace_root).scan().skills
        skill_syncer = SkillSyncer(backend=agent_backend, views_root=skills_root)
        checkpoint_db = str(active_settings.storage.checkpoint_db)
        route_db = str(active_settings.storage.gateway_route_db)
        task_db = str(active_settings.storage.task_db)
        review_audit_db = str(active_settings.storage.review_audit_db)
        max_delegation_depth = active_settings.runtime.max_delegation_depth
        max_tasks_per_root = active_settings.runtime.max_tasks_per_root
        webhook_url = active_settings.runtime.a2a_webhook_url
        webhook_token = (
            active_settings.runtime.a2a_webhook_token
            or active_settings.gateway.bearer_token
            or DEFAULT_GATEWAY_TOKEN
        )
        # agent 和 MCP 配置在这里从声明式配置变成带 model、tools、memory、skills 的
        # runtime 对象。
        main_agent_name, agent_configs = load_agent_configs()
        llm_providers = load_llm_provider_configs()
        permission_config = load_permission_config()
        permission_policy = PermissionPolicy(permission_config)

        # 这几个mcp 是获取全局的mcp配置 进行出初始化？
        mcp_server_configs = load_mcp_server_configs()
        registry = MCPRegistry(mcp_server_configs)
        refresh_result = await registry.refresh()
        for status in refresh_result.server_statuses:
            if status.ok:
                print(f"[mcp] {status.server_name}: ok, tools={status.tool_count}")
            else:
                print(f"[mcp] {status.server_name}: failed, error={status.error}")

        # checkpointer 和 route store 是进程级状态对象。两个 AgentControl 共享同一个
        # checkpointer，让 task 执行状态和主 agent 对话状态落在同一条持久化边界内。
        _ensure_sqlite_parent_dir(checkpoint_db)
        async with AsyncSqliteSaver.from_conn_string(checkpoint_db) as checkpointer:
            # Keep every run dependency open until AgentControl has stopped
            # recovery, drained/cancelled active runs, and persisted interruption.
            with ExitStack() as stores:
                route_store = GatewayRouteStore(route_db)
                stores.callback(route_store.close)
                command_store = GatewayCommandStore(task_db)
                stores.callback(command_store.close)
                task_store = TaskStore(task_db)
                stores.callback(task_store.close)
                mailbox_store = MailboxStore(task_db)
                stores.callback(mailbox_store.close)
                mailbox = AgentMailbox(mailbox_store)
                review_audit_store = ReviewAuditStore(review_audit_db)
                stores.callback(review_audit_store.close)
                unavailable_agents: dict[str, str] = {}
                all_local_specs = await build_all_local_worker_specs(
                    agent_configs,
                    registry,
                    providers=llm_providers,
                    getenv=os.getenv,
                    home_dir=home_dir,
                    unavailable_errors=unavailable_agents,
                )
                all_remote_refs = await build_all_remote_refs(agent_configs)

                # worker_control 是内部调度控制面。它登记所有 local agent 和
                # remote_ref，并强制执行声明式 delegation_targets 访问范围。
                worker_control = AgentControl(
                    all_local_specs,
                    all_remote_refs,
                    checkpointer=checkpointer,
                    backend=agent_backend,
                    mailbox=mailbox,
                    webhook_url=webhook_url,
                    webhook_token=webhook_token,
                    max_delegation_depth=max_delegation_depth,
                    max_tasks_per_root=max_tasks_per_root,
                    node_id=node_id,
                    task_store=task_store,
                    permission_default_profile=permission_policy.default_profile,
                    permission_policy=permission_policy,
                    backend_kind=backend_runtime.kind,
                    workspace_root=home_dir,
                    review_audit_store=review_audit_store,
                    skill_catalog=skill_catalog,
                    skill_syncer=skill_syncer,
                    unavailable_agents=unavailable_agents,
                )
                try:
                    await worker_control.wake_pending_mailbox_tasks()
                    worker_control.start_mailbox_recovery()
                    gateway_service = GatewayTaskModule(
                        main_agent_name=main_agent_name,
                        agent_configs=agent_configs,
                        control=worker_control,
                        route_store=route_store,
                        command_store=command_store,
                        unavailable_agents=unavailable_agents,
                    )
                    print("configured local agents:", sorted(all_local_specs.keys()))
                    print("configured remote refs:", sorted(all_remote_refs.keys()))
                    if unavailable_agents:
                        print("unavailable local agents:", sorted(unavailable_agents))
                    print(
                        "configured public gateway agents:",
                        sorted(
                            name
                            for name, config in agent_configs.items()
                            if config.public
                        ),
                    )
                    print(
                        "configured delegation limits:",
                        f"max_depth={max_delegation_depth}",
                        f"max_tasks_per_root={max_tasks_per_root}",
                    )
                    print(f"configured backend: {backend_runtime.kind} ({home_dir})")
                    print("configured skills:", sorted(skill_catalog.keys()))
                    print(
                        "configured permission default profile:",
                        permission_policy.default_profile,
                    )

                    yield AppRuntime(
                        main_agent_name=main_agent_name,
                        agent_configs=agent_configs,
                        local_agent_specs=all_local_specs,
                        gateway_service=gateway_service,
                        worker_control=worker_control,
                        review_audit_store=review_audit_store,
                        checkpoint_db=checkpoint_db,
                        route_db=route_db,
                        task_db=task_db,
                        review_audit_db=review_audit_db,
                        permission_default_profile=permission_policy.default_profile,
                        skill_catalog=skill_catalog,
                        skill_syncer=skill_syncer,
                    )
                finally:
                    await worker_control.close()
    finally:
        backend_runtime.close()


def create_bootstrapped_gateway_app(
    settings: RuntimeSettings | None = None,
) -> FastAPI:
    """创建已经接入共享 runtime bootstrap 的 FastAPI 应用。

    Uvicorn 需要一个 app factory，但真正的 runtime 对象是异步资源，只应该在
    FastAPI lifespan 内存在。这个函数负责把两者接起来，并在路由层约定如何从
    request.app.state 取到当前可用的 Gateway Task Module。
    """

    active_settings = settings or configure_runtime_environment()
    bearer_token = active_settings.gateway.bearer_token
    gateway_host = active_settings.gateway.host

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        """在 HTTP 服务生命周期内打开 runtime，并把它暴露到 app.state。"""

        if bearer_token == DEFAULT_GATEWAY_TOKEN and not _is_loopback_gateway_host(
            gateway_host
        ):
            raise SystemExit(
                "Insecure configuration: set a non-default GATEWAY_BEARER_TOKEN "
                "before exposing Gateway outside localhost."
            )
        async with bootstrap_application(active_settings) as runtime:
            try:
                app.state.app_runtime = runtime
                app.state.gateway_service = runtime.gateway_service
                app.state.gateway_ready = True
                yield
            finally:
                # Stop accepting new traffic before runtime resources are closed.
                app.state.gateway_ready = False

    app = FastAPI(title="ruyi-agent Gateway", lifespan=lifespan)
    app.state.gateway_ready = False
    attach_gateway_routes(
        app,
        service_getter=lambda request: request.app.state.gateway_service,
        bearer_token=bearer_token,
        readiness_getter=lambda request: bool(request.app.state.gateway_ready),
    )
    return app
