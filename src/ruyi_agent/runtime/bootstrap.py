from __future__ import annotations

import os
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from ipaddress import ip_address
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

from ruyi_agent.config.runtime_settings import configure_runtime_environment
from ruyi_agent.runtime.mailbox.service import AgentMailbox
from ruyi_agent.runtime.delegation.async_runtime import AgentControl
from ruyi_agent.integrations.backend.runtime import create_backend_runtime
from ruyi_agent.config.loader import (
    LocalWorkerSpec,
    RemoteRef,
    build_all_local_worker_specs,
    build_all_remote_refs,
    load_agent_configs,
    load_llm_provider_configs,
    load_mcp_server_configs,
    load_permission_config,
    select_local_worker_specs_for_agent,
    select_remote_refs_for_agent,
)
from ruyi_agent.runtime.delegation.context import validate_node_id
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

# 默认的配置 代码优先会从env读取。
# 注意：dev-token 只适合本机开发；对外暴露 Gateway 必须显式设置强 GATEWAY_BEARER_TOKEN。
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


def _read_positive_int_env(name: str, default: int) -> int:
    raw_value = os.getenv(name)
    if raw_value is None or raw_value == "":
        return default
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a positive integer") from exc
    if value < 1:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _read_node_id_env() -> str:
    raw_value = os.getenv("AGENT_NODE_ID")
    if raw_value is None or not raw_value.strip():
        return DEFAULT_AGENT_NODE_ID
    return validate_node_id(raw_value.strip())


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
    agent_configs: dict[str, dict[str, Any]]
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


def _build_scoped_tool_factory(
    control_ref: dict[str, AgentControl],
    agent_name: str,
):
    """返回一个延迟工厂，用于在 worker_control 就绪后生成指定 agent 的委派工具列表。

    worker_control 在 all_local_specs 构建完成之后才创建，存在初始化顺序依赖。
    通过捕获可变字典 control_ref 而非直接捕获 worker_control，工厂函数在被调用时
    才读取 control_ref["control"]，此时 worker_control 已填入，从而绕开循环依赖。
    agent_name 固定在闭包中，确保每个 agent 只获得自己被授权的 scoped 工具集。
    """

    def build_tools() -> list[Any]:
        return control_ref["control"].build_tools_for(agent_name)

    return build_tools


def _attach_delegation_scopes_to_local_specs(
    *,
    agent_configs: dict[str, dict[str, Any]],
    all_local_specs: dict[str, LocalWorkerSpec],
    all_remote_refs: dict[str, RemoteRef],
    worker_control_ref: dict[str, AgentControl],
) -> dict[str, LocalWorkerSpec]:
    """为每个 local agent 注入其 delegation scope。

    按 agents.toml 中每个 agent 的 workers 配置，筛选出它可委派的本地 spec 和
    remote ref，并绑定一个延迟工厂 build_delegation_tools，让工具在 worker_control
    就绪后才生成。最后做一次二次遍历，确保嵌套 spec 对象与完整图保持一致。
    """
    specs: dict[str, LocalWorkerSpec] = {}
    for agent_name, base_spec in all_local_specs.items():
        delegation_local_specs = select_local_worker_specs_for_agent(
            agent_name,
            agent_configs,
            all_local_specs,
        )
        delegation_remote_refs = select_remote_refs_for_agent(
            agent_name,
            agent_configs,
            all_remote_refs,
        )
        has_delegation_targets = bool(delegation_local_specs or delegation_remote_refs)
        has_task_tools = (
            has_delegation_targets
            if base_spec.system_tools is None
            else bool(
                base_spec.system_tools
                & {
                    "spawn_agent",
                    "wait_agent",
                    "check_agent",
                    "send_input",
                    "cancel_agent",
                    "list_agents",
                }
            )
        )
        specs[agent_name] = replace(
            base_spec,
            delegation_local_worker_specs=(
                delegation_local_specs if has_delegation_targets else None
            ),
            delegation_remote_refs=(
                delegation_remote_refs if has_delegation_targets else None
            ),
            build_delegation_tools=(
                _build_scoped_tool_factory(worker_control_ref, agent_name)
                if has_task_tools
                else None
            ),
        )

    # Keep nested spec objects consistent with the fully attached graph. Runtime
    # execution resolves agents by name through AgentControl, but the prompt
    # middleware also carries these specs and should not expose stale child specs.
    for spec in specs.values():
        if spec.delegation_local_worker_specs:
            spec.delegation_local_worker_specs = {
                child_name: specs[child_name]
                for child_name in spec.delegation_local_worker_specs
            }
    return specs


@asynccontextmanager
async def bootstrap_application():
    """装配并持有当前进程内共享的应用运行时。

    Gateway 和 Channel Adapter 需要同一套运行对象：backend、
    MCP registry、checkpointer、worker 控制面、主 agent 和 Gateway service。
    这个上下文管理器把启动契约集中在一个地方，并负责在退出时关闭需要释放的资源。
    """

    configure_runtime_environment()

    node_id = _read_node_id_env()

    # backend 决定 skills、memory 和执行状态所在的位置，所以要先创建 backend，
    # 再把声明式配置翻译成真正可运行的 agent spec。
    backend_runtime = create_backend_runtime()
    home_dir = backend_runtime.home_dir
    skills_root = backend_runtime.skills_root
    agent_backend = backend_runtime.backend
    host_workspace_root = Path(os.getenv("LOCAL_BACKEND_ROOT", os.getcwd())).resolve()
    skill_catalog = SkillCatalog(workspace_root=host_workspace_root).scan().skills
    skill_syncer = SkillSyncer(backend=agent_backend, views_root=skills_root)
    checkpoint_db = os.getenv("CHECKPOINT_DB", DEFAULT_CHECKPOINT_DB)
    route_db = os.getenv("GATEWAY_ROUTE_DB", DEFAULT_GATEWAY_ROUTE_DB)
    task_db = os.getenv("TASK_DB", DEFAULT_TASK_DB)
    review_audit_db = os.getenv("REVIEW_AUDIT_DB", DEFAULT_REVIEW_AUDIT_DB)
    max_delegation_depth = _read_positive_int_env(
        "AGENT_MAX_DELEGATION_DEPTH",
        DEFAULT_MAX_DELEGATION_DEPTH,
    )
    max_tasks_per_root = _read_positive_int_env(
        "AGENT_MAX_TASKS_PER_ROOT",
        DEFAULT_MAX_TASKS_PER_ROOT,
    )
    webhook_url = os.getenv("A2A_WEBHOOK_URL")
    webhook_token = (
        os.getenv("A2A_WEBHOOK_TOKEN")
        or os.getenv("GATEWAY_BEARER_TOKEN")
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

    route_store: GatewayRouteStore | None = None
    command_store: GatewayCommandStore | None = None
    task_store: TaskStore | None = None
    mailbox_store: MailboxStore | None = None
    review_audit_store: ReviewAuditStore | None = None
    worker_control: AgentControl | None = None
    try:
        # checkpointer 和 route store 是进程级状态对象。两个 AgentControl 共享同一个
        # checkpointer，让 task 执行状态和主 agent 对话状态落在同一条持久化边界内。
        _ensure_sqlite_parent_dir(checkpoint_db)
        async with AsyncSqliteSaver.from_conn_string(checkpoint_db) as checkpointer:
            route_store = GatewayRouteStore(route_db)
            command_store = GatewayCommandStore(task_db)
            task_store = TaskStore(task_db)
            mailbox_store = MailboxStore(task_db)
            mailbox = AgentMailbox(mailbox_store)
            review_audit_store = ReviewAuditStore(review_audit_db)
            unavailable_agents: dict[str, str] = {}
            base_local_specs = await build_all_local_worker_specs(
                agent_configs,
                registry,
                providers=llm_providers,
                getenv=os.getenv,
                home_dir=home_dir,
                skills_root=skills_root,
                unavailable_errors=unavailable_agents,
            )
            all_remote_refs = await build_all_remote_refs(agent_configs)
            worker_control_ref: dict[str, AgentControl] = {}
            all_local_specs = _attach_delegation_scopes_to_local_specs(
                agent_configs=agent_configs,
                all_local_specs=base_local_specs,
                all_remote_refs=all_remote_refs,
                worker_control_ref=worker_control_ref,
            )

            # worker_control 是内部调度控制面。它登记所有 local agent 和 remote_ref，
            # 但每个 agent 实际能调用哪些 target 由自己的 scoped delegation tools 决定。
            worker_control = AgentControl(
                all_local_specs,  # 这些 local 的是已经注入国subagent的了。
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
            )
            worker_control_ref["control"] = worker_control
            await worker_control.wake_pending_mailbox_tasks()
            worker_control.start_mailbox_recovery()
            # Gateway Task Module 负责把任务路由到 public 本地 agent 或 remote_ref。
            # public 本地 agent 也在 worker_control 里执行，
            # 这样它的 delegation tools 与父 task 归属在同一个 TaskManager 内。
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
                    name for name, config in agent_configs.items() if config["public"]
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
        if worker_control is not None:
            await worker_control.close()
        if route_store is not None:
            route_store.close()
        if command_store is not None:
            command_store.close()
        if task_store is not None:
            task_store.close()
        if mailbox_store is not None:
            mailbox_store.close()
        if review_audit_store is not None:
            review_audit_store.close()
        backend_runtime.close()


def create_bootstrapped_gateway_app() -> FastAPI:
    """创建已经接入共享 runtime bootstrap 的 FastAPI 应用。

    Uvicorn 需要一个 app factory，但真正的 runtime 对象是异步资源，只应该在
    FastAPI lifespan 内存在。这个函数负责把两者接起来，并在路由层约定如何从
    request.app.state 取到当前可用的 Gateway Task Module。
    """

    configure_runtime_environment()
    bearer_token = os.getenv("GATEWAY_BEARER_TOKEN") or DEFAULT_GATEWAY_TOKEN
    gateway_host = os.getenv("GATEWAY_HOST", DEFAULT_GATEWAY_HOST)

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
        async with bootstrap_application() as runtime:
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
