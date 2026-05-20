from __future__ import annotations

import os
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

from ruyi_agent.runtime.mailbox.service import AgentMailbox
from ruyi_agent.runtime.delegation.async_runtime import AgentControl
from ruyi_agent.integrations.backend.runtime import create_backend_runtime
from ruyi_agent.config.loader import (
    LocalWorkerSpec,
    RemoteRef,
    build_all_local_worker_specs,
    build_all_remote_refs,
    build_public_remote_refs,
    load_agent_configs,
    load_llm_provider_configs,
    load_mcp_server_configs,
    load_permission_config,
    select_local_worker_specs_for_agent,
    select_public_local_worker_specs,
    select_remote_refs_for_agent,
)
from ruyi_agent.runtime.delegation.context import validate_node_id
from ruyi_agent.channels.http.api import (
    AgentControlGatewayRuntime,
    GatewayService,
    attach_gateway_routes,
)
from ruyi_agent.storage.gateway_route_store import GatewayRouteStore
from ruyi_agent.integrations.mcp.registry import MCPRegistry
from ruyi_agent.runtime.agent_factory import create_runtime_agent
from ruyi_agent.storage.task_store import TaskStore
from ruyi_agent.control_plane.permissions import PermissionPolicy
from ruyi_agent.storage.review_audit import ReviewAuditStore

# 默认的配置 代码优先会从env读取
# 注意：不要在生产环境依赖默认 token；生产必须显式设置 GATEWAY_BEARER_TOKEN。
DEFAULT_GATEWAY_TOKEN = "dev-token"
DEFAULT_GATEWAY_HOST = "127.0.0.1"
DEFAULT_GATEWAY_PORT = 8000
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


def _read_required_node_id_env() -> str:
    raw_value = os.getenv("AGENT_NODE_ID")
    if raw_value is None or not raw_value.strip():
        raise ValueError(
            "AGENT_NODE_ID must be set to a stable node identifier for Gateway "
            "delegation loop detection."
        )
    return validate_node_id(raw_value.strip())


@dataclass(slots=True)
class AppRuntime:
    """保存 TUI、Gateway 和 channel adapter 共同依赖的长生命周期运行对象。"""

    main_agent_name: str
    agent_configs: dict[str, dict[str, Any]]
    local_agent_specs: dict[str, LocalWorkerSpec]
    gateway_service: GatewayService
    worker_control: AgentControl
    gateway_control: AgentControl
    checkpoint_db: str
    route_db: str
    task_db: str
    _build_local_agent: Callable[[str], Any]
    _local_agent_cache: dict[str, Any]
    review_audit_store: ReviewAuditStore | None = None
    review_audit_db: str = ""
    permission_default_profile: str = ""

    def list_local_agent_names(self) -> list[str]:
        return sorted(self.local_agent_specs)

    def get_default_local_agent(self) -> Any:
        return self.get_local_agent(self.main_agent_name)

    def get_local_agent(self, agent_name: str) -> Any:
        if agent_name not in self.agent_configs:
            raise ValueError(f"Unknown agent: {agent_name}")
        if agent_name not in self.local_agent_specs:
            kind = self.agent_configs[agent_name]["kind"]
            raise ValueError(
                f"Agent '{agent_name}' has kind='{kind}' and cannot be streamed locally."
            )
        agent = self._local_agent_cache.get(agent_name)
        if agent is None:
            agent = self._build_local_agent(agent_name)
            self._local_agent_cache[agent_name] = agent
        return agent

    def resolve_root_permission_profile(self, agent_name: str) -> str:
        spec = self.local_agent_specs[agent_name]
        return spec.permission_profile or self.permission_default_profile


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
                if has_delegation_targets
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

    TUI、本地交互和 Gateway/channel adapter 都需要同一套运行对象：backend、
    MCP registry、checkpointer、worker 控制面、主 agent 和 Gateway service。
    这个上下文管理器把启动契约集中在一个地方，并负责在退出时关闭需要释放的资源。
    """

    load_dotenv()

    node_id = _read_required_node_id_env()

    # backend 决定 skills、memory 和执行状态所在的位置，所以要先创建 backend，
    # 再把声明式配置翻译成真正可运行的 agent spec。
    backend_runtime = create_backend_runtime()
    home_dir = backend_runtime.home_dir
    skills_root = backend_runtime.skills_root
    agent_backend = backend_runtime.backend
    checkpoint_db = os.getenv("CHECKPOINT_DB", "checkpoints.sqlite")
    route_db = os.getenv("GATEWAY_ROUTE_DB", "gateway_routes.sqlite")
    task_db = os.getenv("TASK_DB", "tasks.sqlite")
    review_audit_db = os.getenv("REVIEW_AUDIT_DB", "review_audit.sqlite")
    max_delegation_depth = _read_positive_int_env(
        "AGENT_MAX_DELEGATION_DEPTH",
        DEFAULT_MAX_DELEGATION_DEPTH,
    )
    max_tasks_per_root = _read_positive_int_env(
        "AGENT_MAX_TASKS_PER_ROOT",
        DEFAULT_MAX_TASKS_PER_ROOT,
    )
    webhook_url = os.getenv("A2A_WEBHOOK_URL")
    webhook_token = os.getenv(
        "A2A_WEBHOOK_TOKEN",
        os.getenv("GATEWAY_BEARER_TOKEN", DEFAULT_GATEWAY_TOKEN),
    )
    mailbox = AgentMailbox()

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
    task_store: TaskStore | None = None
    review_audit_store: ReviewAuditStore | None = None
    try:
        # checkpointer 和 route store 是进程级状态对象。两个 AgentControl 共享同一个
        # checkpointer，让 task 执行状态和主 agent 对话状态落在同一条持久化边界内。
        async with AsyncSqliteSaver.from_conn_string(checkpoint_db) as checkpointer:
            route_store = GatewayRouteStore(route_db)
            task_store = TaskStore(task_db)
            review_audit_store = ReviewAuditStore(review_audit_db)
            base_local_specs = await build_all_local_worker_specs(
                agent_configs,
                registry,
                providers=llm_providers,
                getenv=os.getenv,
                home_dir=home_dir,
                skills_root=skills_root,
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
            )
            worker_control_ref["control"] = worker_control
            main_spec = all_local_specs[main_agent_name]

            gateway_public_local_specs = select_public_local_worker_specs(
                agent_configs,
                all_local_specs,
            )
            gateway_public_remote_refs = await build_public_remote_refs(agent_configs)

            # gateway_control 和 worker_control 故意分开：HTTP 入口只能看到 public
            # target；内部 control 登记完整集合，但工具层按 caller scope 限制 spawn。
            gateway_control = AgentControl(
                gateway_public_local_specs,
                gateway_public_remote_refs,
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
            )

            # TUI/interactive 入口按名称选择 local agent。agent 创建必须等
            # worker_control 准备好，因为 delegation tools 依赖它。
            def build_local_agent(agent_name: str) -> Any:
                spec = all_local_specs[agent_name]
                return create_runtime_agent(
                    model=spec.model,
                    system_prompt=spec.system_prompt,
                    tools=spec.tools,
                    local_worker_specs=spec.delegation_local_worker_specs,
                    remote_refs=spec.delegation_remote_refs,
                    build_worker_tools=spec.build_delegation_tools,
                    memory=spec.memory,
                    skills=spec.skills,
                    backend=agent_backend,
                    checkpointer=checkpointer,
                    mailbox=mailbox,
                    load_tasks_for_thread=worker_control.load_tasks_for_thread,
                    permission_policy=permission_policy,
                    backend_kind=backend_runtime.kind,
                    workspace_root=home_dir,
                    permission_profile=(
                        spec.permission_profile
                        or permission_policy.default_profile
                    ),
                    review_audit_store=review_audit_store,
                    tool_search_registry=(
                        spec.tool_search_registry if spec.tool_search else None
                    ),
                    tool_search_server_names=spec.tool_search_server_names,
                    tool_search_tool_names=spec.tool_search_tool_names,
                    name=agent_name,
                )

            # GatewayService 是 HTTP 侧的门面，负责把外部 task 路由到 public 本地
            # agent 或 remote_ref。public 本地 agent 也在 worker_control 里执行，
            # 这样它的 delegation tools 与父 task 归属在同一个 TaskManager 内。
            gateway_service = GatewayService(  # 具体看gateway_http.py
                main_agent_name=main_agent_name,
                agent_configs=agent_configs,
                runtime=AgentControlGatewayRuntime(worker_control),  # 为什么是worker_control
                route_store=route_store,
            )
            print("configured local agents:", sorted(all_local_specs.keys()))
            print(
                "configured main delegation local targets:",
                sorted((main_spec.delegation_local_worker_specs or {}).keys()),
            )
            print(
                "configured public gateway agents:",
                sorted(gateway_public_local_specs.keys()),
            )
            print("configured remote refs:", sorted(all_remote_refs.keys()))
            print(
                "configured public gateway remote refs:",
                sorted(gateway_public_remote_refs.keys()),
            )
            print(
                "configured delegation limits:",
                f"max_depth={max_delegation_depth}",
                f"max_tasks_per_root={max_tasks_per_root}",
            )
            print(f"configured backend: {backend_runtime.kind} ({home_dir})")
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
                gateway_control=gateway_control,
                review_audit_store=review_audit_store,
                checkpoint_db=checkpoint_db,
                route_db=route_db,
                task_db=task_db,
                review_audit_db=review_audit_db,
                permission_default_profile=permission_policy.default_profile,
                _build_local_agent=build_local_agent,
                _local_agent_cache={},
            )
    finally:
        if route_store is not None:
            route_store.close()
        if task_store is not None:
            task_store.close()
        if review_audit_store is not None:
            review_audit_store.close()
        backend_runtime.close()


def create_bootstrapped_gateway_app() -> FastAPI:
    """创建已经接入共享 runtime bootstrap 的 FastAPI 应用。

    Uvicorn 需要一个 app factory，但真正的 runtime 对象是异步资源，只应该在
    FastAPI lifespan 内存在。这个函数负责把两者接起来，并在路由层约定如何从
    request.app.state 取到当前可用的 GatewayService。
    """

    load_dotenv()
    bearer_token = os.getenv("GATEWAY_BEARER_TOKEN", DEFAULT_GATEWAY_TOKEN)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        """在 HTTP 服务生命周期内打开 runtime，并把它暴露到 app.state。"""

        if not bearer_token or bearer_token == DEFAULT_GATEWAY_TOKEN:
            raise SystemExit(
                "Insecure configuration: set a non-default GATEWAY_BEARER_TOKEN "
                "before starting gateway/telegram mode."
            )
        async with bootstrap_application() as runtime:
            app.state.app_runtime = runtime
            app.state.gateway_service = runtime.gateway_service
            yield

    app = FastAPI(title="ruyi-agent Gateway", lifespan=lifespan)
    attach_gateway_routes(
        app,
        service_getter=lambda request: request.app.state.gateway_service,
        bearer_token=bearer_token,
    )
    return app
