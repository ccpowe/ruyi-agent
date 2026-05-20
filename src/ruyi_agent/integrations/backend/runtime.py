from __future__ import annotations

import os
import time
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from daytona import (
    CreateSandboxFromSnapshotParams,
    Daytona,
    DaytonaConfig,
    DaytonaNotFoundError,
)
from daytona.common.errors import DaytonaError
from deepagents.backends import CompositeBackend, LocalShellBackend
from deepagents.backends.protocol import (
    ExecuteResponse,
    FileDownloadResponse,
    FileUploadResponse,
)
from langchain_daytona import DaytonaSandbox

DEFAULT_BACKEND_KIND = "daytona"
DEFAULT_SANDBOX_NAME = "ruyi-agent"
LOCAL_VIRTUAL_WORKSPACE_ROOT = "/"


class AutoStartDaytonaSandbox(DaytonaSandbox):
    """给 DaytonaSandbox 增加按需健康检查和自动启动能力。

    agent 调用 backend 时可能距离进程启动已经过了一段时间，Daytona sandbox
    也可能被外部停止、仍在启动中，或者处于可恢复错误状态。这个包装层把
    “使用前确保 sandbox 可用”收口在 backend 边界，避免上层 agent 工具每次
    execute / upload / download 都要理解 Daytona 的生命周期状态。
    """

    def __init__(
        self,
        *,
        sandbox: Any,
        timeout: int = 30 * 60,
        sync_polling_interval: float = 0.1,
        healthcheck_ttl: float = 2.0,
    ) -> None:
        super().__init__(
            sandbox=sandbox,
            timeout=timeout,
            sync_polling_interval=sync_polling_interval,
        )
        self._healthcheck_ttl = healthcheck_ttl
        self._last_healthcheck_at = 0.0

    def _refresh_if_needed(self) -> None:
        """按 TTL 刷新 sandbox 状态，避免每次 backend 调用都打远端状态接口。"""

        now = time.monotonic()
        if now - self._last_healthcheck_at < self._healthcheck_ttl:
            return
        self._sandbox.refresh_data()
        self._last_healthcheck_at = now

    def _ensure_ready(self) -> str | None:
        """在真正执行 backend 操作前，把 sandbox 尽量推进到可用状态。

        返回 None 表示 sandbox 已可用；返回字符串表示准备失败的可读原因。
        这里选择返回错误文本而不是抛异常，是为了让文件/命令工具能把失败
        转成普通 tool result，避免一次 sandbox 抖动直接中断整轮 agent 执行。
        """

        try:
            self._refresh_if_needed()
            raw_state = self._sandbox.state
            state = (
                getattr(raw_state, "name", None)
                or getattr(raw_state, "value", None)
                or str(raw_state)
            ).upper()

            if state == "STARTED":
                return None

            if state == "STARTING":
                self._sandbox.wait_for_sandbox_start()
            elif state == "STOPPED":
                self._sandbox.start()
            elif state == "ERROR" and getattr(self._sandbox, "recoverable", False):
                self._sandbox.recover()
            else:
                return (
                    f"Daytona sandbox '{self._sandbox.name}' is unavailable. "
                    f"state={self._sandbox.state}, "
                    f"recoverable={getattr(self._sandbox, 'recoverable', None)}"
                )

            self._sandbox.refresh_data()
            self._last_healthcheck_at = time.monotonic()
            return None
        except Exception as exc:
            return (
                f"Daytona sandbox '{self._sandbox.name}' could not be prepared: {exc}"
            )

    def execute(self, command: str, *, timeout: int | None = None):
        """执行 shell 命令前确保 Daytona sandbox 已启动或已恢复。"""

        error = self._ensure_ready()
        if error:
            return ExecuteResponse(
                output=f"Error: Execution unavailable. {error}",
                exit_code=1,
                truncated=False,
            )
        return super().execute(command, timeout=timeout)

    def upload_files(self, files: list[tuple[str, bytes]]):
        """上传文件前确保 Daytona sandbox 已启动或已恢复。"""

        error = self._ensure_ready()
        if error:
            return [
                FileUploadResponse(path=path, error="invalid_path") for path, _ in files
            ]
        return super().upload_files(files)

    def download_files(self, paths: list[str]):
        """下载文件前确保 Daytona sandbox 已启动或已恢复。"""

        error = self._ensure_ready()
        if error:
            return [
                FileDownloadResponse(path=path, content=None, error="file_not_found")
                for path in paths
            ]
        return super().download_files(paths)


@dataclass(slots=True)
class BackendRuntime:
    """保存 agent runtime 使用 backend 时需要的统一运行信息。

    上层装配层不应该关心底层是 Daytona 还是本地 shell，只需要拿到统一的
    CompositeBackend、运行根目录和 skills 根目录。可选的 sandbox 只用于
    close() 时做生命周期清理。
    """

    kind: str
    backend: CompositeBackend
    home_dir: str
    skills_root: str
    _sandbox: Any | None = None

    def close(self) -> None:
        """关闭 runtime 持有的外部执行资源。"""

        if self._sandbox is not None:
            _stop_sandbox(self._sandbox)


def _create_sandbox() -> Any:
    """获取或创建当前进程使用的 Daytona sandbox。

    Daytona backend 需要一个可执行、可读写文件的远端环境。这里优先复用固定
    名称的 sandbox，找不到时才新建，避免每次启动项目都创建新的远端环境。
    """

    config = DaytonaConfig(
        api_key=os.getenv("DAYTONA_API_KEY"),
        api_url=os.getenv("DAYTONA_API_URL"),
        target=os.getenv("DAYTONA_TARGET"),
    )
    daytona = Daytona(config)
    sandbox_name = os.getenv("DAYTONA_SANDBOX_NAME", DEFAULT_SANDBOX_NAME)
    try:
        sandbox = daytona.get(sandbox_name)
        if str(sandbox.state) != "STARTED":
            sandbox.start()
    except DaytonaNotFoundError:
        sandbox = daytona.create(
            CreateSandboxFromSnapshotParams(
                name=sandbox_name,
                language="python",
            )
        )
    return sandbox


def _stop_sandbox(sandbox: Any) -> None:
    """在进程退出时尽量停止 Daytona sandbox。

    stop 操作可能遇到 sandbox 已停止的竞态，这种情况不应让应用退出流程失败；
    其他 Daytona 错误仍然向外抛出，避免掩盖真实清理问题。
    """

    try:
        sandbox.refresh_data()
        raw_state = sandbox.state
        state = (
            getattr(raw_state, "name", None)
            or getattr(raw_state, "value", None)
            or str(raw_state)
        ).upper()
        if state not in {"STOPPED", "STOPPING", "ARCHIVED", "DESTROYED"}:
            sandbox.stop()
    except DaytonaError as exc:
        if "Sandbox is not started" not in str(exc):
            raise


def _env_bool(name: str, *, default: bool) -> bool:
    """按项目约定解析环境变量里的布尔开关。"""  # 可以用来限制wokespace 需要结合权限管理

    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _local_virtual_path(raw_path: str | None, root_dir: Path) -> str:
    """把本地 shell 配置路径映射到 LocalShellBackend 的虚拟路径空间。"""

    if raw_path is None or not raw_path.strip():
        return LOCAL_VIRTUAL_WORKSPACE_ROOT

    raw = raw_path.strip()
    candidate = Path(raw)
    if not candidate.is_absolute():
        return str(PurePosixPath("/") / raw)

    resolved = candidate.resolve()
    try:
        relative = resolved.relative_to(root_dir)
    except ValueError as exc:
        raise ValueError(
            "LOCAL_BACKEND_SKILLS_ROOT must be inside LOCAL_BACKEND_ROOT "
            "when BACKEND_KIND=local"
        ) from exc

    if relative == Path("."):
        return LOCAL_VIRTUAL_WORKSPACE_ROOT
    return str(PurePosixPath("/") / relative.as_posix())


def _create_daytona_backend_runtime() -> BackendRuntime:
    """创建基于 Daytona sandbox 的 backend runtime。

    这是默认运行方式：agent 的 shell、文件读写和 artifact 都落在 Daytona
    sandbox 里，从而和宿主机隔离，同时保留一个稳定的 home_dir 给 memory 和
    skills 路径映射使用。
    """

    sandbox = _create_sandbox()
    home_dir = sandbox.get_user_home_dir()
    backend = AutoStartDaytonaSandbox(sandbox=sandbox)
    agent_backend = CompositeBackend(
        default=backend,
        routes={},
        artifacts_root=home_dir,
    )
    return BackendRuntime(
        kind="daytona",
        backend=agent_backend,
        home_dir=home_dir,
        skills_root=f"{home_dir}/skills",
        _sandbox=sandbox,
    )


def _create_local_backend_runtime() -> BackendRuntime:
    """创建基于本地 shell 的 backend runtime。

    local backend 主要用于开发和测试：它复用与 sandbox backend 相同的
    CompositeBackend 接口。文件工具使用虚拟根目录 `/` 映射到
    LOCAL_BACKEND_ROOT，避免普通文件工具越过工作区；命令仍会直接在
    LOCAL_BACKEND_ROOT 下以当前用户权限执行，因此没有 Daytona 的进程隔离。
    """

    root_dir = Path(os.getenv("LOCAL_BACKEND_ROOT", os.getcwd())).resolve()
    timeout = int(os.getenv("LOCAL_BACKEND_TIMEOUT", "120"))
    max_output_bytes = int(os.getenv("LOCAL_BACKEND_MAX_OUTPUT_BYTES", "100000"))
    inherit_env = _env_bool("LOCAL_BACKEND_INHERIT_ENV", default=True)
    local_backend = LocalShellBackend(
        root_dir=root_dir,
        virtual_mode=True,
        timeout=timeout,
        max_output_bytes=max_output_bytes,
        inherit_env=inherit_env,
    )
    home_dir = LOCAL_VIRTUAL_WORKSPACE_ROOT
    skills_root = _local_virtual_path(os.getenv("LOCAL_BACKEND_SKILLS_ROOT"), root_dir)
    agent_backend = CompositeBackend(
        default=local_backend,
        routes={},
        artifacts_root=home_dir,
    )
    return BackendRuntime(
        kind="local",
        backend=agent_backend,
        home_dir=home_dir,
        skills_root=skills_root,
    )


def create_backend_runtime() -> BackendRuntime:
    """根据 BACKEND_KIND 创建当前进程使用的 backend runtime。

    app_runtime 只调用这个工厂函数，不直接依赖 Daytona 或 LocalShell 的创建细节。
    这样 CLI、Gateway 和测试都可以通过环境变量切换执行环境，而不需要改装配代码。
    """

    kind = os.getenv("BACKEND_KIND", DEFAULT_BACKEND_KIND).strip().lower()
    if kind == "daytona":
        return _create_daytona_backend_runtime()
    if kind in {"local", "localshell", "local_shell"}:
        return _create_local_backend_runtime()
    raise ValueError(
        f"Unsupported BACKEND_KIND: {kind!r}. Expected 'daytona' or 'local'."
    )
