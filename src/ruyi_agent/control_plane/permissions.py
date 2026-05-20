"""
权限策略 - 工具调用与命令执行的本地权限评估

这个模块实现了针对工具调用的权限判定逻辑，用于在不同权限配置下决定
某次调用是直接放行、要求人工审批，还是直接拒绝。

核心功能：
1. 解析 execute 命令并识别危险信号
2. 基于 profile、tool 配置和前缀规则计算权限结果
3. 为上层返回统一的决策、原因和风险标签

使用场景：
- 本地 agent 在执行工具前做权限预检查
- 不同 agent 或线程切换不同的权限 profile
- 对 shell 命令进行轻量级风险筛查

数据流：
  tool_args/context → 命令分析 → profile/rule 匹配 → PermissionResult

关键概念：
- profile: 一组权限策略定义，决定默认行为和例外规则
- execute_rule: 针对命令前缀的细粒度匹配规则
- risk: 对潜在危险操作的风险分类标签
"""

from __future__ import annotations

import re
import shlex
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


DEFAULT_ALLOWED_DECISIONS = ["approve", "edit", "reject"]
SHELL_CONTROL_PATTERN = re.compile(r"(\|\||&&|;|\||>>|>|<|\$\(|`)")
SHELL_CONTROL_TOKENS = {"||", "&&", ";", "|", ">>", ">", "<"}
KNOWN_EXECUTE_RISKS = {
    "command_parse_error",
    "shell_control_operator",
    "empty_command",
    "privilege_escalation",
    "git_destructive",
    "destructive_filesystem",
    "recursive_permission_change",
    "dependency_install",
}


class PermissionDecision(str, Enum):
    """权限决策枚举。"""

    ALLOW = "allow"
    REQUIRE_APPROVAL = "require_approval"
    DENY = "deny"


@dataclass(slots=True)
class ToolPermissionConfig:
    """
    单个工具的权限配置

    用于描述某个工具的默认决策，以及当需要人工审核时允许给出的可选动作。

    Attributes:
        policy: 当前工具的默认权限决策
        allowed_decisions: 审核界面允许用户选择的动作集合
        description: 该配置的说明文字，用于返回给上层或展示给用户
    """

    policy: PermissionDecision
    allowed_decisions: list[str] = field(default_factory=list)
    description: str | None = None


@dataclass(slots=True)
class ExecuteRuleConfig:
    """
    execute 命令的前缀匹配规则

    规则采用“token 前缀完全匹配”的方式，适合描述如 `git status`、
    `uv run pytest` 这类结构稳定的命令入口。

    Attributes:
        match: 命令前缀 token 列表
        policy: 匹配后的权限决策
        allowed_decisions: 该规则覆盖的可选审核动作
        description: 命中规则后的说明文字
    """

    match: list[str]
    policy: PermissionDecision
    allowed_decisions: list[str] = field(default_factory=list)
    description: str | None = None


@dataclass(slots=True)
class PermissionProfile:
    """
    一组完整的权限配置集合

    profile 将工具级默认策略与 execute 的细粒度前缀规则组织在一起，
    便于按 agent、线程或运行环境切换权限行为。

    Attributes:
        name: profile 名称
        description: profile 的用途说明
        tools: 各工具对应的默认权限配置
        execute_rules: 仅针对 execute 工具生效的前缀规则列表
        execute_review_risks: 需要升级为人工审批的 execute 风险标签；
            None 表示所有已识别风险都需要审批
    """

    name: str
    description: str | None = None
    tools: dict[str, ToolPermissionConfig] = field(default_factory=dict)
    execute_rules: list[ExecuteRuleConfig] = field(default_factory=list)
    execute_review_risks: set[str] | None = None


@dataclass(slots=True)
class PermissionConfig:
    """
    权限系统的总配置

    Attributes:
        default_profile: 默认使用的 profile 名称
        profiles: 所有可用 profile 的映射表
    """

    default_profile: str
    profiles: dict[str, PermissionProfile]


@dataclass(slots=True)
class PermissionContext:
    """
    一次权限判定的上下文信息

    这些字段不会直接决定规则匹配，但会参与 profile 选择、结果记录和后续审计。

    Attributes:
        agent_name: 发起调用的 agent 名称
        profile_name: 请求显式指定的 profile 名称
        backend_kind: 后端类型标识
        workspace_root: 当前工作区根目录
        thread_id: 线程标识
        task_id: 任务标识
    """

    agent_name: str | None
    profile_name: str | None
    backend_kind: str
    workspace_root: str
    thread_id: str | None = None
    task_id: str | None = None


@dataclass(slots=True)
class PermissionResult:
    """
    权限判定结果

    Attributes:
        decision: 最终权限决策
        allowed_decisions: 若需审批时允许用户选择的动作
        reason: 给上层展示或记录的原因说明
        risk: 风险分类标签
        profile_name: 实际生效的 profile 名称
    """

    decision: PermissionDecision
    allowed_decisions: list[str]
    reason: str
    risk: str = "unknown"
    profile_name: str | None = None


@dataclass(slots=True)
class CommandAnalysis:
    """
    shell 命令的解析结果

    Attributes:
        command: 原始命令字符串
        tokens: `shlex.split` 后的 token 序列
        parse_error: 解析失败时的错误信息
        has_shell_control: 是否包含管道、重定向、子命令等 shell 控制符
    """

    command: str
    tokens: list[str]
    parse_error: str | None
    has_shell_control: bool


def _normal_allowed_decisions(values: list[str] | None) -> list[str]:
    """返回规范化后的可选审批动作列表。"""

    if not values:
        return list(DEFAULT_ALLOWED_DECISIONS)
    return list(values)


def analyze_command(command: Any) -> CommandAnalysis:
    """
    解析 execute 命令字符串

    该函数只做轻量分析：判断类型、检查 shell 控制符，并尝试用 `shlex`
    拆分 token，为后续风险识别和前缀规则匹配提供统一输入。

    Args:
        command: 待分析的命令对象

    Returns:
        命令分析结果，包含 token、解析错误和 shell 控制符标记
    """

    if not isinstance(command, str):
        return CommandAnalysis(
            command="",
            tokens=[],
            parse_error="command must be a string",
            has_shell_control=False,
        )
    has_shell_control = bool(SHELL_CONTROL_PATTERN.search(command))
    try:
        tokens = shlex.split(command)
    except ValueError as exc:
        return CommandAnalysis(
            command=command,
            tokens=[],
            parse_error=str(exc),
            has_shell_control=has_shell_control,
        )
    return CommandAnalysis(
        command=command,
        tokens=tokens,
        parse_error=None,
        has_shell_control=has_shell_control,
    )


def _matches_prefix(tokens: list[str], prefix: list[str]) -> bool:
    """判断 token 序列是否以前缀规则完整开头。"""

    if not prefix or len(tokens) < len(prefix):
        return False
    return tokens[: len(prefix)] == prefix


def _command_segments(tokens: list[str]) -> list[list[str]]:
    """按常见 shell 控制 token 粗略拆分命令段，用于 deny 前缀兜底。"""

    segments: list[list[str]] = []
    current: list[str] = []
    for token in tokens:
        if token in SHELL_CONTROL_TOKENS:
            if current:
                segments.append(current)
                current = []
            continue
        current.append(token)
    if current:
        segments.append(current)
    return segments


def _matches_deny_rule(analysis: CommandAnalysis, rule: ExecuteRuleConfig) -> bool:
    """判断 deny 规则是否命中整条命令或任意 shell 子命令段。"""

    if _matches_prefix(analysis.tokens, rule.match):
        return True
    if not analysis.has_shell_control:
        return False
    return any(
        _matches_prefix(segment, rule.match)
        for segment in _command_segments(analysis.tokens)
    )


def _risk_requires_review(profile: PermissionProfile, risk: str) -> bool:
    """判断当前 profile 是否把某个风险升级为人工审批。"""

    if profile.execute_review_risks is None:
        return True
    return risk in profile.execute_review_risks


def _detect_risk(analysis: CommandAnalysis) -> str | None:
    """
    根据命令分析结果识别风险标签

    这里优先识别“应当升级为人工审核”的高风险信号，而不是穷举所有安全规则。

    Args:
        analysis: 命令解析结果

    Returns:
        风险标签；若未识别到风险则返回 `None`
    """

    tokens = analysis.tokens
    if analysis.parse_error:
        return "command_parse_error"
    if analysis.has_shell_control:
        return "shell_control_operator"
    if not tokens:
        return "empty_command"
    if tokens[0] == "sudo":
        return "privilege_escalation"
    if tokens[:3] == ["git", "reset", "--hard"]:
        return "git_destructive"
    if tokens[:2] == ["git", "clean"]:
        return "git_destructive"
    if tokens[0] in {"rm", "rmdir"}:
        return "destructive_filesystem"
    if tokens[0] in {"chmod", "chown"} and "-R" in tokens:
        return "recursive_permission_change"
    if tokens[:2] in (["pip", "install"], ["npm", "install"], ["uv", "add"]):
        return "dependency_install"
    return None


class PermissionPolicy:
    """
    权限策略评估器

    负责将一次工具调用映射为最终的权限决策。对普通工具直接套用工具级策略，
    对 `execute` 工具则进一步做命令解析、风险识别和前缀规则匹配。

    主要功能：
    - resolve_profile_name: 解析本次调用应使用的 profile
    - evaluate: 评估任意工具调用
    - _evaluate_execute: 专门处理 execute 命令的权限细节

    设计要点：
    - 先匹配 deny 规则，保证显式拒绝具有最高优先级
    - 先做风险识别，再做 allow/approval 规则匹配，避免危险命令被宽松规则绕过

    Attributes:
        _config: 全量权限配置
    """

    def __init__(self, config: PermissionConfig) -> None:
        """初始化权限策略评估器。"""

        self._config = config

    @property
    def default_profile(self) -> str:
        """返回默认 profile 名称。"""

        return self._config.default_profile

    def resolve_profile_name(self, profile_name: str | None) -> str:
        """
        解析实际生效的 profile 名称

        若调用方指定的 profile 不存在，则自动回退到默认 profile，
        这样上层无需为每次请求单独处理兜底逻辑。

        Args:
            profile_name: 调用方请求的 profile 名称

        Returns:
            实际可用的 profile 名称
        """

        if profile_name and profile_name in self._config.profiles:
            return profile_name
        return self._config.default_profile

    def evaluate(
        self,
        *,
        tool_name: str,
        tool_args: dict[str, Any],
        context: PermissionContext,
    ) -> PermissionResult:
        """
        评估一次工具调用的权限结果

        Args:
            tool_name: 工具名称
            tool_args: 工具参数
            context: 当前调用上下文

        Returns:
            统一的权限判定结果
        """

        profile_name = self.resolve_profile_name(context.profile_name)
        profile = self._config.profiles[profile_name]
        tool_config = (
            profile.tools.get(tool_name)
            or profile.tools.get("default")
            or ToolPermissionConfig(
                policy=PermissionDecision.REQUIRE_APPROVAL,
                allowed_decisions=list(DEFAULT_ALLOWED_DECISIONS),
            )
        )

        if tool_name == "execute":
            return self._evaluate_execute(
                tool_args=tool_args,
                profile=profile,
                tool_config=tool_config,
                profile_name=profile_name,
            )

        return PermissionResult(
            decision=tool_config.policy,
            allowed_decisions=_normal_allowed_decisions(tool_config.allowed_decisions),
            reason=tool_config.description
            or f"tool policy for {tool_name}: {tool_config.policy.value}",
            risk="tool_policy",
            profile_name=profile_name,
        )

    def _evaluate_execute(
        self,
        *,
        tool_args: dict[str, Any],
        profile: PermissionProfile,
        tool_config: ToolPermissionConfig,
        profile_name: str,
    ) -> PermissionResult:
        """
        评估 execute 工具的权限结果

        处理顺序刻意分成三层：
        1. 先拦截显式 deny 前缀规则
        2. 再识别需要人工复核的风险命令
        3. 最后才应用 allow / require_approval 前缀规则和默认策略

        这样可以保证危险命令不会被后续宽松规则意外放行。

        Args:
            tool_args: execute 工具参数
            profile: 当前生效的权限 profile
            tool_config: execute 工具的默认配置
            profile_name: 当前生效的 profile 名称

        Returns:
            execute 命令的权限判定结果
        """

        analysis = analyze_command(tool_args.get("command"))

        for rule in profile.execute_rules:
            if rule.policy != PermissionDecision.DENY:
                continue
            # deny 规则优先级最高，命中后立即拒绝，不再继续评估后续规则。
            if _matches_deny_rule(analysis, rule):
                return PermissionResult(
                    decision=PermissionDecision.DENY,
                    allowed_decisions=[],
                    reason=rule.description
                    or "execute command matched a deny prefix rule",
                    risk="execute_deny_rule",
                    profile_name=profile_name,
                )

        risk = _detect_risk(analysis)
        if risk is not None and _risk_requires_review(profile, risk):
            # 风险识别先于 allow 规则，避免危险命令因前缀宽松而被自动放行。
            return PermissionResult(
                decision=PermissionDecision.REQUIRE_APPROVAL,
                allowed_decisions=_normal_allowed_decisions(
                    tool_config.allowed_decisions
                ),
                reason=f"execute command requires review: {risk}",
                risk=risk,
                profile_name=profile_name,
            )

        for rule in profile.execute_rules:
            if rule.policy == PermissionDecision.DENY:
                continue
            if not _matches_prefix(analysis.tokens, rule.match):
                continue
            return PermissionResult(
                decision=rule.policy,
                allowed_decisions=_normal_allowed_decisions(
                    rule.allowed_decisions or tool_config.allowed_decisions
                ),
                reason=rule.description
                or f"execute command matched prefix rule: {' '.join(rule.match)}",
                risk="execute_prefix_rule",
                profile_name=profile_name,
            )

        return PermissionResult(
            decision=tool_config.policy,
            allowed_decisions=_normal_allowed_decisions(tool_config.allowed_decisions),
            reason=tool_config.description
            or f"execute default policy: {tool_config.policy.value}",
            risk="execute_default",
            profile_name=profile_name,
        )
