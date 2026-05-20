"""Adapters for third-party middleware used by this project.

This module keeps direct imports from deepagents/langchain middleware in one place
so project-owned runtime assembly can evolve without scattering SDK details.
"""

from langchain.agents.middleware import TodoListMiddleware
from langchain_anthropic.middleware import AnthropicPromptCachingMiddleware

from deepagents.middleware.filesystem import FilesystemMiddleware
from deepagents.middleware.memory import MemoryMiddleware
from deepagents.middleware.patch_tool_calls import PatchToolCallsMiddleware
from deepagents.middleware.skills import SkillsMiddleware
from deepagents.middleware.summarization import create_summarization_middleware

__all__ = [
    "AnthropicPromptCachingMiddleware",
    "FilesystemMiddleware",
    "MemoryMiddleware",
    "PatchToolCallsMiddleware",
    "SkillsMiddleware",
    "TodoListMiddleware",
    "create_summarization_middleware",
]
