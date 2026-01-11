"""
LLM 提供商基类
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class LLMResponse:
    """LLM 响应结构"""
    content: str
    model: str
    provider: str
    usage: dict = field(default_factory=dict)  # {"prompt_tokens": N, "completion_tokens": M}
    latency_ms: int = 0
    raw_response: Optional[dict] = None


class LLMProvider(ABC):
    """LLM 提供商基类"""

    @property
    @abstractmethod
    def name(self) -> str:
        """提供商名称"""
        ...

    @abstractmethod
    async def complete(
        self,
        prompt: str,
        *,
        system: Optional[str] = None,
        temperature: float = 0.7,
        max_tokens: int = 4096,
        timeout: float = 60.0,
    ) -> LLMResponse:
        """执行补全请求"""
        ...

    @abstractmethod
    def is_available(self) -> bool:
        """检查 API Key 是否配置"""
        ...

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} available={self.is_available()}>"
