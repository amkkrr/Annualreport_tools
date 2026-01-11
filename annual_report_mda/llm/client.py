"""
统一 LLM 客户端，支持多提供商降级
"""
from __future__ import annotations

import logging
from typing import Optional

from .providers.base import LLMProvider, LLMResponse
from .providers.deepseek import DeepSeekProvider
from .providers.qwen import QwenProvider
from .providers.claude import ClaudeProvider
from .providers.openai import OpenAIProvider


_LOG = logging.getLogger(__name__)


class LLMError(Exception):
    """LLM 调用错误基类"""
    pass


class LLMAllProvidersFailedError(LLMError):
    """所有提供商均失败"""
    def __init__(self, errors: dict[str, Exception]):
        self.errors = errors
        providers = ", ".join(errors.keys())
        super().__init__(f"所有 LLM 提供商均失败: {providers}")


class LLMJSONParseError(LLMError):
    """LLM 返回的 JSON 解析失败"""
    pass


# 默认提供商优先级
DEFAULT_FALLBACK_ORDER = ["deepseek", "qwen", "claude", "openai"]


class LLMClient:
    """统一 LLM 客户端，支持多提供商降级"""

    def __init__(
        self,
        providers: Optional[list[str]] = None,
        fallback_order: Optional[list[str]] = None,
    ):
        """
        初始化 LLM 客户端。

        Args:
            providers: 启用的提供商列表，默认全部
            fallback_order: 降级顺序，默认 ["deepseek", "qwen", "claude", "openai"]
        """
        self._fallback_order = fallback_order or DEFAULT_FALLBACK_ORDER

        # 初始化所有提供商
        self._all_providers: dict[str, LLMProvider] = {
            "deepseek": DeepSeekProvider(),
            "qwen": QwenProvider(),
            "claude": ClaudeProvider(),
            "openai": OpenAIProvider(),
        }

        # 过滤启用的提供商
        if providers:
            self._providers = {k: v for k, v in self._all_providers.items() if k in providers}
        else:
            self._providers = self._all_providers

        # 熔断状态
        self._failure_counts: dict[str, int] = {k: 0 for k in self._providers}
        self._circuit_broken: dict[str, bool] = {k: False for k in self._providers}
        self._failure_threshold = 5

    def get_available_providers(self) -> list[str]:
        """获取可用的提供商列表（已配置 API Key 且未熔断）"""
        available = []
        for name in self._fallback_order:
            if name not in self._providers:
                continue
            provider = self._providers[name]
            if provider.is_available() and not self._circuit_broken.get(name, False):
                available.append(name)
        return available

    def _record_success(self, provider: str) -> None:
        """记录成功，重置失败计数"""
        self._failure_counts[provider] = 0

    def _record_failure(self, provider: str) -> None:
        """记录失败，可能触发熔断"""
        self._failure_counts[provider] = self._failure_counts.get(provider, 0) + 1
        if self._failure_counts[provider] >= self._failure_threshold:
            self._circuit_broken[provider] = True
            _LOG.warning(f"提供商 {provider} 熔断: 连续失败 {self._failure_threshold} 次")

    def reset_circuit_breaker(self, provider: Optional[str] = None) -> None:
        """重置熔断器"""
        if provider:
            self._circuit_broken[provider] = False
            self._failure_counts[provider] = 0
        else:
            self._circuit_broken = {k: False for k in self._providers}
            self._failure_counts = {k: 0 for k in self._providers}

    async def complete(
        self,
        prompt: str,
        *,
        system: Optional[str] = None,
        temperature: float = 0.7,
        max_tokens: int = 4096,
        provider: Optional[str] = None,
        retry_on_failure: bool = True,
        timeout: float = 60.0,
    ) -> LLMResponse:
        """
        执行补全，失败时自动降级到下一个提供商。

        Args:
            prompt: 用户 Prompt
            system: 系统 Prompt
            temperature: 采样温度
            max_tokens: 最大生成长度
            provider: 指定提供商，None 则按优先级选择
            retry_on_failure: 是否在失败时重试其他提供商
            timeout: 请求超时时间

        Returns:
            LLMResponse

        Raises:
            LLMAllProvidersFailedError: 所有提供商均失败
        """
        # 确定尝试顺序
        if provider:
            providers_to_try = [provider]
        else:
            providers_to_try = self.get_available_providers()

        if not providers_to_try:
            raise LLMAllProvidersFailedError({"all": ValueError("没有可用的 LLM 提供商")})

        errors: dict[str, Exception] = {}

        for name in providers_to_try:
            if name not in self._providers:
                continue

            llm_provider = self._providers[name]
            if not llm_provider.is_available():
                continue

            try:
                _LOG.debug(f"尝试提供商: {name}")
                response = await llm_provider.complete(
                    prompt,
                    system=system,
                    temperature=temperature,
                    max_tokens=max_tokens,
                    timeout=timeout,
                )
                self._record_success(name)
                return response

            except Exception as e:
                _LOG.warning(f"提供商 {name} 失败: {e}")
                errors[name] = e
                self._record_failure(name)

                if not retry_on_failure:
                    break

        raise LLMAllProvidersFailedError(errors)

    async def complete_with_json(
        self,
        prompt: str,
        *,
        system: Optional[str] = None,
        temperature: float = 0.3,
        max_tokens: int = 4096,
        provider: Optional[str] = None,
    ) -> dict:
        """
        执行补全并解析 JSON 响应。

        Args:
            prompt: 用户 Prompt
            system: 系统 Prompt
            temperature: 采样温度（JSON 输出建议用较低值）
            max_tokens: 最大生成长度
            provider: 指定提供商

        Returns:
            解析后的 dict

        Raises:
            LLMJSONParseError: JSON 解析失败
        """
        import json
        import re

        response = await self.complete(
            prompt,
            system=system,
            temperature=temperature,
            max_tokens=max_tokens,
            provider=provider,
        )

        content = response.content

        # 提取 JSON 代码块
        json_match = re.search(r"```json\s*(.*?)\s*```", content, re.DOTALL)
        if json_match:
            content = json_match.group(1)
        else:
            # 尝试提取普通代码块
            code_match = re.search(r"```\s*(.*?)\s*```", content, re.DOTALL)
            if code_match:
                content = code_match.group(1)

        try:
            return json.loads(content.strip())
        except json.JSONDecodeError as e:
            raise LLMJSONParseError(
                f"无法解析 LLM 返回的 JSON: {e}\n原始内容: {content[:500]}"
            ) from e
