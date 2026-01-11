"""
LLM 客户端单元测试
"""

import os
from unittest.mock import patch

import pytest

from annual_report_mda.llm.client import (
    LLMClient,
    LLMJSONParseError,
    LLMResponse,
)
from annual_report_mda.llm.providers.base import LLMProvider


class MockProvider(LLMProvider):
    """测试用 Mock 提供商"""

    def __init__(self, name: str, available: bool = True, response: str = "test response"):
        self._name = name
        self._available = available
        self._response = response
        self._should_fail = False

    @property
    def name(self) -> str:
        return self._name

    def is_available(self) -> bool:
        return self._available

    async def complete(self, prompt: str, **kwargs) -> LLMResponse:
        if self._should_fail:
            raise RuntimeError(f"{self._name} failed")
        return LLMResponse(
            content=self._response,
            model="mock-model",
            provider=self._name,
            usage={"prompt_tokens": 10, "completion_tokens": 5},
            latency_ms=100,
        )


class TestLLMResponse:
    """LLMResponse 数据类测试"""

    def test_basic_response(self):
        response = LLMResponse(
            content="Hello",
            model="gpt-4",
            provider="openai",
        )
        assert response.content == "Hello"
        assert response.model == "gpt-4"
        assert response.provider == "openai"
        assert response.usage == {}
        assert response.latency_ms == 0

    def test_response_with_usage(self):
        response = LLMResponse(
            content="Hello",
            model="gpt-4",
            provider="openai",
            usage={"prompt_tokens": 10, "completion_tokens": 5},
            latency_ms=150,
        )
        assert response.usage["prompt_tokens"] == 10
        assert response.latency_ms == 150


class TestLLMClient:
    """LLMClient 测试"""

    def test_get_available_providers_empty(self):
        """测试无可用提供商"""
        client = LLMClient()
        # 默认情况下，如果没有配置 API Key，providers 不可用
        # 这里我们只是检查方法可以调用
        providers = client.get_available_providers()
        assert isinstance(providers, list)

    @pytest.mark.asyncio
    async def test_complete_with_json_valid(self):
        """测试 JSON 解析 - 有效 JSON"""
        client = LLMClient()

        # Mock complete 方法
        async def mock_complete(*args, **kwargs):
            return LLMResponse(
                content='```json\n{"key": "value"}\n```',
                model="test",
                provider="test",
            )

        client.complete = mock_complete

        result = await client.complete_with_json("test prompt")
        assert result == {"key": "value"}

    @pytest.mark.asyncio
    async def test_complete_with_json_invalid(self):
        """测试 JSON 解析 - 无效 JSON"""
        client = LLMClient()

        async def mock_complete(*args, **kwargs):
            return LLMResponse(
                content="not valid json",
                model="test",
                provider="test",
            )

        client.complete = mock_complete

        with pytest.raises(LLMJSONParseError):
            await client.complete_with_json("test prompt")

    def test_circuit_breaker_reset(self):
        """测试熔断器重置"""
        client = LLMClient()

        # 手动设置熔断状态
        client._circuit_broken["deepseek"] = True
        client._failure_counts["deepseek"] = 10

        # 重置单个提供商
        client.reset_circuit_breaker("deepseek")
        assert client._circuit_broken["deepseek"] is False
        assert client._failure_counts["deepseek"] == 0

        # 重置所有
        client._circuit_broken["qwen"] = True
        client.reset_circuit_breaker()
        assert all(not v for v in client._circuit_broken.values())


class TestLLMProviders:
    """各 LLM 提供商测试"""

    def test_deepseek_availability(self):
        """测试 DeepSeek 可用性检查"""
        from annual_report_mda.llm.providers.deepseek import DeepSeekProvider

        # 无 API Key
        with patch.dict(os.environ, {}, clear=True):
            provider = DeepSeekProvider()
            assert provider.is_available() is False

        # 有 API Key
        provider = DeepSeekProvider(api_key="test-key")
        assert provider.is_available() is True
        assert provider.name == "deepseek"

    def test_qwen_availability(self):
        """测试 Qwen 可用性检查"""
        from annual_report_mda.llm.providers.qwen import QwenProvider

        provider = QwenProvider(api_key="test-key")
        assert provider.is_available() is True
        assert provider.name == "qwen"

    def test_claude_availability(self):
        """测试 Claude 可用性检查"""
        from annual_report_mda.llm.providers.claude import ClaudeProvider

        provider = ClaudeProvider(api_key="test-key")
        assert provider.is_available() is True
        assert provider.name == "claude"

    def test_openai_availability(self):
        """测试 OpenAI 可用性检查"""
        from annual_report_mda.llm.providers.openai import OpenAIProvider

        provider = OpenAIProvider(api_key="test-key")
        assert provider.is_available() is True
        assert provider.name == "openai"
