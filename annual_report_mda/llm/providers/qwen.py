"""
Qwen (通义千问) LLM 提供商 - 使用 OpenAI-compatible API

阿里云通义千问支持 OpenAI-compatible 接口，无需额外 SDK。
API 文档: https://help.aliyun.com/zh/model-studio/developer-reference/
"""

from __future__ import annotations

import os
import time

import httpx

from .base import LLMProvider, LLMResponse


class QwenProvider(LLMProvider):
    """Qwen (通义千问) LLM 提供商"""

    API_BASE = "https://dashscope.aliyuncs.com/compatible-mode/v1"
    DEFAULT_MODEL = "qwen-plus"

    def __init__(
        self,
        api_key: str | None = None,
        model: str | None = None,
        api_base: str | None = None,
    ):
        self._api_key = api_key or os.getenv("QWEN_API_KEY") or os.getenv("DASHSCOPE_API_KEY")
        self._model = model or self.DEFAULT_MODEL
        self._api_base = api_base or self.API_BASE

    @property
    def name(self) -> str:
        return "qwen"

    def is_available(self) -> bool:
        return bool(self._api_key)

    async def complete(
        self,
        prompt: str,
        *,
        system: str | None = None,
        temperature: float = 0.7,
        max_tokens: int = 4096,
        timeout: float = 60.0,
    ) -> LLMResponse:
        if not self.is_available():
            raise ValueError("Qwen API key not configured")

        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})

        payload = {
            "model": self._model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }

        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
        }

        start_time = time.perf_counter()

        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(
                f"{self._api_base}/chat/completions",
                json=payload,
                headers=headers,
            )
            response.raise_for_status()
            data = response.json()

        latency_ms = int((time.perf_counter() - start_time) * 1000)

        choice = data.get("choices", [{}])[0]
        content = choice.get("message", {}).get("content", "")
        usage = data.get("usage", {})

        return LLMResponse(
            content=content,
            model=data.get("model", self._model),
            provider=self.name,
            usage={
                "prompt_tokens": usage.get("prompt_tokens", 0),
                "completion_tokens": usage.get("completion_tokens", 0),
            },
            latency_ms=latency_ms,
            raw_response=data,
        )
