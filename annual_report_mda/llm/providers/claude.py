"""
Claude (Anthropic) LLM 提供商
"""
from __future__ import annotations

import os
import time
from typing import Optional

import httpx

from .base import LLMProvider, LLMResponse


class ClaudeProvider(LLMProvider):
    """Claude LLM 提供商，使用 Anthropic Messages API"""

    API_BASE = "https://api.anthropic.com/v1"
    DEFAULT_MODEL = "claude-sonnet-4-20250514"
    API_VERSION = "2023-06-01"

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        api_base: Optional[str] = None,
    ):
        self._api_key = api_key or os.getenv("ANTHROPIC_API_KEY")
        self._model = model or self.DEFAULT_MODEL
        self._api_base = api_base or self.API_BASE

    @property
    def name(self) -> str:
        return "claude"

    def is_available(self) -> bool:
        return bool(self._api_key)

    async def complete(
        self,
        prompt: str,
        *,
        system: Optional[str] = None,
        temperature: float = 0.7,
        max_tokens: int = 4096,
        timeout: float = 60.0,
    ) -> LLMResponse:
        if not self.is_available():
            raise ValueError("Anthropic API key not configured")

        messages = [{"role": "user", "content": prompt}]

        payload = {
            "model": self._model,
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": temperature,
        }

        if system:
            payload["system"] = system

        headers = {
            "x-api-key": self._api_key,
            "anthropic-version": self.API_VERSION,
            "Content-Type": "application/json",
        }

        start_time = time.perf_counter()

        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(
                f"{self._api_base}/messages",
                json=payload,
                headers=headers,
            )
            response.raise_for_status()
            data = response.json()

        latency_ms = int((time.perf_counter() - start_time) * 1000)

        # Claude Messages API 响应格式
        content_blocks = data.get("content", [])
        content = ""
        for block in content_blocks:
            if block.get("type") == "text":
                content += block.get("text", "")

        usage = data.get("usage", {})

        return LLMResponse(
            content=content,
            model=data.get("model", self._model),
            provider=self.name,
            usage={
                "prompt_tokens": usage.get("input_tokens", 0),
                "completion_tokens": usage.get("output_tokens", 0),
            },
            latency_ms=latency_ms,
            raw_response=data,
        )
