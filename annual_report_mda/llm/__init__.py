"""
LLM 模块 - 统一的多提供商 LLM 客户端

支持的提供商:
- DeepSeek (OpenAI-compatible)
- Qwen / 通义千问 (dashscope)
- Claude (anthropic)
- OpenAI (openai)

使用示例:
    from annual_report_mda.llm import LLMClient

    client = LLMClient()
    response = await client.complete("你好，请介绍一下自己")
    print(response.content)
"""

from .client import LLMClient, LLMResponse, LLMError, LLMAllProvidersFailedError
from .providers.base import LLMProvider

__all__ = [
    "LLMClient",
    "LLMResponse",
    "LLMProvider",
    "LLMError",
    "LLMAllProvidersFailedError",
]
