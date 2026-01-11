"""
自适应学习模块 - LLM 驱动的提取优化

包含:
- Self-Refine: 迭代改进提取质量
- Few-shot: 动态检索相似案例
- StrategyWeights: 策略权重自适应
- FailurePatterns: 失败模式学习
"""

from .failure_patterns import FailurePattern, FailurePatternStore
from .few_shot import FewShotSample, FewShotStore
from .self_refine import RefineResult, SelfRefineLoop
from .strategy_weights import StrategyWeights

__all__ = [
    "SelfRefineLoop",
    "RefineResult",
    "FewShotStore",
    "FewShotSample",
    "StrategyWeights",
    "FailurePatternStore",
    "FailurePattern",
]
