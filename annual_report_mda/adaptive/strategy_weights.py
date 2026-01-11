"""
策略权重自适应
"""

from __future__ import annotations

import json
import logging
import random
from pathlib import Path

_LOG = logging.getLogger(__name__)


class StrategyWeights:
    """策略权重管理器"""

    STRATEGIES = ["generic", "toc", "custom", "llm_learned"]

    def __init__(self, store_path: str = "data/strategy_stats.json"):
        self.store_path = Path(store_path)
        self._stats: dict[str, dict[str, int]] = {}
        self._load()

    def _load(self) -> None:
        """加载统计数据"""
        if self.store_path.exists():
            try:
                with open(self.store_path, encoding="utf-8") as f:
                    self._stats = json.load(f)
                _LOG.info(f"加载策略统计: {self._stats}")
            except (json.JSONDecodeError, TypeError) as e:
                _LOG.warning(f"加载策略统计失败: {e}")
                self._stats = {}

        # 确保所有策略都有统计
        for strategy in self.STRATEGIES:
            if strategy not in self._stats:
                self._stats[strategy] = {"attempts": 0, "success": 0}

    def save(self) -> None:
        """保存统计数据"""
        self.store_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.store_path, "w", encoding="utf-8") as f:
            json.dump(self._stats, f, indent=2)

    def record(self, strategy: str, success: bool) -> None:
        """记录策略执行结果"""
        if strategy not in self._stats:
            self._stats[strategy] = {"attempts": 0, "success": 0}

        self._stats[strategy]["attempts"] += 1
        if success:
            self._stats[strategy]["success"] += 1

        _LOG.debug(
            f"策略 {strategy}: attempts={self._stats[strategy]['attempts']}, "
            f"success={self._stats[strategy]['success']}"
        )

    def get_weight(self, strategy: str) -> float:
        """
        计算策略权重。

        公式: success_rate + exploration_bonus
        exploration_bonus = 1 / (attempts + 10)
        """
        if strategy not in self._stats:
            return 0.5  # 未知策略默认权重

        stats = self._stats[strategy]
        attempts = stats["attempts"]
        success = stats["success"]

        success_rate = success / max(attempts, 1)
        exploration_bonus = 1 / (attempts + 10)

        return success_rate + exploration_bonus

    def get_success_rate(self, strategy: str) -> float:
        """获取策略成功率"""
        if strategy not in self._stats:
            return 0.0

        stats = self._stats[strategy]
        attempts = stats["attempts"]
        success = stats["success"]

        return success / max(attempts, 1)

    def select_strategy(self, available: list[str] = None) -> str:
        """
        基于权重选择策略（带探索）。

        使用 softmax 采样，保证探索性。
        """
        if available is None:
            available = self.STRATEGIES

        weights = [self.get_weight(s) for s in available]
        total = sum(weights)

        if total == 0:
            return random.choice(available)

        probs = [w / total for w in weights]
        return random.choices(available, weights=probs, k=1)[0]

    def get_priority_order(self) -> list[str]:
        """获取策略优先级排序"""
        return sorted(self.STRATEGIES, key=lambda s: -self.get_weight(s))

    def get_stats_summary(self) -> dict:
        """获取统计摘要"""
        summary = {}
        for strategy in self.STRATEGIES:
            stats = self._stats.get(strategy, {"attempts": 0, "success": 0})
            summary[strategy] = {
                "attempts": stats["attempts"],
                "success": stats["success"],
                "success_rate": self.get_success_rate(strategy),
                "weight": self.get_weight(strategy),
            }
        return summary
