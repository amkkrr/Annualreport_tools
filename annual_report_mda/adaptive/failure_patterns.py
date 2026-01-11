"""
失败模式学习
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from pathlib import Path

_LOG = logging.getLogger(__name__)


@dataclass
class FailurePattern:
    """失败模式"""

    pattern_id: str
    description: str
    match_conditions: dict  # 匹配条件
    exclusion_rule: str  # 排除规则
    occurrences: int  # 出现次数


class FailurePatternStore:
    """失败模式存储"""

    def __init__(self, store_path: str = "data/failure_patterns.json"):
        self.store_path = Path(store_path)
        self._patterns: list[FailurePattern] = []
        self._load()

    def _load(self) -> None:
        """加载失败模式"""
        if self.store_path.exists():
            try:
                with open(self.store_path, encoding="utf-8") as f:
                    data = json.load(f)
                    self._patterns = [FailurePattern(**p) for p in data.get("patterns", [])]
                _LOG.info(f"加载 {len(self._patterns)} 个失败模式")
            except (json.JSONDecodeError, TypeError) as e:
                _LOG.warning(f"加载失败模式失败: {e}")
                self._patterns = []

    def save(self) -> None:
        """保存失败模式"""
        self.store_path.parent.mkdir(parents=True, exist_ok=True)
        data = {"version": "1.0", "patterns": [asdict(p) for p in self._patterns]}
        with open(self.store_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def add_failure(
        self,
        stock_code: str,
        year: int,
        error_type: str,
        error_message: str,
        extraction_result: dict | None = None,
    ) -> None:
        """添加失败记录，自动归类模式"""
        pattern_id = self._classify_failure(error_type, error_message)

        for p in self._patterns:
            if p.pattern_id == pattern_id:
                p.occurrences += 1
                _LOG.debug(f"失败模式 {pattern_id} 出现次数: {p.occurrences}")
                return

        # 新模式
        self._patterns.append(
            FailurePattern(
                pattern_id=pattern_id,
                description=f"{error_type}: {error_message[:100]}",
                match_conditions={"error_type": error_type},
                exclusion_rule="",  # 待 LLM 分析生成
                occurrences=1,
            )
        )
        _LOG.info(f"新增失败模式: {pattern_id}")

    def _classify_failure(self, error_type: str, error_message: str) -> str:
        """简单失败分类"""
        message_lower = error_message.lower()

        if "目录" in error_message or "toc" in message_lower:
            return "TOC_PARSE_FAILED"
        if "边界" in error_message or "boundary" in message_lower:
            return "BOUNDARY_DETECTION_FAILED"
        if "空" in error_message or "empty" in message_lower:
            return "EMPTY_RESULT"
        if "乱码" in error_message or "garbled" in message_lower:
            return "ENCODING_ERROR"
        if "超时" in error_message or "timeout" in message_lower:
            return "TIMEOUT_ERROR"
        if "api" in message_lower or "rate" in message_lower:
            return "API_ERROR"
        return f"OTHER_{error_type}"

    def get_exclusion_prompts(self, min_occurrences: int = 3) -> list[str]:
        """获取排除提示（用于负面 prompt）"""
        prompts = []
        for p in self._patterns:
            if p.exclusion_rule and p.occurrences >= min_occurrences:
                prompts.append(f"避免: {p.exclusion_rule}")
        return prompts

    def get_frequent_patterns(self, min_occurrences: int = 3) -> list[FailurePattern]:
        """获取频繁失败模式"""
        return [p for p in self._patterns if p.occurrences >= min_occurrences]

    def update_exclusion_rule(self, pattern_id: str, rule: str) -> bool:
        """更新排除规则（通常由 LLM 分析后调用）"""
        for p in self._patterns:
            if p.pattern_id == pattern_id:
                p.exclusion_rule = rule
                return True
        return False

    def get_stats_summary(self) -> dict:
        """获取统计摘要"""
        return {
            "total_patterns": len(self._patterns),
            "total_occurrences": sum(p.occurrences for p in self._patterns),
            "top_patterns": sorted(
                [(p.pattern_id, p.occurrences) for p in self._patterns], key=lambda x: -x[1]
            )[:5],
        }

    def __len__(self) -> int:
        return len(self._patterns)
