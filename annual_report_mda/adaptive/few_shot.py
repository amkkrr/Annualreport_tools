"""
动态 Few-shot 样本存储
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from pathlib import Path

_LOG = logging.getLogger(__name__)


@dataclass
class FewShotSample:
    """Few-shot 样本"""

    stock_code: str
    year: int
    industry: str
    toc_signature: str  # 目录结构哈希
    start_pattern: str
    end_pattern: str
    keywords: list[str]
    quality_score: float
    char_count: int


class FewShotStore:
    """Few-shot 样本存储"""

    def __init__(self, store_path: str = "data/success_samples.json"):
        self.store_path = Path(store_path)
        self._samples: list[FewShotSample] = []
        self._load()

    def _load(self) -> None:
        """加载样本库"""
        if self.store_path.exists():
            try:
                with open(self.store_path, encoding="utf-8") as f:
                    data = json.load(f)
                    self._samples = [FewShotSample(**s) for s in data.get("samples", [])]
                _LOG.info(f"加载 {len(self._samples)} 个 few-shot 样本")
            except (json.JSONDecodeError, TypeError) as e:
                _LOG.warning(f"加载样本库失败: {e}")
                self._samples = []

    def save(self) -> None:
        """保存样本库"""
        self.store_path.parent.mkdir(parents=True, exist_ok=True)
        data = {"version": "1.0", "samples": [asdict(s) for s in self._samples]}
        with open(self.store_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        _LOG.info(f"保存 {len(self._samples)} 个 few-shot 样本")

    def add(self, sample: FewShotSample) -> None:
        """添加样本"""
        # 检查是否已存在（同一公司同一年份）
        for i, s in enumerate(self._samples):
            if s.stock_code == sample.stock_code and s.year == sample.year:
                self._samples[i] = sample  # 更新
                _LOG.debug(f"更新样本: {sample.stock_code}/{sample.year}")
                return
        self._samples.append(sample)
        _LOG.debug(f"新增样本: {sample.stock_code}/{sample.year}")

    def find_similar(
        self,
        keywords: list[str],
        industry: str | None = None,
        toc_signature: str | None = None,
        top_k: int = 3,
    ) -> list[FewShotSample]:
        """
        查找相似样本。

        使用 Jaccard 相似度匹配关键词，优先同行业和相同目录结构。
        """
        if not self._samples:
            return []

        scored = []
        target_set = set(keywords)

        for sample in self._samples:
            sample_set = set(sample.keywords)

            # Jaccard 相似度
            intersection = len(target_set & sample_set)
            union = len(target_set | sample_set)
            jaccard = intersection / union if union > 0 else 0

            # 同行业加分
            industry_bonus = 0.2 if industry and sample.industry == industry else 0

            # 相同目录结构加分
            toc_bonus = 0.3 if toc_signature and sample.toc_signature == toc_signature else 0

            # 质量评分权重
            quality_weight = sample.quality_score / 100.0

            score = jaccard * quality_weight + industry_bonus + toc_bonus
            scored.append((score, sample))

        scored.sort(key=lambda x: -x[0])
        return [s for _, s in scored[:top_k]]

    def format_few_shot_prompt(self, samples: list[FewShotSample]) -> str:
        """格式化 few-shot 示例"""
        if not samples:
            return ""

        lines = ["以下是相似年报的成功提取案例：\n"]

        for i, sample in enumerate(samples, 1):
            lines.append(f"### 案例 {i}: {sample.stock_code} ({sample.year})")
            lines.append(f"- 行业: {sample.industry}")
            lines.append(f"- 起始标题: `{sample.start_pattern}`")
            lines.append(f"- 结束标题: `{sample.end_pattern}`")
            lines.append(f"- 提取字数: {sample.char_count}")
            lines.append(f"- 质量评分: {sample.quality_score}\n")

        return "\n".join(lines)

    def __len__(self) -> int:
        return len(self._samples)
