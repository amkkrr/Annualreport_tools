from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional, Sequence


MDA_TITLES: list[str] = [
    "董事会报告",
    "董事局报告",
    "经营情况讨论与分析",
    "经营层讨论与分析",
    "管理层讨论与分析",
    "管理层分析与讨论",
    "董事会工作报告",
    "董事局工作报告",
    "经营分析与讨论",
    "讨论与分析",
    "业务回顾",
    "业务回顾与展望",
    "董事会报告书",
    "董事会工作汇报",
    "Management Discussion and Analysis",
    "MD&A",
]

MDA_PATTERNS: list[str] = [
    r"第[一二三四五六七八九十百零\d]+[章节部分]\s*管理层讨论与分析",
    r"第[一二三四五六七八九十百零\d]+[章节部分]\s*董事会报告",
    r"[一二三四五六七八九十百零\d]+[、\.]\s*董事会报告",
    r"[一二三四五六七八九十百零\d]+[、\.]\s*管理层讨论与分析",
]

NEXT_TITLES: list[str] = [
    "监事会报告",
    "监事会工作报告",
    "重要事项",
    "公司治理",
    "财务报告",
    "审计报告",
]


DEFAULT_KEYWORDS: list[str] = ["主营业务", "收入", "同比", "毛利率", "现金流", "行业", "展望"]


@dataclass(frozen=True)
class ScoreDetail:
    keyword_hit_count: int
    keyword_total: int
    dots_count: int
    length: int


def calculate_mda_score(text: str, *, keywords: Optional[Sequence[str]] = None) -> tuple[float, ScoreDetail]:
    """
    计算文本“像 MD&A 的程度”(0.0-1.0)。
    - 关键词命中（营收、同比、现金流等）是正向特征
    - 目录/表格引导线（……/....）密集是负向特征
    """
    if not text:
        return 0.0, ScoreDetail(keyword_hit_count=0, keyword_total=0, dots_count=0, length=0)

    length = len(text)
    if length < 500:
        return 0.0, ScoreDetail(keyword_hit_count=0, keyword_total=0, dots_count=0, length=length)

    keywords_list = list(keywords) if keywords is not None else list(DEFAULT_KEYWORDS)

    keyword_hit_count = sum(1 for k in keywords_list if k and k in text)
    keyword_total = max(len(keywords_list), 1)

    dots_count = text.count("...") + text.count("…") + text.count("……") + len(re.findall(r"\.{4,}", text))

    score = (keyword_hit_count / keyword_total) * 0.8
    if dots_count < 10:
        score += 0.2

    if score < 0.0:
        score = 0.0
    if score > 1.0:
        score = 1.0

    return score, ScoreDetail(
        keyword_hit_count=keyword_hit_count,
        keyword_total=keyword_total,
        dots_count=dots_count,
        length=length,
    )
