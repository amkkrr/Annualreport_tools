from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Optional, Sequence


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


# =============================================================================
# 负向特征检测
# =============================================================================

# 表格残留检测：连续 N 行仅含数字/小数点/空格/百分号/负号
_TABLE_LINE_RE = re.compile(r"^[\d\s.,\-%+]+$")
_TABLE_CONSECUTIVE_THRESHOLD = 3

# 页眉干扰检测：短行长度阈值
_HEADER_MAX_LEN = 50
_HEADER_REPEAT_THRESHOLD = 3

# 乱码检测：合法字符范围
_VALID_CHARS_RE = re.compile(
    r"[\u4e00-\u9fff"  # 中文
    r"a-zA-Z"  # 英文
    r"0-9"  # 数字
    r"\s"  # 空白
    r"。，、；：？！""''【】（）《》"  # 中文标点
    r".,;:?!\"'()\[\]{}<>"  # 英文标点
    r"\-+=%$#@&*/_~`\n\r\t"  # 特殊符号
    r"]"
)
_GARBLED_RATIO_THRESHOLD = 0.05


@dataclass(frozen=True)
class NegativeFeatures:
    """负向特征检测结果"""

    table_residue_score: int  # 表格残留扣分 (0 or 15)
    header_noise_score: int  # 页眉干扰扣分 (0 or 10)
    garbled_ratio: float  # 乱码比例 (0.0-1.0)
    garbled_penalty: int  # 乱码扣分 (0 or 20)

    table_residue_lines: int  # 检测到的表格行数
    repeated_headers: list[str] = field(default_factory=list)  # 检测到的重复页眉


def detect_table_residue(text: str) -> tuple[bool, int]:
    """
    检测表格残留（连续数字行）。

    规则：连续 3 行以上满足「仅含数字、小数点、空格、百分号」

    Returns:
        (is_detected, max_consecutive_count)
    """
    if not text:
        return False, 0

    lines = text.splitlines()
    max_consecutive = 0
    current_consecutive = 0

    for line in lines:
        stripped = line.strip()
        if not stripped:
            current_consecutive = 0
            continue
        if _TABLE_LINE_RE.match(stripped):
            current_consecutive += 1
            max_consecutive = max(max_consecutive, current_consecutive)
        else:
            current_consecutive = 0

    return max_consecutive >= _TABLE_CONSECUTIVE_THRESHOLD, max_consecutive


def detect_header_noise(text: str) -> tuple[bool, list[str]]:
    """
    检测页眉干扰（重复短行）。

    规则：长度 <50 的行出现次数 >3 且内容相同

    Returns:
        (is_detected, repeated_lines)
    """
    if not text:
        return False, []

    lines = text.splitlines()
    short_line_counts: dict[str, int] = {}

    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        if len(stripped) < _HEADER_MAX_LEN:
            short_line_counts[stripped] = short_line_counts.get(stripped, 0) + 1

    repeated = [
        line
        for line, count in short_line_counts.items()
        if count > _HEADER_REPEAT_THRESHOLD
    ]

    return len(repeated) > 0, repeated


def calculate_garbled_ratio(text: str) -> float:
    """
    计算乱码比例。

    乱码定义：非合法字符的占比。

    Returns:
        ratio: 0.0-1.0
    """
    if not text:
        return 0.0

    valid_count = len(_VALID_CHARS_RE.findall(text))
    total = len(text)

    if total == 0:
        return 0.0

    return 1.0 - (valid_count / total)


def detect_negative_features(text: str) -> NegativeFeatures:
    """
    检测文本中的负向特征。

    Args:
        text: MD&A 文本

    Returns:
        NegativeFeatures: 包含各负向特征检测结果
    """
    if not text:
        return NegativeFeatures(
            table_residue_score=0,
            header_noise_score=0,
            garbled_ratio=0.0,
            garbled_penalty=0,
            table_residue_lines=0,
            repeated_headers=[],
        )

    # 表格残留检测
    table_detected, table_lines = detect_table_residue(text)
    table_score = 15 if table_detected else 0

    # 页眉干扰检测
    header_detected, repeated_headers = detect_header_noise(text)
    header_score = 10 if header_detected else 0

    # 乱码比例检测
    garbled_ratio = calculate_garbled_ratio(text)
    garbled_penalty = 20 if garbled_ratio > _GARBLED_RATIO_THRESHOLD else 0

    return NegativeFeatures(
        table_residue_score=table_score,
        header_noise_score=header_score,
        garbled_ratio=garbled_ratio,
        garbled_penalty=garbled_penalty,
        table_residue_lines=table_lines,
        repeated_headers=repeated_headers,
    )


# =============================================================================
# 综合质量评分
# =============================================================================

# 各质量标记对应的扣分
_FLAG_PENALTIES: dict[str, int] = {
    "FLAG_LENGTH_ABNORMAL": 10,
    "FLAG_CONTENT_MISMATCH": 15,
    "FLAG_TAIL_OVERLAP": 10,
    "FLAG_PAGE_BOUNDARY_MISSING": 5,
    "FLAG_TOC_MISMATCH": 10,
    "FLAG_EXTRACT_FAILED": 100,  # 直接归零
    "FLAG_YOY_CHANGE_HIGH": 5,
}

# 低分阈值
NEEDS_REVIEW_THRESHOLD = 60

# 目录引导线过多的阈值
_DOTS_EXCESS_THRESHOLD = 10


@dataclass(frozen=True)
class QualityScore:
    """综合质量评分结果"""

    score: int  # 0-100
    needs_review: bool
    penalties: dict[str, int] = field(default_factory=dict)  # 各项扣分明细


def calculate_quality_score(
    text: str,
    quality_flags: Optional[Sequence[str]],
    score_detail: Optional[ScoreDetail],
) -> QualityScore:
    """
    计算综合质量评分。

    Args:
        text: MD&A 文本
        quality_flags: 现有质量标记
        score_detail: 现有评分细节

    Returns:
        QualityScore: 包含最终评分和 needs_review 标记
    """
    if not text:
        return QualityScore(score=0, needs_review=True, penalties={"empty_text": 100})

    base = 100
    penalties: dict[str, int] = {}

    # 1. 现有质量标记扣分
    if quality_flags:
        for flag in quality_flags:
            if flag in _FLAG_PENALTIES:
                penalties[flag.lower()] = _FLAG_PENALTIES[flag]

    # 2. 目录引导线扣分 (基于 score_detail)
    if score_detail and score_detail.dots_count >= _DOTS_EXCESS_THRESHOLD:
        penalties["dots_excess"] = 20

    # 3. 新增负向特征检测
    neg_features = detect_negative_features(text)
    if neg_features.table_residue_score > 0:
        penalties["table_residue"] = neg_features.table_residue_score
    if neg_features.header_noise_score > 0:
        penalties["header_noise"] = neg_features.header_noise_score
    if neg_features.garbled_penalty > 0:
        penalties["garbled_text"] = neg_features.garbled_penalty

    # 4. 计算最终分数
    total_penalty = sum(penalties.values())
    final_score = max(0, base - total_penalty)

    return QualityScore(
        score=final_score,
        needs_review=final_score < NEEDS_REVIEW_THRESHOLD,
        penalties=penalties,
    )


# =============================================================================
# L3 时序校验（年际变化检测）
# =============================================================================

# 时序校验阈值：相似度低于此值触发 FLAG_YOY_CHANGE_HIGH
YOY_SIMILARITY_THRESHOLD = 0.3


def calculate_text_similarity(text1: str, text2: str) -> float:
    """
    计算两段文本的相似度（Jaccard Index）。

    使用字符级 n-gram（n=3）计算 Jaccard 相似度，
    适用于中文文本的相似度比较。

    Args:
        text1: 文本 1
        text2: 文本 2

    Returns:
        相似度 0.0-1.0，1.0 表示完全相同
    """
    if not text1 or not text2:
        return 0.0

    if text1 == text2:
        return 1.0

    # 使用字符级 3-gram
    n = 3

    def get_ngrams(text: str, n: int) -> set[str]:
        """提取 n-gram 集合。"""
        # 移除空白字符以减少噪音
        cleaned = "".join(text.split())
        if len(cleaned) < n:
            return {cleaned} if cleaned else set()
        return {cleaned[i : i + n] for i in range(len(cleaned) - n + 1)}

    ngrams1 = get_ngrams(text1, n)
    ngrams2 = get_ngrams(text2, n)

    if not ngrams1 or not ngrams2:
        return 0.0

    # Jaccard Index: |A ∩ B| / |A ∪ B|
    intersection = len(ngrams1 & ngrams2)
    union = len(ngrams1 | ngrams2)

    if union == 0:
        return 0.0

    return intersection / union


def detect_yoy_change(
    current_text: str,
    prev_text: Optional[str],
    *,
    similarity_threshold: float = YOY_SIMILARITY_THRESHOLD,
) -> tuple[bool, float]:
    """
    检测年际变化是否异常。

    通过比较当年和上年 MD&A 文本的相似度，检测是否存在异常的年际变化。
    如果相似度低于阈值，可能表示：
    - 提取边界错误
    - 公司发生重大变化
    - 年报格式变化

    Args:
        current_text: 当年 MD&A 文本
        prev_text: 上年 MD&A 文本（None 表示无上年数据，跳过校验）
        similarity_threshold: 相似度阈值，低于此值触发异常标记

    Returns:
        (is_abnormal, similarity_score)
        - is_abnormal: True 表示变化异常，应添加 FLAG_YOY_CHANGE_HIGH
        - similarity_score: 计算的相似度分数
    """
    # 无上年数据，跳过校验
    if prev_text is None:
        return False, 1.0

    # 空文本处理
    if not current_text or not prev_text:
        return False, 0.0

    similarity = calculate_text_similarity(current_text, prev_text)

    # 相似度低于阈值视为异常
    is_abnormal = similarity < similarity_threshold

    return is_abnormal, similarity
