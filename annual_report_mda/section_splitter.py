"""MD&A 文本字段切分模块。

将 MD&A 文本切分为"经营回顾"和"未来展望"两个子字段。
"""

from __future__ import annotations

import re
from dataclasses import dataclass

# 未来展望类关键词（按优先级排序）
OUTLOOK_PATTERNS: list[str] = [
    # 章节级标题
    r"第[一二三四五六七八九十\d]+[章节部分]\s*[：:]*\s*.*(?:未来|展望|发展战略)",
    # 编号标题
    r"[一二三四五六七八九十\d]+[、\.]\s*(?:公司)?(?:未来发展|发展展望|未来展望)",
    r"[（\(][一二三四五六七八九十\d]+[）\)]\s*(?:公司)?(?:未来发展|发展展望)",
    # 常见标题
    r"(?:对公司)?未来发展(?:的)?(?:展望|战略)",
    r"公司(?:未来)?发展战略",
    r"(?:公司)?(?:未来|下一年度)(?:的)?经营计划",
    r"未来(?:业务)?发展展望",
]

# 预编译正则表达式
_OUTLOOK_REGEXES = [re.compile(p, re.IGNORECASE) for p in OUTLOOK_PATTERNS]


@dataclass(frozen=True)
class MdaSections:
    """MD&A 切分结果。"""

    review: str  # 经营回顾部分
    outlook: str | None  # 未来展望部分（可能为空）
    split_keyword: str | None  # 触发切分的关键词
    split_position: int | None  # 切分位置（字符偏移）


def split_mda_sections(mda_text: str) -> MdaSections:
    """
    将 MD&A 文本切分为经营回顾和未来展望两部分。

    Args:
        mda_text: 完整的 MD&A 文本

    Returns:
        MdaSections 对象，包含切分后的两部分及切分信息
    """
    if not mda_text or not mda_text.strip():
        return MdaSections(
            review="",
            outlook=None,
            split_keyword=None,
            split_position=None,
        )

    best_match: re.Match | None = None
    best_pattern_idx = len(OUTLOOK_PATTERNS)  # 用于优先级比较

    # 逐行搜索，匹配标题行
    lines = mda_text.splitlines()
    char_offset = 0

    for line in lines:
        stripped = line.strip()
        # 跳过空行
        if not stripped:
            char_offset += len(line) + 1  # +1 for newline
            continue

        # 跳过过长的行（不太可能是标题）
        if len(stripped) > 80:
            char_offset += len(line) + 1
            continue

        for pattern_idx, regex in enumerate(_OUTLOOK_REGEXES):
            match = regex.search(stripped)
            if match:
                # 选择优先级最高（索引最小）且位置最靠前的匹配
                if pattern_idx < best_pattern_idx or (
                    pattern_idx == best_pattern_idx
                    and (best_match is None or char_offset < best_match.start())
                ):
                    # 创建一个带有正确位置的 match-like 对象
                    # 使用 staticmethod 避免实例作为第一个参数传入 lambda
                    best_match = type(
                        "MatchResult",
                        (),
                        {
                            "start": staticmethod(lambda s=char_offset: s),
                            "group": staticmethod(lambda s=match.group(): s),
                        },
                    )()
                    best_pattern_idx = pattern_idx
                break  # 一行只匹配一次

        char_offset += len(line) + 1

    if best_match is None:
        # 未找到切分点，整体作为经营回顾
        return MdaSections(
            review=mda_text,
            outlook=None,
            split_keyword=None,
            split_position=None,
        )

    split_pos = best_match.start()
    keyword = best_match.group()

    review = mda_text[:split_pos].rstrip()
    outlook = mda_text[split_pos:].strip()

    # 如果切分后任一部分过短，视为无效切分
    if len(review) < 500 or len(outlook) < 200:
        return MdaSections(
            review=mda_text,
            outlook=None,
            split_keyword=None,
            split_position=None,
        )

    return MdaSections(
        review=review,
        outlook=outlook,
        split_keyword=keyword,
        split_position=split_pos,
    )
