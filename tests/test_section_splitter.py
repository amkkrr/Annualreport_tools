"""
测试 section_splitter.py 模块的 MD&A 文本字段切分功能。
"""

from __future__ import annotations

from annual_report_mda.section_splitter import MdaSections, split_mda_sections


def _make_review_text(min_length: int) -> str:
    # 多行构造，模拟真实 MD&A 内容结构
    line = "报告期内公司经营情况良好，营业收入与净利润稳步增长。"
    parts: list[str] = []
    while len("\n".join(parts)) < min_length:
        parts.append(line * 3)
    return "\n".join(parts)


def _make_outlook_text(min_length: int) -> str:
    line = "未来公司将继续推进技术创新，优化产品结构，提升核心竞争力。"
    parts: list[str] = []
    while len("\n".join(parts)) < min_length:
        parts.append(line * 3)
    return "\n".join(parts)


class TestSplitMdaSections:
    """测试 MD&A 文本切分（经营回顾 / 未来展望）。"""

    def test_empty_text_returns_empty_sections(self) -> None:
        result = split_mda_sections("")
        assert isinstance(result, MdaSections)
        assert result.review == ""
        assert result.outlook is None
        assert result.split_keyword is None
        assert result.split_position is None

    def test_no_split_point_returns_review_only(self) -> None:
        review = _make_review_text(800)
        text = f"{review}\n本节不包含未来展望相关标题。\n结尾。"
        result = split_mda_sections(text)

        assert result.review == text
        assert result.outlook is None
        assert result.split_keyword is None
        assert result.split_position is None

    def test_split_found_but_too_short_returns_unsplit(self) -> None:
        # review < 500，outlook >= 200，即使找到切分点也视为无效
        review = _make_review_text(200)
        header = "三、公司未来发展的展望"
        outlook = _make_outlook_text(300)
        text = f"{review}\n{header}\n{outlook}"
        result = split_mda_sections(text)

        assert result.review == text
        assert result.outlook is None
        assert result.split_keyword is None
        assert result.split_position is None

    def test_valid_split_returns_sections_and_metadata(self) -> None:
        review = _make_review_text(800)
        header = "三、公司未来发展的展望"
        outlook = _make_outlook_text(300)
        text = f"{review}\n{header}\n{outlook}"

        result = split_mda_sections(text)

        assert result.outlook is not None
        assert result.split_position is not None
        assert result.split_keyword is not None

        expected_pos = text.index(header)
        assert result.split_position == expected_pos
        assert text[result.split_position :].lstrip().startswith(header)

        assert len(result.review) >= 500
        assert len(result.outlook) >= 200
        assert "未来" in result.split_keyword

    def test_long_header_line_is_ignored(self) -> None:
        # 超过 80 字符的标题行会被跳过，即使包含关键字
        review = _make_review_text(800)
        long_header = "三、公司未来发展展望" + "（重要提示）" * 20  # 使行长度超过 80
        outlook = _make_outlook_text(300)
        text = f"{review}\n{long_header}\n{outlook}"

        result = split_mda_sections(text)

        assert result.review == text
        assert result.outlook is None
        assert result.split_keyword is None
        assert result.split_position is None
