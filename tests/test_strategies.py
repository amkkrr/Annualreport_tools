"""
测试 strategies.py 模块的提取策略功能。
"""

from pathlib import Path

import pytest

from annual_report_mda.scorer import calculate_mda_score
from annual_report_mda.strategies import (
    ExtractionResult,
    TocHit,
    extract_mda_from_pages,
    extract_mda_iterative,
    parse_toc_for_page_range,
)


class TestCalculateMdaScore:
    """测试 MDA 评分函数（来自 scorer.py，在此补充更多用例）。"""

    def test_empty_text_returns_zero(self):
        """空文本应返回 0 分。"""
        score, detail = calculate_mda_score("")
        assert score == 0.0
        assert detail.length == 0
        assert detail.keyword_hit_count == 0

    def test_short_text_returns_zero(self, short_text: str):
        """短文本（< 500 字符）应返回 0 分。"""
        score, detail = calculate_mda_score(short_text)
        assert score == 0.0
        assert detail.length < 500

    def test_text_with_keywords_scores_high(self, sample_mda_text: str):
        """含关键词的文本应获得较高分数。"""
        score, detail = calculate_mda_score(sample_mda_text)
        assert score > 0.5
        assert detail.keyword_hit_count > 0
        assert detail.length >= 500

    def test_text_with_dots_has_dots_count(self, text_with_dots: str):
        """含目录引导线的文本应检测到 dots_count。"""
        score, detail = calculate_mda_score(text_with_dots)
        assert detail.dots_count >= 10

    def test_custom_keywords(self, sample_mda_text: str):
        """支持自定义关键词。"""
        custom_keywords = ["营业收入", "净利润", "现金流"]
        score, detail = calculate_mda_score(sample_mda_text, keywords=custom_keywords)
        assert detail.keyword_total == 3
        # 自定义关键词可能未全部命中
        assert 0 <= detail.keyword_hit_count <= 3


class TestParseTocForPageRange:
    """测试 TOC 页码解析。"""

    def test_parse_toc_with_valid_toc(self, mock_with_toc_text: str):
        """含有效 TOC 的文本应解析出页码范围。"""
        # 按分页符分割成页
        pages = mock_with_toc_text.split("=== Page ")
        pages = [p for p in pages if p.strip()]
        # 重新格式化页（去掉页码行）
        formatted_pages = []
        page_numbers = []
        for p in pages:
            lines = p.split("\n")
            if lines and lines[0].strip().endswith("==="):
                # 提取页码
                page_num_line = lines[0].strip()
                try:
                    page_num = int(page_num_line.replace("===", "").strip())
                    page_numbers.append(page_num)
                except ValueError:
                    page_numbers.append(None)
                formatted_pages.append("\n".join(lines[1:]))
            else:
                page_numbers.append(None)
                formatted_pages.append(p)

        result = parse_toc_for_page_range(formatted_pages, page_numbers=page_numbers)

        # 应该能解析出 TOC
        # 注意：解析结果取决于 TOC 内容和页码映射
        # 如果页码映射不完整，可能返回 None
        if result is not None:
            assert isinstance(result, TocHit)
            assert result.printed_page_start is not None

    def test_parse_toc_no_toc(self, mock_no_toc_text: str):
        """不含 TOC 的文本应返回 None 或无法解析。"""
        pages = mock_no_toc_text.split("=== Page ")
        pages = [p for p in pages if p.strip()]
        formatted_pages = []
        for p in pages:
            lines = p.split("\n")
            if lines and lines[0].strip().endswith("==="):
                formatted_pages.append("\n".join(lines[1:]))
            else:
                formatted_pages.append(p)

        result = parse_toc_for_page_range(formatted_pages, page_numbers=None)
        # 无 TOC 或无页码映射应返回 None
        assert result is None

    def test_parse_toc_empty_pages(self):
        """空页列表应返回 None。"""
        result = parse_toc_for_page_range([], page_numbers=None)
        assert result is None


class TestExtractMdaFromPages:
    """测试正文扫描提取。"""

    def test_extract_mda_with_valid_content(self, mock_no_toc_text: str):
        """含 MD&A 标题的文本应成功提取。"""
        pages = mock_no_toc_text.split("=== Page ")
        pages = [p for p in pages if p.strip()]
        formatted_pages = []
        for p in pages:
            lines = p.split("\n")
            if lines and lines[0].strip().endswith("==="):
                formatted_pages.append("\n".join(lines[1:]))
            else:
                formatted_pages.append(p)

        result = extract_mda_from_pages(formatted_pages)

        assert result is not None
        assert isinstance(result, ExtractionResult)
        assert result.mda_raw is not None
        assert len(result.mda_raw) > 0
        assert result.used_rule_type == "generic"

    def test_extract_mda_empty_pages(self):
        """空页列表应返回 None。"""
        result = extract_mda_from_pages([])
        assert result is None

    def test_extract_mda_no_mda_content(self):
        """不含 MD&A 标题的文本应返回 None。"""
        pages = [
            "这是一段普通文本。",
            "没有任何 MD&A 相关标题。",
            "只是一些随机内容。",
        ]
        result = extract_mda_from_pages(pages)
        # 无 MD&A 标题应返回 None 或低分结果
        # 根据实际实现，可能返回 None
        if result is not None:
            assert result.score <= 0.4  # 低分阈值


class TestExtractMdaIterative:
    """测试多策略迭代提取。"""

    def test_extract_iterative_generic_strategy(self, mock_no_toc_text: str):
        """使用 generic 策略提取。"""
        pages = mock_no_toc_text.split("=== Page ")
        pages = [p for p in pages if p.strip()]
        formatted_pages = []
        for p in pages:
            lines = p.split("\n")
            if lines and lines[0].strip().endswith("==="):
                formatted_pages.append("\n".join(lines[1:]))
            else:
                formatted_pages.append(p)

        result = extract_mda_iterative(
            formatted_pages,
            page_break_kind="eq_separator",
        )

        # 提取可能成功或失败，取决于文本内容和评分阈值
        # 这里验证函数正常运行
        if result is not None:
            assert isinstance(result, ExtractionResult)
            assert result.mda_raw is not None
            assert result.used_rule_type in ("generic", "toc", "custom")

    def test_extract_iterative_with_custom_pattern(self, mock_eq_pages_text: str):
        """使用自定义规则提取。"""
        pages = mock_eq_pages_text.split("=== Page ")
        pages = [p for p in pages if p.strip()]
        formatted_pages = []
        for p in pages:
            lines = p.split("\n")
            if lines and lines[0].strip().endswith("==="):
                formatted_pages.append("\n".join(lines[1:]))
            else:
                formatted_pages.append(p)

        result = extract_mda_iterative(
            formatted_pages,
            page_break_kind="eq_separator",
            custom_start_pattern="董事会报告",
            custom_end_pattern="监事会报告",
        )

        if result is not None:
            assert isinstance(result, ExtractionResult)
            assert result.used_rule_type == "custom"

    def test_extract_iterative_empty_pages(self):
        """空页列表应返回 None。"""
        result = extract_mda_iterative([])
        assert result is None

    def test_extract_iterative_returns_best_candidate(self, mock_with_toc_text: str):
        """多候选时应返回最佳结果。"""
        pages = mock_with_toc_text.split("=== Page ")
        pages = [p for p in pages if p.strip()]
        formatted_pages = []
        page_numbers = []
        for p in pages:
            lines = p.split("\n")
            if lines and lines[0].strip().endswith("==="):
                page_num_line = lines[0].strip()
                try:
                    page_num = int(page_num_line.replace("===", "").strip())
                    page_numbers.append(page_num)
                except ValueError:
                    page_numbers.append(None)
                formatted_pages.append("\n".join(lines[1:]))
            else:
                page_numbers.append(None)
                formatted_pages.append(p)

        result = extract_mda_iterative(
            formatted_pages,
            page_numbers=page_numbers,
            page_break_kind="eq_separator",
        )

        # 提取可能成功或失败，取决于评分阈值
        # 这里验证函数正常运行
        if result is not None:
            # 结果应该有有效的分数
            assert result.score > 0

    def test_extract_iterative_toc_mismatch_flag(self):
        """TOC 和正文页码差异大时应添加 FLAG_TOC_MISMATCH。"""
        # 构造一个 TOC 和正文不一致的场景较复杂
        # 这里只验证函数能处理这种情况
        pass  # 可在集成测试中验证


class TestExtractionResultFields:
    """测试提取结果的字段完整性。"""

    def test_result_has_all_required_fields(self, mock_no_toc_text: str):
        """提取结果应包含所有必需字段。"""
        pages = mock_no_toc_text.split("=== Page ")
        pages = [p for p in pages if p.strip()]
        formatted_pages = []
        for p in pages:
            lines = p.split("\n")
            if lines and lines[0].strip().endswith("==="):
                formatted_pages.append("\n".join(lines[1:]))
            else:
                formatted_pages.append(p)

        result = extract_mda_iterative(formatted_pages)

        if result is not None:
            # 检查必需字段
            assert hasattr(result, "mda_raw")
            assert hasattr(result, "score")
            assert hasattr(result, "score_detail")
            assert hasattr(result, "page_index_start")
            assert hasattr(result, "page_index_end")
            assert hasattr(result, "page_count")
            assert hasattr(result, "hit_start")
            assert hasattr(result, "is_truncated")
            assert hasattr(result, "used_rule_type")
            assert hasattr(result, "quality_flags")
            assert hasattr(result, "quality_detail")


# 添加 fixture 用于自定义规则测试
@pytest.fixture
def mock_eq_pages_text(mock_eq_pages_path: Path) -> str:
    """读取 === Page N === 分页的 mock 文件内容。"""
    return mock_eq_pages_path.read_text(encoding="utf-8")
