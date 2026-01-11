"""
测试 scorer.py 模块的负向特征检测和综合质量评分功能。
"""

from annual_report_mda.scorer import (
    ScoreDetail,
    calculate_garbled_ratio,
    calculate_mda_score,
    calculate_quality_score,
    detect_header_noise,
    detect_negative_features,
    detect_table_residue,
)


class TestDetectTableResidue:
    """测试表格残留检测"""

    def test_no_table_residue(self):
        text = """
        公司主营业务收入同比增长10%。
        毛利率保持稳定。
        现金流充裕。
        """
        detected, count = detect_table_residue(text)
        assert not detected
        assert count < 3

    def test_table_residue_detected(self):
        text = """
        公司主营业务收入同比增长10%。
        123.45
        678.90
        234.56
        789.01
        毛利率保持稳定。
        """
        detected, count = detect_table_residue(text)
        assert detected
        assert count >= 3

    def test_table_residue_with_percentage(self):
        text = """
        业绩说明
        12.5%
        23.4%
        34.5%
        45.6%
        增长明显
        """
        detected, count = detect_table_residue(text)
        assert detected
        assert count >= 3

    def test_empty_text(self):
        detected, count = detect_table_residue("")
        assert not detected
        assert count == 0


class TestDetectHeaderNoise:
    """测试页眉干扰检测"""

    def test_no_header_noise(self):
        text = """
        公司主营业务收入同比增长10%。
        毛利率保持稳定。
        现金流充裕。
        """
        detected, headers = detect_header_noise(text)
        assert not detected
        assert len(headers) == 0

    def test_header_noise_detected(self):
        text = """
        贵州茅台2023年年度报告
        公司主营业务收入同比增长10%。
        贵州茅台2023年年度报告
        毛利率保持稳定。
        贵州茅台2023年年度报告
        现金流充裕。
        贵州茅台2023年年度报告
        展望未来发展。
        """
        detected, headers = detect_header_noise(text)
        assert detected
        assert "贵州茅台2023年年度报告" in headers

    def test_empty_text(self):
        detected, headers = detect_header_noise("")
        assert not detected
        assert len(headers) == 0


class TestCalculateGarbledRatio:
    """测试乱码比例计算"""

    def test_clean_chinese_text(self):
        text = "公司主营业务收入同比增长10%。毛利率保持稳定。"
        ratio = calculate_garbled_ratio(text)
        assert ratio < 0.01  # 几乎无乱码

    def test_mixed_text(self):
        text = "Revenue increased by 10%. 收入增长10%。"
        ratio = calculate_garbled_ratio(text)
        assert ratio < 0.05  # 正常混合文本

    def test_garbled_text(self):
        # 包含大量非法字符
        text = "公司\x00\x01\x02\x03\x04业务\x05\x06\x07\x08收入"
        ratio = calculate_garbled_ratio(text)
        assert ratio > 0.05  # 乱码比例超标

    def test_empty_text(self):
        ratio = calculate_garbled_ratio("")
        assert ratio == 0.0


class TestDetectNegativeFeatures:
    """测试综合负向特征检测"""

    def test_clean_text(self):
        text = "公司主营业务收入同比增长10%，毛利率保持稳定，现金流充裕。"
        features = detect_negative_features(text)
        assert features.table_residue_score == 0
        assert features.header_noise_score == 0
        assert features.garbled_penalty == 0

    def test_text_with_table_residue(self):
        text = """
        业绩说明
        123.45
        678.90
        234.56
        其他信息
        """
        features = detect_negative_features(text)
        assert features.table_residue_score == 15
        assert features.table_residue_lines >= 3

    def test_text_with_header_noise(self):
        text = """
        页眉文字
        正文内容1
        页眉文字
        正文内容2
        页眉文字
        正文内容3
        页眉文字
        正文内容4
        """
        features = detect_negative_features(text)
        assert features.header_noise_score == 10
        assert "页眉文字" in features.repeated_headers

    def test_empty_text(self):
        features = detect_negative_features("")
        assert features.table_residue_score == 0
        assert features.header_noise_score == 0
        assert features.garbled_ratio == 0.0
        assert features.garbled_penalty == 0


class TestCalculateQualityScore:
    """测试综合质量评分"""

    def test_high_quality_text(self):
        text = "公司主营业务收入同比增长10%，毛利率保持稳定，现金流充裕。" * 100
        score_detail = ScoreDetail(
            keyword_hit_count=5, keyword_total=7, dots_count=0, length=len(text)
        )
        result = calculate_quality_score(text, quality_flags=[], score_detail=score_detail)

        assert result.score >= 80
        assert not result.needs_review

    def test_low_quality_with_flags(self):
        text = "短文本"
        score_detail = ScoreDetail(
            keyword_hit_count=0, keyword_total=7, dots_count=0, length=len(text)
        )
        result = calculate_quality_score(
            text,
            quality_flags=["FLAG_LENGTH_ABNORMAL", "FLAG_CONTENT_MISMATCH"],
            score_detail=score_detail,
        )

        # 验证扣分项存在
        assert "flag_length_abnormal" in result.penalties
        assert "flag_content_mismatch" in result.penalties
        # 分数应该低于 100（有扣分）
        assert result.score < 100
        # 扣分总计 10 + 15 = 25，所以分数应该是 75
        assert result.score == 75

    def test_extract_failed(self):
        result = calculate_quality_score(
            text="",
            quality_flags=["FLAG_EXTRACT_FAILED"],
            score_detail=None,
        )

        assert result.score == 0
        assert result.needs_review

    def test_dots_excess_penalty(self):
        text = "公司主营业务" + "......" * 50 + "收入增长"
        score_detail = ScoreDetail(
            keyword_hit_count=3, keyword_total=7, dots_count=50, length=len(text)
        )
        result = calculate_quality_score(text, quality_flags=[], score_detail=score_detail)

        assert "dots_excess" in result.penalties
        assert result.penalties["dots_excess"] == 20

    def test_table_residue_penalty(self):
        text = (
            """
        业绩说明
        123.45
        678.90
        234.56
        其他信息正文内容
        """
            * 20
        )  # 重复以达到足够长度
        score_detail = ScoreDetail(
            keyword_hit_count=3, keyword_total=7, dots_count=0, length=len(text)
        )
        result = calculate_quality_score(text, quality_flags=[], score_detail=score_detail)

        assert "table_residue" in result.penalties
        assert result.penalties["table_residue"] == 15

    def test_multiple_penalties_accumulate(self):
        text = (
            """
        页眉干扰
        123.45
        678.90
        234.56
        页眉干扰
        正文内容
        页眉干扰
        更多内容
        页眉干扰
        结束
        """
            * 10
        )
        score_detail = ScoreDetail(
            keyword_hit_count=0, keyword_total=7, dots_count=15, length=len(text)
        )
        result = calculate_quality_score(
            text,
            quality_flags=["FLAG_CONTENT_MISMATCH"],
            score_detail=score_detail,
        )

        # 多项扣分应累加
        total_penalty = sum(result.penalties.values())
        assert result.score == max(0, 100 - total_penalty)

    def test_score_never_negative(self):
        text = "乱码\x00\x01\x02\x03" * 100
        score_detail = ScoreDetail(
            keyword_hit_count=0, keyword_total=7, dots_count=100, length=len(text)
        )
        result = calculate_quality_score(
            text,
            quality_flags=["FLAG_LENGTH_ABNORMAL", "FLAG_CONTENT_MISMATCH", "FLAG_TAIL_OVERLAP"],
            score_detail=score_detail,
        )

        assert result.score >= 0


class TestCalculateMdaScore:
    """测试原有的 MDA 评分函数"""

    def test_empty_text(self):
        score, detail = calculate_mda_score("")
        assert score == 0.0
        assert detail.length == 0

    def test_short_text(self):
        score, detail = calculate_mda_score("短文本")
        assert score == 0.0
        assert detail.length < 500

    def test_text_with_keywords(self):
        text = "公司主营业务收入同比增长，毛利率保持稳定，现金流充裕，行业展望良好。" * 50
        score, detail = calculate_mda_score(text)
        assert score > 0.5
        assert detail.keyword_hit_count > 0

    def test_text_with_dots(self):
        text = "目录......10......20......30" * 100
        score, detail = calculate_mda_score(text)
        assert detail.dots_count >= 10
        # 目录引导线过多会扣分
        assert score < 1.0
