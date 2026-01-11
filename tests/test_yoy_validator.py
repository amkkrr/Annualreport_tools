"""
测试 L3 时序校验功能（FLAG_YOY_CHANGE_HIGH）。
"""
import pytest

from annual_report_mda.scorer import (
    calculate_text_similarity,
    detect_yoy_change,
    YOY_SIMILARITY_THRESHOLD,
)


class TestCalculateTextSimilarity:
    """测试文本相似度计算。"""

    def test_identical_texts(self):
        """完全相同的文本应返回 1.0。"""
        text = "公司主营业务收入同比增长10%，毛利率保持稳定。"
        similarity = calculate_text_similarity(text, text)
        assert similarity == 1.0

    def test_empty_texts(self):
        """空文本应返回 0.0。"""
        assert calculate_text_similarity("", "") == 0.0
        assert calculate_text_similarity("abc", "") == 0.0
        assert calculate_text_similarity("", "abc") == 0.0

    def test_similar_texts(self):
        """相似文本应返回较高相似度。"""
        text1 = "公司主营业务收入同比增长10%，毛利率保持稳定在35%。"
        text2 = "公司主营业务收入同比增长15%，毛利率保持稳定在36%。"
        similarity = calculate_text_similarity(text1, text2)
        assert similarity > 0.5  # 应该较相似

    def test_different_texts(self):
        """完全不同的文本应返回较低相似度。"""
        text1 = "公司主营业务收入同比增长"
        text2 = "今天天气很好阳光明媚"
        similarity = calculate_text_similarity(text1, text2)
        assert similarity < 0.3  # 应该不相似

    def test_short_texts(self):
        """短文本也能计算相似度。"""
        text1 = "AB"
        text2 = "AB"
        similarity = calculate_text_similarity(text1, text2)
        assert similarity == 1.0

        text3 = "AB"
        text4 = "CD"
        similarity2 = calculate_text_similarity(text3, text4)
        assert similarity2 < 1.0

    def test_whitespace_normalized(self):
        """空白字符应被忽略。"""
        text1 = "公司 主营 业务"
        text2 = "公司主营业务"
        similarity = calculate_text_similarity(text1, text2)
        assert similarity == 1.0

    def test_long_similar_texts(self):
        """长文本的相似度计算。"""
        base = "公司主营业务收入同比增长，毛利率保持稳定，现金流充裕。" * 100
        text1 = base
        text2 = base + "额外的一些内容。"
        similarity = calculate_text_similarity(text1, text2)
        # 由于添加了少量内容，相似度应该较高但不一定 > 0.9
        assert similarity > 0.7  # 大部分相同


class TestDetectYoyChange:
    """测试年际变化检测。"""

    def test_no_prev_year_data(self):
        """无上年数据时应跳过校验。"""
        is_abnormal, similarity = detect_yoy_change("当年文本", None)
        assert is_abnormal is False
        assert similarity == 1.0

    def test_similar_years(self):
        """相似的两年文本不应触发异常。"""
        current = "公司主营业务收入同比增长10%，毛利率保持稳定在35%。行业发展良好。" * 50
        prev = "公司主营业务收入同比增长8%，毛利率保持稳定在34%。行业发展稳定。" * 50
        is_abnormal, similarity = detect_yoy_change(current, prev)
        assert is_abnormal is False
        assert similarity > YOY_SIMILARITY_THRESHOLD

    def test_different_years(self):
        """差异较大的两年文本应触发异常。"""
        current = "今天天气很好，阳光明媚，适合出门游玩。" * 50
        prev = "公司主营业务收入同比增长，毛利率保持稳定，现金流充裕。" * 50
        is_abnormal, similarity = detect_yoy_change(current, prev)
        assert is_abnormal is True
        assert similarity < YOY_SIMILARITY_THRESHOLD

    def test_empty_current_text(self):
        """当年文本为空时。"""
        is_abnormal, similarity = detect_yoy_change("", "上年文本")
        assert is_abnormal is False
        assert similarity == 0.0

    def test_empty_prev_text(self):
        """上年文本为空时。"""
        is_abnormal, similarity = detect_yoy_change("当年文本", "")
        assert is_abnormal is False
        assert similarity == 0.0

    def test_custom_threshold(self):
        """自定义阈值。"""
        current = "ABCDEFGHIJ" * 10
        prev = "ABCDEFGHIJ" * 10 + "KLMNOP" * 10

        # 使用严格阈值
        is_abnormal_strict, similarity = detect_yoy_change(
            current, prev, similarity_threshold=0.95
        )

        # 使用宽松阈值
        is_abnormal_loose, _ = detect_yoy_change(
            current, prev, similarity_threshold=0.1
        )

        # 严格阈值更容易触发异常
        assert similarity < 0.95
        assert is_abnormal_strict is True
        assert is_abnormal_loose is False

    def test_realistic_mda_texts(self, sample_mda_text: str):
        """使用真实 MD&A 样本测试。"""
        # 模拟同一公司连续两年的文本（应该较相似）
        current_year = sample_mda_text
        prev_year = sample_mda_text.replace("15%", "12%").replace("35%", "33%")

        is_abnormal, similarity = detect_yoy_change(current_year, prev_year)

        # 同一公司连续年份应该较相似
        assert similarity > 0.7
        assert is_abnormal is False


class TestYoyValidatorIntegration:
    """时序校验集成测试。"""

    def test_threshold_boundary(self):
        """测试阈值边界情况。"""
        # 构造刚好在阈值附近的文本
        base = "公司主营业务" * 100

        # 完全相同
        is_abnormal1, sim1 = detect_yoy_change(base, base)
        assert is_abnormal1 is False
        assert sim1 == 1.0

        # 略有不同
        modified = base[:len(base) // 2] + "收入增长" * 50
        is_abnormal2, sim2 = detect_yoy_change(base, modified)
        # 相似度应该在某个范围内
        assert 0 < sim2 < 1

    def test_flag_name_consistency(self):
        """验证 FLAG 名称与 scorer.py 中定义一致。"""
        from annual_report_mda.scorer import _FLAG_PENALTIES

        assert "FLAG_YOY_CHANGE_HIGH" in _FLAG_PENALTIES
        assert _FLAG_PENALTIES["FLAG_YOY_CHANGE_HIGH"] == 5
