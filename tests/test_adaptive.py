"""
自适应学习模块单元测试
"""

import tempfile
from pathlib import Path

import pytest

from annual_report_mda.adaptive.failure_patterns import FailurePatternStore
from annual_report_mda.adaptive.few_shot import FewShotSample, FewShotStore
from annual_report_mda.adaptive.strategy_weights import StrategyWeights


class TestFewShotSample:
    """FewShotSample 测试"""

    def test_sample_creation(self):
        sample = FewShotSample(
            stock_code="600519",
            year=2023,
            industry="白酒",
            toc_signature="abc123",
            start_pattern="第三节 管理层讨论与分析",
            end_pattern="第四节 公司治理",
            keywords=["收入", "同比", "毛利率"],
            quality_score=95.0,
            char_count=15000,
        )
        assert sample.stock_code == "600519"
        assert sample.year == 2023
        assert len(sample.keywords) == 3


class TestFewShotStore:
    """FewShotStore 测试"""

    def test_add_and_find(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            store_path = f.name

        try:
            store = FewShotStore(store_path)
            assert len(store) == 0

            # 添加样本
            sample1 = FewShotSample(
                stock_code="600519",
                year=2023,
                industry="白酒",
                toc_signature="abc123",
                start_pattern="第三节",
                end_pattern="第四节",
                keywords=["收入", "同比", "毛利率", "现金流"],
                quality_score=95.0,
                char_count=15000,
            )
            store.add(sample1)
            assert len(store) == 1

            # 添加另一个样本
            sample2 = FewShotSample(
                stock_code="000858",
                year=2023,
                industry="白酒",
                toc_signature="def456",
                start_pattern="第三节",
                end_pattern="第四节",
                keywords=["收入", "利润", "同比"],
                quality_score=88.0,
                char_count=12000,
            )
            store.add(sample2)
            assert len(store) == 2

            # 查找相似样本
            similar = store.find_similar(
                keywords=["收入", "同比", "利润"],
                industry="白酒",
                top_k=2,
            )
            assert len(similar) <= 2

            # 保存和重新加载
            store.save()
            store2 = FewShotStore(store_path)
            assert len(store2) == 2

        finally:
            Path(store_path).unlink(missing_ok=True)

    def test_update_existing(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            store_path = f.name

        try:
            store = FewShotStore(store_path)

            sample1 = FewShotSample(
                stock_code="600519",
                year=2023,
                industry="白酒",
                toc_signature="abc123",
                start_pattern="第三节",
                end_pattern="第四节",
                keywords=["收入"],
                quality_score=80.0,
                char_count=10000,
            )
            store.add(sample1)

            # 更新同一个样本
            sample2 = FewShotSample(
                stock_code="600519",
                year=2023,
                industry="白酒",
                toc_signature="abc123",
                start_pattern="第三节 管理层讨论与分析",  # 更新
                end_pattern="第四节 公司治理",
                keywords=["收入", "同比"],  # 更新
                quality_score=95.0,  # 更新
                char_count=15000,
            )
            store.add(sample2)

            # 仍然只有一个样本
            assert len(store) == 1

        finally:
            Path(store_path).unlink(missing_ok=True)

    def test_format_few_shot_prompt(self):
        store = FewShotStore("/nonexistent/path.json")

        samples = [
            FewShotSample(
                stock_code="600519",
                year=2023,
                industry="白酒",
                toc_signature="abc",
                start_pattern="第三节",
                end_pattern="第四节",
                keywords=["收入"],
                quality_score=95.0,
                char_count=15000,
            )
        ]

        prompt = store.format_few_shot_prompt(samples)
        assert "600519" in prompt
        assert "2023" in prompt
        assert "白酒" in prompt


class TestStrategyWeights:
    """StrategyWeights 测试"""

    def test_record_and_weight(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            store_path = f.name

        try:
            weights = StrategyWeights(store_path)

            # 记录结果
            weights.record("generic", success=True)
            weights.record("generic", success=True)
            weights.record("generic", success=False)

            weights.record("toc", success=True)
            weights.record("toc", success=False)
            weights.record("toc", success=False)

            # 检查权重
            generic_weight = weights.get_weight("generic")
            toc_weight = weights.get_weight("toc")

            # generic 成功率更高，权重应该更大
            assert generic_weight > toc_weight

            # 成功率检查
            assert weights.get_success_rate("generic") == pytest.approx(2 / 3, rel=0.01)
            assert weights.get_success_rate("toc") == pytest.approx(1 / 3, rel=0.01)

        finally:
            Path(store_path).unlink(missing_ok=True)

    def test_select_strategy(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            store_path = f.name

        try:
            weights = StrategyWeights(store_path)

            # 选择应该返回有效策略
            strategy = weights.select_strategy(["generic", "toc"])
            assert strategy in ["generic", "toc"]

            strategy = weights.select_strategy()
            assert strategy in StrategyWeights.STRATEGIES

        finally:
            Path(store_path).unlink(missing_ok=True)

    def test_priority_order(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            store_path = f.name

        try:
            weights = StrategyWeights(store_path)

            # 模拟不同成功率
            for _ in range(10):
                weights.record("generic", success=True)
            for _ in range(5):
                weights.record("toc", success=True)
            for _ in range(5):
                weights.record("toc", success=False)

            order = weights.get_priority_order()
            # generic 应该排在前面
            assert order.index("generic") < order.index("toc")

        finally:
            Path(store_path).unlink(missing_ok=True)


class TestFailurePatternStore:
    """FailurePatternStore 测试"""

    def test_add_failure(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            store_path = f.name

        try:
            store = FailurePatternStore(store_path)
            assert len(store) == 0

            # 添加失败
            store.add_failure(
                stock_code="600519",
                year=2023,
                error_type="EXTRACT_FAILED",
                error_message="无法识别目录结构",
            )
            assert len(store) == 1

            # 添加相同模式的失败
            store.add_failure(
                stock_code="000858",
                year=2023,
                error_type="EXTRACT_FAILED",
                error_message="目录解析失败",
            )
            # 应该归类到同一个模式
            assert len(store) == 1

            # 检查出现次数
            patterns = store.get_frequent_patterns(min_occurrences=2)
            assert len(patterns) == 1
            assert patterns[0].occurrences == 2

        finally:
            Path(store_path).unlink(missing_ok=True)

    def test_classify_failure(self):
        store = FailurePatternStore("/nonexistent/path.json")

        # 测试分类
        assert store._classify_failure("ERR", "无法解析目录") == "TOC_PARSE_FAILED"
        assert store._classify_failure("ERR", "边界检测失败") == "BOUNDARY_DETECTION_FAILED"
        assert store._classify_failure("ERR", "结果为空") == "EMPTY_RESULT"
        assert store._classify_failure("ERR", "文本乱码") == "ENCODING_ERROR"
        assert store._classify_failure("ERR", "请求超时") == "TIMEOUT_ERROR"
        assert store._classify_failure("ERR", "其他错误") == "OTHER_ERR"

    def test_update_exclusion_rule(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            store_path = f.name

        try:
            store = FailurePatternStore(store_path)

            store.add_failure("600519", 2023, "ERR", "目录解析失败")

            # 更新排除规则
            result = store.update_exclusion_rule(
                "TOC_PARSE_FAILED", "避免使用 TOC 解析器处理无目录页的年报"
            )
            assert result is True

            # 获取排除提示
            prompts = store.get_exclusion_prompts(min_occurrences=1)
            assert len(prompts) == 1
            assert "避免" in prompts[0]

        finally:
            Path(store_path).unlink(missing_ok=True)

    def test_stats_summary(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            store_path = f.name

        try:
            store = FailurePatternStore(store_path)

            for i in range(5):
                store.add_failure(f"60051{i}", 2023, "ERR", "目录解析失败")

            for i in range(3):
                store.add_failure(f"00085{i}", 2023, "ERR", "边界检测失败")

            summary = store.get_stats_summary()
            assert summary["total_patterns"] == 2
            assert summary["total_occurrences"] == 8

        finally:
            Path(store_path).unlink(missing_ok=True)
