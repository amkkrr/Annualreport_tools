"""M2 里程碑验收测试

验收项:
    M2-01: 全流程驱动 - 状态正确更新
    M2-02: 断点续传 - 增量逻辑有效
    M2-03: 黄金集评估 - 输出 Precision/Recall/F1
    M2-04: 质量评分 - quality_score 字段有值
    M2-05: 低分标记 - needs_review = true when score < 60
    M2-06: 负向检测 - 表格残留等扣分

注意: 在双数据库架构下，元数据存储在 SQLite，MDA 文本存储在 DuckDB。
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def temp_sqlite():
    """创建临时 SQLite 数据库用于元数据测试。"""
    from annual_report_mda import sqlite_db

    db_path = tempfile.mktemp(suffix=".db")
    conn = sqlite_db.get_connection(db_path)
    sqlite_db.init_db(conn)
    yield conn
    conn.close()
    Path(db_path).unlink(missing_ok=True)


@pytest.fixture
def temp_duckdb():
    """创建临时 DuckDB 数据库用于 MDA 文本测试。"""
    from annual_report_mda.db import init_db

    db_path = tempfile.mktemp(suffix=".duckdb")
    conn = init_db(db_path, attach_sqlite=False)
    yield conn
    conn.close()
    Path(db_path).unlink(missing_ok=True)


class TestM2_01_FullPipelineStatusUpdates:
    """M2-01: 全流程驱动 - 验证状态流转"""

    def test_status_fields_exist_in_reports_table(self, temp_sqlite):
        """验证 reports 表包含状态字段"""
        result = temp_sqlite.execute("PRAGMA table_info(reports)").fetchall()
        columns = {row[1] for row in result}

        assert "download_status" in columns
        assert "convert_status" in columns
        assert "extract_status" in columns

    def test_status_default_values(self, temp_sqlite):
        """验证状态默认值为 pending"""
        from annual_report_mda import sqlite_db

        sqlite_db.insert_report(
            temp_sqlite,
            stock_code="600519",
            year=2023,
            url="https://example.com/report.pdf",
        )

        report = sqlite_db.get_report(temp_sqlite, "600519", 2023)
        assert report["download_status"] == "pending"
        assert report["convert_status"] == "pending"
        assert report["extract_status"] == "pending"

    def test_status_updates_correctly(self, temp_sqlite):
        """验证状态更新正确"""
        from annual_report_mda import sqlite_db

        sqlite_db.insert_report(
            temp_sqlite,
            stock_code="600519",
            year=2023,
            url="https://example.com/report.pdf",
        )

        # 更新下载状态
        sqlite_db.update_report_status(
            temp_sqlite,
            stock_code="600519",
            year=2023,
            download_status="success",
            pdf_path="/data/600519_2023.pdf",
            downloaded_at=True,
        )

        report = sqlite_db.get_report(temp_sqlite, "600519", 2023)
        assert report["download_status"] == "success"
        assert report["pdf_path"] == "/data/600519_2023.pdf"
        assert report["downloaded_at"] is not None

        # 更新转换状态
        sqlite_db.update_report_status(
            temp_sqlite,
            stock_code="600519",
            year=2023,
            convert_status="success",
            txt_path="/data/600519_2023.txt",
            converted_at=True,
        )

        report = sqlite_db.get_report(temp_sqlite, "600519", 2023)
        assert report["convert_status"] == "success"
        assert report["txt_path"] == "/data/600519_2023.txt"


class TestM2_02_ResumeFromInterruption:
    """M2-02: 断点续传 - 验证增量逻辑"""

    def test_incremental_skip_already_processed(self, temp_duckdb):
        """验证已处理的记录被跳过"""
        from annual_report_mda.data_manager import (
            MDAUpsertRecord,
            should_skip_incremental,
            upsert_mda_text,
        )

        # 插入一条已处理的记录（char_count >= 500 才算成功处理）
        mda_text = "测试文本内容" * 100  # 600 字符
        record = MDAUpsertRecord(
            stock_code="600519",
            year=2023,
            mda_raw=mda_text,
            char_count=len(mda_text),
            page_index_start=10,
            page_index_end=30,
            page_count=20,
            source_path="/data/600519_2023.txt",
            source_sha256="abc123",
            extractor_version="0.3.0",
            quality_score=85,
            needs_review=False,
        )
        upsert_mda_text(temp_duckdb, record)

        # 验证增量检查返回 True（应跳过）
        should_skip = should_skip_incremental(
            temp_duckdb,
            stock_code="600519",
            year=2023,
            source_sha256="abc123",
        )
        assert should_skip is True

    def test_incremental_process_new_file(self, temp_duckdb):
        """验证新文件不被跳过"""
        from annual_report_mda.data_manager import should_skip_incremental

        # 没有记录时应返回 False（不跳过）
        should_skip = should_skip_incremental(
            temp_duckdb,
            stock_code="600519",
            year=2023,
            source_sha256="new_hash_123",
        )
        assert should_skip is False

    def test_incremental_reprocess_changed_file(self, temp_duckdb):
        """验证修改后的文件被重新处理"""
        from annual_report_mda.data_manager import (
            MDAUpsertRecord,
            should_skip_incremental,
            upsert_mda_text,
        )

        # 插入一条记录
        record = MDAUpsertRecord(
            stock_code="600519",
            year=2023,
            mda_raw="测试文本" * 100,
            char_count=400,
            page_index_start=10,
            page_index_end=30,
            page_count=20,
            source_path="/data/600519_2023.txt",
            source_sha256="old_hash",
            extractor_version="0.3.0",
            quality_score=85,
            needs_review=False,
        )
        upsert_mda_text(temp_duckdb, record)

        # 使用不同的 hash 检查，应返回 False（不跳过）
        should_skip = should_skip_incremental(
            temp_duckdb,
            stock_code="600519",
            year=2023,
            source_sha256="new_hash_456",
        )
        assert should_skip is False


class TestM2_03_GoldenSetEvaluation:
    """M2-03: 黄金集评估 - 验证评估脚本"""

    @pytest.mark.skipif(
        not Path("data/golden_set_fixed_v5.json").exists(),
        reason="黄金数据集不存在 (CI 环境)",
    )
    def test_golden_set_file_exists(self):
        """验证黄金数据集文件存在"""
        golden_path = Path("data/golden_set_fixed_v5.json")
        assert golden_path.exists(), f"黄金数据集不存在: {golden_path}"

    @pytest.mark.skipif(
        not Path("data/golden_set_fixed_v5.json").exists(),
        reason="黄金数据集不存在 (CI 环境)",
    )
    def test_golden_set_structure(self):
        """验证黄金数据集结构正确"""
        golden_path = Path("data/golden_set_fixed_v5.json")
        with open(golden_path, encoding="utf-8") as f:
            data = json.load(f)

        assert "samples" in data
        assert len(data["samples"]) > 0

        # 检查第一个样本的结构
        sample = data["samples"][0]
        assert "id" in sample
        assert "stock_code" in sample
        assert "year" in sample
        assert "source_txt_path" in sample
        assert "golden_boundary" in sample

    @pytest.mark.skipif(
        not Path("data/golden_set_fixed_v5.json").exists(),
        reason="黄金数据集不存在 (CI 环境)",
    )
    def test_golden_set_has_valid_samples(self):
        """验证黄金数据集有足够的有效样本"""
        golden_path = Path("data/golden_set_fixed_v5.json")
        with open(golden_path, encoding="utf-8") as f:
            data = json.load(f)

        valid_samples = [s for s in data["samples"] if "error" not in s]
        assert len(valid_samples) >= 50, f"有效样本数不足: {len(valid_samples)}"

    def test_evaluation_script_importable(self):
        """验证评估脚本可导入"""
        import sys

        sys.path.insert(0, "scripts")
        try:
            from evaluate_extraction import evaluate_single, run_evaluation

            assert callable(evaluate_single)
            assert callable(run_evaluation)
        finally:
            sys.path.pop(0)


class TestM2_04_QualityScorePopulated:
    """M2-04: 质量评分 - 验证 quality_score 字段"""

    def test_mda_text_has_quality_score_column(self, temp_duckdb):
        """验证 mda_text 表包含 quality_score 列"""
        result = temp_duckdb.execute("PRAGMA table_info(mda_text)").fetchall()
        columns = {row[1] for row in result}

        assert "quality_score" in columns

    def test_quality_score_range(self, temp_duckdb):
        """验证评分范围为 0-100"""
        from annual_report_mda.scorer import ScoreDetail, calculate_quality_score

        # 高质量文本
        high_quality = "公司主营业务收入同比增长，毛利率保持稳定，现金流充裕。" * 100
        score_detail = ScoreDetail(
            keyword_hit_count=5, keyword_total=7, dots_count=0, length=len(high_quality)
        )
        result = calculate_quality_score(high_quality, quality_flags=[], score_detail=score_detail)

        assert 0 <= result.score <= 100

    def test_quality_score_stored_in_db(self, temp_duckdb):
        """验证评分可以正确存储到数据库"""
        from annual_report_mda.data_manager import MDAUpsertRecord, upsert_mda_text

        record = MDAUpsertRecord(
            stock_code="600519",
            year=2023,
            mda_raw="测试文本" * 100,
            char_count=400,
            page_index_start=10,
            page_index_end=30,
            page_count=20,
            source_path="/data/600519_2023.txt",
            source_sha256="test_hash",
            extractor_version="0.3.0",
            quality_score=85,
            needs_review=False,
        )
        upsert_mda_text(temp_duckdb, record)

        result = temp_duckdb.execute(
            "SELECT quality_score FROM mda_text WHERE stock_code = '600519' AND year = 2023"
        ).fetchone()

        assert result is not None
        assert result[0] == 85


class TestM2_05_LowScoreNeedsReview:
    """M2-05: 低分标记 - 验证 needs_review 逻辑"""

    def test_needs_review_threshold(self):
        """验证低分阈值为 60"""
        from annual_report_mda.scorer import NEEDS_REVIEW_THRESHOLD

        assert NEEDS_REVIEW_THRESHOLD == 60

    def test_low_score_triggers_needs_review(self):
        """验证低分触发 needs_review"""
        from annual_report_mda.scorer import calculate_quality_score

        # 空文本应该得 0 分并需要审核
        result = calculate_quality_score(
            text="",
            quality_flags=["FLAG_EXTRACT_FAILED"],
            score_detail=None,
        )

        assert result.score == 0
        assert result.needs_review is True

    def test_high_score_no_review(self):
        """验证高分不需要审核"""
        from annual_report_mda.scorer import ScoreDetail, calculate_quality_score

        text = "公司主营业务收入同比增长，毛利率保持稳定，现金流充裕。" * 100
        score_detail = ScoreDetail(
            keyword_hit_count=5, keyword_total=7, dots_count=0, length=len(text)
        )
        result = calculate_quality_score(text, quality_flags=[], score_detail=score_detail)

        assert result.score >= 60
        assert result.needs_review is False

    def test_needs_review_stored_in_db(self, temp_duckdb):
        """验证 needs_review 可以正确存储到数据库"""
        from annual_report_mda.data_manager import MDAUpsertRecord, upsert_mda_text

        # 低分记录
        record = MDAUpsertRecord(
            stock_code="600519",
            year=2023,
            mda_raw="短文本",
            char_count=6,
            page_index_start=10,
            page_index_end=11,
            page_count=1,
            source_path="/data/600519_2023.txt",
            source_sha256="test_hash",
            extractor_version="0.3.0",
            quality_score=30,
            needs_review=True,
        )
        upsert_mda_text(temp_duckdb, record)

        result = temp_duckdb.execute(
            "SELECT needs_review FROM mda_text WHERE stock_code = '600519' AND year = 2023"
        ).fetchone()

        assert result is not None
        assert result[0] is True


class TestM2_06_NegativeFeatureDetection:
    """M2-06: 负向检测 - 验证表格残留等扣分"""

    def test_table_residue_detection(self):
        """验证表格残留检测"""
        from annual_report_mda.scorer import detect_table_residue

        text_with_table = """
        业绩说明
        123.45
        678.90
        234.56
        789.01
        其他内容
        """
        detected, count = detect_table_residue(text_with_table)

        assert detected is True
        assert count >= 3

    def test_header_noise_detection(self):
        """验证页眉干扰检测"""
        from annual_report_mda.scorer import detect_header_noise

        text_with_header = """
        年度报告
        正文内容1
        年度报告
        正文内容2
        年度报告
        正文内容3
        年度报告
        正文内容4
        """
        detected, headers = detect_header_noise(text_with_header)

        assert detected is True
        assert "年度报告" in headers

    def test_garbled_text_detection(self):
        """验证乱码检测"""
        from annual_report_mda.scorer import calculate_garbled_ratio

        clean_text = "公司主营业务收入同比增长。"
        garbled_text = "公司\x00\x01\x02\x03业务\x05\x06\x07\x08收入"

        clean_ratio = calculate_garbled_ratio(clean_text)
        garbled_ratio = calculate_garbled_ratio(garbled_text)

        assert clean_ratio < 0.05
        assert garbled_ratio > 0.05

    def test_negative_features_reduce_score(self):
        """验证负向特征降低评分"""
        from annual_report_mda.scorer import ScoreDetail, calculate_quality_score

        # 含表格残留的文本
        text_with_issues = (
            """
        业绩说明
        123.45
        678.90
        234.56
        其他信息正文内容
        """
            * 20
        )

        score_detail = ScoreDetail(
            keyword_hit_count=3, keyword_total=7, dots_count=0, length=len(text_with_issues)
        )
        result = calculate_quality_score(
            text_with_issues, quality_flags=[], score_detail=score_detail
        )

        assert "table_residue" in result.penalties
        assert result.penalties["table_residue"] == 15

    def test_penalties_recorded_in_quality_detail(self, temp_duckdb):
        """验证扣分明细记录在 quality_detail 中"""
        from annual_report_mda.scorer import ScoreDetail, calculate_quality_score

        text = "目录" + "......" * 50 + "正文"
        score_detail = ScoreDetail(
            keyword_hit_count=0, keyword_total=7, dots_count=50, length=len(text)
        )
        result = calculate_quality_score(text, quality_flags=[], score_detail=score_detail)

        assert "dots_excess" in result.penalties
        assert result.penalties["dots_excess"] == 20


class TestM2_Q1_ExtractionAccuracy:
    """M2-Q1: 提取准确率 - F1 >= 0.85"""

    @pytest.mark.skip(reason="需要完整运行评估脚本，在集成测试中验证")
    def test_f1_score_threshold(self):
        """验证 F1 分数达标"""
        pass


class TestM2_Q2_ExtractionPerformance:
    """M2-Q2: 提取性能 - < 5s"""

    def test_scorer_performance(self):
        """验证评分器性能"""
        import time

        from annual_report_mda.scorer import ScoreDetail, calculate_quality_score

        text = "公司主营业务收入同比增长，毛利率保持稳定。" * 1000
        score_detail = ScoreDetail(
            keyword_hit_count=5, keyword_total=7, dots_count=0, length=len(text)
        )

        start = time.time()
        for _ in range(100):
            calculate_quality_score(text, quality_flags=[], score_detail=score_detail)
        elapsed = time.time() - start

        # 100 次评分应在 1 秒内完成
        assert elapsed < 1.0, f"评分器性能不达标: {elapsed:.2f}s / 100次"


class TestM2_Q3_ScoringConsistency:
    """M2-Q3: 评分一致性"""

    def test_same_text_same_score(self):
        """验证相同文本多次评分结果一致"""
        from annual_report_mda.scorer import ScoreDetail, calculate_quality_score

        text = "公司主营业务收入同比增长，毛利率保持稳定，现金流充裕。" * 50
        score_detail = ScoreDetail(
            keyword_hit_count=5, keyword_total=7, dots_count=0, length=len(text)
        )

        scores = []
        for _ in range(10):
            result = calculate_quality_score(text, quality_flags=[], score_detail=score_detail)
            scores.append(result.score)

        # 所有评分应相同
        assert len(set(scores)) == 1, f"评分不一致: {scores}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
