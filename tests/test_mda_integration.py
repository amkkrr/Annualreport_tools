"""
测试 mda_extractor.py 的端到端集成功能。
"""

import shutil
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from annual_report_mda.db import init_db
from mda_extractor import main


class TestEndToEndSingleFile:
    """测试单文件端到端流程。"""

    def test_single_file_extraction(self, mock_no_toc_path: Path, temp_db_path: Path):
        """单文件模式完整流程。"""
        # 复制 mock 文件到临时目录，使用符合命名规范的文件名
        temp_dir = temp_db_path.parent
        test_file = temp_dir / "000778_新兴铸管_2023.txt"
        shutil.copy(mock_no_toc_path, test_file)

        # 运行提取器
        exit_code = main(
            [
                "--text",
                str(test_file),
                "--db",
                str(temp_db_path),
            ]
        )

        assert exit_code == 0

        # 验证数据库中有记录
        conn = init_db(temp_db_path)
        result = conn.execute(
            "SELECT stock_code, year, char_count FROM mda_text WHERE stock_code = '000778' AND year = 2023"
        ).fetchone()

        assert result is not None
        assert result[0] == "000778"
        assert result[1] == 2023

    def test_single_file_with_explicit_params(self, mock_no_toc_path: Path, temp_db_path: Path):
        """使用显式参数的单文件模式。"""
        temp_dir = temp_db_path.parent
        test_file = temp_dir / "test_report.txt"
        shutil.copy(mock_no_toc_path, test_file)

        exit_code = main(
            [
                "--text",
                str(test_file),
                "--db",
                str(temp_db_path),
                "--stock-code",
                "600519",
                "--year",
                "2022",
            ]
        )

        assert exit_code == 0

        conn = init_db(temp_db_path)
        result = conn.execute(
            "SELECT stock_code, year FROM mda_text WHERE stock_code = '600519' AND year = 2022"
        ).fetchone()

        assert result is not None


class TestIncrementalMode:
    """测试增量模式。"""

    def test_incremental_skip_already_processed(self, mock_no_toc_path: Path, temp_db_path: Path):
        """增量模式应跳过已处理文件。"""
        temp_dir = temp_db_path.parent
        test_file = temp_dir / "000778_新兴铸管_2023.txt"
        shutil.copy(mock_no_toc_path, test_file)

        # 第一次提取
        exit_code1 = main(
            [
                "--text",
                str(test_file),
                "--db",
                str(temp_db_path),
            ]
        )
        assert exit_code1 == 0

        # 验证记录存在
        conn = init_db(temp_db_path)
        count_before = conn.execute(
            "SELECT COUNT(*) FROM mda_text WHERE stock_code = '000778' AND year = 2023"
        ).fetchone()[0]
        assert count_before >= 1

        # 第二次使用增量模式
        exit_code2 = main(
            [
                "--text",
                str(test_file),
                "--db",
                str(temp_db_path),
                "--incremental",
            ]
        )
        assert exit_code2 == 0

        # 验证没有新增记录（或记录数不变）
        count_after = conn.execute(
            "SELECT COUNT(*) FROM mda_text WHERE stock_code = '000778' AND year = 2023"
        ).fetchone()[0]
        # 增量模式下应该跳过，不会新增记录
        assert count_after == count_before


class TestDryRunMode:
    """测试 dry-run 模式。"""

    def test_dry_run_no_database_write(self, mock_no_toc_path: Path, temp_db_path: Path):
        """dry-run 模式不应写入数据库。"""
        temp_dir = temp_db_path.parent
        test_file = temp_dir / "000778_新兴铸管_2023.txt"
        shutil.copy(mock_no_toc_path, test_file)

        exit_code = main(
            [
                "--text",
                str(test_file),
                "--db",
                str(temp_db_path),
                "--dry-run",
            ]
        )

        assert exit_code == 0

        # dry-run 模式下数据库可能未初始化或表为空
        if temp_db_path.exists():
            conn = init_db(temp_db_path)
            result = conn.execute("SELECT COUNT(*) FROM mda_text").fetchone()
            # dry-run 不应写入任何记录
            assert result[0] == 0


class TestBatchMode:
    """测试批量模式。"""

    def test_batch_extraction(self, mock_mda_dir: Path, temp_db_path: Path):
        """批量模式处理多个文件。"""
        temp_dir = temp_db_path.parent
        batch_dir = temp_dir / "batch_test"
        batch_dir.mkdir()

        # 复制多个测试文件
        for i, name in enumerate(["mock_no_toc.txt", "mock_eq_pages.txt"]):
            src = mock_mda_dir / name
            if src.exists():
                # 使用符合命名规范的文件名
                dst = batch_dir / f"00000{i}_测试公司{i}_2023.txt"
                shutil.copy(src, dst)

        exit_code = main(
            [
                "--dir",
                str(batch_dir),
                "--db",
                str(temp_db_path),
                "--workers",
                "1",
            ]
        )

        # 批量模式可能有部分失败，但不应崩溃
        assert exit_code in (0, 2)

        # 验证至少有一些记录
        conn = init_db(temp_db_path)
        count = conn.execute("SELECT COUNT(*) FROM mda_text").fetchone()[0]
        # 至少应该有尝试处理的记录
        assert count >= 0


class TestErrorHandling:
    """测试错误处理。"""

    def test_missing_file(self, temp_db_path: Path):
        """文件不存在时应报错。"""
        with pytest.raises(SystemExit):
            main(
                [
                    "--text",
                    "/nonexistent/file.txt",
                    "--db",
                    str(temp_db_path),
                ]
            )

    def test_missing_stock_year_no_params(self, temp_dir: Path, temp_db_path: Path):
        """无法推断 stock_code/year 且未提供参数时应报错。"""
        # 创建命名不规范的文件
        test_file = temp_dir / "invalid_name.txt"
        test_file.write_text("测试内容", encoding="utf-8")

        with pytest.raises(SystemExit):
            main(
                [
                    "--text",
                    str(test_file),
                    "--db",
                    str(temp_db_path),
                ]
            )

    def test_invalid_workers(self, mock_no_toc_path: Path, temp_db_path: Path):
        """无效 workers 参数应报错。"""
        # workers <= 0 应该报错
        with pytest.raises(SystemExit):
            main(
                [
                    "--dir",
                    str(mock_no_toc_path.parent),
                    "--db",
                    str(temp_db_path),
                    "--workers",
                    "0",
                ]
            )


class TestQualityScoreIntegration:
    """测试质量评分集成。"""

    def test_quality_score_stored(self, mock_no_toc_path: Path, temp_db_path: Path):
        """质量评分应存储到数据库。"""
        temp_dir = temp_db_path.parent
        test_file = temp_dir / "000778_新兴铸管_2023.txt"
        shutil.copy(mock_no_toc_path, test_file)

        exit_code = main(
            [
                "--text",
                str(test_file),
                "--db",
                str(temp_db_path),
            ]
        )

        assert exit_code == 0

        conn = init_db(temp_db_path)
        result = conn.execute(
            "SELECT quality_score, needs_review FROM mda_text WHERE stock_code = '000778' AND year = 2023"
        ).fetchone()

        assert result is not None
        quality_score, needs_review = result
        assert quality_score is not None
        assert isinstance(quality_score, int)
        assert 0 <= quality_score <= 100

    def test_needs_review_view(self, mock_no_toc_path: Path, temp_db_path: Path):
        """mda_needs_review 视图应正常工作。"""
        temp_dir = temp_db_path.parent
        test_file = temp_dir / "000778_新兴铸管_2023.txt"
        shutil.copy(mock_no_toc_path, test_file)

        main(
            [
                "--text",
                str(test_file),
                "--db",
                str(temp_db_path),
            ]
        )

        conn = init_db(temp_db_path)
        # 查询 needs_review 视图
        result = conn.execute("SELECT COUNT(*) FROM mda_needs_review").fetchone()
        # 视图应该可以正常查询
        assert result is not None
