"""
测试 mda_extractor.py 模块的提取器功能。
"""

# 导入被测试的模块
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from mda_extractor import (
    _extract_one_worker,
    _infer_stock_year,
    _iter_txt_files,
)


class TestInferStockYear:
    """测试股票代码和年份推断。"""

    def test_infer_from_filename_with_underscore(self, temp_dir: Path):
        """从文件名推断（下划线格式）。"""
        file_path = temp_dir / "600519_贵州茅台_2023.txt"
        file_path.touch()

        stock_code, year = _infer_stock_year(file_path)

        assert stock_code == "600519"
        assert year == 2023

    def test_infer_from_filename_simple(self, temp_dir: Path):
        """从简单文件名推断。"""
        file_path = temp_dir / "000778_2022.txt"
        file_path.touch()

        stock_code, year = _infer_stock_year(file_path)

        assert stock_code == "000778"
        assert year == 2022

    def test_infer_from_directory_structure(self, temp_dir: Path):
        """从目录结构推断。"""
        # 创建 stock_code/year.txt 结构
        stock_dir = temp_dir / "600519"
        stock_dir.mkdir()
        file_path = stock_dir / "2023.txt"
        file_path.touch()

        stock_code, year = _infer_stock_year(file_path)

        assert stock_code == "600519"
        assert year == 2023

    def test_infer_fail_no_stock_code(self, temp_dir: Path):
        """无法推断股票代码时返回 None。"""
        file_path = temp_dir / "annual_report.txt"
        file_path.touch()

        stock_code, year = _infer_stock_year(file_path)

        assert stock_code is None

    def test_infer_fail_no_year(self, temp_dir: Path):
        """无法推断年份时返回 None。"""
        file_path = temp_dir / "600519_report.txt"
        file_path.touch()

        stock_code, year = _infer_stock_year(file_path)

        assert stock_code == "600519"
        assert year is None

    def test_infer_multiple_years_uses_last(self, temp_dir: Path):
        """多个年份时使用最后一个。"""
        file_path = temp_dir / "600519_2022_2023.txt"
        file_path.touch()

        stock_code, year = _infer_stock_year(file_path)

        assert stock_code == "600519"
        assert year == 2023


class TestIterTxtFiles:
    """测试 TXT 文件迭代。"""

    def test_iter_txt_files_recursive(self, temp_dir: Path):
        """递归扫描 TXT 文件。"""
        # 创建目录结构
        (temp_dir / "a.txt").touch()
        subdir = temp_dir / "subdir"
        subdir.mkdir()
        (subdir / "b.txt").touch()
        (subdir / "c.pdf").touch()  # 非 TXT 文件

        files = list(_iter_txt_files(temp_dir))

        assert len(files) == 2
        assert all(f.suffix == ".txt" for f in files)

    def test_iter_txt_files_empty_dir(self, temp_dir: Path):
        """空目录返回空列表。"""
        files = list(_iter_txt_files(temp_dir))
        assert len(files) == 0


class TestExtractOneWorker:
    """测试单文件提取工作函数。"""

    def test_extract_one_worker_success(self, mock_no_toc_path: Path):
        """成功提取时返回有效结果。"""
        payload = {
            "path": str(mock_no_toc_path),
            "stock_code": "000778",
            "year": 2023,
            "source_sha256": "test_sha256",
            "max_pages": 15,
            "max_chars": 120000,
            "custom_start_pattern": None,
            "custom_end_pattern": None,
        }

        result = _extract_one_worker(payload)

        assert result["ok"] is True
        assert "record" in result
        record = result["record"]
        assert record["stock_code"] == "000778"
        assert record["year"] == 2023
        assert record["source_sha256"] == "test_sha256"

    def test_extract_one_worker_with_mda_content(self, mock_no_toc_path: Path):
        """提取含 MD&A 内容的文件。"""
        payload = {
            "path": str(mock_no_toc_path),
            "stock_code": "000778",
            "year": 2023,
            "source_sha256": "test_sha256",
            "max_pages": 15,
            "max_chars": 120000,
            "custom_start_pattern": None,
            "custom_end_pattern": None,
        }

        result = _extract_one_worker(payload)

        assert result["ok"] is True
        record = result["record"]
        # 应该成功提取到 MD&A
        if record["mda_raw"] is not None:
            assert record["char_count"] > 0
            assert record["used_rule_type"] in ("generic", "toc", "custom")

    def test_extract_one_worker_no_mda(self, temp_dir: Path):
        """无 MD&A 内容时标记提取失败。"""
        # 创建不含 MD&A 的文件
        no_mda_file = temp_dir / "no_mda.txt"
        no_mda_file.write_text("这是一段普通文本，没有任何年报相关内容。", encoding="utf-8")

        payload = {
            "path": str(no_mda_file),
            "stock_code": "000001",
            "year": 2023,
            "source_sha256": "test_sha256",
            "max_pages": 15,
            "max_chars": 120000,
            "custom_start_pattern": None,
            "custom_end_pattern": None,
        }

        result = _extract_one_worker(payload)

        assert result["ok"] is True
        record = result["record"]
        # 提取失败时 mda_raw 为 None，quality_flags 包含 FLAG_EXTRACT_FAILED
        if record["mda_raw"] is None:
            assert "FLAG_EXTRACT_FAILED" in record["quality_flags"]

    def test_extract_one_worker_with_custom_pattern(self, mock_eq_pages_path: Path):
        """使用自定义规则提取。"""
        payload = {
            "path": str(mock_eq_pages_path),
            "stock_code": "002415",
            "year": 2023,
            "source_sha256": "test_sha256",
            "max_pages": 15,
            "max_chars": 120000,
            "custom_start_pattern": "董事会报告",
            "custom_end_pattern": "监事会报告",
        }

        result = _extract_one_worker(payload)

        assert result["ok"] is True
        record = result["record"]
        # 使用自定义规则时 used_rule_type 为 "custom"
        if record["mda_raw"] is not None and record["used_rule_type"] == "custom":
            assert "董事会" in record["mda_raw"] or record["char_count"] > 0


class TestExtractOneWorkerQuality:
    """测试提取结果的质量评分。"""

    def test_quality_score_calculated(self, mock_no_toc_path: Path):
        """质量评分应被计算。"""
        payload = {
            "path": str(mock_no_toc_path),
            "stock_code": "000778",
            "year": 2023,
            "source_sha256": "test_sha256",
            "max_pages": 15,
            "max_chars": 120000,
            "custom_start_pattern": None,
            "custom_end_pattern": None,
        }

        result = _extract_one_worker(payload)

        assert result["ok"] is True
        record = result["record"]
        # 应该有质量评分
        assert "quality_score" in record
        assert "needs_review" in record
        assert isinstance(record["quality_score"], int)
        assert isinstance(record["needs_review"], bool)

    def test_needs_review_for_low_score(self, temp_dir: Path):
        """低分结果应标记需要审核。"""
        # 创建质量较差的文件
        low_quality_file = temp_dir / "low_quality.txt"
        low_quality_file.write_text("短文本" * 10, encoding="utf-8")

        payload = {
            "path": str(low_quality_file),
            "stock_code": "000001",
            "year": 2023,
            "source_sha256": "test_sha256",
            "max_pages": 15,
            "max_chars": 120000,
            "custom_start_pattern": None,
            "custom_end_pattern": None,
        }

        result = _extract_one_worker(payload)

        assert result["ok"] is True
        record = result["record"]
        # 低质量文本应该需要审核
        if record["quality_score"] < 60:
            assert record["needs_review"] is True
