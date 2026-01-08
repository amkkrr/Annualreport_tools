"""DuckDB 核心化单元测试 (M1 验证)

测试范围:
    - M1-03: companies 表 CRUD
    - M1-04: reports 表 CRUD 及增量逻辑
    - M1-05: 视图查询
    - M1-06: 迁移脚本幂等性
"""

from __future__ import annotations

import tempfile
from datetime import date
from pathlib import Path

import pytest


@pytest.fixture
def temp_db():
    """创建临时数据库用于测试。"""
    from annual_report_mda.db import init_db

    # 使用 mktemp 获取临时路径（不创建文件）
    db_path = tempfile.mktemp(suffix=".duckdb")

    conn = init_db(db_path)
    yield conn
    conn.close()
    Path(db_path).unlink(missing_ok=True)


class TestCompaniesTable:
    """M1-03: companies 表测试"""

    def test_upsert_company_insert(self, temp_db):
        """测试新增公司"""
        from annual_report_mda.db import upsert_company, get_company

        upsert_company(
            temp_db,
            stock_code="600519",
            short_name="贵州茅台",
            full_name="贵州茅台酒股份有限公司",
            plate="sh",
        )

        company = get_company(temp_db, "600519")
        assert company is not None
        assert company["stock_code"] == "600519"
        assert company["short_name"] == "贵州茅台"
        assert company["full_name"] == "贵州茅台酒股份有限公司"
        assert company["plate"] == "sh"

    def test_upsert_company_update(self, temp_db):
        """测试更新公司信息"""
        from annual_report_mda.db import upsert_company, get_company

        # 首次插入
        upsert_company(
            temp_db,
            stock_code="600519",
            short_name="贵州茅台",
        )

        # 更新
        upsert_company(
            temp_db,
            stock_code="600519",
            short_name="茅台集团",
            full_name="贵州茅台酒股份有限公司",
        )

        company = get_company(temp_db, "600519")
        assert company["short_name"] == "茅台集团"
        assert company["full_name"] == "贵州茅台酒股份有限公司"

    def test_get_company_not_found(self, temp_db):
        """测试查询不存在的公司"""
        from annual_report_mda.db import get_company

        company = get_company(temp_db, "999999")
        assert company is None


class TestReportsTable:
    """M1-04: reports 表测试"""

    def test_insert_report_new(self, temp_db):
        """测试新增年报记录"""
        from annual_report_mda.db import insert_report, get_report

        is_new = insert_report(
            temp_db,
            stock_code="600519",
            year=2023,
            url="https://example.com/report.pdf",
            title="2023年年度报告",
        )

        assert is_new is True

        report = get_report(temp_db, "600519", 2023)
        assert report is not None
        assert report["stock_code"] == "600519"
        assert report["year"] == 2023
        assert report["url"] == "https://example.com/report.pdf"
        assert report["download_status"] == "pending"

    def test_insert_report_duplicate_skip(self, temp_db):
        """测试重复插入跳过（增量逻辑）"""
        from annual_report_mda.db import insert_report

        # 首次插入
        is_new_1 = insert_report(
            temp_db,
            stock_code="600519",
            year=2023,
            url="https://example.com/report.pdf",
        )
        assert is_new_1 is True

        # 重复插入应跳过
        is_new_2 = insert_report(
            temp_db,
            stock_code="600519",
            year=2023,
            url="https://example.com/new_url.pdf",  # 不同 URL
        )
        assert is_new_2 is False

    def test_update_report_status(self, temp_db):
        """测试更新年报状态"""
        from annual_report_mda.db import insert_report, update_report_status, get_report

        insert_report(
            temp_db,
            stock_code="600519",
            year=2023,
            url="https://example.com/report.pdf",
        )

        update_report_status(
            temp_db,
            stock_code="600519",
            year=2023,
            download_status="success",
            pdf_path="/data/600519_贵州茅台_2023.pdf",
            pdf_size_bytes=1024000,
            downloaded_at=True,
        )

        report = get_report(temp_db, "600519", 2023)
        assert report["download_status"] == "success"
        assert report["pdf_path"] == "/data/600519_贵州茅台_2023.pdf"
        assert report["pdf_size_bytes"] == 1024000
        assert report["downloaded_at"] is not None

    def test_report_exists(self, temp_db):
        """测试年报存在性检查"""
        from annual_report_mda.db import insert_report, report_exists

        assert report_exists(temp_db, "600519", 2023) is False

        insert_report(
            temp_db,
            stock_code="600519",
            year=2023,
            url="https://example.com/report.pdf",
        )

        assert report_exists(temp_db, "600519", 2023) is True


class TestPendingTasksViews:
    """M1-05: 视图查询测试"""

    def test_get_pending_downloads(self, temp_db):
        """测试待下载任务查询"""
        from annual_report_mda.db import (
            insert_report,
            upsert_company,
            get_pending_downloads,
            update_report_status,
        )

        # 准备数据
        upsert_company(temp_db, stock_code="600519", short_name="贵州茅台")
        upsert_company(temp_db, stock_code="000001", short_name="平安银行")

        insert_report(temp_db, stock_code="600519", year=2023, url="https://a.com/1.pdf")
        insert_report(temp_db, stock_code="000001", year=2023, url="https://a.com/2.pdf")
        insert_report(temp_db, stock_code="600519", year=2022, url="https://a.com/3.pdf")

        # 标记一个为已下载
        update_report_status(temp_db, stock_code="600519", year=2023, download_status="success")

        # 查询待下载
        pending = get_pending_downloads(temp_db)
        assert len(pending) == 2

        # 按年份过滤
        pending_2023 = get_pending_downloads(temp_db, year=2023)
        assert len(pending_2023) == 1
        assert pending_2023[0]["stock_code"] == "000001"

    def test_get_pending_converts(self, temp_db):
        """测试待转换任务查询"""
        from annual_report_mda.db import (
            insert_report,
            upsert_company,
            get_pending_converts,
            update_report_status,
        )

        upsert_company(temp_db, stock_code="600519", short_name="贵州茅台")

        insert_report(temp_db, stock_code="600519", year=2023, url="https://a.com/1.pdf")
        insert_report(temp_db, stock_code="600519", year=2022, url="https://a.com/2.pdf")

        # 一个已下载待转换，一个仍待下载
        update_report_status(
            temp_db,
            stock_code="600519",
            year=2023,
            download_status="success",
            pdf_path="/data/600519_贵州茅台_2023.pdf",
        )

        pending = get_pending_converts(temp_db)
        assert len(pending) == 1
        assert pending[0]["year"] == 2023

    def test_reports_progress_view(self, temp_db):
        """测试进度总览视图"""
        from annual_report_mda.db import (
            insert_report,
            update_report_status,
        )

        # 插入测试数据
        for code in ["600519", "000001", "000002"]:
            insert_report(temp_db, stock_code=code, year=2023, url=f"https://a.com/{code}.pdf")

        update_report_status(temp_db, stock_code="600519", year=2023, download_status="success")
        update_report_status(temp_db, stock_code="000001", year=2023, download_status="success", convert_status="success")

        # 查询进度视图
        result = temp_db.execute("SELECT * FROM reports_progress WHERE year = 2023").fetchone()
        assert result is not None
        # year, total, downloaded, converted, extracted
        assert result[0] == 2023  # year
        assert result[1] == 3     # total
        assert result[2] == 2     # downloaded
        assert result[3] == 1     # converted
        assert result[4] == 0     # extracted


class TestMigrationIdempotency:
    """M1-06: 迁移脚本幂等性测试"""

    def test_migration_idempotent(self, temp_db):
        """测试迁移操作的幂等性"""
        from annual_report_mda.db import insert_report, upsert_company

        # 模拟迁移操作
        data = [
            ("600519", "贵州茅台", 2023, "https://a.com/1.pdf"),
            ("600519", "贵州茅台", 2022, "https://a.com/2.pdf"),
            ("000001", "平安银行", 2023, "https://a.com/3.pdf"),
        ]

        def do_migration():
            new_count = 0
            for stock_code, name, year, url in data:
                upsert_company(temp_db, stock_code=stock_code, short_name=name)
                if insert_report(temp_db, stock_code=stock_code, year=year, url=url, source="excel_migration"):
                    new_count += 1
            return new_count

        # 首次迁移
        first_run = do_migration()
        assert first_run == 3

        # 重复迁移应返回 0 新增
        second_run = do_migration()
        assert second_run == 0

        # 数据库中仍只有 3 条记录
        count = temp_db.execute("SELECT COUNT(*) FROM reports").fetchone()[0]
        assert count == 3


class TestExtractionErrors:
    """extraction_errors 表测试"""

    def test_insert_extraction_error(self, temp_db):
        """测试插入提取错误"""
        from annual_report_mda.db import insert_extraction_error

        insert_extraction_error(
            temp_db,
            stock_code="600519",
            year=2023,
            source_path="/data/600519_贵州茅台_2023.txt",
            source_sha256="abc123",
            error_type="PARSE_ERROR",
            error_message="无法解析目录结构",
        )

        result = temp_db.execute(
            "SELECT * FROM extraction_errors WHERE stock_code = '600519'"
        ).fetchone()
        assert result is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
