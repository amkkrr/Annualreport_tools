"""DuckDB 核心化单元测试 (M1 验证)

测试范围:
    - M1-03: companies 表 CRUD (SQLite)
    - M1-04: reports 表 CRUD 及增量逻辑 (SQLite)
    - M1-05: 视图查询 (SQLite)
    - M1-06: 迁移脚本幂等性

注意: 在双数据库架构下，元数据存储在 SQLite，MDA 文本存储在 DuckDB。
"""

from __future__ import annotations

import tempfile
from contextlib import contextmanager
from pathlib import Path

import pytest


@contextmanager
def temp_sqlite_context():
    """创建临时 SQLite 数据库并返回连接。"""
    from annual_report_mda import sqlite_db

    db_path = tempfile.mktemp(suffix=".db")
    conn = None
    try:
        conn = sqlite_db.get_connection(db_path)
        sqlite_db.init_db(conn)
        yield conn
    finally:
        if conn:
            conn.close()
        Path(db_path).unlink(missing_ok=True)


@pytest.fixture
def temp_sqlite():
    """创建临时 SQLite 数据库用于元数据测试。"""
    with temp_sqlite_context() as conn:
        yield conn


@pytest.fixture
def temp_duckdb():
    """创建临时 DuckDB 数据库用于 MDA 文本测试。"""
    from annual_report_mda.db import init_db

    db_path = tempfile.mktemp(suffix=".duckdb")

    conn = init_db(db_path, attach_sqlite=False)
    yield conn
    conn.close()
    Path(db_path).unlink(missing_ok=True)


class TestCompaniesTable:
    """M1-03: companies 表测试 (SQLite)"""

    def test_upsert_company_insert(self, temp_sqlite):
        """测试新增公司"""
        from annual_report_mda import sqlite_db

        sqlite_db.upsert_company(
            temp_sqlite,
            stock_code="600519",
            short_name="贵州茅台",
            full_name="贵州茅台酒股份有限公司",
            plate="sh",
        )

        company = sqlite_db.get_company(temp_sqlite, "600519")
        assert company is not None
        assert company["stock_code"] == "600519"
        assert company["short_name"] == "贵州茅台"
        assert company["full_name"] == "贵州茅台酒股份有限公司"
        assert company["plate"] == "sh"

    def test_upsert_company_update(self, temp_sqlite):
        """测试更新公司信息"""
        from annual_report_mda import sqlite_db

        # 首次插入
        sqlite_db.upsert_company(
            temp_sqlite,
            stock_code="600519",
            short_name="贵州茅台",
        )

        # 更新
        sqlite_db.upsert_company(
            temp_sqlite,
            stock_code="600519",
            short_name="茅台集团",
            full_name="贵州茅台酒股份有限公司",
        )

        company = sqlite_db.get_company(temp_sqlite, "600519")
        assert company["short_name"] == "茅台集团"
        assert company["full_name"] == "贵州茅台酒股份有限公司"

    def test_get_company_not_found(self, temp_sqlite):
        """测试查询不存在的公司"""
        from annual_report_mda import sqlite_db

        company = sqlite_db.get_company(temp_sqlite, "999999")
        assert company is None


class TestReportsTable:
    """M1-04: reports 表测试 (SQLite)"""

    def test_insert_report_new(self, temp_sqlite):
        """测试新增年报记录"""
        from annual_report_mda import sqlite_db

        is_new = sqlite_db.insert_report(
            temp_sqlite,
            stock_code="600519",
            year=2023,
            url="https://example.com/report.pdf",
            title="2023年年度报告",
        )

        assert is_new is True

        report = sqlite_db.get_report(temp_sqlite, "600519", 2023)
        assert report is not None
        assert report["stock_code"] == "600519"
        assert report["year"] == 2023
        assert report["url"] == "https://example.com/report.pdf"
        assert report["download_status"] == "pending"

    def test_insert_report_duplicate_skip(self, temp_sqlite):
        """测试重复插入跳过（增量逻辑）"""
        from annual_report_mda import sqlite_db

        # 首次插入
        is_new_1 = sqlite_db.insert_report(
            temp_sqlite,
            stock_code="600519",
            year=2023,
            url="https://example.com/report.pdf",
        )
        assert is_new_1 is True

        # 重复插入应跳过
        is_new_2 = sqlite_db.insert_report(
            temp_sqlite,
            stock_code="600519",
            year=2023,
            url="https://example.com/new_url.pdf",  # 不同 URL
        )
        assert is_new_2 is False

    def test_update_report_status(self, temp_sqlite):
        """测试更新年报状态"""
        from annual_report_mda import sqlite_db

        sqlite_db.insert_report(
            temp_sqlite,
            stock_code="600519",
            year=2023,
            url="https://example.com/report.pdf",
        )

        sqlite_db.update_report_status(
            temp_sqlite,
            stock_code="600519",
            year=2023,
            download_status="success",
            pdf_path="/data/600519_贵州茅台_2023.pdf",
            pdf_size_bytes=1024000,
            downloaded_at=True,
        )

        report = sqlite_db.get_report(temp_sqlite, "600519", 2023)
        assert report["download_status"] == "success"
        assert report["pdf_path"] == "/data/600519_贵州茅台_2023.pdf"
        assert report["pdf_size_bytes"] == 1024000
        assert report["downloaded_at"] is not None

    def test_report_exists(self, temp_sqlite):
        """测试年报存在性检查"""
        from annual_report_mda import sqlite_db

        assert sqlite_db.report_exists(temp_sqlite, "600519", 2023) is False

        sqlite_db.insert_report(
            temp_sqlite,
            stock_code="600519",
            year=2023,
            url="https://example.com/report.pdf",
        )

        assert sqlite_db.report_exists(temp_sqlite, "600519", 2023) is True


class TestPendingTasksViews:
    """M1-05: 视图查询测试 (SQLite)"""

    def test_get_pending_downloads(self, temp_sqlite):
        """测试待下载任务查询"""
        from annual_report_mda import sqlite_db

        # 准备数据
        sqlite_db.upsert_company(temp_sqlite, stock_code="600519", short_name="贵州茅台")
        sqlite_db.upsert_company(temp_sqlite, stock_code="000001", short_name="平安银行")

        sqlite_db.insert_report(
            temp_sqlite, stock_code="600519", year=2023, url="https://a.com/1.pdf"
        )
        sqlite_db.insert_report(
            temp_sqlite, stock_code="000001", year=2023, url="https://a.com/2.pdf"
        )
        sqlite_db.insert_report(
            temp_sqlite, stock_code="600519", year=2022, url="https://a.com/3.pdf"
        )

        # 标记一个为已下载
        sqlite_db.update_report_status(
            temp_sqlite, stock_code="600519", year=2023, download_status="success"
        )

        # 查询待下载
        pending = sqlite_db.get_pending_downloads(temp_sqlite)
        assert len(pending) == 2

        # 按年份过滤
        pending_2023 = sqlite_db.get_pending_downloads(temp_sqlite, year=2023)
        assert len(pending_2023) == 1
        assert pending_2023[0]["stock_code"] == "000001"

    def test_get_pending_converts(self, temp_sqlite):
        """测试待转换任务查询"""
        from annual_report_mda import sqlite_db

        sqlite_db.upsert_company(temp_sqlite, stock_code="600519", short_name="贵州茅台")

        sqlite_db.insert_report(
            temp_sqlite, stock_code="600519", year=2023, url="https://a.com/1.pdf"
        )
        sqlite_db.insert_report(
            temp_sqlite, stock_code="600519", year=2022, url="https://a.com/2.pdf"
        )

        # 一个已下载待转换，一个仍待下载
        sqlite_db.update_report_status(
            temp_sqlite,
            stock_code="600519",
            year=2023,
            download_status="success",
            pdf_path="/data/600519_贵州茅台_2023.pdf",
        )

        pending = sqlite_db.get_pending_converts(temp_sqlite)
        assert len(pending) == 1
        assert pending[0]["year"] == 2023

    def test_reports_progress_query(self, temp_sqlite):
        """测试进度总览查询"""
        from annual_report_mda import sqlite_db

        # 插入测试数据
        for code in ["600519", "000001", "000002"]:
            sqlite_db.insert_report(
                temp_sqlite, stock_code=code, year=2023, url=f"https://a.com/{code}.pdf"
            )

        sqlite_db.update_report_status(
            temp_sqlite, stock_code="600519", year=2023, download_status="success"
        )
        sqlite_db.update_report_status(
            temp_sqlite,
            stock_code="000001",
            year=2023,
            download_status="success",
            convert_status="success",
        )

        # 查询进度 - 使用 SQL 查询替代视图
        result = temp_sqlite.execute(
            """
            SELECT
                year,
                COUNT(*) as total,
                SUM(CASE WHEN download_status = 'success' THEN 1 ELSE 0 END) as downloaded,
                SUM(CASE WHEN convert_status = 'success' THEN 1 ELSE 0 END) as converted,
                SUM(CASE WHEN extract_status = 'success' THEN 1 ELSE 0 END) as extracted
            FROM reports
            WHERE year = 2023
            GROUP BY year
            """
        ).fetchone()
        assert result is not None
        # year, total, downloaded, converted, extracted
        assert result[0] == 2023  # year
        assert result[1] == 3  # total
        assert result[2] == 2  # downloaded
        assert result[3] == 1  # converted
        assert result[4] == 0  # extracted


class TestMigrationIdempotency:
    """M1-06: 迁移脚本幂等性测试"""

    def test_migration_idempotent(self, temp_sqlite):
        """测试迁移操作的幂等性"""
        from annual_report_mda import sqlite_db

        # 模拟迁移操作
        data = [
            ("600519", "贵州茅台", 2023, "https://a.com/1.pdf"),
            ("600519", "贵州茅台", 2022, "https://a.com/2.pdf"),
            ("000001", "平安银行", 2023, "https://a.com/3.pdf"),
        ]

        def do_migration():
            new_count = 0
            for stock_code, name, year, url in data:
                sqlite_db.upsert_company(temp_sqlite, stock_code=stock_code, short_name=name)
                if sqlite_db.insert_report(
                    temp_sqlite, stock_code=stock_code, year=year, url=url, source="excel_migration"
                ):
                    new_count += 1
            return new_count

        # 首次迁移
        first_run = do_migration()
        assert first_run == 3

        # 重复迁移应返回 0 新增
        second_run = do_migration()
        assert second_run == 0

        # 数据库中仍只有 3 条记录
        count = temp_sqlite.execute("SELECT COUNT(*) FROM reports").fetchone()[0]
        assert count == 3


class TestExtractionErrors:
    """extraction_errors 表测试 (SQLite)"""

    def test_insert_extraction_error(self, temp_sqlite):
        """测试插入提取错误"""
        from annual_report_mda import sqlite_db

        sqlite_db.insert_extraction_error(
            temp_sqlite,
            stock_code="600519",
            year=2023,
            source_path="/data/600519_贵州茅台_2023.txt",
            source_sha256="abc123",
            error_type="PARSE_ERROR",
            error_message="无法解析目录结构",
        )

        result = temp_sqlite.execute(
            "SELECT * FROM extraction_errors WHERE stock_code = '600519'"
        ).fetchone()
        assert result is not None


class TestMdaTextTable:
    """mda_text 表测试 (DuckDB)"""

    def test_insert_mda_text(self, temp_duckdb):
        """测试插入 MDA 文本"""
        from annual_report_mda.db import get_mda_text, insert_mda_text

        insert_mda_text(
            temp_duckdb,
            stock_code="600519",
            year=2023,
            mda_raw="这是管理层讨论与分析的内容...",
            char_count=500,
            quality_score=85,
            source_sha256="abc123",
        )

        mda = get_mda_text(temp_duckdb, "600519", 2023)
        assert mda is not None
        assert mda["stock_code"] == "600519"
        assert mda["year"] == 2023
        assert mda["char_count"] == 500
        assert mda["quality_score"] == 85

    def test_mda_exists(self, temp_duckdb):
        """测试 MDA 存在性检查"""
        from annual_report_mda.db import insert_mda_text, mda_exists

        assert mda_exists(temp_duckdb, "600519", 2023) is False

        insert_mda_text(
            temp_duckdb,
            stock_code="600519",
            year=2023,
            mda_raw="内容",
            char_count=2,
            source_sha256="abc123",
        )

        assert mda_exists(temp_duckdb, "600519", 2023) is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
