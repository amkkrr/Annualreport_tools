from __future__ import annotations

from pathlib import Path
from typing import Union

from .utils import ensure_parent_dir, utc_now


def init_db(db_path: Union[str, Path]) -> "duckdb.DuckDBPyConnection":
    """
    初始化 DuckDB（建表/建视图），并返回连接对象。
    """
    import duckdb  # 延迟导入，避免依赖缺失时导入期崩溃

    db_path_str = str(db_path)
    ensure_parent_dir(db_path_str)
    conn = duckdb.connect(database=db_path_str)

    _create_tables(conn)
    _create_views(conn)

    return conn


def _create_tables(conn: "duckdb.DuckDBPyConnection") -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS mda_text (
            stock_code VARCHAR,
            year INTEGER,
            mda_raw TEXT,
            char_count INTEGER,

            page_index_start INTEGER,
            page_index_end INTEGER,
            page_count INTEGER,

            printed_page_start INTEGER,
            printed_page_end INTEGER,

            hit_start VARCHAR,
            hit_end VARCHAR,

            is_truncated BOOLEAN,
            truncation_reason VARCHAR,

            quality_flags JSON,
            quality_detail JSON,

            source_path VARCHAR,
            source_sha256 VARCHAR,
            extractor_version VARCHAR,
            extracted_at TIMESTAMP,
            used_rule_type VARCHAR,

            PRIMARY KEY (stock_code, year, source_sha256)
        );
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS extraction_rules (
            stock_code VARCHAR,
            year INTEGER,
            report_signature VARCHAR,

            start_pattern VARCHAR,
            end_pattern VARCHAR,
            rule_source VARCHAR,
            updated_at TIMESTAMP,

            PRIMARY KEY (stock_code, year)
        );
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS extraction_errors (
            stock_code VARCHAR,
            year INTEGER,
            source_path VARCHAR,
            source_sha256 VARCHAR,

            error_type VARCHAR,
            error_message TEXT,

            provider VARCHAR,
            http_status INTEGER,
            trace_id VARCHAR,

            created_at TIMESTAMP
        );
        """
    )


def _create_views(conn: "duckdb.DuckDBPyConnection") -> None:
    conn.execute(
        """
        CREATE OR REPLACE VIEW mda_text_latest AS
        SELECT *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY stock_code, year
                    ORDER BY extracted_at DESC
                ) AS rn
            FROM mda_text
        ) t
        WHERE rn = 1;
        """
    )


def insert_extraction_error(
    conn: "duckdb.DuckDBPyConnection",
    *,
    stock_code: str | None,
    year: int | None,
    source_path: str,
    source_sha256: str | None,
    error_type: str,
    error_message: str,
    provider: str | None = None,
    http_status: int | None = None,
    trace_id: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO extraction_errors (
            stock_code,
            year,
            source_path,
            source_sha256,
            error_type,
            error_message,
            provider,
            http_status,
            trace_id,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        (
            stock_code,
            year,
            source_path,
            source_sha256,
            error_type,
            error_message,
            provider,
            http_status,
            trace_id,
            utc_now(),
        ),
    )