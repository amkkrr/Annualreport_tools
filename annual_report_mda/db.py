"""DuckDB database operations for OLAP analysis and mda_text storage.

This module provides DuckDB-based storage for large text content (mda_text)
and supports federated queries via ATTACH to the SQLite metadata database.

For metadata operations (companies, reports, etc.), use sqlite_db.py instead.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .utils import ensure_parent_dir, utc_now

DEFAULT_DUCKDB_PATH = Path("data/annual_reports.duckdb")
DEFAULT_SQLITE_PATH = Path("data/metadata.db")


def init_db(
    db_path: str | Path = DEFAULT_DUCKDB_PATH,
    sqlite_path: str | Path | None = DEFAULT_SQLITE_PATH,
    read_only: bool = False,
    attach_sqlite: bool = False,
) -> duckdb.DuckDBPyConnection:
    """Initializes a DuckDB connection with optional SQLite federation.

    Args:
        db_path: Path to the DuckDB database file.
        sqlite_path: Path to the SQLite metadata database for federated queries.
        read_only: If True, open the database in read-only mode.
        attach_sqlite: If True and sqlite_path exists, ATTACH SQLite for federation.
                       Defaults to False for backward compatibility.

    Returns:
        A configured DuckDB connection object.
    """
    import duckdb  # Lazy import to avoid crash when duckdb is not installed

    db_path = Path(db_path)
    if not read_only:
        ensure_parent_dir(str(db_path))

    conn = duckdb.connect(database=str(db_path), read_only=read_only)

    # Create mda_text table if not exists
    if not read_only:
        _create_mda_table(conn)
        _create_views(conn)

    # ATTACH SQLite for federated queries
    if attach_sqlite and sqlite_path:
        sqlite_path = Path(sqlite_path)
        if sqlite_path.exists():
            _attach_sqlite(conn, sqlite_path, create_views=not read_only)

    return conn


def _create_mda_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Creates the mda_text table for storing extracted MD&A content."""
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

            quality_score INTEGER,
            needs_review BOOLEAN DEFAULT FALSE,

            source_path VARCHAR,
            source_sha256 VARCHAR,
            extractor_version VARCHAR,
            extracted_at TIMESTAMP,
            used_rule_type VARCHAR,

            mda_review TEXT,
            mda_outlook TEXT,
            outlook_split_position INTEGER,

            PRIMARY KEY (stock_code, year, source_sha256)
        );
        """
    )


def _create_views(conn: duckdb.DuckDBPyConnection) -> None:
    """Creates DuckDB views for common queries."""
    # Latest mda_text per stock/year
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

    # MDA records needing review
    conn.execute(
        """
        CREATE OR REPLACE VIEW mda_needs_review AS
        SELECT
            stock_code,
            year,
            quality_score,
            quality_flags,
            char_count,
            source_path,
            extracted_at
        FROM mda_text
        WHERE needs_review = true
        ORDER BY quality_score ASC, extracted_at DESC;
        """
    )


def _attach_sqlite(
    conn: duckdb.DuckDBPyConnection,
    sqlite_path: Path,
    create_views: bool = False,
) -> None:
    """Attaches SQLite database for federated queries.

    After attaching, SQLite tables are accessible via the 'meta' schema:
    - meta.companies
    - meta.reports
    - meta.extraction_rules
    - etc.
    """
    conn.execute("INSTALL sqlite;")
    conn.execute("LOAD sqlite;")
    conn.execute(f"ATTACH '{sqlite_path}' AS meta (TYPE SQLITE, READ_ONLY);")

    # Create federated views only if not read-only
    if create_views:
        _create_federated_views(conn)


def _create_federated_views(conn: duckdb.DuckDBPyConnection) -> None:
    """Creates views that join DuckDB and SQLite data."""
    # Progress view using SQLite reports table
    conn.execute(
        """
        CREATE OR REPLACE VIEW reports_progress AS
        SELECT
            year,
            COUNT(*) as total,
            SUM(CASE WHEN download_status = 'success' THEN 1 ELSE 0 END) as downloaded,
            SUM(CASE WHEN convert_status = 'success' THEN 1 ELSE 0 END) as converted,
            SUM(CASE WHEN extract_status = 'success' THEN 1 ELSE 0 END) as extracted
        FROM meta.reports
        GROUP BY year
        ORDER BY year DESC;
        """
    )

    # Pending downloads view
    conn.execute(
        """
        CREATE OR REPLACE VIEW pending_downloads AS
        SELECT r.stock_code, c.short_name, r.year, r.url
        FROM meta.reports r
        LEFT JOIN meta.companies c ON r.stock_code = c.stock_code
        WHERE r.download_status = 'pending'
        ORDER BY r.year DESC, r.stock_code;
        """
    )

    # Pending converts view
    conn.execute(
        """
        CREATE OR REPLACE VIEW pending_converts AS
        SELECT r.stock_code, c.short_name, r.year, r.pdf_path
        FROM meta.reports r
        LEFT JOIN meta.companies c ON r.stock_code = c.stock_code
        WHERE r.download_status = 'success' AND r.convert_status = 'pending'
        ORDER BY r.year DESC, r.stock_code;
        """
    )

    # MDA with company info view
    conn.execute(
        """
        CREATE OR REPLACE VIEW mda_with_company AS
        SELECT
            m.*,
            c.short_name,
            c.full_name,
            c.trade_name
        FROM mda_text_latest m
        LEFT JOIN meta.companies c ON m.stock_code = c.stock_code;
        """
    )


# =============================================================================
# mda_text 表操作
# =============================================================================


def insert_mda_text(
    conn: duckdb.DuckDBPyConnection,
    *,
    stock_code: str,
    year: int,
    mda_raw: str,
    char_count: int,
    page_index_start: int | None = None,
    page_index_end: int | None = None,
    page_count: int | None = None,
    printed_page_start: int | None = None,
    printed_page_end: int | None = None,
    hit_start: str | None = None,
    hit_end: str | None = None,
    is_truncated: bool = False,
    truncation_reason: str | None = None,
    quality_flags: dict | None = None,
    quality_detail: dict | None = None,
    quality_score: int | None = None,
    needs_review: bool = False,
    source_path: str | None = None,
    source_sha256: str | None = None,
    extractor_version: str | None = None,
    used_rule_type: str | None = None,
    mda_review: str | None = None,
    mda_outlook: str | None = None,
    outlook_split_position: int | None = None,
) -> None:
    """Inserts a new MDA text record."""
    import json

    now = utc_now()
    conn.execute(
        """
        INSERT INTO mda_text (
            stock_code, year, mda_raw, char_count,
            page_index_start, page_index_end, page_count,
            printed_page_start, printed_page_end,
            hit_start, hit_end,
            is_truncated, truncation_reason,
            quality_flags, quality_detail,
            quality_score, needs_review,
            source_path, source_sha256, extractor_version,
            extracted_at, used_rule_type,
            mda_review, mda_outlook, outlook_split_position
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (stock_code, year, source_sha256) DO UPDATE SET
            mda_raw = EXCLUDED.mda_raw,
            char_count = EXCLUDED.char_count,
            page_index_start = EXCLUDED.page_index_start,
            page_index_end = EXCLUDED.page_index_end,
            page_count = EXCLUDED.page_count,
            printed_page_start = EXCLUDED.printed_page_start,
            printed_page_end = EXCLUDED.printed_page_end,
            hit_start = EXCLUDED.hit_start,
            hit_end = EXCLUDED.hit_end,
            is_truncated = EXCLUDED.is_truncated,
            truncation_reason = EXCLUDED.truncation_reason,
            quality_flags = EXCLUDED.quality_flags,
            quality_detail = EXCLUDED.quality_detail,
            quality_score = EXCLUDED.quality_score,
            needs_review = EXCLUDED.needs_review,
            extractor_version = EXCLUDED.extractor_version,
            extracted_at = EXCLUDED.extracted_at,
            used_rule_type = EXCLUDED.used_rule_type,
            mda_review = EXCLUDED.mda_review,
            mda_outlook = EXCLUDED.mda_outlook,
            outlook_split_position = EXCLUDED.outlook_split_position;
        """,
        (
            stock_code,
            year,
            mda_raw,
            char_count,
            page_index_start,
            page_index_end,
            page_count,
            printed_page_start,
            printed_page_end,
            hit_start,
            hit_end,
            is_truncated,
            truncation_reason,
            json.dumps(quality_flags) if quality_flags else None,
            json.dumps(quality_detail) if quality_detail else None,
            quality_score,
            needs_review,
            source_path,
            source_sha256,
            extractor_version,
            now,
            used_rule_type,
            mda_review,
            mda_outlook,
            outlook_split_position,
        ),
    )


def get_mda_text(
    conn: duckdb.DuckDBPyConnection,
    stock_code: str,
    year: int,
) -> dict[str, Any] | None:
    """Retrieves the latest MDA text record for a stock/year."""
    result = conn.execute(
        """
        SELECT * FROM mda_text_latest
        WHERE stock_code = ? AND year = ?
        """,
        (stock_code, year),
    ).fetchone()

    if result is None:
        return None

    columns = [desc[0] for desc in conn.description]
    return dict(zip(columns, result))


def mda_exists(
    conn: duckdb.DuckDBPyConnection,
    stock_code: str,
    year: int,
) -> bool:
    """Checks if an MDA text record exists for a stock/year."""
    result = conn.execute(
        "SELECT 1 FROM mda_text WHERE stock_code = ? AND year = ? LIMIT 1",
        (stock_code, year),
    ).fetchone()
    return result is not None


def get_mda_stats(
    conn: duckdb.DuckDBPyConnection,
) -> dict[str, Any]:
    """Returns statistics about the MDA text table."""
    result = conn.execute(
        """
        SELECT
            COUNT(*) as total_records,
            COUNT(DISTINCT stock_code) as unique_stocks,
            COUNT(DISTINCT year) as unique_years,
            AVG(char_count) as avg_char_count,
            SUM(CASE WHEN needs_review THEN 1 ELSE 0 END) as needs_review_count,
            AVG(quality_score) as avg_quality_score
        FROM mda_text_latest
        """
    ).fetchone()

    columns = [desc[0] for desc in conn.description]
    return dict(zip(columns, result))


def get_mda_by_year(
    conn: duckdb.DuckDBPyConnection,
    year: int,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Retrieves all MDA text records for a specific year."""
    sql = """
        SELECT * FROM mda_text_latest
        WHERE year = ?
        ORDER BY stock_code
    """
    params: list[Any] = [year]

    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)

    result = conn.execute(sql, params).fetchall()
    columns = [desc[0] for desc in conn.description]
    return [dict(zip(columns, row)) for row in result]


# =============================================================================
# 兼容性别名 (Deprecated - 使用 sqlite_db.py 替代)
# =============================================================================

# 以下函数已迁移到 sqlite_db.py，保留别名仅用于向后兼容
# 请使用 from annual_report_mda import sqlite_db 并调用 sqlite_db.xxx()


def _deprecated_warning(func_name: str) -> None:
    import warnings

    warnings.warn(
        f"db.{func_name}() is deprecated. Use sqlite_db.{func_name}() instead.",
        DeprecationWarning,
        stacklevel=3,
    )


def _drop_conn_arg(args: tuple) -> tuple:
    """Drops the first argument if it looks like a database connection.

    Old API passed a DuckDB connection as first arg, new API doesn't need it.
    """
    if args and hasattr(args[0], "execute"):
        return args[1:]
    return args


def upsert_company(*args, **kwargs) -> None:
    """Deprecated: Use sqlite_db.upsert_company() instead."""
    _deprecated_warning("upsert_company")
    from . import sqlite_db

    if args and hasattr(args[0], "execute"):
        sqlite_db.upsert_company(args[0], *args[1:], **kwargs)
        return

    with sqlite_db.connection_context() as conn:
        sqlite_db.upsert_company(conn, *args, **kwargs)


def get_company(*args, **kwargs):
    """Deprecated: Use sqlite_db.get_company() instead."""
    _deprecated_warning("get_company")
    from . import sqlite_db

    if args and hasattr(args[0], "execute"):
        return sqlite_db.get_company(args[0], *args[1:], **kwargs)

    with sqlite_db.connection_context(read_only=True) as conn:
        return sqlite_db.get_company(conn, *args, **kwargs)


def insert_report(*args, **kwargs) -> bool:
    """Deprecated: Use sqlite_db.insert_report() instead."""
    _deprecated_warning("insert_report")
    from . import sqlite_db

    if args and hasattr(args[0], "execute"):
        return sqlite_db.insert_report(args[0], *args[1:], **kwargs)

    with sqlite_db.connection_context() as conn:
        return sqlite_db.insert_report(conn, *args, **kwargs)


def update_report_status(*args, **kwargs) -> None:
    """Deprecated: Use sqlite_db.update_report_status() instead."""
    _deprecated_warning("update_report_status")
    from . import sqlite_db

    if args and hasattr(args[0], "execute"):
        sqlite_db.update_report_status(args[0], *args[1:], **kwargs)
        return

    with sqlite_db.connection_context() as conn:
        sqlite_db.update_report_status(conn, *args, **kwargs)


def get_pending_downloads(*args, **kwargs) -> list[dict]:
    """Deprecated: Use sqlite_db.get_pending_downloads() instead."""
    _deprecated_warning("get_pending_downloads")
    from . import sqlite_db

    if args and hasattr(args[0], "execute"):
        return sqlite_db.get_pending_downloads(args[0], *args[1:], **kwargs)

    with sqlite_db.connection_context(read_only=True) as conn:
        return sqlite_db.get_pending_downloads(conn, *args, **kwargs)


def get_pending_converts(*args, **kwargs) -> list[dict]:
    """Deprecated: Use sqlite_db.get_pending_converts() instead."""
    _deprecated_warning("get_pending_converts")
    from . import sqlite_db

    if args and hasattr(args[0], "execute"):
        return sqlite_db.get_pending_converts(args[0], *args[1:], **kwargs)

    with sqlite_db.connection_context(read_only=True) as conn:
        return sqlite_db.get_pending_converts(conn, *args, **kwargs)


def report_exists(*args, **kwargs) -> bool:
    """Deprecated: Use sqlite_db.report_exists() instead."""
    _deprecated_warning("report_exists")
    from . import sqlite_db

    if args and hasattr(args[0], "execute"):
        return sqlite_db.report_exists(args[0], *args[1:], **kwargs)

    with sqlite_db.connection_context(read_only=True) as conn:
        return sqlite_db.report_exists(conn, *args, **kwargs)


def get_report(*args, **kwargs):
    """Deprecated: Use sqlite_db.get_report() instead."""
    _deprecated_warning("get_report")
    from . import sqlite_db

    if args and hasattr(args[0], "execute"):
        return sqlite_db.get_report(args[0], *args[1:], **kwargs)

    with sqlite_db.connection_context(read_only=True) as conn:
        return sqlite_db.get_report(conn, *args, **kwargs)


def insert_llm_call_log(*args, **kwargs) -> None:
    """Deprecated: Use sqlite_db.insert_llm_call_log() instead."""
    _deprecated_warning("insert_llm_call_log")
    from . import sqlite_db

    args = _drop_conn_arg(args)
    with sqlite_db.connection_context() as conn:
        sqlite_db.insert_llm_call_log(conn, *args, **kwargs)


def upsert_strategy_stats(*args, **kwargs) -> None:
    """Deprecated: Use sqlite_db.upsert_strategy_stats() instead."""
    _deprecated_warning("upsert_strategy_stats")
    from . import sqlite_db

    args = _drop_conn_arg(args)
    with sqlite_db.connection_context() as conn:
        sqlite_db.upsert_strategy_stats(conn, *args, **kwargs)


def get_strategy_stats(*args, **kwargs) -> dict:
    """Deprecated: Use sqlite_db.get_strategy_stats() instead."""
    _deprecated_warning("get_strategy_stats")
    from . import sqlite_db

    args = _drop_conn_arg(args)
    with sqlite_db.connection_context(read_only=True) as conn:
        return sqlite_db.get_strategy_stats(conn)


def insert_extraction_error(*args, **kwargs) -> None:
    """Deprecated: Use sqlite_db.insert_extraction_error() instead."""
    _deprecated_warning("insert_extraction_error")
    from . import sqlite_db

    args = _drop_conn_arg(args)
    with sqlite_db.connection_context() as conn:
        sqlite_db.insert_extraction_error(conn, *args, **kwargs)


def upsert_extraction_rule(*args, **kwargs) -> None:
    """Deprecated: Use sqlite_db.upsert_extraction_rule() instead."""
    _deprecated_warning("upsert_extraction_rule")
    from . import sqlite_db

    args = _drop_conn_arg(args)
    with sqlite_db.connection_context() as conn:
        sqlite_db.upsert_extraction_rule(conn, *args, **kwargs)
