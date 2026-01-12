"""Database utilities for WebUI.

This module provides database access for the Streamlit WebUI:
- SQLite: metadata queries (companies, reports, etc.) with WAL for concurrent access
- DuckDB: mda_text queries for OLAP analysis

The dual-database architecture allows the WebUI to read data concurrently
while crawlers/extractors are writing to the database.
"""

from __future__ import annotations

import logging
import sqlite3
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from annual_report_mda import sqlite_db

DEFAULT_SQLITE_PATH = Path("data/metadata.db")
DEFAULT_DUCKDB_PATH = Path("data/annual_reports.duckdb")

logger = logging.getLogger(__name__)


def get_sqlite_connection(
    db_path: Path = DEFAULT_SQLITE_PATH,
) -> sqlite3.Connection | None:
    """Gets a new SQLite connection for each query (short connection strategy).

    Using short connections avoids blocking writers during long-running queries
    and prevents "database is locked" errors during concurrent access.
    """
    if not db_path.exists():
        logger.warning(f"SQLite database not found: {db_path}")
        return None

    try:
        conn = sqlite_db.get_connection(db_path, read_only=True)
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to SQLite: {e}")
        st.error(f"数据库连接失败: {e}")
        return None


def get_duckdb_connection(db_path: Path = DEFAULT_DUCKDB_PATH):
    """Gets a DuckDB connection for mda_text queries.

    Note: DuckDB is used only for mda_text (OLAP) queries.
    For metadata queries, use SQLite via get_sqlite_connection().
    """
    if not db_path.exists():
        logger.warning(f"DuckDB database not found: {db_path}")
        return None

    try:
        import duckdb

        return duckdb.connect(database=str(db_path), read_only=True)
    except Exception as e:
        logger.error(f"Failed to connect to DuckDB: {e}")
        return None


# =============================================================================
# Reports Progress Queries (SQLite)
# =============================================================================


@st.cache_data(ttl=60)
def get_reports_progress() -> pd.DataFrame:
    """Queries the annual processing progress from SQLite."""
    conn = get_sqlite_connection()
    if conn is None:
        return pd.DataFrame()

    try:
        sql = """
            SELECT
                year,
                COUNT(*) as total,
                SUM(CASE WHEN download_status = 'success' THEN 1 ELSE 0 END) as downloaded,
                SUM(CASE WHEN convert_status = 'success' THEN 1 ELSE 0 END) as converted,
                SUM(CASE WHEN extract_status = 'success' THEN 1 ELSE 0 END) as extracted
            FROM reports
            GROUP BY year
            ORDER BY year DESC
        """
        cursor = conn.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        return pd.DataFrame(rows, columns=columns)
    except sqlite3.Error as e:
        st.error(f"查询年度进度失败: {e}")
        return pd.DataFrame()
    finally:
        conn.close()


@st.cache_data(ttl=60)
def get_pending_downloads(limit: int = 100) -> pd.DataFrame:
    """Queries the queue of pending downloads from SQLite."""
    conn = get_sqlite_connection()
    if conn is None:
        return pd.DataFrame()

    try:
        sql = """
            SELECT r.stock_code, c.short_name, r.year, r.url
            FROM reports r
            LEFT JOIN companies c ON r.stock_code = c.stock_code
            WHERE r.download_status = 'pending'
            ORDER BY r.year DESC, r.stock_code
            LIMIT ?
        """
        cursor = conn.execute(sql, (limit,))
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        return pd.DataFrame(rows, columns=columns)
    except sqlite3.Error as e:
        st.error(f"查询待下载队列失败: {e}")
        return pd.DataFrame()
    finally:
        conn.close()


@st.cache_data(ttl=60)
def get_pending_converts(limit: int = 100) -> pd.DataFrame:
    """Queries the queue of pending conversions from SQLite."""
    conn = get_sqlite_connection()
    if conn is None:
        return pd.DataFrame()

    try:
        sql = """
            SELECT r.stock_code, c.short_name, r.year, r.pdf_path
            FROM reports r
            LEFT JOIN companies c ON r.stock_code = c.stock_code
            WHERE r.download_status = 'success' AND r.convert_status = 'pending'
            ORDER BY r.year DESC, r.stock_code
            LIMIT ?
        """
        cursor = conn.execute(sql, (limit,))
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        return pd.DataFrame(rows, columns=columns)
    except sqlite3.Error as e:
        st.error(f"查询待转换队列失败: {e}")
        return pd.DataFrame()
    finally:
        conn.close()


@st.cache_data(ttl=10)
def get_counts() -> dict[str, int]:
    """Gets the counts for various queues from SQLite and DuckDB."""
    default_counts = {
        "pending_downloads": 0,
        "pending_converts": 0,
        "mda_needs_review": 0,
        "total_extracted": 0,
    }

    # Query SQLite for metadata counts
    sqlite_conn = get_sqlite_connection()
    if sqlite_conn is not None:
        try:
            default_counts["pending_downloads"] = sqlite_conn.execute(
                "SELECT COUNT(*) FROM reports WHERE download_status = 'pending'"
            ).fetchone()[0]

            default_counts["pending_converts"] = sqlite_conn.execute(
                "SELECT COUNT(*) FROM reports WHERE download_status = 'success' AND convert_status = 'pending'"
            ).fetchone()[0]

            default_counts["total_extracted"] = sqlite_conn.execute(
                "SELECT COUNT(*) FROM reports WHERE extract_status = 'success'"
            ).fetchone()[0]
        except sqlite3.Error as e:
            logger.error(f"Failed to get SQLite counts: {e}")
        finally:
            sqlite_conn.close()

    # Query DuckDB for mda_text counts
    duckdb_conn = get_duckdb_connection()
    if duckdb_conn is not None:
        try:
            result = duckdb_conn.execute(
                "SELECT COUNT(*) FROM mda_text WHERE needs_review = true"
            ).fetchone()
            default_counts["mda_needs_review"] = result[0] if result else 0
        except Exception as e:
            logger.debug(f"mda_text query failed (may not exist): {e}")
        finally:
            duckdb_conn.close()

    return default_counts


# =============================================================================
# MDA Queries (DuckDB)
# =============================================================================


@st.cache_data(ttl=60)
def get_mda_needs_review(limit: int = 100) -> pd.DataFrame:
    """Queries MD&A sections that need review from DuckDB."""
    conn = get_duckdb_connection()
    if conn is None:
        return pd.DataFrame()

    try:
        sql = f"""
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
            ORDER BY quality_score ASC, extracted_at DESC
            LIMIT {limit}
        """
        return conn.execute(sql).df()
    except Exception as e:
        st.error(f"查询待审核MDA失败: {e}")
        return pd.DataFrame()
    finally:
        conn.close()


# =============================================================================
# Filter and Search Queries (SQLite)
# =============================================================================


@st.cache_data(ttl=300)
def get_filter_options() -> dict:
    """Gets unique values for filter dropdowns from SQLite."""
    default_options = {"trades": [], "plates": [], "min_year": 2010, "max_year": 2024}

    conn = get_sqlite_connection()
    if conn is None:
        return default_options

    try:
        # Get unique trade names
        trades_cursor = conn.execute(
            "SELECT DISTINCT trade_name FROM companies WHERE trade_name IS NOT NULL ORDER BY trade_name"
        )
        trades = [row[0] for row in trades_cursor.fetchall()]

        # Get unique plates
        plates_cursor = conn.execute(
            "SELECT DISTINCT plate FROM companies WHERE plate IS NOT NULL ORDER BY plate"
        )
        plates = [row[0] for row in plates_cursor.fetchall()]

        # Get year range
        years_cursor = conn.execute("SELECT MIN(year), MAX(year) FROM reports")
        years = years_cursor.fetchone()

        return {
            "trades": trades,
            "plates": plates,
            "min_year": years[0] or 2010,
            "max_year": years[1] or 2024,
        }
    except sqlite3.Error as e:
        logger.error(f"Failed to get filter options: {e}")
        return default_options
    finally:
        conn.close()


@st.cache_data(ttl=10)
def search_reports(
    query: str | None = None,
    trades: list[str] | None = None,
    years: tuple[int, int] | None = None,
    download_status: str | None = None,
    convert_status: str | None = None,
    extract_status: str | None = None,
    limit: int = 500,
) -> pd.DataFrame:
    """Searches and filters reports based on multiple criteria from SQLite."""
    conn = get_sqlite_connection()
    if conn is None:
        return pd.DataFrame()

    where_clauses = []
    params: list = []

    if query:
        where_clauses.append("(c.stock_code LIKE ? OR c.short_name LIKE ?)")
        params.extend([f"%{query}%", f"%{query}%"])

    if trades:
        placeholders = ",".join(["?"] * len(trades))
        where_clauses.append(f"c.trade_name IN ({placeholders})")
        params.extend(trades)

    if years:
        where_clauses.append("r.year BETWEEN ? AND ?")
        params.extend(years)

    for status_field, status_value in [
        ("download_status", download_status),
        ("convert_status", convert_status),
        ("extract_status", extract_status),
    ]:
        if status_value and status_value != "全部":
            where_clauses.append(f"r.{status_field} = ?")
            params.append(status_value)

    where_str = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    sql = f"""
        SELECT
            c.stock_code,
            c.short_name,
            r.year,
            r.download_status,
            r.convert_status,
            r.extract_status,
            c.plate,
            c.trade_name
        FROM reports r
        JOIN companies c ON r.stock_code = c.stock_code
        {where_str}
        ORDER BY r.year DESC, c.stock_code
        LIMIT ?
    """
    params.append(limit)

    try:
        cursor = conn.execute(sql, params)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        return pd.DataFrame(rows, columns=columns)
    except sqlite3.Error as e:
        st.error(f"查询报告失败: {e}")
        return pd.DataFrame()
    finally:
        conn.close()


# =============================================================================
# Deprecated Compatibility Functions
# =============================================================================

# Old function signatures for backward compatibility
# These will be removed in a future version


def get_connection(db_path: Path = DEFAULT_DUCKDB_PATH):
    """Deprecated: Use get_sqlite_connection() for metadata or get_duckdb_connection() for mda_text."""
    import warnings

    warnings.warn(
        "get_connection() is deprecated. Use get_sqlite_connection() or get_duckdb_connection().",
        DeprecationWarning,
        stacklevel=2,
    )
    return get_duckdb_connection(db_path)


def get_write_connection(db_path: Path = DEFAULT_DUCKDB_PATH):
    """Deprecated: Use sqlite_db.get_connection() for writable SQLite access."""
    import warnings

    warnings.warn(
        "get_write_connection() is deprecated. Use sqlite_db.get_connection() instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return get_duckdb_connection(db_path)
