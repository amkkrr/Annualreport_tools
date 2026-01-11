"""DuckDB database utilities for WebUI."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

DEFAULT_DB_PATH = Path("data/annual_reports.duckdb")


@st.cache_resource
def get_connection(db_path: Path = DEFAULT_DB_PATH) -> duckdb.DuckDBPyConnection | None:
    """Gets a cached, read-only DuckDB connection."""
    try:
        return duckdb.connect(database=str(db_path), read_only=True)
    except duckdb.IOException as e:
        st.error(f"数据库连接失败: {e}\n请确认数据库文件存在于: {db_path.absolute()}")
        return None


@st.cache_data(ttl=60)
def get_reports_progress(_conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Queries the annual processing progress."""
    if _conn is None:
        return pd.DataFrame()
    try:
        # Use inline query instead of view for robustness
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
        return _conn.execute(sql).df()
    except duckdb.Error as e:
        st.error(f"查询年度进度失败: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=60)
def get_pending_downloads(_conn: duckdb.DuckDBPyConnection, limit: int = 100) -> pd.DataFrame:
    """Queries the queue of pending downloads."""
    if _conn is None:
        return pd.DataFrame()
    try:
        # Use inline query instead of view for robustness
        sql = f"""
            SELECT r.stock_code, c.short_name, r.year, r.url
            FROM reports r
            LEFT JOIN companies c ON r.stock_code = c.stock_code
            WHERE r.download_status = 'pending'
            ORDER BY r.year DESC, r.stock_code
            LIMIT {limit}
        """
        return _conn.execute(sql).df()
    except duckdb.Error as e:
        st.error(f"查询待下载队列失败: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=60)
def get_pending_converts(_conn: duckdb.DuckDBPyConnection, limit: int = 100) -> pd.DataFrame:
    """Queries the queue of pending conversions."""
    if _conn is None:
        return pd.DataFrame()
    try:
        # Use inline query instead of view for robustness
        sql = f"""
            SELECT r.stock_code, c.short_name, r.year, r.pdf_path
            FROM reports r
            LEFT JOIN companies c ON r.stock_code = c.stock_code
            WHERE r.download_status = 'success' AND r.convert_status = 'pending'
            ORDER BY r.year DESC, r.stock_code
            LIMIT {limit}
        """
        return _conn.execute(sql).df()
    except duckdb.Error as e:
        st.error(f"查询待转换队列失败: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=60)
def get_mda_needs_review(_conn: duckdb.DuckDBPyConnection, limit: int = 100) -> pd.DataFrame:
    """Queries MD&A sections that need review."""
    if _conn is None:
        return pd.DataFrame()
    try:
        # Use inline query instead of view for robustness
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
        return _conn.execute(sql).df()
    except duckdb.Error as e:
        st.error(f"查询待审核MDA失败: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=10)
def get_counts(_conn: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """Gets the counts for various queues."""
    default_counts = {
        "pending_downloads": 0,
        "pending_converts": 0,
        "mda_needs_review": 0,
        "total_extracted": 0,
    }
    if _conn is None:
        return default_counts
    try:
        # Use inline queries instead of views for robustness
        pending_downloads = _conn.execute(
            "SELECT COUNT(*) FROM reports WHERE download_status = 'pending'"
        ).fetchone()[0]

        pending_converts = _conn.execute(
            "SELECT COUNT(*) FROM reports WHERE download_status = 'success' AND convert_status = 'pending'"
        ).fetchone()[0]

        # Check if mda_text table exists before querying
        try:
            mda_needs_review = _conn.execute(
                "SELECT COUNT(*) FROM mda_text WHERE needs_review = true"
            ).fetchone()[0]
        except duckdb.Error:
            mda_needs_review = 0

        total_extracted = _conn.execute(
            "SELECT COUNT(*) FROM reports WHERE extract_status = 'success'"
        ).fetchone()[0]

        return {
            "pending_downloads": pending_downloads,
            "pending_converts": pending_converts,
            "mda_needs_review": mda_needs_review,
            "total_extracted": total_extracted,
        }
    except duckdb.Error as e:
        st.error(f"获取队列计数失败: {e}")
        return default_counts


# =============================================================================
# 年报浏览器查询函数
# =============================================================================


@st.cache_data(ttl=300)
def get_filter_options(_conn: duckdb.DuckDBPyConnection) -> dict:
    """Gets unique values for filter dropdowns."""
    if _conn is None:
        return {"trades": [], "plates": [], "min_year": 2010, "max_year": 2024}
    try:
        trades = (
            _conn.execute(
                "SELECT DISTINCT trade_name FROM companies WHERE trade_name IS NOT NULL ORDER BY trade_name"
            )
            .df()["trade_name"]
            .tolist()
        )

        plates = (
            _conn.execute(
                "SELECT DISTINCT plate FROM companies WHERE plate IS NOT NULL ORDER BY plate"
            )
            .df()["plate"]
            .tolist()
        )

        years = _conn.execute("SELECT MIN(year), MAX(year) FROM reports").fetchone()

        return {
            "trades": trades,
            "plates": plates,
            "min_year": years[0] or 2010,
            "max_year": years[1] or 2024,
        }
    except duckdb.Error:
        return {"trades": [], "plates": [], "min_year": 2010, "max_year": 2024}


@st.cache_data(ttl=10)
def search_reports(
    _conn: duckdb.DuckDBPyConnection,
    query: str | None = None,
    trades: list[str] | None = None,
    years: tuple[int, int] | None = None,
    download_status: str | None = None,
    convert_status: str | None = None,
    extract_status: str | None = None,
    limit: int = 500,
) -> pd.DataFrame:
    """Searches and filters reports based on multiple criteria."""
    if _conn is None:
        return pd.DataFrame()

    where_clauses = []
    params = []

    if query:
        where_clauses.append("(c.stock_code ILIKE ? OR c.short_name ILIKE ?)")
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
        LIMIT {limit}
    """

    try:
        return _conn.execute(sql, params).df()
    except duckdb.Error as e:
        st.error(f"查询报告失败: {e}")
        return pd.DataFrame()


def get_write_connection(db_path: Path = DEFAULT_DB_PATH) -> duckdb.DuckDBPyConnection | None:
    """Gets a writable DuckDB connection (not cached)."""
    try:
        return duckdb.connect(database=str(db_path), read_only=False)
    except duckdb.IOException as e:
        st.error(f"数据库连接失败: {e}")
        return None
