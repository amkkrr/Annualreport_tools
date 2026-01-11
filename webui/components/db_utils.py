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
        return _conn.execute("SELECT * FROM reports_progress").df()
    except duckdb.Error as e:
        st.error(f"查询年度进度失败: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=60)
def get_pending_downloads(_conn: duckdb.DuckDBPyConnection, limit: int = 100) -> pd.DataFrame:
    """Queries the queue of pending downloads."""
    if _conn is None:
        return pd.DataFrame()
    try:
        return _conn.execute(f"SELECT * FROM pending_downloads LIMIT {limit}").df()
    except duckdb.Error as e:
        st.error(f"查询待下载队列失败: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=60)
def get_pending_converts(_conn: duckdb.DuckDBPyConnection, limit: int = 100) -> pd.DataFrame:
    """Queries the queue of pending conversions."""
    if _conn is None:
        return pd.DataFrame()
    try:
        return _conn.execute(f"SELECT * FROM pending_converts LIMIT {limit}").df()
    except duckdb.Error as e:
        st.error(f"查询待转换队列失败: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=60)
def get_mda_needs_review(_conn: duckdb.DuckDBPyConnection, limit: int = 100) -> pd.DataFrame:
    """Queries MD&A sections that need review."""
    if _conn is None:
        return pd.DataFrame()
    try:
        return _conn.execute(f"SELECT * FROM mda_needs_review LIMIT {limit}").df()
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
        return {
            "pending_downloads": _conn.execute("SELECT COUNT(*) FROM pending_downloads").fetchone()[
                0
            ],
            "pending_converts": _conn.execute("SELECT COUNT(*) FROM pending_converts").fetchone()[
                0
            ],
            "mda_needs_review": _conn.execute("SELECT COUNT(*) FROM mda_needs_review").fetchone()[
                0
            ],
            "total_extracted": _conn.execute(
                "SELECT COUNT(*) FROM reports WHERE extract_status = 'success'"
            ).fetchone()[0],
        }
    except duckdb.Error as e:
        st.error(f"获取队列计数失败: {e}")
        return default_counts
