"""SQLite database operations for OLTP metadata storage.

This module provides SQLite-based storage for high-frequency read/write tables
that were previously stored in DuckDB. Using SQLite with WAL mode enables
concurrent access from WebUI and backend scripts.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Generator
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from typing import Any

from .utils import ensure_parent_dir, utc_now

DEFAULT_SQLITE_PATH = Path("data/metadata.db")


def get_connection(
    db_path: str | Path = DEFAULT_SQLITE_PATH,
    read_only: bool = False,
    busy_timeout: int = 5000,
    journal_mode: str = "WAL",
    synchronous: str = "NORMAL",
) -> sqlite3.Connection:
    """Creates and configures a new SQLite connection.

    Args:
        db_path: Path to the SQLite database file.
        read_only: If True, open the database in read-only mode.
        busy_timeout: Timeout in milliseconds to wait for a lock.
        journal_mode: The journaling mode for the database (e.g., "WAL").
        synchronous: The synchronous setting (e.g., "NORMAL").

    Returns:
        A configured sqlite3.Connection object.
    """
    db_path = Path(db_path)
    if not read_only:
        ensure_parent_dir(str(db_path))

    uri_params = [f"mode={'ro' if read_only else 'rwc'}"]
    uri = f"file:{db_path.resolve()}?{'&'.join(uri_params)}"

    conn = sqlite3.connect(uri, uri=True, check_same_thread=False, timeout=busy_timeout / 1000.0)
    conn.row_factory = sqlite3.Row

    # Set pragmas
    conn.execute(f"PRAGMA busy_timeout = {busy_timeout};")
    if not read_only:
        conn.execute(f"PRAGMA journal_mode = {journal_mode};")
        conn.execute(f"PRAGMA synchronous = {synchronous};")

    return conn


@contextmanager
def connection_context(
    db_path: str | Path = DEFAULT_SQLITE_PATH,
    read_only: bool = False,
    auto_init: bool = True,
    **kwargs: Any,
) -> Generator[sqlite3.Connection, None, None]:
    """Provides a transactional SQLite connection as a context manager.

    Args:
        db_path: Path to the SQLite database file.
        read_only: If True, open the database in read-only mode.
        auto_init: If True, automatically initialize tables if they don't exist.
        **kwargs: Additional arguments for get_connection.

    Yields:
        The sqlite3.Connection object.
    """
    conn = get_connection(db_path, read_only=read_only, **kwargs)
    try:
        # Auto-initialize tables if not read-only
        if auto_init and not read_only:
            init_db(conn)
        yield conn
        if not read_only:
            conn.commit()
    except Exception:
        if not read_only:
            conn.rollback()
        raise
    finally:
        conn.close()


def init_db(conn: sqlite3.Connection) -> None:
    """Initializes the SQLite database by creating all necessary tables and indexes."""
    conn.executescript(
        """
        -- 公司基本信息表
        CREATE TABLE IF NOT EXISTS companies (
            stock_code TEXT PRIMARY KEY,
            short_name TEXT NOT NULL,
            full_name TEXT,
            plate TEXT,
            trade TEXT,
            trade_name TEXT,
            first_seen_at TEXT,
            updated_at TEXT
        );

        -- 年报元数据与生命周期管理表
        CREATE TABLE IF NOT EXISTS reports (
            stock_code TEXT NOT NULL,
            year INTEGER NOT NULL,
            announcement_id TEXT,
            title TEXT,
            url TEXT NOT NULL,
            publish_date TEXT,

            download_status TEXT DEFAULT 'pending',
            convert_status TEXT DEFAULT 'pending',
            extract_status TEXT DEFAULT 'pending',

            download_error TEXT,
            convert_error TEXT,
            extract_error TEXT,
            download_retries INTEGER DEFAULT 0,
            convert_retries INTEGER DEFAULT 0,

            pdf_path TEXT,
            txt_path TEXT,
            pdf_size_bytes INTEGER,
            pdf_sha256 TEXT,
            txt_sha256 TEXT,

            crawled_at TEXT,
            downloaded_at TEXT,
            converted_at TEXT,
            updated_at TEXT,

            source TEXT DEFAULT 'cninfo',

            PRIMARY KEY (stock_code, year)
        );

        -- 索引：加速状态查询
        CREATE INDEX IF NOT EXISTS idx_reports_download_status ON reports(download_status);
        CREATE INDEX IF NOT EXISTS idx_reports_convert_status ON reports(convert_status);
        CREATE INDEX IF NOT EXISTS idx_reports_extract_status ON reports(extract_status);
        CREATE INDEX IF NOT EXISTS idx_reports_year ON reports(year);

        -- 提取规则表
        CREATE TABLE IF NOT EXISTS extraction_rules (
            stock_code TEXT NOT NULL,
            year INTEGER NOT NULL,
            report_signature TEXT,
            start_pattern TEXT,
            end_pattern TEXT,
            rule_source TEXT,
            updated_at TEXT,
            PRIMARY KEY (stock_code, year)
        );

        -- 提取错误日志表
        CREATE TABLE IF NOT EXISTS extraction_errors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stock_code TEXT,
            year INTEGER,
            source_path TEXT,
            source_sha256 TEXT,
            error_type TEXT,
            error_message TEXT,
            provider TEXT,
            http_status INTEGER,
            trace_id TEXT,
            created_at TEXT
        );

        -- 策略统计表
        CREATE TABLE IF NOT EXISTS strategy_stats (
            strategy TEXT PRIMARY KEY,
            attempts INTEGER DEFAULT 0,
            success INTEGER DEFAULT 0,
            last_updated TEXT
        );

        -- LLM 调用日志表
        CREATE TABLE IF NOT EXISTS llm_call_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stock_code TEXT,
            year INTEGER,
            provider TEXT,
            model TEXT,
            prompt_type TEXT,
            prompt_tokens INTEGER,
            completion_tokens INTEGER,
            latency_ms INTEGER,
            success INTEGER,
            error_message TEXT,
            created_at TEXT
        );
        """
    )


# =============================================================================
# companies 表操作
# =============================================================================


def upsert_company(
    conn: sqlite3.Connection,
    *,
    stock_code: str,
    short_name: str,
    full_name: str | None = None,
    plate: str | None = None,
    trade: str | None = None,
    trade_name: str | None = None,
) -> None:
    """Inserts or updates a company's information."""
    now_iso = utc_now().isoformat()
    conn.execute(
        """
        INSERT INTO companies (stock_code, short_name, full_name, plate, trade, trade_name, first_seen_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (stock_code) DO UPDATE SET
            short_name = excluded.short_name,
            full_name = COALESCE(excluded.full_name, full_name),
            plate = COALESCE(excluded.plate, plate),
            trade = COALESCE(excluded.trade, trade),
            trade_name = COALESCE(excluded.trade_name, trade_name),
            updated_at = excluded.updated_at;
        """,
        (stock_code, short_name, full_name, plate, trade, trade_name, now_iso, now_iso),
    )


def get_company(conn: sqlite3.Connection, stock_code: str) -> dict[str, Any] | None:
    """Retrieves a company's information."""
    cursor = conn.execute("SELECT * FROM companies WHERE stock_code = ?", (stock_code,))
    row = cursor.fetchone()
    return dict(row) if row else None


# =============================================================================
# reports 表操作
# =============================================================================


def insert_report(
    conn: sqlite3.Connection,
    *,
    stock_code: str,
    year: int,
    url: str,
    title: str | None = None,
    announcement_id: str | None = None,
    publish_date: date | str | None = None,
    source: str = "cninfo",
) -> bool:
    """Inserts a new report record if it doesn't exist.

    Returns True if a new record was inserted.
    """
    now_iso = utc_now().isoformat()
    publish_date_iso = publish_date.isoformat() if isinstance(publish_date, date) else publish_date
    try:
        conn.execute(
            """
            INSERT INTO reports (stock_code, year, url, title, announcement_id, publish_date, source, crawled_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                stock_code,
                year,
                url,
                title,
                announcement_id,
                publish_date_iso,
                source,
                now_iso,
                now_iso,
            ),
        )
        return True
    except sqlite3.IntegrityError:
        return False


def update_report_status(
    conn: sqlite3.Connection,
    *,
    stock_code: str,
    year: int,
    download_status: str | None = None,
    convert_status: str | None = None,
    extract_status: str | None = None,
    pdf_path: str | None = None,
    txt_path: str | None = None,
    pdf_size_bytes: int | None = None,
    pdf_sha256: str | None = None,
    txt_sha256: str | None = None,
    download_error: str | None = None,
    convert_error: str | None = None,
    extract_error: str | None = None,
    downloaded_at: bool = False,
    converted_at: bool = False,
) -> None:
    """Dynamically updates the processing status of a report."""
    updates = []
    params: list[Any] = []

    now_iso = utc_now().isoformat()

    # Fields to update directly
    field_map = {
        "download_status": download_status,
        "convert_status": convert_status,
        "extract_status": extract_status,
        "pdf_path": pdf_path,
        "txt_path": txt_path,
        "pdf_size_bytes": pdf_size_bytes,
        "pdf_sha256": pdf_sha256,
        "txt_sha256": txt_sha256,
        "download_error": download_error,
        "convert_error": convert_error,
        "extract_error": extract_error,
    }

    for field, value in field_map.items():
        if value is not None:
            updates.append(f"{field} = ?")
            params.append(value)

    # Timestamp fields
    if downloaded_at:
        updates.append("downloaded_at = ?")
        params.append(now_iso)
    if converted_at:
        updates.append("converted_at = ?")
        params.append(now_iso)

    if not updates:
        return

    updates.append("updated_at = ?")
    params.append(now_iso)

    params.extend([stock_code, year])

    sql = f"UPDATE reports SET {', '.join(updates)} WHERE stock_code = ? AND year = ?"
    conn.execute(sql, params)


def get_pending_downloads(
    conn: sqlite3.Connection,
    *,
    year: int | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Fetches a list of reports pending download."""
    sql = """
        SELECT r.stock_code, c.short_name, r.year, r.url, r.title
        FROM reports r
        LEFT JOIN companies c ON r.stock_code = c.stock_code
        WHERE r.download_status = 'pending'
    """
    params: list[Any] = []

    if year is not None:
        sql += " AND r.year = ?"
        params.append(year)

    sql += " ORDER BY r.year DESC, r.stock_code"

    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)

    cursor = conn.execute(sql, params)
    return [dict(row) for row in cursor.fetchall()]


def get_pending_converts(
    conn: sqlite3.Connection,
    *,
    year: int | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Fetches a list of reports pending conversion."""
    sql = """
        SELECT r.stock_code, c.short_name, r.year, r.pdf_path, r.url
        FROM reports r
        LEFT JOIN companies c ON r.stock_code = c.stock_code
        WHERE r.download_status = 'success' AND r.convert_status = 'pending'
    """
    params: list[Any] = []

    if year is not None:
        sql += " AND r.year = ?"
        params.append(year)

    sql += " ORDER BY r.year DESC, r.stock_code"

    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)

    cursor = conn.execute(sql, params)
    return [dict(row) for row in cursor.fetchall()]


def report_exists(conn: sqlite3.Connection, stock_code: str, year: int) -> bool:
    """Checks if a report record exists."""
    cursor = conn.execute(
        "SELECT 1 FROM reports WHERE stock_code = ? AND year = ?",
        (stock_code, year),
    )
    return cursor.fetchone() is not None


def get_report(conn: sqlite3.Connection, stock_code: str, year: int) -> dict[str, Any] | None:
    """Retrieves a single report record."""
    cursor = conn.execute(
        "SELECT * FROM reports WHERE stock_code = ? AND year = ?",
        (stock_code, year),
    )
    row = cursor.fetchone()
    return dict(row) if row else None


# =============================================================================
# LLM & Extraction Related Operations
# =============================================================================


def insert_llm_call_log(
    conn: sqlite3.Connection,
    *,
    stock_code: str | None,
    year: int | None,
    provider: str,
    model: str,
    prompt_type: str,
    prompt_tokens: int,
    completion_tokens: int,
    latency_ms: int,
    success: bool,
    error_message: str | None = None,
) -> None:
    """Inserts a new LLM call log entry."""
    conn.execute(
        """
        INSERT INTO llm_call_logs (
            stock_code, year, provider, model, prompt_type,
            prompt_tokens, completion_tokens, latency_ms,
            success, error_message, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        (
            stock_code,
            year,
            provider,
            model,
            prompt_type,
            prompt_tokens,
            completion_tokens,
            latency_ms,
            1 if success else 0,
            error_message,
            utc_now().isoformat(),
        ),
    )


def upsert_strategy_stats(conn: sqlite3.Connection, strategy: str, success: bool) -> None:
    """Updates strategy statistics."""
    now_iso = utc_now().isoformat()
    conn.execute(
        """
        INSERT INTO strategy_stats (strategy, attempts, success, last_updated)
        VALUES (?, 1, ?, ?)
        ON CONFLICT (strategy) DO UPDATE SET
            attempts = attempts + 1,
            success = success + excluded.success,
            last_updated = excluded.last_updated;
        """,
        (strategy, 1 if success else 0, now_iso),
    )


def get_strategy_stats(conn: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    """Retrieves all strategy statistics."""
    cursor = conn.execute("SELECT * FROM strategy_stats")
    stats = {}
    for row in cursor.fetchall():
        row_dict = dict(row)
        stats[row_dict["strategy"]] = {
            "attempts": row_dict["attempts"],
            "success": row_dict["success"],
            "last_updated": row_dict["last_updated"],
        }
    return stats


def insert_extraction_error(
    conn: sqlite3.Connection,
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
    """Inserts a new extraction error record."""
    conn.execute(
        """
        INSERT INTO extraction_errors (
            stock_code, year, source_path, source_sha256,
            error_type, error_message, provider, http_status,
            trace_id, created_at
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
            utc_now().isoformat(),
        ),
    )


def upsert_extraction_rule(
    conn: sqlite3.Connection,
    *,
    stock_code: str,
    year: int,
    start_pattern: str,
    end_pattern: str,
    report_signature: str | None = None,
    rule_source: str = "llm_learned",
) -> None:
    """Inserts or updates an extraction rule."""
    now_iso = utc_now().isoformat()
    conn.execute(
        """
        INSERT INTO extraction_rules (
            stock_code, year, report_signature,
            start_pattern, end_pattern, rule_source, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (stock_code, year) DO UPDATE SET
            report_signature = excluded.report_signature,
            start_pattern = excluded.start_pattern,
            end_pattern = excluded.end_pattern,
            rule_source = excluded.rule_source,
            updated_at = excluded.updated_at;
        """,
        (
            stock_code,
            year,
            report_signature,
            start_pattern,
            end_pattern,
            rule_source,
            now_iso,
        ),
    )
