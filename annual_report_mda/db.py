from __future__ import annotations

from datetime import date
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
    # === 公司基本信息表 ===
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS companies (
            stock_code VARCHAR PRIMARY KEY,
            short_name VARCHAR NOT NULL,
            full_name VARCHAR,
            plate VARCHAR,
            trade VARCHAR,
            trade_name VARCHAR,
            first_seen_at TIMESTAMP,
            updated_at TIMESTAMP
        );
        """
    )

    # === 年报元数据与生命周期管理表 ===
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS reports (
            stock_code VARCHAR NOT NULL,
            year INTEGER NOT NULL,
            announcement_id VARCHAR,
            title VARCHAR,
            url VARCHAR NOT NULL,
            publish_date DATE,

            download_status VARCHAR DEFAULT 'pending',
            convert_status VARCHAR DEFAULT 'pending',
            extract_status VARCHAR DEFAULT 'pending',

            download_error VARCHAR,
            convert_error VARCHAR,
            extract_error VARCHAR,
            download_retries INTEGER DEFAULT 0,
            convert_retries INTEGER DEFAULT 0,

            pdf_path VARCHAR,
            txt_path VARCHAR,
            pdf_size_bytes BIGINT,
            pdf_sha256 VARCHAR,
            txt_sha256 VARCHAR,

            crawled_at TIMESTAMP,
            downloaded_at TIMESTAMP,
            converted_at TIMESTAMP,
            updated_at TIMESTAMP,

            source VARCHAR DEFAULT 'cninfo',

            PRIMARY KEY (stock_code, year)
        );
        """
    )

    # === 索引 ===
    # 注意: DuckDB 对带索引列的 UPDATE 操作有已知限制
    # 可能导致 "Duplicate key violates primary key constraint" 错误
    # 因此这里不创建状态列索引，依靠 DuckDB 的优化器进行查询优化
    # 参考: https://duckdb.org/docs/sql/indexes

    # === MDA 提取结果表 ===
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

    # === 策略统计表 (LLM 自适应学习) ===
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS strategy_stats (
            strategy VARCHAR PRIMARY KEY,
            attempts INTEGER DEFAULT 0,
            success INTEGER DEFAULT 0,
            last_updated TIMESTAMP
        );
        """
    )

    # === LLM 调用日志表 ===
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS llm_call_logs (
            id INTEGER PRIMARY KEY,
            stock_code VARCHAR,
            year INTEGER,
            provider VARCHAR,
            model VARCHAR,
            prompt_type VARCHAR,
            prompt_tokens INTEGER,
            completion_tokens INTEGER,
            latency_ms INTEGER,
            success BOOLEAN,
            error_message TEXT,
            created_at TIMESTAMP
        );
        """
    )


def _create_views(conn: "duckdb.DuckDBPyConnection") -> None:
    # === MDA 最新记录视图 ===
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

    # === 年报处理进度总览视图 ===
    conn.execute(
        """
        CREATE OR REPLACE VIEW reports_progress AS
        SELECT
            year,
            COUNT(*) as total,
            SUM(CASE WHEN download_status = 'success' THEN 1 ELSE 0 END) as downloaded,
            SUM(CASE WHEN convert_status = 'success' THEN 1 ELSE 0 END) as converted,
            SUM(CASE WHEN extract_status = 'success' THEN 1 ELSE 0 END) as extracted
        FROM reports
        GROUP BY year
        ORDER BY year DESC;
        """
    )

    # === 待下载任务视图 ===
    conn.execute(
        """
        CREATE OR REPLACE VIEW pending_downloads AS
        SELECT r.stock_code, c.short_name, r.year, r.url
        FROM reports r
        LEFT JOIN companies c ON r.stock_code = c.stock_code
        WHERE r.download_status = 'pending'
        ORDER BY r.year DESC, r.stock_code;
        """
    )

    # === 待转换任务视图 ===
    conn.execute(
        """
        CREATE OR REPLACE VIEW pending_converts AS
        SELECT r.stock_code, c.short_name, r.year, r.pdf_path
        FROM reports r
        LEFT JOIN companies c ON r.stock_code = c.stock_code
        WHERE r.download_status = 'success' AND r.convert_status = 'pending'
        ORDER BY r.year DESC, r.stock_code;
        """
    )

    # === 待审核 MDA 视图 ===
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


# =============================================================================
# companies 表操作
# =============================================================================


def upsert_company(
    conn: "duckdb.DuckDBPyConnection",
    *,
    stock_code: str,
    short_name: str,
    full_name: str | None = None,
    plate: str | None = None,
    trade: str | None = None,
    trade_name: str | None = None,
) -> None:
    """插入或更新公司信息。"""
    now = utc_now()
    conn.execute(
        """
        INSERT INTO companies (stock_code, short_name, full_name, plate, trade, trade_name, first_seen_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (stock_code) DO UPDATE SET
            short_name = EXCLUDED.short_name,
            full_name = COALESCE(EXCLUDED.full_name, companies.full_name),
            plate = COALESCE(EXCLUDED.plate, companies.plate),
            trade = COALESCE(EXCLUDED.trade, companies.trade),
            trade_name = COALESCE(EXCLUDED.trade_name, companies.trade_name),
            updated_at = EXCLUDED.updated_at;
        """,
        (stock_code, short_name, full_name, plate, trade, trade_name, now, now),
    )


def get_company(
    conn: "duckdb.DuckDBPyConnection",
    stock_code: str,
) -> dict | None:
    """获取公司信息。"""
    result = conn.execute(
        "SELECT * FROM companies WHERE stock_code = ?",
        (stock_code,),
    ).fetchone()
    if result is None:
        return None
    columns = [desc[0] for desc in conn.description]
    return dict(zip(columns, result))


# =============================================================================
# reports 表操作
# =============================================================================


def insert_report(
    conn: "duckdb.DuckDBPyConnection",
    *,
    stock_code: str,
    year: int,
    url: str,
    title: str | None = None,
    announcement_id: str | None = None,
    publish_date: date | None = None,
    source: str = "cninfo",
) -> bool:
    """插入年报记录（增量模式，已存在则跳过）。返回是否新增。"""
    # 先检查是否已存在
    existing = conn.execute(
        "SELECT 1 FROM reports WHERE stock_code = ? AND year = ?",
        (stock_code, year),
    ).fetchone()
    if existing is not None:
        return False

    now = utc_now()
    conn.execute(
        """
        INSERT INTO reports (stock_code, year, url, title, announcement_id, publish_date, source, crawled_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        (stock_code, year, url, title, announcement_id, publish_date, source, now, now),
    )
    return True


def update_report_status(
    conn: "duckdb.DuckDBPyConnection",
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
    """更新年报处理状态。动态构建 SET 子句，仅更新非 None 字段。"""
    updates = []
    params = []

    if download_status is not None:
        updates.append("download_status = ?")
        params.append(download_status)
    if convert_status is not None:
        updates.append("convert_status = ?")
        params.append(convert_status)
    if extract_status is not None:
        updates.append("extract_status = ?")
        params.append(extract_status)
    if pdf_path is not None:
        updates.append("pdf_path = ?")
        params.append(pdf_path)
    if txt_path is not None:
        updates.append("txt_path = ?")
        params.append(txt_path)
    if pdf_size_bytes is not None:
        updates.append("pdf_size_bytes = ?")
        params.append(pdf_size_bytes)
    if pdf_sha256 is not None:
        updates.append("pdf_sha256 = ?")
        params.append(pdf_sha256)
    if txt_sha256 is not None:
        updates.append("txt_sha256 = ?")
        params.append(txt_sha256)
    if download_error is not None:
        updates.append("download_error = ?")
        params.append(download_error)
    if convert_error is not None:
        updates.append("convert_error = ?")
        params.append(convert_error)
    if extract_error is not None:
        updates.append("extract_error = ?")
        params.append(extract_error)
    if downloaded_at:
        updates.append("downloaded_at = ?")
        params.append(utc_now())
    if converted_at:
        updates.append("converted_at = ?")
        params.append(utc_now())

    if not updates:
        return

    updates.append("updated_at = ?")
    params.append(utc_now())

    params.extend([stock_code, year])

    sql = f"UPDATE reports SET {', '.join(updates)} WHERE stock_code = ? AND year = ?"
    conn.execute(sql, params)


def get_pending_downloads(
    conn: "duckdb.DuckDBPyConnection",
    *,
    year: int | None = None,
    limit: int | None = None,
) -> list[dict]:
    """获取待下载任务列表。"""
    sql = """
        SELECT r.stock_code, c.short_name, r.year, r.url, r.title
        FROM reports r
        LEFT JOIN companies c ON r.stock_code = c.stock_code
        WHERE r.download_status = 'pending'
    """
    params = []

    if year is not None:
        sql += " AND r.year = ?"
        params.append(year)

    sql += " ORDER BY r.year DESC, r.stock_code"

    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)

    result = conn.execute(sql, params).fetchall()
    columns = [desc[0] for desc in conn.description]
    return [dict(zip(columns, row)) for row in result]


def get_pending_converts(
    conn: "duckdb.DuckDBPyConnection",
    *,
    year: int | None = None,
    limit: int | None = None,
) -> list[dict]:
    """获取待转换任务列表。"""
    sql = """
        SELECT r.stock_code, c.short_name, r.year, r.pdf_path, r.url
        FROM reports r
        LEFT JOIN companies c ON r.stock_code = c.stock_code
        WHERE r.download_status = 'success' AND r.convert_status = 'pending'
    """
    params = []

    if year is not None:
        sql += " AND r.year = ?"
        params.append(year)

    sql += " ORDER BY r.year DESC, r.stock_code"

    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)

    result = conn.execute(sql, params).fetchall()
    columns = [desc[0] for desc in conn.description]
    return [dict(zip(columns, row)) for row in result]


def report_exists(
    conn: "duckdb.DuckDBPyConnection",
    stock_code: str,
    year: int,
) -> bool:
    """检查年报记录是否存在。"""
    result = conn.execute(
        "SELECT 1 FROM reports WHERE stock_code = ? AND year = ?",
        (stock_code, year),
    ).fetchone()
    return result is not None


def get_report(
    conn: "duckdb.DuckDBPyConnection",
    stock_code: str,
    year: int,
) -> dict | None:
    """获取年报记录。"""
    result = conn.execute(
        "SELECT * FROM reports WHERE stock_code = ? AND year = ?",
        (stock_code, year),
    ).fetchone()
    if result is None:
        return None
    columns = [desc[0] for desc in conn.description]
    return dict(zip(columns, result))


# =============================================================================
# LLM 相关表操作
# =============================================================================


def insert_llm_call_log(
    conn: "duckdb.DuckDBPyConnection",
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
    """插入 LLM 调用日志。"""
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
            success,
            error_message,
            utc_now(),
        ),
    )


def upsert_strategy_stats(
    conn: "duckdb.DuckDBPyConnection",
    strategy: str,
    success: bool,
) -> None:
    """更新策略统计。"""
    conn.execute(
        """
        INSERT INTO strategy_stats (strategy, attempts, success, last_updated)
        VALUES (?, 1, ?, ?)
        ON CONFLICT (strategy) DO UPDATE SET
            attempts = strategy_stats.attempts + 1,
            success = strategy_stats.success + CASE WHEN ? THEN 1 ELSE 0 END,
            last_updated = ?;
        """,
        (
            strategy,
            1 if success else 0,
            utc_now(),
            success,
            utc_now(),
        ),
    )


def get_strategy_stats(
    conn: "duckdb.DuckDBPyConnection",
) -> dict[str, dict]:
    """获取策略统计。"""
    result = conn.execute("SELECT * FROM strategy_stats").fetchall()
    if not result:
        return {}

    columns = [desc[0] for desc in conn.description]
    stats = {}
    for row in result:
        row_dict = dict(zip(columns, row))
        stats[row_dict["strategy"]] = {
            "attempts": row_dict["attempts"],
            "success": row_dict["success"],
        }
    return stats


def upsert_extraction_rule(
    conn: "duckdb.DuckDBPyConnection",
    *,
    stock_code: str,
    year: int,
    start_pattern: str,
    end_pattern: str,
    report_signature: str | None = None,
    rule_source: str = "llm_learned",
) -> None:
    """插入或更新提取规则。"""
    conn.execute(
        """
        INSERT INTO extraction_rules (
            stock_code, year, report_signature,
            start_pattern, end_pattern,
            rule_source, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (stock_code, year) DO UPDATE SET
            start_pattern = EXCLUDED.start_pattern,
            end_pattern = EXCLUDED.end_pattern,
            report_signature = EXCLUDED.report_signature,
            rule_source = EXCLUDED.rule_source,
            updated_at = EXCLUDED.updated_at;
        """,
        (
            stock_code,
            year,
            report_signature,
            start_pattern,
            end_pattern,
            rule_source,
            utc_now(),
        ),
    )