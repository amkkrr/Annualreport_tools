from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from .utils import utc_now

SUCCESS_CHAR_COUNT_MIN = 500


def compute_file_sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True)
class MDAUpsertRecord:
    stock_code: str
    year: int

    mda_raw: str | None
    char_count: int | None

    page_index_start: int | None
    page_index_end: int | None
    page_count: int | None

    printed_page_start: int | None = None
    printed_page_end: int | None = None

    hit_start: str | None = None
    hit_end: str | None = None

    is_truncated: bool | None = None
    truncation_reason: str | None = None

    quality_flags: Sequence[str] | None = None
    quality_detail: dict[str, Any] | None = None

    source_path: str = ""
    source_sha256: str = ""

    extractor_version: str = ""
    used_rule_type: str | None = None
    extracted_at: datetime = field(default_factory=utc_now)

    # 字段切分结果
    mda_review: str | None = None
    mda_outlook: str | None = None
    outlook_split_position: int | None = None

    # 综合质量评分
    quality_score: int | None = None
    needs_review: bool = False


def is_successful_record(mda_raw: str | None, char_count: int | None) -> bool:
    if mda_raw is None:
        return False
    if char_count is None:
        return len(mda_raw) >= SUCCESS_CHAR_COUNT_MIN
    return char_count >= SUCCESS_CHAR_COUNT_MIN


def should_skip_incremental(
    conn: duckdb.DuckDBPyConnection,
    *,
    stock_code: str,
    year: int,
    source_sha256: str,
    min_char_count: int = SUCCESS_CHAR_COUNT_MIN,
) -> bool:
    row = conn.execute(
        """
        SELECT mda_raw, char_count
        FROM mda_text
        WHERE stock_code = ? AND year = ? AND source_sha256 = ?
        LIMIT 1;
        """,
        (stock_code, year, source_sha256),
    ).fetchone()

    if not row:
        return False

    mda_raw, char_count = row
    if mda_raw is None:
        return False
    if char_count is None:
        return len(mda_raw) >= min_char_count
    return int(char_count) >= min_char_count


def upsert_mda_text(conn: duckdb.DuckDBPyConnection, record: MDAUpsertRecord) -> None:
    quality_flags_json = (
        json.dumps(list(record.quality_flags), ensure_ascii=False)
        if record.quality_flags is not None
        else None
    )
    quality_detail_json = (
        json.dumps(record.quality_detail, ensure_ascii=False)
        if record.quality_detail is not None
        else None
    )

    conn.execute(
        """
        INSERT INTO mda_text (
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
            quality_flags,
            quality_detail,
            quality_score,
            needs_review,
            source_path,
            source_sha256,
            extractor_version,
            extracted_at,
            used_rule_type,
            mda_review,
            mda_outlook,
            outlook_split_position
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (stock_code, year, source_sha256) DO UPDATE SET
            mda_raw = excluded.mda_raw,
            char_count = excluded.char_count,
            page_index_start = excluded.page_index_start,
            page_index_end = excluded.page_index_end,
            page_count = excluded.page_count,
            printed_page_start = excluded.printed_page_start,
            printed_page_end = excluded.printed_page_end,
            hit_start = excluded.hit_start,
            hit_end = excluded.hit_end,
            is_truncated = excluded.is_truncated,
            truncation_reason = excluded.truncation_reason,
            quality_flags = excluded.quality_flags,
            quality_detail = excluded.quality_detail,
            quality_score = excluded.quality_score,
            needs_review = excluded.needs_review,
            source_path = excluded.source_path,
            extractor_version = excluded.extractor_version,
            extracted_at = excluded.extracted_at,
            used_rule_type = excluded.used_rule_type,
            mda_review = excluded.mda_review,
            mda_outlook = excluded.mda_outlook,
            outlook_split_position = excluded.outlook_split_position;
        """,
        (
            record.stock_code,
            record.year,
            record.mda_raw,
            record.char_count,
            record.page_index_start,
            record.page_index_end,
            record.page_count,
            record.printed_page_start,
            record.printed_page_end,
            record.hit_start,
            record.hit_end,
            record.is_truncated,
            record.truncation_reason,
            quality_flags_json,
            quality_detail_json,
            record.quality_score,
            record.needs_review,
            record.source_path,
            record.source_sha256,
            record.extractor_version,
            record.extracted_at,
            record.used_rule_type,
            record.mda_review,
            record.mda_outlook,
            record.outlook_split_position,
        ),
    )


def get_extraction_rule(
    conn: duckdb.DuckDBPyConnection,
    *,
    stock_code: str,
    year: int,
) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT report_signature, start_pattern, end_pattern, rule_source, updated_at
        FROM extraction_rules
        WHERE stock_code = ? AND year = ?
        LIMIT 1;
        """,
        (stock_code, year),
    ).fetchone()

    if not row:
        return None

    report_signature, start_pattern, end_pattern, rule_source, updated_at = row
    return {
        "report_signature": report_signature,
        "start_pattern": start_pattern,
        "end_pattern": end_pattern,
        "rule_source": rule_source,
        "updated_at": updated_at,
    }


def upsert_extraction_rule(
    conn: duckdb.DuckDBPyConnection,
    *,
    stock_code: str,
    year: int,
    start_pattern: str,
    end_pattern: str,
    rule_source: str,
    report_signature: str | None = None,
    updated_at: datetime | None = None,
) -> None:
    if updated_at is None:
        updated_at = __import__("annual_report_mda.utils", fromlist=["utc_now"]).utc_now()

    conn.execute(
        """
        INSERT INTO extraction_rules (
            stock_code,
            year,
            report_signature,
            start_pattern,
            end_pattern,
            rule_source,
            updated_at
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
            updated_at,
        ),
    )
