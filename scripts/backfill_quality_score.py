#!/usr/bin/env python3
"""
M2 验收 - 质量评分回填脚本

对现有 mda_text 记录重新计算 quality_score 和 needs_review。

用法:
    python scripts/backfill_quality_score.py [--dry-run] [--db PATH] [--limit N]
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from annual_report_mda.utils import configure_logging

_LOG = logging.getLogger(__name__)


def parse_score_detail_from_quality_detail(quality_detail: str | dict | None):
    """
    从 quality_detail JSON 中解析 score_detail。

    Returns:
        ScoreDetail | None
    """
    from annual_report_mda.scorer import ScoreDetail

    if not quality_detail:
        return None

    if isinstance(quality_detail, str):
        try:
            quality_detail = json.loads(quality_detail)
        except json.JSONDecodeError:
            return None

    if not isinstance(quality_detail, dict):
        return None

    sd = quality_detail.get("score_detail")
    if not sd or not isinstance(sd, dict):
        return None

    try:
        return ScoreDetail(
            keyword_hit_count=sd.get("keyword_hit_count", 0),
            keyword_total=sd.get("keyword_total", 1),
            dots_count=sd.get("dots_count", 0),
            length=sd.get("length", 0),
        )
    except Exception:
        return None


def parse_quality_flags(quality_flags: str | list | None) -> list[str]:
    """解析 quality_flags 字段。"""
    if not quality_flags:
        return []

    if isinstance(quality_flags, str):
        try:
            quality_flags = json.loads(quality_flags)
        except json.JSONDecodeError:
            return []

    if isinstance(quality_flags, list):
        return [str(f) for f in quality_flags if f]

    return []


def backfill_quality_scores(db_path: str, dry_run: bool = False, limit: int | None = None) -> dict:
    """
    回填质量评分。

    Returns:
        {
            "processed": int,
            "updated": int,
            "skipped": int,
            "errors": int,
            "success": bool
        }
    """
    import duckdb

    from annual_report_mda.scorer import calculate_quality_score

    conn = duckdb.connect(db_path)

    # 查询需要回填的记录
    sql = """
        SELECT stock_code, year, source_sha256, mda_raw, quality_flags, quality_detail
        FROM mda_text
        WHERE quality_score IS NULL OR needs_review IS NULL
    """
    if limit:
        sql += f" LIMIT {limit}"

    records = conn.execute(sql).fetchall()
    columns = ["stock_code", "year", "source_sha256", "mda_raw", "quality_flags", "quality_detail"]

    _LOG.info(f"找到 {len(records)} 条需要回填的记录")

    processed = 0
    updated = 0
    skipped = 0
    errors = 0

    for row in records:
        record = dict(zip(columns, row))
        stock_code = record["stock_code"]
        year = record["year"]
        source_sha256 = record["source_sha256"]
        mda_raw = record["mda_raw"] or ""
        quality_flags = parse_quality_flags(record["quality_flags"])
        score_detail = parse_score_detail_from_quality_detail(record["quality_detail"])

        processed += 1

        try:
            result = calculate_quality_score(
                text=mda_raw,
                quality_flags=quality_flags,
                score_detail=score_detail,
            )

            if dry_run:
                _LOG.debug(
                    f"[DRY-RUN] {stock_code}-{year}: score={result.score}, needs_review={result.needs_review}"
                )
            else:
                # 更新记录
                conn.execute(
                    """
                    UPDATE mda_text
                    SET quality_score = ?, needs_review = ?
                    WHERE stock_code = ? AND year = ? AND source_sha256 = ?
                    """,
                    (result.score, result.needs_review, stock_code, year, source_sha256),
                )
                _LOG.debug(f"已更新: {stock_code}-{year} score={result.score}")

            updated += 1

        except Exception as e:
            _LOG.warning(f"处理失败 {stock_code}-{year}: {e}")
            errors += 1

    conn.close()

    _LOG.info(f"回填完成: 处理 {processed}, 更新 {updated}, 跳过 {skipped}, 错误 {errors}")
    return {
        "processed": processed,
        "updated": updated,
        "skipped": skipped,
        "errors": errors,
        "success": errors == 0,
    }


def verify_backfill(db_path: str) -> dict:
    """验证回填结果。"""
    import duckdb

    conn = duckdb.connect(db_path, read_only=True)

    total = conn.execute("SELECT COUNT(*) FROM mda_text").fetchone()[0]
    with_score = conn.execute(
        "SELECT COUNT(*) FROM mda_text WHERE quality_score IS NOT NULL"
    ).fetchone()[0]
    needs_review_count = conn.execute(
        "SELECT COUNT(*) FROM mda_text WHERE needs_review = true"
    ).fetchone()[0]
    low_score_count = conn.execute(
        "SELECT COUNT(*) FROM mda_text WHERE quality_score < 60"
    ).fetchone()[0]

    # 采样检查
    sample = conn.execute(
        "SELECT stock_code, year, quality_score, needs_review FROM mda_text WHERE quality_score IS NOT NULL LIMIT 5"
    ).fetchall()

    conn.close()

    result = {
        "total_records": total,
        "with_score": with_score,
        "coverage": round(with_score / total * 100, 2) if total > 0 else 0,
        "needs_review_count": needs_review_count,
        "low_score_count": low_score_count,
        "sample": sample,
    }

    _LOG.info("验证结果:")
    _LOG.info(f"  总记录: {total}")
    _LOG.info(f"  有评分: {with_score} ({result['coverage']}%)")
    _LOG.info(f"  需审核: {needs_review_count}")
    _LOG.info(f"  低分(<60): {low_score_count}")

    return result


def main():
    parser = argparse.ArgumentParser(
        description="M2 验收 - 质量评分回填",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--db",
        default="data/annual_reports.duckdb",
        help="DuckDB 数据库路径 (默认 data/annual_reports.duckdb)",
    )
    parser.add_argument("--dry-run", action="store_true", help="仅打印将执行的操作，不实际修改")
    parser.add_argument("--limit", type=int, default=None, help="限制处理记录数（用于测试）")
    parser.add_argument("--verify", action="store_true", help="仅验证回填结果")

    args = parser.parse_args()

    if not Path(args.db).exists():
        _LOG.error(f"数据库文件不存在: {args.db}")
        sys.exit(1)

    if args.verify:
        verify_backfill(args.db)
        sys.exit(0)

    result = backfill_quality_scores(args.db, dry_run=args.dry_run, limit=args.limit)

    if not result["success"]:
        _LOG.warning("回填过程中有错误发生")

    if not args.dry_run:
        verify_backfill(args.db)


if __name__ == "__main__":
    configure_logging(level="INFO")
    main()
