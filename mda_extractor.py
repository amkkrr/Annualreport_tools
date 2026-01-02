from __future__ import annotations

import argparse
import logging
import re
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict
from pathlib import Path
from typing import Any, Iterable, Optional

from annual_report_mda import EXTRACTOR_VERSION
from annual_report_mda.data_manager import MDAUpsertRecord, compute_file_sha256, should_skip_incremental, upsert_mda_text
from annual_report_mda.db import init_db, insert_extraction_error
from annual_report_mda.strategies import MAX_CHARS_DEFAULT, MAX_PAGES_DEFAULT, extract_mda_iterative
from annual_report_mda.text_loader import load_pages
from annual_report_mda.utils import configure_logging, load_dotenv_if_present


_LOG = logging.getLogger(__name__)


_STOCK_RE = re.compile(r"(?<!\d)(\d{6})(?!\d)")
_YEAR_RE = re.compile(r"(?<!\d)(20\d{2})(?!\d)")


def _iter_txt_files(root: Path) -> Iterable[Path]:
    for path in root.rglob("*.txt"):
        if path.is_file():
            yield path


def _infer_stock_year(path: Path) -> tuple[Optional[str], Optional[int]]:
    parent = path.parent.name
    if parent.isdigit() and len(parent) == 6:
        stock = parent
    else:
        m = _STOCK_RE.search(path.stem)
        stock = m.group(1) if m else None

    years = _YEAR_RE.findall(path.stem)
    year = int(years[-1]) if years else None
    return stock, year


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mda_extractor.py",
        description="A-share annual report MD&A text extractor (CPU-only).",
        formatter_class=argparse.RawTextHelpFormatter,
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--text", help="单文件模式：上游抽取的单个 *.txt。")
    group.add_argument("--dir", help="批量模式：扫描目录下所有 *.txt（递归）。")

    parser.add_argument("--db", default="data/annual_reports.duckdb", help="DuckDB 路径（默认 data/annual_reports.duckdb）。")
    parser.add_argument("--workers", type=int, default=4, help="并发数（默认 4）。")
    parser.add_argument("--incremental", action="store_true", help="增量模式：幂等键已成功入库则跳过。")
    parser.add_argument("--dry-run", action="store_true", help="仅跑提取，不写入数据库。")

    parser.add_argument("--stock-code", help="覆盖自动解析的 stock_code（仅建议单文件调试用）。")
    parser.add_argument("--year", type=int, help="覆盖自动解析的 year（仅建议单文件调试用）。")

    parser.add_argument("--max-pages", type=int, default=MAX_PAGES_DEFAULT, help=f"最大页数截断（默认 {MAX_PAGES_DEFAULT}）。")
    parser.add_argument("--max-chars", type=int, default=MAX_CHARS_DEFAULT, help=f"最大字符截断（默认 {MAX_CHARS_DEFAULT}）。")

    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="日志级别（默认 INFO）。")

    return parser


def _extract_one_worker(payload: dict[str, Any]) -> dict[str, Any]:
    path = Path(payload["path"])
    stock_code = payload["stock_code"]
    year = payload["year"]
    source_sha256 = payload["source_sha256"]
    max_pages = payload["max_pages"]
    max_chars = payload["max_chars"]
    custom_start_pattern = payload.get("custom_start_pattern")
    custom_end_pattern = payload.get("custom_end_pattern")

    load_result = load_pages(path)
    pages = load_result.pages

    extracted = extract_mda_iterative(
        pages,
        page_numbers=load_result.page_numbers,
        page_break_kind=load_result.page_break_kind,
        max_pages=max_pages,
        max_chars=max_chars,
        custom_start_pattern=custom_start_pattern,
        custom_end_pattern=custom_end_pattern,
    )

    if extracted is None:
        record = MDAUpsertRecord(
            stock_code=stock_code,
            year=year,
            mda_raw=None,
            char_count=None,
            page_index_start=None,
            page_index_end=None,
            page_count=None,
            printed_page_start=None,
            printed_page_end=None,
            hit_start=None,
            hit_end=None,
            is_truncated=None,
            truncation_reason=None,
            quality_flags=["FLAG_EXTRACT_FAILED"],
            quality_detail={
                "page_break_kind": load_result.page_break_kind,
                "encoding_used": load_result.encoding_used,
            },
            source_path=str(path),
            source_sha256=source_sha256,
            extractor_version=EXTRACTOR_VERSION,
            used_rule_type=None,
        )
        return {"ok": True, "record": asdict(record)}

    record = MDAUpsertRecord(
        stock_code=stock_code,
        year=year,
        mda_raw=extracted.mda_raw,
        char_count=len(extracted.mda_raw),
        page_index_start=extracted.page_index_start,
        page_index_end=extracted.page_index_end,
        page_count=extracted.page_count,
        printed_page_start=extracted.printed_page_start,
        printed_page_end=extracted.printed_page_end,
        hit_start=extracted.hit_start,
        hit_end=extracted.hit_end,
        is_truncated=extracted.is_truncated,
        truncation_reason=extracted.truncation_reason,
        quality_flags=extracted.quality_flags,
        quality_detail={
            **extracted.quality_detail,
            "encoding_used": load_result.encoding_used,
            "score_detail": asdict(extracted.score_detail),
        },
        source_path=str(path),
        source_sha256=source_sha256,
        extractor_version=EXTRACTOR_VERSION,
        used_rule_type=extracted.used_rule_type,
    )
    return {"ok": True, "record": asdict(record)}


def _submit_jobs(
    *,
    conn: "duckdb.DuckDBPyConnection",
    paths: list[Path],
    workers: int,
    incremental: bool,
    max_pages: int,
    max_chars: int,
) -> list[dict[str, Any]]:
    jobs: list[dict[str, Any]] = []

    for path in paths:
        stock_code, year = _infer_stock_year(path)
        if stock_code is None or year is None:
            jobs.append(
                {
                    "ok": False,
                    "error": {
                        "stock_code": None,
                        "year": None,
                        "source_path": str(path),
                        "source_sha256": None,
                        "error_type": "PARSE_ERROR",
                        "error_message": "无法从路径推断 stock_code/year；请使用 --stock-code/--year 或调整文件命名/目录结构。",
                    },
                }
            )
            continue

        source_sha256 = compute_file_sha256(path)
        if incremental and should_skip_incremental(conn, stock_code=stock_code, year=year, source_sha256=source_sha256):
            _LOG.info("增量跳过: %s", path)
            continue

        rule_row = conn.execute(
            """
            SELECT start_pattern, end_pattern
            FROM extraction_rules
            WHERE stock_code = ? AND year = ?
            LIMIT 1;
            """,
            (stock_code, year),
        ).fetchone()

        custom_start_pattern = rule_row[0] if rule_row else None
        custom_end_pattern = rule_row[1] if rule_row else None

        jobs.append(
            {
                "ok": True,
                "payload": {
                    "path": str(path),
                    "stock_code": stock_code,
                    "year": year,
                    "source_sha256": source_sha256,
                    "max_pages": max_pages,
                    "max_chars": max_chars,
                    "custom_start_pattern": custom_start_pattern,
                    "custom_end_pattern": custom_end_pattern,
                },
            }
        )

    if not (workers > 0):
        raise SystemExit("--workers 必须 > 0")

    results: list[dict[str, Any]] = []
    with ProcessPoolExecutor(max_workers=workers) as pool:
        future_to_payload = {
            pool.submit(_extract_one_worker, job["payload"]): job["payload"]
            for job in jobs
            if job.get("payload") is not None
        }

        for fut in as_completed(future_to_payload):
            payload = future_to_payload[fut]
            try:
                results.append(fut.result())
            except Exception as e:
                results.append(
                    {
                        "ok": False,
                        "error": {
                            "stock_code": payload.get("stock_code"),
                            "year": payload.get("year"),
                            "source_path": payload.get("path"),
                            "source_sha256": payload.get("source_sha256"),
                            "error_type": "EXTRACT_EXCEPTION",
                            "error_message": str(e),
                        },
                    }
                )

    for job in jobs:
        if job.get("payload") is None:
            results.append(job)

    return results


def _run_dry_run(args: argparse.Namespace) -> int:
    if args.text:
        path = Path(args.text)
        if not path.exists() or not path.is_file():
            raise SystemExit(f"文件不存在或不可读: {path}")

        stock_code = args.stock_code
        year = args.year
        if stock_code is None or year is None:
            inferred_stock, inferred_year = _infer_stock_year(path)
            stock_code = stock_code or inferred_stock
            year = year or inferred_year

        if stock_code is None or year is None:
            raise SystemExit("dry-run 单文件模式无法推断 stock_code/year，请传 --stock-code 与 --year。")

        source_sha256 = compute_file_sha256(path)
        res = _extract_one_worker(
            {
                "path": str(path),
                "stock_code": stock_code,
                "year": year,
                "source_sha256": source_sha256,
                "max_pages": args.max_pages,
                "max_chars": args.max_chars,
                "custom_start_pattern": None,
                "custom_end_pattern": None,
            }
        )
        record = MDAUpsertRecord(**res["record"])
        _LOG.info("dry-run: char_count=%s used_rule_type=%s", record.char_count, record.used_rule_type)
        return 0

    root = Path(args.dir)
    if not root.exists() or not root.is_dir():
        raise SystemExit(f"目录不存在或不可读: {root}")

    paths = sorted(_iter_txt_files(root))
    _LOG.info("dry-run: 扫描到 %d 个 TXT 文件", len(paths))

    payloads: list[dict[str, Any]] = []
    err_count = 0
    for path in paths:
        stock_code, year = _infer_stock_year(path)
        if stock_code is None or year is None:
            err_count += 1
            _LOG.warning("dry-run 跳过（无法推断 stock_code/year）: %s", path)
            continue
        payloads.append(
            {
                "path": str(path),
                "stock_code": stock_code,
                "year": year,
                "source_sha256": compute_file_sha256(path),
                "max_pages": args.max_pages,
                "max_chars": args.max_chars,
                "custom_start_pattern": None,
                "custom_end_pattern": None,
            }
        )

    if not (args.workers > 0):
        raise SystemExit("--workers 必须 > 0")

    ok_count = 0
    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        future_to_payload = {pool.submit(_extract_one_worker, p): p for p in payloads}
        for fut in as_completed(future_to_payload):
            try:
                fut.result()
                ok_count += 1
            except Exception as e:
                err_count += 1
                payload = future_to_payload[fut]
                _LOG.warning("dry-run 提取异常: %s (%s)", payload.get("path"), e)

    _LOG.info("dry-run 完成: ok=%d err=%d", ok_count, err_count)
    return 0 if err_count == 0 else 2


def main(argv: Optional[list[str]] = None) -> int:
    load_dotenv_if_present()
    parser = _build_parser()
    args = parser.parse_args(argv)

    configure_logging(args.log_level)

    if args.dry_run:
        return _run_dry_run(args)

    conn = init_db(args.db)

    if args.text:
        path = Path(args.text)
        if not path.exists() or not path.is_file():
            raise SystemExit(f"文件不存在或不可读: {path}")

        stock_code = args.stock_code
        year = args.year
        if stock_code is None or year is None:
            inferred_stock, inferred_year = _infer_stock_year(path)
            stock_code = stock_code or inferred_stock
            year = year or inferred_year

        if stock_code is None or year is None:
            raise SystemExit("单文件模式无法推断 stock_code/year，请传 --stock-code 与 --year。")

        source_sha256 = compute_file_sha256(path)
        if args.incremental and should_skip_incremental(conn, stock_code=stock_code, year=year, source_sha256=source_sha256):
            _LOG.info("增量跳过: %s", path)
            return 0

        rule_row = conn.execute(
            """
            SELECT start_pattern, end_pattern
            FROM extraction_rules
            WHERE stock_code = ? AND year = ?
            LIMIT 1;
            """,
            (stock_code, year),
        ).fetchone()

        custom_start_pattern = rule_row[0] if rule_row else None
        custom_end_pattern = rule_row[1] if rule_row else None

        try:
            res = _extract_one_worker(
                {
                    "path": str(path),
                    "stock_code": stock_code,
                    "year": year,
                    "source_sha256": source_sha256,
                    "max_pages": args.max_pages,
                    "max_chars": args.max_chars,
                    "custom_start_pattern": custom_start_pattern,
                    "custom_end_pattern": custom_end_pattern,
                }
            )
        except Exception as e:
            insert_extraction_error(
                conn,
                stock_code=stock_code,
                year=year,
                source_path=str(path),
                source_sha256=source_sha256,
                error_type="EXTRACT_EXCEPTION",
                error_message=str(e),
            )
            raise

        if not res.get("ok"):
            err = res["error"]
            insert_extraction_error(
                conn,
                stock_code=err.get("stock_code"),
                year=err.get("year"),
                source_path=err.get("source_path") or str(path),
                source_sha256=err.get("source_sha256") or source_sha256,
                error_type=err.get("error_type") or "UNKNOWN",
                error_message=err.get("error_message") or "",
            )
            return 2

        record = MDAUpsertRecord(**res["record"])
        if args.dry_run:
            _LOG.info("dry-run: char_count=%s used_rule_type=%s", record.char_count, record.used_rule_type)
            return 0

        upsert_mda_text(conn, record)
        _LOG.info("写入完成: %s %s sha=%s", record.stock_code, record.year, record.source_sha256)
        return 0

    root = Path(args.dir)
    if not root.exists() or not root.is_dir():
        raise SystemExit(f"目录不存在或不可读: {root}")

    paths = sorted(_iter_txt_files(root))
    _LOG.info("扫描到 %d 个 TXT 文件", len(paths))

    results = _submit_jobs(
        conn=conn,
        paths=paths,
        workers=args.workers,
        incremental=args.incremental,
        max_pages=args.max_pages,
        max_chars=args.max_chars,
    )

    ok_count = 0
    err_count = 0
    for item in results:
        if item.get("ok") is True and item.get("record") is not None:
            ok_count += 1
            if args.dry_run:
                continue
            record = MDAUpsertRecord(**item["record"])
            upsert_mda_text(conn, record)
        else:
            err_count += 1
            err = item.get("error") or {}
            insert_extraction_error(
                conn,
                stock_code=err.get("stock_code"),
                year=err.get("year"),
                source_path=err.get("source_path") or "",
                source_sha256=err.get("source_sha256"),
                error_type=err.get("error_type") or "UNKNOWN",
                error_message=err.get("error_message") or "",
            )

    _LOG.info("完成: ok=%d err=%d dry_run=%s", ok_count, err_count, args.dry_run)
    return 0 if err_count == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())