#!/usr/bin/env python3
"""
M2 验收 - Schema 迁移脚本

为 mda_text 表添加缺失的字段:
- quality_score INTEGER
- needs_review BOOLEAN DEFAULT FALSE
- mda_review TEXT
- mda_outlook TEXT
- outlook_split_position INTEGER

用法:
    python scripts/migrate_mda_schema.py [--dry-run] [--db PATH]
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
_LOG = logging.getLogger(__name__)


MIGRATION_COLUMNS = [
    ("quality_score", "INTEGER"),
    ("needs_review", "BOOLEAN DEFAULT FALSE"),
    ("mda_review", "TEXT"),
    ("mda_outlook", "TEXT"),
    ("outlook_split_position", "INTEGER"),
]


def get_existing_columns(conn) -> set[str]:
    """获取 mda_text 表现有列名。"""
    result = conn.execute("PRAGMA table_info(mda_text)").fetchall()
    return {row[1] for row in result}


def migrate_schema(db_path: str, dry_run: bool = False) -> dict:
    """
    执行 Schema 迁移。

    Returns:
        {
            "added_columns": list[str],
            "skipped_columns": list[str],
            "success": bool
        }
    """
    import duckdb

    conn = duckdb.connect(db_path)

    existing = get_existing_columns(conn)
    _LOG.info(f"现有列数: {len(existing)}")

    added = []
    skipped = []

    for col_name, col_type in MIGRATION_COLUMNS:
        if col_name in existing:
            _LOG.info(f"列已存在，跳过: {col_name}")
            skipped.append(col_name)
            continue

        sql = f"ALTER TABLE mda_text ADD COLUMN {col_name} {col_type}"
        if dry_run:
            _LOG.info(f"[DRY-RUN] 将执行: {sql}")
        else:
            try:
                conn.execute(sql)
                _LOG.info(f"已添加列: {col_name} ({col_type})")
                added.append(col_name)
            except Exception as e:
                _LOG.error(f"添加列失败 {col_name}: {e}")
                conn.close()
                return {
                    "added_columns": added,
                    "skipped_columns": skipped,
                    "success": False,
                    "error": str(e),
                }

    conn.close()

    _LOG.info(f"迁移完成: 添加 {len(added)} 列, 跳过 {len(skipped)} 列")
    return {"added_columns": added, "skipped_columns": skipped, "success": True}


def verify_migration(db_path: str) -> bool:
    """验证迁移是否成功。"""
    import duckdb

    conn = duckdb.connect(db_path, read_only=True)
    existing = get_existing_columns(conn)
    conn.close()

    expected = {col_name for col_name, _ in MIGRATION_COLUMNS}
    missing = expected - existing

    if missing:
        _LOG.error(f"迁移验证失败，缺少列: {missing}")
        return False

    _LOG.info("迁移验证通过")
    return True


def main():
    parser = argparse.ArgumentParser(
        description="M2 验收 - mda_text 表 Schema 迁移",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--db",
        default="data/annual_reports.duckdb",
        help="DuckDB 数据库路径 (默认 data/annual_reports.duckdb)",
    )
    parser.add_argument("--dry-run", action="store_true", help="仅打印将执行的操作，不实际修改")
    parser.add_argument("--verify", action="store_true", help="仅验证迁移是否完成")

    args = parser.parse_args()

    if not Path(args.db).exists():
        _LOG.error(f"数据库文件不存在: {args.db}")
        sys.exit(1)

    if args.verify:
        success = verify_migration(args.db)
        sys.exit(0 if success else 1)

    result = migrate_schema(args.db, dry_run=args.dry_run)

    if not result["success"]:
        _LOG.error(f"迁移失败: {result.get('error')}")
        sys.exit(1)

    if not args.dry_run:
        verify_migration(args.db)


if __name__ == "__main__":
    main()
