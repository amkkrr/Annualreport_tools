#!/usr/bin/env python3
"""从 DuckDB 迁移元数据表到 SQLite。

此脚本将以下表从 DuckDB 迁移到 SQLite:
- companies
- reports
- extraction_rules
- extraction_errors
- strategy_stats
- llm_call_logs

保留 DuckDB 中的 mda_text 表用于 OLAP 分析。
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

import duckdb

from annual_report_mda import sqlite_db

DUCKDB_PATH = Path("data/annual_reports.duckdb")
SQLITE_PATH = Path("data/metadata.db")
BACKUP_PATH = Path("data/annual_reports.duckdb.bak")

TABLES_TO_MIGRATE = [
    "companies",
    "reports",
    "extraction_rules",
    "extraction_errors",
    "strategy_stats",
    "llm_call_logs",
]


def migrate_table(
    duck_conn: duckdb.DuckDBPyConnection,
    sqlite_conn,
    table: str,
    dry_run: bool = False,
) -> tuple[int, int]:
    """迁移单个表。

    Returns:
        (duck_count, sqlite_count) 元组
    """
    try:
        df = duck_conn.execute(f"SELECT * FROM {table}").df()
        duck_count = len(df)
        print(f"  迁移 {table}: {duck_count} 条记录")

        if dry_run or duck_count == 0:
            return (duck_count, 0)

        # 先清空目标表，避免主键冲突
        sqlite_conn.execute(f"DELETE FROM {table}")

        # 使用 pandas 写入 SQLite
        df.to_sql(table, sqlite_conn, if_exists="append", index=False)

        # 校验行数
        cursor = sqlite_conn.execute(f"SELECT COUNT(*) FROM {table}")
        sqlite_count = cursor.fetchone()[0]

        if duck_count != sqlite_count:
            print(f"  ⚠️ 警告: 行数不匹配 DuckDB={duck_count} vs SQLite={sqlite_count}")

        return (duck_count, sqlite_count)

    except duckdb.CatalogException:
        print(f"  跳过 {table}: 表不存在于 DuckDB")
        return (0, 0)
    except Exception as e:
        print(f"  ❌ 迁移 {table} 失败: {e}")
        raise


def sample_validate(
    duck_conn: duckdb.DuckDBPyConnection,
    sqlite_conn,
    table: str,
    sample_size: int = 5,
) -> bool:
    """抽样校验数据内容。"""
    try:
        # 从 DuckDB 抽样
        duck_df = duck_conn.execute(f"SELECT * FROM {table} LIMIT {sample_size}").df()

        if duck_df.empty:
            return True

        # 获取主键列 (假设第一列是主键之一)
        pk_col = duck_df.columns[0]
        pk_values = duck_df[pk_col].tolist()

        # 从 SQLite 查询对应记录
        placeholders = ",".join(["?" for _ in pk_values])
        cursor = sqlite_conn.execute(
            f"SELECT * FROM {table} WHERE {pk_col} IN ({placeholders})",
            pk_values,
        )
        sqlite_rows = cursor.fetchall()

        if len(sqlite_rows) != len(pk_values):
            print(f"  ⚠️ {table} 抽样校验: 找到 {len(sqlite_rows)}/{len(pk_values)} 条")
            return False

        return True

    except Exception as e:
        print(f"  ⚠️ {table} 抽样校验失败: {e}")
        return False


def main(
    dry_run: bool = False,
    skip_backup: bool = False,
    drop_tables: bool = False,
) -> int:
    """执行迁移。

    Args:
        dry_run: 预览模式，不实际修改数据
        skip_backup: 跳过备份步骤
        drop_tables: 迁移成功后从 DuckDB 删除已迁移的表

    Returns:
        0 表示成功，非 0 表示失败
    """
    print("=" * 60)
    print("DuckDB → SQLite 元数据迁移工具")
    print("=" * 60)

    if dry_run:
        print("🔍 预览模式 - 不会实际修改数据\n")
    else:
        print("⚡ 执行模式 - 将修改数据库\n")

    # 检查 DuckDB 文件是否存在
    if not DUCKDB_PATH.exists():
        print(f"❌ DuckDB 文件不存在: {DUCKDB_PATH}")
        return 1

    # 1. 备份
    if not skip_backup and not dry_run:
        print(f"📦 备份 {DUCKDB_PATH} → {BACKUP_PATH}")
        shutil.copy(DUCKDB_PATH, BACKUP_PATH)
        print(f"   备份完成: {BACKUP_PATH.stat().st_size / 1024 / 1024:.2f} MB\n")
    elif dry_run:
        print(f"📦 [预览] 将备份 {DUCKDB_PATH} → {BACKUP_PATH}\n")

    # 2. 连接数据库
    print("🔗 连接数据库...")
    duck_conn = duckdb.connect(str(DUCKDB_PATH), read_only=dry_run)
    sqlite_conn = sqlite_db.get_connection(SQLITE_PATH)

    # 3. 初始化 SQLite 表结构
    print("📋 初始化 SQLite 表结构...")
    if not dry_run:
        sqlite_db.init_db(sqlite_conn)
        sqlite_conn.commit()
    print("   表结构初始化完成\n")

    # 4. 迁移数据
    print("📊 迁移数据...")
    total_duck = 0
    total_sqlite = 0
    migration_results = {}

    for table in TABLES_TO_MIGRATE:
        duck_count, sqlite_count = migrate_table(duck_conn, sqlite_conn, table, dry_run)
        migration_results[table] = (duck_count, sqlite_count)
        total_duck += duck_count
        total_sqlite += sqlite_count

    # 提交 SQLite
    if not dry_run:
        sqlite_conn.commit()

    print(f"\n   总计: DuckDB {total_duck} 条 → SQLite {total_sqlite} 条\n")

    # 5. 抽样校验
    if not dry_run:
        print("🔬 抽样校验...")
        all_valid = True
        for table in TABLES_TO_MIGRATE:
            if migration_results[table][0] > 0:
                valid = sample_validate(duck_conn, sqlite_conn, table)
                if valid:
                    print(f"   ✅ {table} 校验通过")
                else:
                    all_valid = False

        if not all_valid:
            print("\n⚠️ 部分表校验失败，请检查数据")
        print()

    # 6. 可选: 从 DuckDB 删除已迁移的表
    if drop_tables and not dry_run:
        print("🗑️ 从 DuckDB 删除已迁移的表...")
        for table in TABLES_TO_MIGRATE:
            try:
                duck_conn.execute(f"DROP TABLE IF EXISTS {table}")
                print(f"   删除 {table}")
            except Exception as e:
                print(f"   ⚠️ 删除 {table} 失败: {e}")

        duck_conn.execute("VACUUM")
        print("   VACUUM 完成\n")
    elif drop_tables and dry_run:
        print("🗑️ [预览] 将删除以下表:")
        for table in TABLES_TO_MIGRATE:
            print(f"   - {table}")
        print()

    # 7. 清理
    duck_conn.close()
    sqlite_conn.close()

    print("=" * 60)
    print("✅ 迁移完成!")
    print("=" * 60)
    print("\n后续步骤:")
    print("1. 验证 WebUI 可正常访问数据")
    print("2. 测试爬虫可正常写入 SQLite")
    print("3. 测试 DuckDB 联邦查询 (ATTACH SQLite)")

    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="从 DuckDB 迁移元数据表到 SQLite",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 预览模式
  python scripts/migrate_duckdb_to_sqlite.py --dry-run

  # 执行迁移 (保留 DuckDB 表)
  python scripts/migrate_duckdb_to_sqlite.py

  # 执行迁移并删除 DuckDB 中的旧表
  python scripts/migrate_duckdb_to_sqlite.py --drop-tables
        """,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="预览模式，不实际修改数据",
    )
    parser.add_argument(
        "--skip-backup",
        action="store_true",
        help="跳过备份步骤",
    )
    parser.add_argument(
        "--drop-tables",
        action="store_true",
        help="迁移成功后从 DuckDB 删除已迁移的表",
    )

    args = parser.parse_args()
    sys.exit(
        main(
            dry_run=args.dry_run,
            skip_backup=args.skip_backup,
            drop_tables=args.drop_tables,
        )
    )
