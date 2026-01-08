#!/usr/bin/env python
"""Excel 年报链接数据迁移到 DuckDB 的一次性脚本。

用法:
    python scripts/migrate_excel_to_duckdb.py --dry-run  # 预览
    python scripts/migrate_excel_to_duckdb.py            # 执行

特性:
    - 幂等: 重复执行不产生重复数据
    - 安全: 只读取 Excel，不修改原文件
    - 可恢复: 支持中断后继续
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def migrate_excel_to_duckdb(
    excel_path: Path,
    db_path: Path,
    dry_run: bool = False,
) -> tuple[int, int, int]:
    """执行迁移。返回 (总数, 新增数, 跳过数)。"""
    try:
        import pandas as pd
    except ImportError:
        logging.error("缺少 pandas 依赖，请安装: pip install pandas openpyxl")
        raise SystemExit(1)

    if not excel_path.exists():
        logging.error(f"Excel 文件不存在: {excel_path}")
        raise SystemExit(1)

    df = pd.read_excel(excel_path)
    logging.info(f"读取 Excel: {len(df)} 条记录")

    # 验证必需列
    required_cols = ["公司代码", "公司简称", "年份", "年报链接"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        logging.error(f"Excel 缺少必需列: {missing_cols}")
        raise SystemExit(1)

    if dry_run:
        logging.info("[DRY-RUN] 预览模式，不实际写入数据库")
        # 统计预期结果
        unique_companies = df["公司代码"].nunique()
        unique_reports = df.drop_duplicates(subset=["公司代码", "年份"]).shape[0]
        logging.info(f"[DRY-RUN] 预期写入: {unique_companies} 家公司, {unique_reports} 条年报记录")
        return len(df), 0, 0

    # 延迟导入数据库模块
    from annual_report_mda.db import init_db, insert_report, upsert_company

    # 确保数据库目录存在
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = init_db(db_path)

    new_count = 0
    skip_count = 0

    for idx, row in df.iterrows():
        stock_code = str(row["公司代码"]).zfill(6)
        short_name = str(row["公司简称"])
        year = int(row["年份"])
        url = str(row["年报链接"])
        title = row.get("标题", None)
        if title is not None:
            title = str(title) if not (isinstance(title, float) and title != title) else None

        # Upsert 公司
        upsert_company(conn, stock_code=stock_code, short_name=short_name)

        # 插入年报记录
        is_new = insert_report(
            conn,
            stock_code=stock_code,
            year=year,
            url=url,
            title=title,
            source="excel_migration",
        )

        if is_new:
            new_count += 1
        else:
            skip_count += 1

        # 每 1000 条输出进度
        if (idx + 1) % 1000 == 0:
            logging.info(f"进度: {idx + 1}/{len(df)} (新增: {new_count}, 跳过: {skip_count})")

    conn.close()
    return len(df), new_count, skip_count


def main() -> None:
    parser = argparse.ArgumentParser(
        description="迁移 Excel 年报链接到 DuckDB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
    # 预览模式（不写入数据库）
    python scripts/migrate_excel_to_duckdb.py --dry-run

    # 执行迁移
    python scripts/migrate_excel_to_duckdb.py

    # 指定自定义路径
    python scripts/migrate_excel_to_duckdb.py --excel my_data.xlsx --db my_db.duckdb
""",
    )
    parser.add_argument(
        "--excel",
        default="res/AnnualReport_links_2004_2023.xlsx",
        help="Excel 文件路径（默认: res/AnnualReport_links_2004_2023.xlsx）",
    )
    parser.add_argument(
        "--db",
        default="data/annual_reports.duckdb",
        help="DuckDB 数据库路径（默认: data/annual_reports.duckdb）",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="预览模式，只读取 Excel 不写入数据库",
    )

    args = parser.parse_args()

    logging.info("=" * 60)
    logging.info("Excel → DuckDB 迁移工具")
    logging.info(f"Excel 路径: {args.excel}")
    logging.info(f"DuckDB 路径: {args.db}")
    logging.info(f"预览模式: {args.dry_run}")
    logging.info("=" * 60)

    total, new, skip = migrate_excel_to_duckdb(
        Path(args.excel),
        Path(args.db),
        args.dry_run,
    )

    logging.info("=" * 60)
    logging.info(f"迁移完成: 总计 {total}, 新增 {new}, 跳过 {skip}")
    logging.info("=" * 60)


if __name__ == "__main__":
    main()
