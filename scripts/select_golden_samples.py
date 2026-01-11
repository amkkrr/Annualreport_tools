#!/usr/bin/env python3
"""
样本选取脚本 - 从已转换的 TXT 文件中选取黄金数据集样本

用法:
    python scripts/select_golden_samples.py --output data/samples.json --count 100
"""

import argparse
import json
import random
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import duckdb


def get_txt_files(txt_root: str) -> list[dict]:
    """
    扫描 TXT 目录，获取所有已转换的文件信息。

    Args:
        txt_root: TXT 文件根目录

    Returns:
        [{
            "txt_path": str,
            "stock_code": str,
            "year": int,
            "company_name": str,
            "file_size": int
        }, ...]
    """
    txt_root_path = Path(txt_root)
    if not txt_root_path.exists():
        raise FileNotFoundError(f"TXT 根目录不存在: {txt_root}")

    files = []

    # 遍历年份目录
    for year_dir in txt_root_path.iterdir():
        if not year_dir.is_dir():
            continue

        txt_dir = year_dir / "txt"
        if not txt_dir.exists():
            continue

        try:
            year = int(year_dir.name)
        except ValueError:
            continue

        # 扫描 TXT 文件
        for txt_file in txt_dir.glob("*.txt"):
            # 解析文件名: {stock_code}_{company_name}_{year}.txt
            parts = txt_file.stem.split("_")
            if len(parts) < 2:
                continue

            stock_code = parts[0]
            # 公司名可能包含下划线
            company_name = "_".join(parts[1:-1]) if len(parts) > 2 else parts[1]

            files.append(
                {
                    "txt_path": str(txt_file),
                    "stock_code": stock_code,
                    "year": year,
                    "company_name": company_name,
                    "file_size": txt_file.stat().st_size,
                }
            )

    return files


def get_samples_from_duckdb(db_path: str) -> list[dict]:
    """
    从 DuckDB 获取已有 TXT 文件的样本信息。

    Args:
        db_path: DuckDB 数据库路径

    Returns:
        样本列表
    """
    conn = duckdb.connect(db_path, read_only=True)

    try:
        # 查询已转换的报告
        query = """
        SELECT
            r.stock_code,
            r.year,
            r.sec_name as company_name,
            rp.txt_path
        FROM reports r
        JOIN reports_progress rp ON r.stock_code = rp.stock_code AND r.year = rp.year
        WHERE rp.txt_path IS NOT NULL AND rp.txt_path != ''
        """
        results = conn.execute(query).fetchall()

        samples = []
        for row in results:
            txt_path = row[3]
            if Path(txt_path).exists():
                samples.append(
                    {
                        "txt_path": txt_path,
                        "stock_code": row[0],
                        "year": row[1],
                        "company_name": row[2],
                        "file_size": Path(txt_path).stat().st_size,
                    }
                )

        return samples
    finally:
        conn.close()


def stratified_sample(
    files: list[dict], count: int = 100, year_balance: bool = True, size_diversity: bool = True
) -> list[dict]:
    """
    分层抽样选取样本。

    Args:
        files: 所有可用文件列表
        count: 目标样本数量
        year_balance: 是否按年份平衡
        size_diversity: 是否保证文件大小多样性

    Returns:
        选取的样本列表
    """
    if len(files) <= count:
        return files

    selected = []

    if year_balance:
        # 按年份分组
        by_year = defaultdict(list)
        for f in files:
            by_year[f["year"]].append(f)

        years = sorted(by_year.keys())
        per_year = count // len(years)
        remainder = count % len(years)

        for i, year in enumerate(years):
            year_files = by_year[year]
            year_count = per_year + (1 if i < remainder else 0)

            if size_diversity and len(year_files) > year_count:
                # 按文件大小排序，选取不同大小的样本
                sorted_files = sorted(year_files, key=lambda x: x["file_size"])
                # 等间距抽样
                indices = (
                    [int(i * (len(sorted_files) - 1) / (year_count - 1)) for i in range(year_count)]
                    if year_count > 1
                    else [0]
                )
                selected.extend([sorted_files[i] for i in indices[:year_count]])
            else:
                # 随机抽样
                selected.extend(random.sample(year_files, min(year_count, len(year_files))))
    else:
        selected = random.sample(files, count)

    return selected[:count]


def main():
    parser = argparse.ArgumentParser(
        description="从已转换的 TXT 文件中选取黄金数据集样本",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
    # 从文件系统扫描
    python scripts/select_golden_samples.py \\
        --txt-root outputs/annual_reports \\
        --output data/samples.json \\
        --count 100

    # 从 DuckDB 读取
    python scripts/select_golden_samples.py \\
        --source duckdb \\
        --output data/samples.json \\
        --count 100
        """,
    )

    parser.add_argument(
        "--txt-root", type=str, default="outputs/annual_reports", help="TXT 文件根目录"
    )
    parser.add_argument(
        "--source",
        type=str,
        default="filesystem",
        choices=["filesystem", "duckdb"],
        help="数据来源",
    )
    parser.add_argument(
        "--db-path", type=str, default="data/annual_reports.duckdb", help="DuckDB 数据库路径"
    )
    parser.add_argument("--output", type=str, default="data/samples.json", help="输出样本列表路径")
    parser.add_argument("--count", type=int, default=100, help="目标样本数量")
    parser.add_argument("--seed", type=int, default=42, help="随机种子")

    args = parser.parse_args()

    random.seed(args.seed)

    # 获取文件列表
    print("正在扫描文件...")

    if args.source == "duckdb":
        try:
            files = get_samples_from_duckdb(args.db_path)
        except Exception as e:
            print(f"DuckDB 读取失败: {e}")
            print("回退到文件系统扫描...")
            files = get_txt_files(args.txt_root)
    else:
        files = get_txt_files(args.txt_root)

    if not files:
        print("错误: 未找到任何 TXT 文件")
        sys.exit(1)

    print(f"找到 {len(files)} 个 TXT 文件")

    # 按年份统计
    by_year = defaultdict(int)
    for f in files:
        by_year[f["year"]] += 1

    print("年份分布:")
    for year in sorted(by_year.keys()):
        print(f"  {year}: {by_year[year]} 份")

    # 分层抽样
    selected = stratified_sample(files, args.count)
    print(f"\n已选取 {len(selected)} 个样本")

    # 输出选取结果的年份分布
    selected_by_year = defaultdict(int)
    for s in selected:
        selected_by_year[s["year"]] += 1

    print("选取样本年份分布:")
    for year in sorted(selected_by_year.keys()):
        print(f"  {year}: {selected_by_year[year]} 份")

    # 保存结果
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    output_data = {
        "version": "1.0",
        "created_at": datetime.now().isoformat(),
        "source": args.source,
        "total_available": len(files),
        "selected_count": len(selected),
        "samples": selected,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)

    print(f"\n样本列表已保存至: {args.output}")


if __name__ == "__main__":
    main()
