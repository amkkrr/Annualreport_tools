# DuckDB 核心化实施规格书

> **任务类型**: FEAT+REFAC-S (新增数据库表 + 改造现有脚本)
> **复杂度**: 标准 (S)
> **创建日期**: 2026-01-08
> **关联里程碑**: M1 (配置统一 + DuckDB 写入跑通)

---

## 1. 调研总结 (I1/I2)

### 1.1 现状分析

| 组件 | 当前状态 | 改造需求 |
|------|---------|---------|
| `db.py` | 仅有 `mda_text`, `extraction_rules`, `extraction_errors` 三表 | 新增 `reports`, `companies` 表及 CRUD 函数 |
| `1.report_link_crawler.py` | 写入 Excel (`_save_to_excel`) | 新增 DuckDB 写入路径，支持增量 |
| `2.pdf_batch_converter.py` | 从 Excel 读取任务 | 从 DuckDB 读取，更新状态 |
| `schema.json` | 已定义完整表结构 | 作为实现蓝图 |

### 1.2 可复用资产

- `schema.json`: 完整的表结构定义，可直接转换为 DDL
- `CNINFOClient`: 爬虫 API 客户端，无需改动
- `PDFDownloader`, `PDFToTextConverter`: 下载/转换核心逻辑，无需改动
- `config_manager.py`: 已有配置加载机制，需扩展数据库初始化

### 1.3 技术决策

| 决策点 | 选项 | 选择 | 理由 |
|--------|------|------|------|
| 表创建位置 | 分散各模块 vs 集中 db.py | 集中 db.py | 单一职责，易于维护 |
| 增量策略 | UPSERT vs INSERT IGNORE | `INSERT OR IGNORE` | DuckDB 原生支持，简洁 |
| 兼容模式 | 环境变量 vs CLI 参数 | `--legacy` CLI 参数 | 用户友好，显式声明 |
| 迁移脚本 | 一次性 vs 可重复 | 幂等脚本（可重复执行） | 安全，便于测试 |

---

## 2. 设计规格 (P2)

### 2.1 数据库表结构 (基于 schema.json)

#### 2.1.1 `companies` 表

```sql
CREATE TABLE IF NOT EXISTS companies (
    stock_code VARCHAR PRIMARY KEY,      -- 6位股票代码
    short_name VARCHAR NOT NULL,          -- 公司简称
    full_name VARCHAR,                    -- 公司全称 (可选)
    plate VARCHAR,                        -- 板块代码: sz/sh/szmb/shmb/szcy/shkcp/bj
    trade VARCHAR,                        -- 行业代码
    trade_name VARCHAR,                   -- 行业名称
    first_seen_at TIMESTAMP,              -- 首次发现时间 (UTC)
    updated_at TIMESTAMP                  -- 最后更新时间 (UTC)
);
```

#### 2.1.2 `reports` 表

```sql
CREATE TABLE IF NOT EXISTS reports (
    stock_code VARCHAR NOT NULL,
    year INTEGER NOT NULL,
    announcement_id VARCHAR,              -- 巨潮公告ID
    title VARCHAR,                        -- 公告标题
    url VARCHAR NOT NULL,                 -- PDF 下载链接
    publish_date DATE,                    -- 发布日期

    -- 状态机字段
    download_status VARCHAR DEFAULT 'pending',  -- pending/downloading/success/failed
    convert_status VARCHAR DEFAULT 'pending',   -- pending/converting/success/failed
    extract_status VARCHAR DEFAULT 'pending',   -- pending/extracting/success/failed

    -- 错误信息
    download_error VARCHAR,
    convert_error VARCHAR,
    download_retries INTEGER DEFAULT 0,
    convert_retries INTEGER DEFAULT 0,

    -- 文件路径
    pdf_path VARCHAR,
    txt_path VARCHAR,
    pdf_size_bytes BIGINT,
    pdf_sha256 VARCHAR,
    txt_sha256 VARCHAR,

    -- 时间戳
    crawled_at TIMESTAMP,
    downloaded_at TIMESTAMP,
    converted_at TIMESTAMP,
    updated_at TIMESTAMP,

    -- 数据来源
    source VARCHAR DEFAULT 'cninfo',      -- cninfo/manual/excel_migration

    PRIMARY KEY (stock_code, year)
);

-- 索引
CREATE INDEX IF NOT EXISTS idx_reports_download_status ON reports(download_status);
CREATE INDEX IF NOT EXISTS idx_reports_convert_status ON reports(convert_status);
CREATE INDEX IF NOT EXISTS idx_reports_extract_status ON reports(extract_status);
```

#### 2.1.3 视图

```sql
-- 处理进度总览
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

-- 待下载任务
CREATE OR REPLACE VIEW pending_downloads AS
SELECT r.stock_code, c.short_name, r.year, r.url
FROM reports r
LEFT JOIN companies c ON r.stock_code = c.stock_code
WHERE r.download_status = 'pending'
ORDER BY r.year DESC, r.stock_code;

-- 待转换任务
CREATE OR REPLACE VIEW pending_converts AS
SELECT r.stock_code, c.short_name, r.year, r.pdf_path
FROM reports r
LEFT JOIN companies c ON r.stock_code = c.stock_code
WHERE r.download_status = 'success' AND r.convert_status = 'pending'
ORDER BY r.year DESC, r.stock_code;
```

### 2.2 db.py 扩展 API

#### 2.2.1 新增函数签名

```python
# === companies 表操作 ===
def upsert_company(
    conn: duckdb.DuckDBPyConnection,
    *,
    stock_code: str,
    short_name: str,
    plate: str | None = None,
    trade: str | None = None,
    trade_name: str | None = None,
) -> None:
    """插入或更新公司信息。"""

def get_company(
    conn: duckdb.DuckDBPyConnection,
    stock_code: str,
) -> dict | None:
    """获取公司信息。"""

# === reports 表操作 ===
def insert_report(
    conn: duckdb.DuckDBPyConnection,
    *,
    stock_code: str,
    year: int,
    url: str,
    title: str | None = None,
    publish_date: date | None = None,
    source: str = "cninfo",
) -> bool:
    """插入年报记录（增量模式，已存在则跳过）。返回是否新增。"""

def update_report_status(
    conn: duckdb.DuckDBPyConnection,
    *,
    stock_code: str,
    year: int,
    download_status: str | None = None,
    convert_status: str | None = None,
    extract_status: str | None = None,
    pdf_path: str | None = None,
    txt_path: str | None = None,
    download_error: str | None = None,
    convert_error: str | None = None,
    extract_error: str | None = None,  # P3 审计补充
) -> None:
    """更新年报处理状态。动态构建 SET 子句，仅更新非 None 字段。"""

def get_pending_downloads(
    conn: duckdb.DuckDBPyConnection,
    *,
    year: int | None = None,
    limit: int | None = None,
) -> list[dict]:
    """获取待下载任务列表。"""

def get_pending_converts(
    conn: duckdb.DuckDBPyConnection,
    *,
    year: int | None = None,
    limit: int | None = None,
) -> list[dict]:
    """获取待转换任务列表。"""

def report_exists(
    conn: duckdb.DuckDBPyConnection,
    stock_code: str,
    year: int,
) -> bool:
    """检查年报记录是否存在。"""
```

### 2.3 爬虫改造 (1.report_link_crawler.py)

#### 2.3.1 新增 DuckDB 写入路径

```python
class AnnualReportCrawler:
    def __init__(self, config: CrawlerConfig, db_conn: duckdb.DuckDBPyConnection | None = None):
        self.config = config
        self.client = CNINFOClient(config)
        self.db_conn = db_conn  # 新增：可选的数据库连接

    def _save_to_duckdb(self, data: list[dict]) -> tuple[int, int]:
        """保存数据到 DuckDB。返回 (新增数, 跳过数)。"""
        if self.db_conn is None:
            raise RuntimeError("未提供数据库连接")

        new_count = 0
        skip_count = 0

        for item in data:
            # 先 upsert 公司信息
            upsert_company(
                self.db_conn,
                stock_code=item["company_code"],
                short_name=item["company_name"],
            )

            # 插入年报记录（增量）
            is_new = insert_report(
                self.db_conn,
                stock_code=item["company_code"],
                year=int(item["year"]),
                url=item["url"],
                title=item["title"],
                source="cninfo",
            )

            if is_new:
                new_count += 1
            else:
                skip_count += 1

        return new_count, skip_count

    def run(self) -> None:
        # ... 现有逻辑 ...

        # 根据配置选择保存方式
        if self.db_conn is not None:
            new_count, skip_count = self._save_to_duckdb(parsed_data)
            logging.info(f"DuckDB 写入完成: 新增 {new_count}, 跳过 {skip_count}")
        else:
            self._save_to_excel(parsed_data, str(output_path))
```

#### 2.3.2 CLI 参数扩展

```python
parser.add_argument(
    "--output-mode",
    choices=["excel", "duckdb"],
    default="duckdb",
    help="输出模式: excel (旧版) 或 duckdb (推荐)。",
)
parser.add_argument(
    "--legacy",
    action="store_true",
    help="等同于 --output-mode excel，兼容旧版工作流。",
)
```

### 2.4 转换器改造 (2.pdf_batch_converter.py)

#### 2.4.1 新增 DuckDB 任务源

```python
class AnnualReportProcessor:
    def __init__(
        self,
        config: ConverterConfig,
        db_conn: duckdb.DuckDBPyConnection | None = None,
    ):
        self.config = config
        self.converter = PDFConverter(config)
        self.db_conn = db_conn

    def _load_tasks_from_duckdb(self) -> list[dict]:
        """从 DuckDB 加载待处理任务。"""
        return get_pending_downloads(
            self.db_conn,
            year=self.config.target_year,
        )

    def _update_task_status(
        self,
        stock_code: str,
        year: int,
        success: bool,
        pdf_path: str | None = None,
        txt_path: str | None = None,
        error: str | None = None,
    ) -> None:
        """更新任务状态到 DuckDB。"""
        if success:
            update_report_status(
                self.db_conn,
                stock_code=stock_code,
                year=year,
                download_status="success",
                convert_status="success",
                pdf_path=pdf_path,
                txt_path=txt_path,
            )
        else:
            update_report_status(
                self.db_conn,
                stock_code=stock_code,
                year=year,
                download_status="failed",
                error=error,
            )
```

#### 2.4.2 CLI 参数扩展

```python
parser.add_argument(
    "--source",
    choices=["excel", "duckdb"],
    default="duckdb",
    help="任务来源: excel (旧版) 或 duckdb (推荐)。",
)
parser.add_argument(
    "--legacy",
    action="store_true",
    help="等同于 --source excel，兼容旧版工作流。",
)
```

### 2.5 迁移脚本 (scripts/migrate_excel_to_duckdb.py)

```python
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

import argparse
import logging
from pathlib import Path

import pandas as pd

from annual_report_mda.db import init_db, insert_report, upsert_company
from annual_report_mda.config_manager import load_config


def migrate_excel_to_duckdb(
    excel_path: Path,
    db_path: Path,
    dry_run: bool = False,
) -> tuple[int, int, int]:
    """执行迁移。返回 (总数, 新增数, 跳过数)。"""

    df = pd.read_excel(excel_path)
    logging.info(f"读取 Excel: {len(df)} 条记录")

    if dry_run:
        logging.info("[DRY-RUN] 预览模式，不实际写入数据库")
        return len(df), 0, 0

    conn = init_db(db_path)

    new_count = 0
    skip_count = 0

    for _, row in df.iterrows():
        stock_code = str(row["公司代码"]).zfill(6)
        short_name = row["公司简称"]
        year = int(row["年份"])
        url = row["年报链接"]
        title = row.get("标题", None)

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

    conn.close()
    return len(df), new_count, skip_count


def main():
    parser = argparse.ArgumentParser(description="迁移 Excel 年报链接到 DuckDB")
    parser.add_argument("--excel", default="res/AnnualReport_links_2004_2023.xlsx")
    parser.add_argument("--db", default="data/annual_reports.duckdb")
    parser.add_argument("--dry-run", action="store_true", help="预览模式")
    args = parser.parse_args()

    total, new, skip = migrate_excel_to_duckdb(
        Path(args.excel),
        Path(args.db),
        args.dry_run,
    )

    logging.info(f"迁移完成: 总计 {total}, 新增 {new}, 跳过 {skip}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
```

---

## 3. 实施任务分解 (P4)

### 3.1 任务清单

| # | 任务 | 文件 | 验收标准 | 依赖 |
|---|------|------|---------|------|
| 1 | 扩展 db.py: 添加 companies/reports 表 DDL | `annual_report_mda/db.py` | 调用 `init_db` 后表存在 | - |
| 2 | 扩展 db.py: 添加视图 DDL | `annual_report_mda/db.py` | 视图可查询 | #1 |
| 3 | 扩展 db.py: 添加 CRUD 函数 | `annual_report_mda/db.py` | 函数可调用且返回正确 | #1 |
| 4 | 改造爬虫: 添加 DuckDB 写入路径 | `1.report_link_crawler.py` | `--output-mode duckdb` 可用 | #3 |
| 5 | 改造爬虫: 实现增量逻辑 | `1.report_link_crawler.py` | 重复爬取不产生重复数据 | #4 |
| 6 | 改造转换器: 从 DuckDB 读取任务 | `2.pdf_batch_converter.py` | `--source duckdb` 可用 | #3 |
| 7 | 改造转换器: 更新状态到 DuckDB | `2.pdf_batch_converter.py` | 处理后状态正确更新 | #6 |
| 8 | 编写迁移脚本 | `scripts/migrate_excel_to_duckdb.py` | 迁移后数据完整 | #3 |
| 9 | 添加 `--legacy` 兼容参数 | 两个脚本 | 旧版 Excel 工作流可用 | #4, #6 |
| 10 | 编写单元测试 | `tests/test_m1_*.py` | M1 验收用例通过 | #1-9 |

### 3.2 里程碑验收对照 (M1)

| 验收项 | 任务对应 | 测试方法 |
|--------|---------|---------|
| M1-01 配置加载 | 已完成 (M1.1) | `pytest tests/test_m1_config.py` |
| M1-02 配置校验 | 已完成 (M1.1) | `pytest tests/test_m1_config.py` |
| M1-03 爬虫写入 | #4, #5 | `pytest tests/test_m1_crawler.py` |
| M1-04 增量爬取 | #5 | `pytest tests/test_m1_crawler.py::test_incremental` |
| M1-05 数据迁移 | #8 | `python scripts/migrate_excel_to_duckdb.py && duckdb ...` |
| M1-06 兼容模式 | #9 | `python 2.pdf_batch_converter.py download --legacy ...` |

---

## 4. 风险与缓解

| 风险 | 可能性 | 影响 | 缓解措施 |
|------|--------|------|---------|
| Excel 字段名不一致 | 中 | 迁移失败 | 迁移脚本增加字段映射逻辑 |
| 多进程并发写 DuckDB | 中 | 数据损坏 | 使用连接池或单写多读模式 |
| 旧版用户习惯迁移成本 | 低 | 用户抱怨 | 提供 `--legacy` 兼容模式 |

---

## 5. P3 审计报告

> **审计日期**: 2026-01-08
> **审计状态**: ✅ 通过 (含修复项)

### 5.1 检查清单

| 检查项 | 状态 | 说明 |
|--------|------|------|
| **表结构与 schema.json 一致** | ✅ | DDL 完全匹配 schema.json 定义 |
| **CRUD 函数覆盖完整** | ⚠️ | 缺少 `extract_error` 字段处理，已补充 |
| **TODO.md 任务覆盖** | ✅ | 1.2 和 1.3 所有子项均已覆盖 |
| **M1 验收标准对应** | ✅ | M1-03 至 M1-06 均有任务对应 |
| **CLI 参数一致性** | ⚠️ | 爬虫用 `--output-mode`，转换器用 `--source`，需统一命名 |
| **多进程安全** | ⚠️ | DuckDB 多进程写入需要注意，已在风险中记录 |
| **边界情况处理** | ⚠️ | 迁移脚本未处理 Excel 字段缺失情况 |

### 5.2 发现的问题与修复

#### Issue #1: `update_report_status` 函数缺少 `extract_error` 字段
- **影响**: M2 阶段 extract_status 更新时无法记录错误
- **修复**: 函数签名已补充 `extract_error` 参数

#### Issue #2: CLI 参数命名不一致
- **现状**: 爬虫 `--output-mode`，转换器 `--source`
- **建议**: 统一为 `--mode` 或保持现状（语义上有区别，可接受）
- **决定**: 保持现状 - 爬虫强调"输出"，转换器强调"来源"

#### Issue #3: 迁移脚本字段容错
- **问题**: Excel 可能缺少 `标题` 列
- **修复**: 使用 `row.get("标题", None)` 而非 `row["标题"]`（已在代码中体现）

#### Issue #4: DuckDB 多进程写入
- **问题**: 多进程并发写入同一 DuckDB 文件可能导致锁竞争
- **缓解方案**:
  1. 爬虫：单进程写入（爬取本身是 I/O bound）
  2. 转换器：主进程统一写入，worker 只返回结果
- **实施**: 在任务 #6/#7 中体现

#### Issue #5: `insert_report` 增量语义
- **问题**: `INSERT OR IGNORE` 在 URL 变化时不会更新
- **决定**: 符合预期 - 增量模式下以 (stock_code, year) 为主键，URL 变化视为同一记录

### 5.3 schema.json 对照验证

| schema.json 字段 | 规格书 DDL | 状态 |
|-----------------|-----------|------|
| companies.stock_code | VARCHAR PRIMARY KEY | ✅ |
| companies.short_name | VARCHAR NOT NULL | ✅ |
| companies.full_name | VARCHAR | ✅ |
| companies.plate | VARCHAR | ✅ |
| companies.trade | VARCHAR | ✅ |
| companies.trade_name | VARCHAR | ✅ |
| companies.first_seen_at | TIMESTAMP | ✅ |
| companies.updated_at | TIMESTAMP | ✅ |
| reports.* (22 fields) | 全部匹配 | ✅ |
| 视图 reports_progress | 已定义 | ✅ |
| 视图 pending_tasks | 改名为 pending_downloads | ⚠️ 可接受 |

### 5.4 审计结论

**通过审计**，规格书可进入实施阶段。需在实施时注意：
1. 多进程场景下使用主进程统一写入模式
2. 迁移脚本增加进度显示（5万条数据需要反馈）
3. 单元测试覆盖边界情况（空 Excel、重复执行等）

---

## 6. 附录: 命令速查

```bash
# 初始化数据库
python -c "from annual_report_mda.db import init_db; init_db('data/annual_reports.duckdb')"

# 迁移历史数据
python scripts/migrate_excel_to_duckdb.py --dry-run
python scripts/migrate_excel_to_duckdb.py

# 爬虫 (DuckDB 模式)
python 1.report_link_crawler.py --use-config --output-mode duckdb

# 爬虫 (兼容模式)
python 1.report_link_crawler.py --use-config --legacy

# 下载转换 (DuckDB 模式)
python 2.pdf_batch_converter.py download --use-yaml-config --source duckdb

# 下载转换 (兼容模式)
python 2.pdf_batch_converter.py download --use-yaml-config --legacy

# 查询进度
duckdb data/annual_reports.duckdb "SELECT * FROM reports_progress"

# 运行 M1 测试
pytest tests/test_m1_*.py -v
```
