# 规格书: 双数据库架构 (SQLite + DuckDB)

**文档版本**: 1.1 (Claude 审核完善版)
**日期**: 2026-01-11
**作者**: Gemini (初稿), Claude (审核完善)

## 目录
1. [架构总览](#1-架构总览)
2. [数据模型](#2-数据模型)
3. [配置管理](#3-配置管理)
4. [连接管理](#4-连接管理)
5. [核心接口设计](#5-核心接口设计)
6. [迁移策略](#6-迁移策略)
7. [模块适配](#7-模块适配)
8. [验收标准](#8-验收标准)
9. [风险与缓解](#9-风险与缓解)
10. [实施计划](#10-实施计划)

---

## 1. 架构总览

为了解决当前 DuckDB 单写者模型导致的并发冲突问题，我们引入 SQLite 作为元数据和高频事务性数据的存储后端 (OLTP)，同时保留 DuckDB 用于存储大文本 `mda_text` 并作为主要的分析查询入口 (OLAP)。

### 1.1. 双数据库拓扑图

```ascii
┌─────────────────────────────────────────────────────────────────┐
│                            应用层 (Python)                       │
├───────────────────┬───────────────────┬─────────────────────────┤
│ 爬虫/下载器/提取器 │   WebUI (Streamlit)   │  分析脚本 (Jupyter)    │
│ (高频写入元数据)   │ (高频读取状态)        │  (复杂聚合/联邦查询)   │
└─────────┬─────────┴─────────┬─────────┴──────────┬──────────────┘
          │(读/写)            │(只读)               │(只读)
          │                   │                    │
┌─────────▼─────────┐ ┌───────▼───────┐    ┌───────▼───────┐
│  sqlite_db.py     │ │ db_utils.py   │    │     db.py     │
│  (SQLite 接口层)  │ │ (WebUI 接口层)│    │ (DuckDB 接口层)│
└─────────┬─────────┘ └───────┬───────┘    └───────┬───────┘
          │                   │                    │
          │(并发读写)         │(并发读)             │
          ▼                   ▼                    │
┌─────────────────────────────────────┐            │
│            SQLite (OLTP)            │            │
│  - 路径: data/metadata.db           │            │
│  - 模式: WAL (并发读写)             │            │
│  - 表: companies, reports, logs...  │            │
└─────────────────────────────────────┘            │
          ▲                                        │
          │(ATTACH, 用于联邦查询)                  │
          └────────────────────────────────────────┤
                                                   │
                                                   ▼(写入大文本，联邦查询)
                                          ┌───────────────────────┐
                                          │    DuckDB (OLAP)      │
                                          │  - 路径: data/annual_reports.duckdb
                                          │  - 表: mda_text       │
                                          │  - 视图: 联邦查询视图 │
                                          └───────────────────────┘
```

### 1.2. 数据流向说明

1.  **元数据写入 (OLTP 流)**:
    *   **爬虫**: 写入 SQLite 的 `companies` 和 `reports` 表
    *   **下载器/转换器**: 更新 `reports` 表的状态字段
    *   **提取器**: 更新 `reports.extract_status`，写入 `llm_call_logs`, `extraction_errors` 等

2.  **大文本写入 (OLAP 流)**:
    *   **提取器**: 成功提取 MD&A 文本后，写入 DuckDB 的 `mda_text` 表

3.  **WebUI 读取 (高频读流)**:
    *   通过 `db_utils.py` 连接 SQLite，查询进度和状态

4.  **分析查询 (联邦查询流)**:
    *   通过 `db.py` 连接 DuckDB，自动 `ATTACH` SQLite，支持跨库 JOIN

---

## 2. 数据模型

### 2.1. SQLite 表定义

文件路径: `data/metadata.db`

```sql
-- 公司基本信息表
CREATE TABLE IF NOT EXISTS companies (
    stock_code TEXT PRIMARY KEY,
    short_name TEXT NOT NULL,
    full_name TEXT,
    plate TEXT,
    trade TEXT,
    trade_name TEXT,
    first_seen_at TEXT,  -- ISO8601 格式
    updated_at TEXT
);

-- 年报元数据与生命周期管理表
CREATE TABLE IF NOT EXISTS reports (
    stock_code TEXT NOT NULL,
    year INTEGER NOT NULL,
    announcement_id TEXT,
    title TEXT,
    url TEXT NOT NULL,
    publish_date TEXT,  -- ISO8601 日期

    download_status TEXT DEFAULT 'pending',  -- pending/downloading/success/failed
    convert_status TEXT DEFAULT 'pending',   -- pending/converting/success/failed
    extract_status TEXT DEFAULT 'pending',   -- pending/extracting/success/failed

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
    success INTEGER,  -- 0 或 1
    error_message TEXT,
    created_at TEXT
);
```

### 2.2. DuckDB 表定义

文件路径: `data/annual_reports.duckdb`

```sql
-- MDA 提取结果表 (保持不变)
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
```

### 2.3. 表拆分方案

| 表名                | 原数据库 | 目标数据库 | 理由                               |
| ------------------- | -------- | ---------- | ---------------------------------- |
| `companies`         | DuckDB   | **SQLite** | 元数据，爬虫写入，WebUI 读取       |
| `reports`           | DuckDB   | **SQLite** | 核心状态表，各模块高频更新和读取   |
| `extraction_rules`  | DuckDB   | **SQLite** | 规则元数据，提取器读写             |
| `extraction_errors` | DuckDB   | **SQLite** | 错误日志，提取器高频写入           |
| `strategy_stats`    | DuckDB   | **SQLite** | LLM 统计，提取器高频更新           |
| `llm_call_logs`     | DuckDB   | **SQLite** | LLM 日志，提取器高频写入           |
| `mda_text`          | DuckDB   | **DuckDB** | 大文本 OLAP 数据，分析脚本读取     |

---

## 3. 配置管理

### 3.1. config.yaml 新增配置项

在 `database` 节点下新增 SQLite 配置：

```yaml
# 数据库配置
database:
  # DuckDB (OLAP - 大文本分析)
  duckdb_path: "data/annual_reports.duckdb"

  # SQLite (OLTP - 元数据和状态)
  sqlite_path: "data/metadata.db"

  # SQLite WAL 模式配置
  sqlite:
    busy_timeout: 5000      # 锁等待超时 (毫秒)
    synchronous: "NORMAL"   # FULL/NORMAL/OFF
    journal_mode: "WAL"     # WAL/DELETE/TRUNCATE
```

### 3.2. Pydantic 配置类扩展

```python
# annual_report_mda/config_manager.py

class SQLiteConfig(BaseModel):
    busy_timeout: int = 5000
    synchronous: Literal["FULL", "NORMAL", "OFF"] = "NORMAL"
    journal_mode: Literal["WAL", "DELETE", "TRUNCATE"] = "WAL"

class DatabaseConfig(BaseModel):
    duckdb_path: Path = Path("data/annual_reports.duckdb")
    sqlite_path: Path = Path("data/metadata.db")
    sqlite: SQLiteConfig = SQLiteConfig()

    # 兼容旧配置
    @property
    def path(self) -> Path:
        """兼容旧的 database.path 配置"""
        return self.duckdb_path
```

---

## 4. 连接管理

### 4.1. SQLite 连接工厂

```python
# annual_report_mda/sqlite_db.py

import sqlite3
from pathlib import Path
from contextlib import contextmanager

DEFAULT_SQLITE_PATH = Path("data/metadata.db")

def get_connection(
    db_path: str | Path = DEFAULT_SQLITE_PATH,
    read_only: bool = False,
    busy_timeout: int = 5000,
) -> sqlite3.Connection:
    """创建并配置一个 SQLite 连接。

    Args:
        db_path: 数据库文件路径
        read_only: 是否只读模式
        busy_timeout: 锁等待超时（毫秒）

    Returns:
        配置好的 SQLite 连接
    """
    db_path = Path(db_path)

    # 确保目录存在
    if not read_only:
        db_path.parent.mkdir(parents=True, exist_ok=True)

    # 构建 URI
    mode = "ro" if read_only else "rwc"
    uri = f"file:{db_path}?mode={mode}"

    conn = sqlite3.connect(uri, uri=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row

    # 配置 PRAGMA
    conn.execute(f"PRAGMA busy_timeout = {busy_timeout};")
    if not read_only:
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")

    return conn

@contextmanager
def connection_context(
    db_path: str | Path = DEFAULT_SQLITE_PATH,
    read_only: bool = False,
):
    """SQLite 连接上下文管理器，自动提交/回滚。"""
    conn = get_connection(db_path, read_only=read_only)
    try:
        yield conn
        if not read_only:
            conn.commit()
    except Exception:
        if not read_only:
            conn.rollback()
        raise
    finally:
        conn.close()
```

### 4.2. DuckDB 连接策略

```python
# annual_report_mda/db.py

import duckdb
from pathlib import Path

DEFAULT_DUCKDB_PATH = Path("data/annual_reports.duckdb")
DEFAULT_SQLITE_PATH = Path("data/metadata.db")

def init_db(
    db_path: str | Path = DEFAULT_DUCKDB_PATH,
    sqlite_path: str | Path = DEFAULT_SQLITE_PATH,
    read_only: bool = True,
    attach_sqlite: bool = True,
) -> duckdb.DuckDBPyConnection:
    """初始化 DuckDB 连接，可选 ATTACH SQLite。

    Args:
        db_path: DuckDB 文件路径
        sqlite_path: SQLite 文件路径 (联邦查询用)
        read_only: 是否只读模式
        attach_sqlite: 是否 ATTACH SQLite 进行联邦查询

    Returns:
        DuckDB 连接对象
    """
    conn = duckdb.connect(database=str(db_path), read_only=read_only)

    # 创建 mda_text 表 (如果不存在)
    if not read_only:
        _create_mda_table(conn)

    # ATTACH SQLite
    if attach_sqlite and Path(sqlite_path).exists():
        conn.execute("INSTALL sqlite;")
        conn.execute("LOAD sqlite;")
        conn.execute(f"ATTACH '{sqlite_path}' AS meta (TYPE SQLITE, READ_ONLY);")
        _create_federated_views(conn)

    return conn
```

### 4.3. 连接生命周期

| 组件 | 数据库 | 策略 | 说明 |
|------|--------|------|------|
| WebUI | SQLite | 短连接 | 每次查询新建，使用 `@st.cache_data` 缓存结果 |
| 爬虫 | SQLite | 长连接 | 脚本运行期间保持，使用事务批量写入 |
| 提取器 | SQLite + DuckDB | 混合 | SQLite 更新状态，DuckDB 写入文本 |
| 分析脚本 | DuckDB | 只读 | ATTACH SQLite 进行联邦查询 |

---

## 5. 核心接口设计

### 5.1. sqlite_db.py 完整接口

```python
# annual_report_mda/sqlite_db.py
from __future__ import annotations

import sqlite3
import json
from datetime import datetime
from pathlib import Path
from typing import Any

# === 初始化 ===

def init_db(conn: sqlite3.Connection) -> None:
    """初始化 SQLite 数据库，创建所有表。"""

# === companies 表 ===

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
    """插入或更新公司信息。"""

def get_company(conn: sqlite3.Connection, stock_code: str) -> dict | None:
    """获取公司信息。"""

# === reports 表 ===

def insert_report(
    conn: sqlite3.Connection,
    *,
    stock_code: str,
    year: int,
    url: str,
    title: str | None = None,
    announcement_id: str | None = None,
    publish_date: str | None = None,
    source: str = "cninfo",
) -> bool:
    """插入年报记录，返回是否新增。"""

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
) -> None:
    """动态更新年报处理状态。"""

def get_pending_downloads(
    conn: sqlite3.Connection,
    *,
    year: int | None = None,
    limit: int | None = None,
) -> list[dict]:
    """获取待下载任务列表。"""

def get_pending_converts(
    conn: sqlite3.Connection,
    *,
    year: int | None = None,
    limit: int | None = None,
) -> list[dict]:
    """获取待转换任务列表。"""

def report_exists(conn: sqlite3.Connection, stock_code: str, year: int) -> bool:
    """检查年报记录是否存在。"""

def get_report(conn: sqlite3.Connection, stock_code: str, year: int) -> dict | None:
    """获取年报记录。"""

# === LLM 相关 ===

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
    """插入 LLM 调用日志。"""

def upsert_strategy_stats(
    conn: sqlite3.Connection,
    strategy: str,
    success: bool,
) -> None:
    """更新策略统计。"""

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
    """插入提取错误记录。"""

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
    """插入或更新提取规则。"""
```

### 5.2. db.py 改造后接口

```python
# annual_report_mda/db.py (改造后)
from __future__ import annotations

import duckdb
from pathlib import Path

# === 初始化 ===

def init_db(
    db_path: str | Path,
    sqlite_path: str | Path | None = None,
    read_only: bool = True,
    attach_sqlite: bool = True,
) -> duckdb.DuckDBPyConnection:
    """初始化 DuckDB，可选 ATTACH SQLite。"""

# === mda_text 表 (保留) ===

def insert_mda_text(
    conn: duckdb.DuckDBPyConnection,
    *,
    stock_code: str,
    year: int,
    mda_raw: str,
    # ... 其他参数保持不变
) -> None:
    """插入 MDA 文本记录。"""

def get_mda_text(
    conn: duckdb.DuckDBPyConnection,
    stock_code: str,
    year: int,
) -> dict | None:
    """获取 MDA 文本记录。"""

# === 删除的函数 (迁移到 sqlite_db.py) ===
# - upsert_company
# - get_company
# - insert_report
# - update_report_status
# - get_pending_downloads
# - get_pending_converts
# - report_exists
# - get_report
# - insert_llm_call_log
# - upsert_strategy_stats
# - insert_extraction_error
# - upsert_extraction_rule
```

---

## 6. 迁移策略

### 6.1. 迁移脚本

文件: `scripts/migrate_duckdb_to_sqlite.py`

```python
#!/usr/bin/env python3
"""从 DuckDB 迁移元数据表到 SQLite。"""

import shutil
import sys
from pathlib import Path

import duckdb
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
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

def main(dry_run: bool = False):
    # 1. 备份
    print(f"备份 {DUCKDB_PATH} -> {BACKUP_PATH}")
    if not dry_run:
        shutil.copy(DUCKDB_PATH, BACKUP_PATH)

    # 2. 连接
    duck_conn = duckdb.connect(str(DUCKDB_PATH), read_only=dry_run)
    sqlite_conn = sqlite_db.get_connection(SQLITE_PATH)

    # 3. 初始化 SQLite
    print("初始化 SQLite 表结构...")
    if not dry_run:
        sqlite_db.init_db(sqlite_conn)

    # 4. 迁移数据
    for table in TABLES_TO_MIGRATE:
        try:
            df = duck_conn.execute(f"SELECT * FROM {table}").df()
            duck_count = len(df)
            print(f"迁移 {table}: {duck_count} 条记录")

            if not dry_run and duck_count > 0:
                df.to_sql(table, sqlite_conn, if_exists="append", index=False)

                # 校验
                sqlite_count = sqlite_conn.execute(
                    f"SELECT COUNT(*) FROM {table}"
                ).fetchone()[0]
                assert duck_count == sqlite_count, f"行数不匹配: {duck_count} vs {sqlite_count}"

        except duckdb.CatalogException:
            print(f"跳过 {table}: 表不存在")

    # 5. 提交
    if not dry_run:
        sqlite_conn.commit()

        # 6. 从 DuckDB 删除已迁移的表
        for table in TABLES_TO_MIGRATE:
            try:
                duck_conn.execute(f"DROP TABLE IF EXISTS {table}")
            except Exception as e:
                print(f"警告: 删除 {table} 失败: {e}")

        duck_conn.execute("VACUUM")

    print("迁移完成!")

    duck_conn.close()
    sqlite_conn.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="预览模式")
    args = parser.parse_args()
    main(dry_run=args.dry_run)
```

### 6.2. 数据校验

1. **行数校验**: 迁移后立即比较 COUNT(*)
2. **内容抽样**: 随机抽取 10 条记录逐字段对比
3. **主键校验**: 检查 SQLite 中无重复主键

### 6.3. 回滚方案

```bash
# 1. 停止所有进程
pkill -f "streamlit run"
pkill -f "report_link_crawler"

# 2. 删除 SQLite 文件
rm -f data/metadata.db data/metadata.db-wal data/metadata.db-shm

# 3. 恢复 DuckDB 备份
mv data/annual_reports.duckdb.bak data/annual_reports.duckdb

# 4. 回滚代码
git checkout HEAD~1 -- annual_report_mda/db.py webui/components/db_utils.py
```

---

## 7. 模块适配

### 7.1. 爬虫适配

**文件**: `1.report_link_crawler.py`

```python
# 改前
from annual_report_mda.db import init_db, upsert_company, insert_report
db_conn = init_db(db_path)

# 改后
from annual_report_mda import sqlite_db
sqlite_conn = sqlite_db.get_connection(sqlite_path)
sqlite_db.init_db(sqlite_conn)
# 使用 sqlite_db.upsert_company(), sqlite_db.insert_report()
```

### 7.2. WebUI 适配

**文件**: `webui/components/db_utils.py`

```python
# 改前
import duckdb
@st.cache_resource
def get_connection():
    return duckdb.connect(...)

# 改后
from annual_report_mda import sqlite_db

def get_connection():
    """每次调用获取新连接 (短连接策略)"""
    return sqlite_db.get_connection(DEFAULT_SQLITE_PATH, read_only=True)

@st.cache_data(ttl=60)
def get_reports_progress() -> pd.DataFrame:
    conn = get_connection()
    try:
        cursor = conn.execute("""
            SELECT year, COUNT(*) as total, ...
            FROM reports
            GROUP BY year ORDER BY year DESC
        """)
        columns = [d[0] for d in cursor.description]
        return pd.DataFrame(cursor.fetchall(), columns=columns)
    finally:
        conn.close()
```

### 7.3. 提取器适配

**文件**: `mda_extractor.py`

```python
# 需要双连接
from annual_report_mda import sqlite_db, db

sqlite_conn = sqlite_db.get_connection(sqlite_path)
duckdb_conn = db.init_db(duckdb_path, read_only=False, attach_sqlite=False)

try:
    # 更新状态 -> SQLite
    sqlite_db.update_report_status(sqlite_conn, stock_code=..., extract_status="success")

    # 写入文本 -> DuckDB
    db.insert_mda_text(duckdb_conn, stock_code=..., mda_raw=...)

    sqlite_conn.commit()
finally:
    sqlite_conn.close()
    duckdb_conn.close()
```

---

## 8. 验收标准

### 8.1. 功能验收

| ID        | 验收项          | Given                               | When                              | Then                                               |
| --------- | --------------- | ----------------------------------- | --------------------------------- | -------------------------------------------------- |
| M1.5-01   | **并发读写**    | 爬虫正在向 SQLite 写入数据          | WebUI 刷新"监控仪表盘"            | 正常显示，无 `Database is locked` 错误             |
| M1.5-02   | **联邦查询**    | DuckDB 连接已 ATTACH SQLite         | 执行 `SELECT * FROM meta.companies JOIN mda_text` | 正确返回跨库 JOIN 结果 |
| M1.5-03   | **数据完整性**  | 迁移脚本执行完毕                    | 查询 SQLite `reports` 表          | 行数与迁移前 DuckDB 一致                           |
| M1.5-04   | **脚本兼容性**  | 分析脚本使用联邦查询视图            | 执行原有分析脚本                  | 正常运行，结果一致                                 |
| M1.5-05   | **提取器双写**  | 提取器成功处理年报                  | 检查两个数据库                    | SQLite 状态更新，DuckDB mda_text 新增记录          |

### 8.2. 性能验收

| 指标 | 标准 |
|------|------|
| WebUI 响应时间 | 爬虫运行期间 < 2秒 |
| 联邦查询性能 | 不超过纯 DuckDB 的 120% |
| 爬虫写入吞吐 | 不低于迁移前的 90% |

---

## 9. 风险与缓解

| 风险 | 等级 | 缓解措施 |
|------|------|----------|
| 数据迁移丢失 | 高 | 备份 + 校验 + 回滚方案 |
| 联邦查询性能 | 中 | 索引优化 + 监控 |
| 事务一致性 | 中 | 单库事务 + 最终一致性 |
| WAL 文件问题 | 低 | 文档说明 + 正确关闭连接 |

---

## 10. 实施计划

### 阶段 1: 基础设施 (sqlite_db.py)
1. 创建 `sqlite_db.py`，实现连接管理和表初始化
2. 迁移所有 CRUD 函数
3. 编写单元测试

### 阶段 2: 迁移工具
1. 实现迁移脚本
2. 在测试环境验证

### 阶段 3: 模块适配
1. 改造爬虫
2. 改造 WebUI
3. 改造提取器

### 阶段 4: 联邦查询
1. 改造 db.py，实现 ATTACH
2. 创建联邦查询视图
3. 验证分析脚本兼容性

### 阶段 5: 验收
1. 执行迁移
2. 并发测试
3. 性能验证
