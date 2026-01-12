# 规格书: 双数据库架构 (SQLite + DuckDB)

**文档版本**: 1.0
**日期**: 2026-01-11
**作者**: Gemini

## 目录
1. [架构总览](#1-架构总览)
2. [数据模型](#2-数据模型)
3. [连接管理](#3-连接管理)
4. [核心接口设计](#4-核心接口设计)
5. [迁移策略](#5-迁移策略)
6. [模块适配](#6-模块适配)
7. [验收标准](#7-验收标准)
8. [风险与缓解](#8-风险与缓解)

---

## 1. 架构总览

为了解决当前 DuckDB 单写者模型导致的并发冲突问题，我们引入 SQLite 作为元数据和高频事务性数据的存储后端 (OLTP)，同时保留 DuckDB 用于存储大文本 `mda_text` 并作为主要的分析查询入口 (OLAP)。

### 1.1. 双数据库拓扑图

```ascii
┌─────────────────────────────────────────────────────────────────┐
│                            应用层 (Python)                       │
├───────────────────┬───────────────────┬─────────────────────────┤
│ 爬虫/下载器/提取器 │      WebUI (Streamlit)      │     分析脚本 (Jupyter/Python)   │
│ (高频写入元数据)     │ (高频读取状态/元数据)  │     (复杂聚合/联邦查询)     │
└─────────┬─────────┴─────────┬─────────┴──────────┬──────────────┘
          │(读/写)            │(只读)               │(只读)
          │                   │                    │
┌─────────▼─────────┐ ┌───────▼───────┐    ┌───────▼───────┐
│  sqlite_db.py     │ │ db_utils.py   │    │     db.py     │
│  (SQLite 接口层)  │ │ (WebUI 接口层)│    │ (DuckDB 接口层) │
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
    *   **爬虫 (`1.report_link_crawler.py`)**: 发现新的年报信息，通过 `sqlite_db.py` 写入 SQLite 的 `companies` 和 `reports` 表。
    *   **下载器/转换器 (`2.pdf_batch_converter.py`)**: 更新 `reports` 表中的 `download_status`, `convert_status` 等字段。
    *   **提取器 (`mda_extractor.py`)**: 更新 `reports` 表的 `extract_status`，并将 LLM 调用日志、错误等写入 SQLite 的 `llm_call_logs`, `extraction_errors` 等表。

2.  **大文本写入 (OLAP 流)**:
    *   **提取器 (`mda_extractor.py`)**: 成功提取 MD&A 文本后，通过 `db.py` 将大的 `mda_raw` 文本内容写入 DuckDB 的 `mda_text` 表。这是一个独立的写操作。

3.  **WebUI 读取 (高频读流)**:
    *   **WebUI (`webui/*.py`)**: 通过 `webui/components/db_utils.py` 连接到 SQLite 数据库，实时查询 `reports` 表的进度、待办队列等状态信息，实现高并发、低延迟的 UI 刷新。

4.  **分析查询 (联邦查询流)**:
    *   **分析脚本/Jupyter**: 通过 `db.py` 连接到 DuckDB。`db.py` 在初始化连接时，会自动 `ATTACH` SQLite 数据库 (`data/metadata.db`)。
    *   用户可以像以前一样执行查询。当查询需要关联元数据时 (如 `companies`)，DuckDB 会通过联邦查询引擎从 SQLite 中拉取数据进行 `JOIN`，对上层脚本保持透明。

## 2. 数据模型

### 2.1. SQLite 表定义 (SQL DDL)

以下表将从 DuckDB 迁移至 SQLite。DDL 已根据 SQLite 的标准语法进行了微调 (`VARCHAR` -> `TEXT`, `TIMESTAMP` -> `DATETIME`)。

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
    first_seen_at DATETIME,
    updated_at DATETIME
);

-- 年报元数据与生命周期管理表
CREATE TABLE IF NOT EXISTS reports (
    stock_code TEXT NOT NULL,
    year INTEGER NOT NULL,
    announcement_id TEXT,
    title TEXT,
    url TEXT NOT NULL,
    publish_date DATE,

    download_status TEXT DEFAULT 'pending',
    convert_status TEXT DEFAULT 'pending',
    extract_status TEXT DEFAULT 'pending',

    download_error TEXT,
    convert_error TEXT,
    extract_error TEXT,
    download_retries INTEGER DEFAULT 0,
    convert_retries INTEGER DEFAULT 0,

    pdf_path TEXT,
    txt_path TEXT,
    pdf_size_bytes BIGINT,
    pdf_sha256 TEXT,
    txt_sha256 TEXT,

    crawled_at DATETIME,
    downloaded_at DATETIME,
    converted_at DATETIME,
    updated_at DATETIME,

    source TEXT DEFAULT 'cninfo',

    PRIMARY KEY (stock_code, year)
);
CREATE INDEX IF NOT EXISTS idx_reports_status ON reports (download_status, convert_status, extract_status);

-- 提取规则表
CREATE TABLE IF NOT EXISTS extraction_rules (
    stock_code TEXT,
    year INTEGER,
    report_signature TEXT,

    start_pattern TEXT,
    end_pattern TEXT,
    rule_source TEXT,
    updated_at DATETIME,

    PRIMARY KEY (stock_code, year)
);

-- 提取错误日志表
CREATE TABLE IF NOT EXISTS extraction_errors (
    stock_code TEXT,
    year INTEGER,
    source_path TEXT,
    source_sha256 TEXT,
    error_type TEXT,
    error_message TEXT,
    provider TEXT,
    http_status INTEGER,
    trace_id TEXT,
    created_at DATETIME
);

-- 策略统计表 (LLM 自适应学习)
CREATE TABLE IF NOT EXISTS strategy_stats (
    strategy TEXT PRIMARY KEY,
    attempts INTEGER DEFAULT 0,
    success INTEGER DEFAULT 0,
    last_updated DATETIME
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
    success BOOLEAN,
    error_message TEXT,
    created_at DATETIME
);

```

### 2.2. DuckDB 表定义

DuckDB 中仅保留 `mda_text` 表，用于存储大文本数据，服务于 OLAP 分析。

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

### 2.3. 表字段对比

所有表的字段定义、名称和类型保持不变，仅是存储位置发生了变化。`JSON` 类型在 SQLite 中将作为 `TEXT` 存储，Python 应用层负责序列化和反序列化。

| 表名                | 原数据库 | 目标数据库 | 迁移理由                               |
| ------------------- | -------- | ---------- | -------------------------------------- |
| `companies`         | DuckDB   | **SQLite** | 元数据，爬虫写入，WebUI 读取             |
| `reports`           | DuckDB   | **SQLite** | 核心状态表，各模块高频更新和读取       |
| `extraction_rules`  | DuckDB   | **SQLite** | 规则元数据，提取器读写                 |
| `extraction_errors` | DuckDB   | **SQLite** | 错误日志，提取器高频写入               |
| `strategy_stats`    | DuckDB   | **SQLite** | LLM 统计，提取器高频更新 (UPSERT)      |
| `llm_call_logs`     | DuckDB   | **SQLite** | LLM 日志，提取器高频写入               |
| `mda_text`          | DuckDB   | **DuckDB** | 大文本 OLAP 数据，仅提取器写入，分析脚本读取 |

## 3. 连接管理

### 3.1. SQLite 连接池设计

我们将创建一个简单的连接工厂函数，而不是一个复杂的池。该函数将为每个需要它的线程提供一个独立的连接对象，并配置好 WAL 模式以支持并发。

**文件**: `annual_report_mda/sqlite_db.py`

**核心配置**:
*   `journal_mode=WAL`: 允许多个读者和一个写者并发。
*   `busy_timeout = 5000`: 当数据库被锁时，连接将等待最多 5 秒，而不是立即失败。
*   `check_same_thread=False`: 允许在多线程环境中使用（例如 Streamlit）。
*   返回的连接将启用 `row_factory = sqlite3.Row`，以便可以像字典一样访问列。

**实现**:
```python
# annual_report_mda/sqlite_db.py
import sqlite3
from pathlib import Path

def get_sqlite_connection(db_path: str | Path, read_only: bool = False) -> sqlite3.Connection:
    """创建并配置一个 SQLite 连接。"""
    db_uri = f"file:{db_path}?mode={'ro' if read_only else 'rwc'}"
    conn = sqlite3.connect(db_uri, uri=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    if not read_only:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute("PRAGMA busy_timeout = 5000;")
    return conn
```

### 3.2. DuckDB 连接策略

DuckDB 的连接管理保持简单。
*   **分析场景**: 通过 `db.py` 创建只读连接，并在内部 `ATTACH` SQLite。
*   **提取器写入**: 在需要写入 `mda_text` 时，创建一个独立的、短期的读写连接。

### 3.3. 连接生命周期管理

*   **脚本 (爬虫、转换器、提取器)**: 在脚本开始时创建连接，在 `try...finally` 块中确保连接最终被关闭。对于批量操作，使用事务 (`conn.commit()`, `conn.rollback()`)。
*   **WebUI**: Streamlit 的 `@st.cache_resource` 不再用于管理写密集型数据库的连接。`webui/components/db_utils.py` 将在每个需要数据的函数内部调用 `get_sqlite_connection` 来获取一个短生命周期的只读连接。由于 SQLite 的连接开销很低，这种方式可以有效避免锁问题。

## 4. 核心接口设计

### 4.1. `sqlite_db.py` 接口定义

创建一个新文件 `annual_report_mda/sqlite_db.py`，用于封装所有对 SQLite 的操作。其接口将与旧 `db.py` 中相应的函数类似。

```python
# annual_report_mda/sqlite_db.py
from __future__ import annotations
import sqlite3
from pathlib import Path
from datetime import date

# ... (包含 get_sqlite_connection 函数)

def init_db(conn: sqlite3.Connection) -> None:
    """初始化 SQLite 数据库，创建所有表。"""
    # ... 执行上面 2.1 节的所有 CREATE TABLE DDL ...

# === companies 表操作 ===
def upsert_company(conn: sqlite3.Connection, *, stock_code: str, short_name: str, ...) -> None:
    """插入或更新公司信息。使用 INSERT ... ON CONFLICT ... DO UPDATE。"""
    # ... 实现 ...

def get_company(conn: sqlite3.Connection, stock_code: str) -> dict | None:
    """获取公司信息。"""
    # ... 实现 ...

# === reports 表操作 ===
def insert_report(conn: sqlite3.Connection, *, stock_code: str, year: int, url: str, ...) -> bool:
    """插入年报记录，返回是否新增。"""
    # ... 实现 ...

def update_report_status(conn: sqlite3.Connection, *, stock_code: str, year: int, ...) -> None:
    """动态更新年报处理状态。"""
    # ... 实现 ...

def get_pending_downloads(conn: sqlite3.Connection, *, year: int | None = None, limit: int | None = None) -> list[dict]:
    """获取待下载任务列表。"""
    # ... 实现 ...

# ... (为所有迁移到 SQLite 的表实现类似 CRUD 的辅助函数)
```

### 4.2. `db.py` 改造点

`annual_report_mda/db.py` 将被重构，专注于 DuckDB 的功能：`mda_text` 的操作和联邦查询。

```python
# annual_report_mda/db.py
from __future__ import annotations
import duckdb
from pathlib import Path

def init_db(
    db_path: str | Path,
    sqlite_db_path: str | Path,
    read_only: bool = True
) -> duckdb.DuckDBPyConnection:
    """
    初始化 DuckDB，建表，并 ATTACH SQLite 数据库以进行联邦查询。
    """
    conn = duckdb.connect(database=str(db_path), read_only=read_only)

    # 仅创建 mda_text 表
    conn.execute("""
        CREATE TABLE IF NOT EXISTS mda_text (...);
    """)

    # 安装并加载 SQLite 扩展
    conn.execute("INSTALL sqlite;")
    conn.execute("LOAD sqlite;")

    # ATTACH SQLite 数据库
    conn.execute(f"ATTACH '{sqlite_db_path}' AS meta (TYPE SQLITE, READ_ONLY);")

    # 重建视图以使用联邦查询
    _create_federated_views(conn)

    return conn

def _create_federated_views(conn: duckdb.DuckDBPyConnection) -> None:
    """创建或替换使用联邦查询的视图。"""
    # 例如，reports_progress 现在从 meta.reports 查询
    conn.execute("""
        CREATE OR REPLACE VIEW reports_progress AS
        SELECT year, COUNT(*) as total, ...
        FROM meta.reports
        GROUP BY year ORDER BY year DESC;
    """)
    # ... 为其他需要联邦查询的视图创建类似定义 ...

# === mda_text 表操作 ===
def insert_mda_text(conn: duckdb.DuckDBPyConnection, *, stock_code: str, year: int, mda_raw: str, ...) -> None:
    """插入一条新的 mda_text 记录。"""
    # ... 实现 ...

# === 删除原有的函数 ===
# 删除 upsert_company, insert_report, update_report_status 等所有已迁移到 sqlite_db.py 的函数。
```

## 5. 迁移策略

### 5.1. 迁移脚本伪代码

创建一个一次性脚本 `scripts/migrate_duckdb_to_sqlite.py`。

```python
# scripts/migrate_duckdb_to_sqlite.py
import duckdb
import sqlite3
import shutil
from annual_report_mda import sqlite_db

# 定义路径
DUCKDB_PATH = "data/annual_reports.duckdb"
SQLITE_PATH = "data/metadata.db"
BACKUP_PATH = "data/annual_reports.duckdb.bak"

TABLES_TO_MIGRATE = [
    "companies", "reports", "extraction_rules",
    "extraction_errors", "strategy_stats", "llm_call_logs"
]

def main():
    # 1. 备份
    print(f"Backing up {DUCKDB_PATH} to {BACKUP_PATH}...")
    shutil.copy(DUCKDB_PATH, BACKUP_PATH)

    # 2. 连接数据库
    duck_conn = duckdb.connect(DUCKDB_PATH, read_only=False)
    sqlite_conn = sqlite_db.get_sqlite_connection(SQLITE_PATH)

    # 3. 在 SQLite 中创建表
    sqlite_db.init_db(sqlite_conn)

    # 4. 逐表迁移数据
    for table_name in TABLES_TO_MIGRATE:
        print(f"Migrating table: {table_name}...")

        # 从 DuckDB 读取数据
        data = duck_conn.execute(f"SELECT * FROM {table_name}").df()

        # 写入 SQLite
        # 注意: pandas.to_sql 在处理 ON CONFLICT 时可能有限制，
        # 可能需要手动构建 INSERT 语句或逐行插入
        data.to_sql(table_name, sqlite_conn, if_exists='append', index=False)

        # 数据校验 (见 5.2)
        duck_count = duck_conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        sqlite_count = sqlite_conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

        assert duck_count == sqlite_count, f"Count mismatch for table {table_name}"
        print(f"Table {table_name} migrated successfully. ({sqlite_count} rows)")

    # 5. 从 DuckDB 中删除已迁移的表
    for table_name in TABLES_TO_MIGRATE:
        print(f"Dropping table {table_name} from DuckDB...")
        duck_conn.execute(f"DROP TABLE IF EXISTS {table_name};")

    print("Migration complete. Vacuuming DuckDB...")
    duck_conn.execute("VACUUM;")

    # 6. 关闭连接
    duck_conn.close()
    sqlite_conn.close()

if __name__ == "__main__":
    main()

```

### 5.2. 数据校验方案

1.  **行数校验**: 在迁移脚本中，迁移完每个表后，立即查询原表和新表的 `COUNT(*)` 并断言两者相等。
2.  **内容抽样**: 对于关键表 (如 `reports`)，可以随机抽取几条记录 (例如，特定 `stock_code` 和 `year`)，从两个数据库中都查询出来，然后逐字段比较其内容是否完全一致。
3.  **主键校验**: 查询新表中是否有重复的主键，以确保主键约束被正确迁移。

### 5.3. 回滚方案

如果迁移失败或数据校验不通过：
1.  停止所有应用进程。
2.  删除 `data/metadata.db`, `data/metadata.db-wal`, `data/metadata.db-shm`。
3.  将备份的 `data/annual_reports.duckdb.bak` 文件重命名回 `data/annual_reports.duckdb`。
4.  恢复代码到迁移前的版本。

## 6. 模块适配

### 6.1. 爬虫适配方案

**文件**: `1.report_link_crawler.py`
*   **修改**:
    *   移除 `import annual_report_mda.db as db`。
    *   添加 `import annual_report_mda.sqlite_db as sqlite_db`。
    *   在主逻辑开始时，调用 `sqlite_db.get_sqlite_connection` 获取连接。
    *   所有调用 `db.upsert_company` 和 `db.insert_report` 的地方，全部改为调用 `sqlite_db.upsert_company` 和 `sqlite_db.insert_report`，并传入 SQLite 连接对象。

### 6.2. WebUI 适配方案

**文件**: `webui/components/db_utils.py`
*   **修改**:
    *   移除 `import duckdb`，添加 `import annual_report_mda.sqlite_db as sqlite_db`。
    *   重写 `get_connection` 函数。它不再使用 `@st.cache_resource`，而是直接调用 `sqlite_db.get_sqlite_connection("data/metadata.db", read_only=True)` 并返回连接。
    *   所有数据查询函数 (如 `get_reports_progress`, `get_pending_downloads` 等) 的 `_conn` 参数类型从 `duckdb.DuckDBPyConnection` 改为 `sqlite3.Connection`。
    *   由于 `sqlite3` 查询返回的是元组或 `sqlite3.Row` 对象，需要将 `_conn.execute(...).df()` 的调用方式改为 `pd.DataFrame(_conn.execute(...).fetchall(), columns=[...])` 来构建 DataFrame。

### 6.3. 提取器适配方案

**文件**: `mda_extractor.py`
*   **修改**:
    *   需要同时保留 `db.py` 和 `sqlite_db.py` 的导入。
    *   在主逻辑中，同时获取两个数据库的连接：
        *   `sqlite_conn = sqlite_db.get_sqlite_connection(...)`
        *   `duckdb_conn = duckdb.connect("data/annual_reports.duckdb", read_only=False)` (一个临时的写连接)
    *   当需要更新报告状态或记录日志时，使用 `sqlite_conn` 和 `sqlite_db` 中的函数。
    *   当需要写入最终的 `mda_raw` 文本时，使用 `duckdb_conn` 和 `db.insert_mda_text` 函数。
    *   确保两个连接在处理完一个文件后都被正确关闭。

## 7. 验收标准

### 7.1. 功能验收用例

| ID        | 验收项          | Given                             | When                            | Then                                             |
| --------- | --------------- | --------------------------------- | ------------------------------- | ------------------------------------------------ |
| M1.5-01   | **并发读写**    | 爬虫脚本正在运行，向 SQLite 写入数据 | 用户在 WebUI 中刷新"监控仪表盘"页面 | WebUI 正常显示最新进度，无 `IO Error` 或 `Database is locked` 错误 |
| M1.5-02   | **联邦查询**    | 分析师打开一个 DuckDB 连接          | 执行 `SELECT c.short_name, m.quality_score FROM meta.companies c JOIN mda_text m ON c.stock_code = m.stock_code LIMIT 10;` | 查询成功返回结果，正确关联了 SQLite 和 DuckDB 中的数据 |
| M1.5-03   | **数据完整性**  | 数据迁移脚本执行完毕              | 查询 SQLite 的 `reports` 表行数 | `reports` 表的行数与迁移前 DuckDB 中的行数完全一致 |
| M1.5-04   | **脚本兼容性**  | 分析脚本代码不做修改              | 执行原有的分析脚本（依赖联邦查询视图） | 脚本正常运行，输出与迁移前一致的结果             |
| M1.5-05   | **提取器双写**  | 提取器成功处理一份年报            | 检查数据库                      | SQLite `reports` 表的 `extract_status` 变为 `success`，同时 DuckDB `mda_text` 表新增一条对应记录 |

### 7.2. 性能验收指标

1.  **WebUI 响应速度**: 在爬虫或提取器运行期间，WebUI "监控仪表盘"页面的加载和刷新时间应低于 **2 秒**。
2.  **联邦查询性能**: 对于典型的跨库 `JOIN` 查询，其执行时间应不高于纯 DuckDB 方案下的 **120%**。
3.  **爬虫写入性能**: 爬虫写入 `reports` 表的吞吐量不应低于迁移前的 90%。
4.  **提取器写入延迟**: 提取器在一次成功的提取后，完成对 SQLite 和 DuckDB 的双写操作，总耗时不应超过 **500 毫秒**。

## 8. 风险与缓解

| 风险                               | 等级 | 缓解措施                                                                                                                              |
| ---------------------------------- | ---- | ------------------------------------------------------------------------------------------------------------------------------------- |
| **数据迁移失败或数据丢失**         | 高   | 1. **备份**: 迁移前对 `annual_reports.duckdb` 进行完整物理备份。<br>2. **校验**: 迁移后执行严格的数据校验（行数、内容抽样）。<br>3. **回滚**: 制定清晰的回滚计划。 |
| **联邦查询性能不佳**               | 中   | 1. **索引**: 在 SQLite 的高频查询字段上（如 `stock_code`, `year`, `status`）建立索引。<br>2. **物化视图**: 对于极其复杂的分析，考虑在 DuckDB 中创建物化视图。<br>3. **监控**: 对慢查询进行监控和分析。 |
| **事务一致性问题**                 | 中   | 1. **单库事务**: 将高度耦合的状态更新（如 `reports` 内的多个字段）放在 SQLite 的同一个事务中完成。<br>2. **最终一致性**: 接受 `reports` 状态更新和 `mda_text` 插入之间的短暂不一致。提取器逻辑应能处理重试。 |
| **WAL 文件权限/磁盘空间问题**        | 低   | 1. **文档**: 在项目 `README` 中说明 WAL 模式会额外生成 `-wal` 和 `-shm` 文件。<br>2. **清理**: 确保数据库连接正常关闭，SQLite 会自动清理 WAL 文件。 |
| **DuckDB SQLite 扩展不兼容**      | 低   | 1. **版本锁定**: 在 `requirements.txt` 中明确锁定 `duckdb` 的版本。<br>2. **CI 测试**: 在 CI 流程中加入联邦查询的测试用例，确保环境一致性。 |
| **代码改动范围广，引入新 Bug**     | 中   | 1. **分步实施**: 按照本规格书的模块适配方案逐一改造和测试。<br>2. **代码审查**: 对所有改动进行严格的 Code Review。<br>3. **单元测试**: 为新的 `sqlite_db.py` 模块编写单元测试。 |
