---
task_id: dual-database-architecture
type: FEAT
complexity: F
current_phase: PC
completed_phases: [P0, P0.B, P1, P2.G, P2.R, P2.A, I.G, I.R]

# 分支策略: 在 feature/streamlit-webui 分支上继续开发
branch: feature/streamlit-webui

# v5.0: 状态持久化
next_action: IMPLEMENTATION_DONE
next_action_prompt: |
  实现完成，准备进入 PC 阶段 (提交代码)

blocked: false
blocked_reason: ""

created_at: 2026-01-11
updated_at: 2026-01-11
---

# Phase 1.5: 双数据库并发治理

## 用户确认结果

- **表拆分方案**: 混合存储 (SQLite 存元数据，DuckDB 存大文本 mda_text)
- **分支策略**: 在 `feature/streamlit-webui` 分支继续开发

## P0 问题定义

**类型**: FEAT-F (完整复杂度 - 涉及架构变更、数据模型、多文件改造)

### 背景

当前项目使用 DuckDB 作为唯一数据存储。DuckDB 是一个优秀的 OLAP 数据库，但其**单写者模型**导致以下问题:

1. **并发写入冲突**: 当爬虫运行时，WebUI 无法刷新状态（IO Error / Database locked）
2. **长连接阻塞**: WebUI 的 `@st.cache_resource` 缓存连接会阻塞爬虫写入
3. **运维困难**: 用户需要手动关闭 WebUI 才能运行爬虫

### 现状分析

| 模块 | 当前行为 | 读/写频率 | 锁需求 |
|------|----------|----------|--------|
| `1.report_link_crawler.py` | 直连 DuckDB 写入 | 低频批量写 | 写锁 |
| `2.pdf_batch_converter.py` | 直连 DuckDB 更新状态 | 中频写 | 写锁 |
| `mda_extractor.py` | 直连 DuckDB 写入提取结果 | 中频写 | 写锁 |
| `webui/components/db_utils.py` | 直连 DuckDB 查询 | 高频读 | 读锁 |
| `webui/pages/*.py` | 通过 db_utils 查询 | 高频读 | 读锁 |

**冲突根因**: DuckDB 单写者 + WebUI 长连接 = 爬虫/提取器被阻塞

### 目标架构

```
┌─────────────────────────────────────────────────────────────────┐
│                        应用层                                    │
├───────────────────┬───────────────────┬─────────────────────────┤
│   爬虫/下载器/提取器   │      WebUI       │     分析脚本            │
│   (写入元数据)        │   (查询状态)      │   (复杂聚合分析)        │
└─────────┬─────────┴─────────┬─────────┴──────────┬──────────────┘
          │                   │                    │
          ▼                   ▼                    ▼
┌─────────────────────────────────────┐   ┌───────────────────────┐
│            SQLite (OLTP)            │   │    DuckDB (OLAP)      │
│  - WAL 模式，支持并发读写           │   │  - 只读连接            │
│  - companies, reports 表            │   │  - ATTACH SQLite      │
│  - llm_call_logs, strategy_stats   │   │  - mda_text (分析专用) │
│  - extraction_rules, errors         │   │  - 联邦查询            │
└─────────────────────────────────────┘   └───────────────────────┘
```

### 范围

**做什么**:
1. 引入 SQLite 作为元数据存储（高频读写表）
2. 改造 `db.py`，新增 SQLite 连接管理，开启 WAL 模式
3. 实现 DuckDB 联邦查询（ATTACH SQLite）
4. 迁移爬虫、WebUI 到 SQLite
5. 保留 DuckDB 用于分析场景

**不做什么**:
1. 不改变现有表结构（仅迁移存储位置）
2. 不改变分析脚本的使用方式（通过联邦查询保持兼容）
3. 不引入 ORM（保持原生 SQL）

### 表拆分方案（待 P1 确认）

| 表名 | 目标数据库 | 理由 |
|------|----------|------|
| `companies` | SQLite | 高频读，爬虫写入 |
| `reports` | SQLite | 爬虫/下载器/提取器频繁更新状态 |
| `llm_call_logs` | SQLite | 提取器高频写入 |
| `strategy_stats` | SQLite | 提取器高频更新 |
| `extraction_rules` | SQLite | 提取器读写 |
| `extraction_errors` | SQLite | 提取器写入 |
| `mda_text` | DuckDB | 大文本存储，分析场景 |

### 完成标准

| ID | 验收项 | Given | When | Then |
|----|--------|-------|------|------|
| M1.5-01 | 并发读写 | 爬虫正在写入数据 | WebUI 刷新页面 | WebUI 正常显示，无 IO Error 锁冲突报错 |
| M1.5-02 | 联邦查询 | DuckDB 连接 | 执行跨库 JOIN 查询 | 能正确关联 SQLite 中的公司信息 |
| M1.5-03 | 数据完整性 | 迁移完成 | 查询 SQLite 表 | 数据与迁移前 DuckDB 一致 |
| M1.5-04 | 兼容性 | 分析脚本不修改 | 执行分析查询 | 通过 ATTACH 能访问所有数据 |

### 风险识别

| 风险 | 等级 | 缓解措施 |
|------|------|----------|
| 数据迁移丢失 | 高 | 迁移前备份，迁移后校验 |
| 联邦查询性能 | 中 | 热数据在 SQLite，冷数据在 DuckDB |
| 事务一致性 | 中 | 关键操作使用 SQLite 事务 |

### 涉及文件

```
annual_report_mda/db.py          # 核心改造：新增 SQLite 连接管理
annual_report_mda/sqlite_db.py   # 新建：SQLite 专用操作层
webui/components/db_utils.py     # 改造：切换到 SQLite 查询
1.report_link_crawler.py         # 改造：写入 SQLite
2.pdf_batch_converter.py         # 改造：写入 SQLite
mda_extractor.py                 # 改造：写入 SQLite (元数据) + DuckDB (文本)
scripts/migrate_duckdb_to_sqlite.py  # 新建：迁移脚本
```

---

## P1 技术调研结论

### SQLite WAL 模式验证 ✅

```python
# 开启 WAL 模式
import sqlite3
conn = sqlite3.connect('data/metadata.db', check_same_thread=False)
conn.execute('PRAGMA journal_mode=WAL')
conn.execute('PRAGMA busy_timeout = 5000')  # 等待 5 秒避免锁冲突
conn.execute('PRAGMA synchronous = NORMAL')  # 平衡性能与安全
```

**测试结论**:
- WAL 模式支持 1 写者 + 多读者并发
- 使用 `busy_timeout` 可避免立即失败
- `check_same_thread=False` 允许跨线程使用连接

### DuckDB ATTACH SQLite 验证 ✅

```python
import duckdb
conn = duckdb.connect('data/annual_reports.duckdb')
conn.execute('INSTALL sqlite')
conn.execute('LOAD sqlite')
conn.execute("ATTACH 'data/metadata.db' AS meta (TYPE SQLITE)")

# 联邦查询示例
result = conn.execute("""
    SELECT c.stock_code, c.short_name, m.quality_score
    FROM meta.companies c
    JOIN mda_text m ON c.stock_code = m.stock_code
""").fetchall()
```

**测试结论**:
- DuckDB SQLite 扩展可用
- `ATTACH ... (TYPE SQLITE)` 语法正确
- 联邦 JOIN 查询正常工作

### 最终表拆分方案

| 表名 | 目标数据库 | 理由 |
|------|----------|------|
| `companies` | **SQLite** | 高频读，爬虫写入 |
| `reports` | **SQLite** | 爬虫/下载器/提取器频繁更新状态 |
| `llm_call_logs` | **SQLite** | 提取器高频写入 |
| `strategy_stats` | **SQLite** | 提取器高频更新 |
| `extraction_rules` | **SQLite** | 提取器读写 |
| `extraction_errors` | **SQLite** | 提取器写入 |
| `mda_text` | **DuckDB** | 大文本存储，OLAP 分析场景 |

### 文件布局

```
data/
├── metadata.db           # SQLite (OLTP) - 元数据
├── metadata.db-wal       # WAL 日志
├── metadata.db-shm       # 共享内存
└── annual_reports.duckdb # DuckDB (OLAP) - 大文本 + 联邦查询
```

### 连接管理策略

| 组件 | 数据库 | 连接策略 |
|------|--------|----------|
| WebUI | SQLite | 短连接，每次查询新建 |
| 爬虫 | SQLite | 事务批量写入 |
| 提取器 | SQLite + DuckDB | SQLite 更新状态，DuckDB 写入文本 |
| 分析脚本 | DuckDB | 只读连接 + ATTACH SQLite |

---

## 待用户确认

1. **表拆分方案**: `mda_text` 是否保留在 DuckDB？还是全部迁移到 SQLite？
2. **SQLite 文件位置**: `data/metadata.db` 是否合适？
3. **分支策略**: 是否在当前 `feature/streamlit-webui` 分支上开发，还是创建新分支？

请确认以上问题定义是否正确，确认后将进入 P1 技术调研阶段。
