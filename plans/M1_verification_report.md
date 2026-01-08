# M1 里程碑验收报告

**验收日期**: 2026-01-08
**验收执行**: Claude Code
**里程碑**: 配置统一 + DuckDB 写入跑通

---

## 功能验收结果

| ID | 验收项 | 状态 | 验证方法 |
|----|--------|------|---------|
| M1-01 | 配置加载 | **通过** | `load_config()` 成功读取 `config.yaml`，配置值正确注入 |
| M1-02 | 配置校验 | **通过** | Pydantic 校验空 `target_years`、非法 `plate`、空 `keywords` 均抛出 `ValidationError` |
| M1-03 | 爬虫写入 | **通过** | `insert_report()` 成功写入 DuckDB，含 `stock_code`, `url`, `download_status` |
| M1-04 | 增量爬取 | **通过** | 重复插入返回 `is_new=False`，原有记录不变 |
| M1-05 | 数据迁移 | **通过** | `migrate_excel_to_duckdb.py` 成功迁移 4 条测试数据，必填字段无空值 |
| M1-06 | 兼容模式 | **通过** | `--legacy` 参数强制使用 Excel 数据源 |

---

## 质量验收结果

| ID | 验收项 | 指标 | 状态 |
|----|--------|------|------|
| M1-Q1 | 迁移性能 | 5 万条数据迁移耗时 < 60s | **待验证** (需生产数据) |
| M1-Q2 | 迁移幂等 | 重复执行迁移脚本不产生重复数据 | **通过** |
| M1-Q3 | 配置热加载 | 修改配置后无需重启 | **不适用** (设计为启动时加载) |

---

## 单元测试结果

```
pytest tests/test_duckdb_core.py -v
============================= test session starts ==============================
collected 12 items

tests/test_duckdb_core.py::TestCompaniesTable::test_upsert_company_insert PASSED
tests/test_duckdb_core.py::TestCompaniesTable::test_upsert_company_update PASSED
tests/test_duckdb_core.py::TestCompaniesTable::test_get_company_not_found PASSED
tests/test_duckdb_core.py::TestReportsTable::test_insert_report_new PASSED
tests/test_duckdb_core.py::TestReportsTable::test_insert_report_duplicate_skip PASSED
tests/test_duckdb_core.py::TestReportsTable::test_update_report_status PASSED
tests/test_duckdb_core.py::TestReportsTable::test_report_exists PASSED
tests/test_duckdb_core.py::TestPendingTasksViews::test_get_pending_downloads PASSED
tests/test_duckdb_core.py::TestPendingTasksViews::test_get_pending_converts PASSED
tests/test_duckdb_core.py::TestPendingTasksViews::test_reports_progress_view PASSED
tests/test_duckdb_core.py::TestMigrationIdempotency::test_migration_idempotent PASSED
tests/test_duckdb_core.py::TestExtractionErrors::test_insert_extraction_error PASSED

============================== 12 passed in 0.40s ==============================
```

---

## 核心功能实现清单

### 1.1 统一配置管理

| 文件 | 功能 |
|------|------|
| [config.yaml.example](../config.yaml.example) | 配置模板 |
| [annual_report_mda/config_manager.py](../annual_report_mda/config_manager.py) | Pydantic v2 配置类 + 路径安全校验 |

### 1.2 DuckDB 核心化

| 文件 | 功能 |
|------|------|
| [annual_report_mda/db.py](../annual_report_mda/db.py) | `companies` + `reports` 表定义及 CRUD |
| [1.report_link_crawler.py](../1.report_link_crawler.py) | `_save_to_duckdb()` + `--output-mode duckdb` |
| [2.pdf_batch_converter.py](../2.pdf_batch_converter.py) | `_load_tasks_from_duckdb()` + `--source duckdb` |

### 1.3 迁移与兼容

| 文件 | 功能 |
|------|------|
| [scripts/migrate_excel_to_duckdb.py](../scripts/migrate_excel_to_duckdb.py) | 一次性迁移脚本 |
| 各脚本 | `--legacy` 兼容模式 |

---

## 数据库表结构

### companies 表

| 字段 | 类型 | 约束 |
|------|------|------|
| stock_code | VARCHAR | PRIMARY KEY |
| short_name | VARCHAR | NOT NULL |
| full_name | VARCHAR | |
| plate | VARCHAR | |
| trade | VARCHAR | |
| trade_name | VARCHAR | |
| first_seen_at | TIMESTAMP | |
| updated_at | TIMESTAMP | |

### reports 表

| 字段 | 类型 | 约束 |
|------|------|------|
| stock_code | VARCHAR | PK (复合) |
| year | INTEGER | PK (复合) |
| url | VARCHAR | NOT NULL |
| download_status | VARCHAR | DEFAULT 'pending' |
| convert_status | VARCHAR | DEFAULT 'pending' |
| extract_status | VARCHAR | DEFAULT 'pending' |
| pdf_path | VARCHAR | |
| txt_path | VARCHAR | |
| ... | ... | |

---

## 验收结论

**M1 里程碑验收通过**

所有 6 项功能验收测试通过，核心功能实现完整。可进入下一阶段 (M2: 全流程 DuckDB 驱动 + 质量评估体系)。
