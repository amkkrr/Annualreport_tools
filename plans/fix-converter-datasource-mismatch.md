---
task_id: fix-converter-datasource-mismatch
type: BUG
complexity: L
current_phase: DONE
completed_phases: [P0, P0.B, P0.C, 实现, PC]
next_action: PLAN_COMPLETE
branch: fix/converter-datasource-mismatch
created_at: 2026-01-13
updated_at: 2026-01-13
---

# 计划: 修复下载转换任务数据源不一致问题

## 问题定义

**类型**: BUG-L
**背景**: WebUI 任务管理页面显示的"待下载"任务数量与 converter 脚本实际读取的数据源不一致，导致用户看到有待下载任务但启动后返回 0 条任务。

**根因分析 (深层)**:

问题不仅仅是 `--use-config` vs `--use-yaml-config`，更深层的原因是:

1. `_run_with_yaml_config` 和 `_run_download_from_args` 创建了 **DuckDB** 连接
2. 将 DuckDB 连接传递给 `get_pending_downloads()`
3. `db.py` 的兼容层检查 `hasattr(conn, "execute")`，DuckDB 连接也有 `execute` 方法，所以条件为 True
4. SQLite 查询用 DuckDB 连接执行 → 查不到 `reports` 表 → 返回空列表

**范围**:
- 修改 `2.pdf_batch_converter.py` 中的数据库连接创建逻辑
- 使用 SQLite 连接查询待下载任务，而不是 DuckDB 连接

**完成标准**:
- converter 任务启动后能正确读取 SQLite 数据库中的待下载任务
- WebUI 显示的待下载数量与 converter 实际处理的任务数一致

## 实现记录

### 提交 1: e69fe34
修改 WebUI 使用 `--use-yaml-config` 启动 converter

### 提交 2: affb24d
修复 `config.database.path` 属性访问错误

### 提交 3: 78b74ac
**核心修复**: 使用 SQLite 连接查询待下载任务

修改内容:
1. `_run_with_yaml_config`: 使用 `sqlite_db.get_connection()` 而非 `db.init_db()`
2. `_run_download_from_args`: 同上
3. `_load_tasks_from_duckdb`: 直接调用 `sqlite_db.get_pending_downloads()`
4. `_update_task_status_in_db`: 直接调用 `sqlite_db.update_report_status()`
5. 更新日志信息，移除误导性的 "DuckDB" 描述

## 验证结果

修复前:
```
使用 DuckDB 数据源: data/annual_reports.duckdb
从 DuckDB 加载 0 条待下载任务
```

修复后:
```
使用 SQLite 数据源: data/metadata.db
从数据库加载 1 条待下载任务  # 2023 年有 1 条待下载任务
```

---

## 追加修复: extractor 任务缺少默认输入目录

### 问题描述
WebUI 启动 "提取 MDA" 任务时报错:
```
使用配置模式但未指定 --text 或 --dir，请指定输入路径。
```

### 根因
- WebUI 只传递 `--use-config` 参数
- `mda_extractor.py` 的 `_run_with_yaml_config()` 要求必须同时传递 `--text` 或 `--dir`
- `MdaConfig` 没有定义 `input_dir` 字段

### 提交 4: a8e22d2
**修复**: 在配置中添加默认输入目录

修改内容:
1. `MdaBehaviorConfig` 添加 `input_dir` 字段，默认值 `outputs/annual_reports`
2. `config.yaml.example` 添加 `input_dir` 配置示例
3. `mda_extractor.py` 优先使用 `--dir` 参数，其次使用配置文件中的 `input_dir`
