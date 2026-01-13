---
task_id: fix-converter-datasource-mismatch
type: BUG
complexity: L
current_phase: PC
completed_phases: [P0, P0.B, P0.C, 实现]
next_action: IMPLEMENTATION_DONE
branch: fix/converter-datasource-mismatch
created_at: 2026-01-13
updated_at: 2026-01-13
---

# 计划: 修复下载转换任务数据源不一致问题

## 问题定义

**类型**: BUG-L
**背景**: WebUI 任务管理页面显示的"待下载"任务数量与 converter 脚本实际读取的数据源不一致，导致用户看到有待下载任务但启动后无法处理。

**根因分析**:

1. **WebUI 显示数据来源**: SQLite 数据库 (`data/metadata.db`)
   ```python
   # webui/components/db_utils.py:176-178
   default_counts["pending_downloads"] = sqlite_conn.execute(
       "SELECT COUNT(*) FROM reports WHERE download_status = 'pending'"
   ).fetchone()[0]
   ```

2. **converter 任务启动参数**: `["--use-config"]`
   ```python
   # webui/pages/3_任务管理.py:32-33
   "converter": {
       "args": ["--use-config"],
       ...
   }
   ```

3. **`--use-config` 的行为**: 调用 `_run_with_embedded_config()` 函数，使用硬编码的 Excel 文件路径作为数据源，而非 SQLite 数据库。

4. **结果**: WebUI 显示 SQLite 中有 N 条待下载任务，但 converter 从 Excel 读取任务（可能为空或不同步），导致"待下载任务无法如期启动"。

**范围**:
- 修改 `webui/pages/3_任务管理.py` 中 converter 的启动参数
- 确保 converter 从 SQLite 数据库读取待下载任务列表

**完成标准**:
- converter 任务启动后能正确读取 SQLite 数据库中的待下载任务
- WebUI 显示的待下载数量与 converter 实际处理的任务数一致
- 不影响现有的命令行使用方式

## 调研结论

### 数据流分析

```
爬虫 (crawler)
    │
    ▼ 写入
SQLite: reports 表 (download_status='pending')
    │
    ├──▶ WebUI 读取显示 "待下载: N 条" ✓
    │
    └──▶ converter 应该读取但实际读取 Excel ✗
```

### 技术方案选择

**方案 A**: 修改 args 为 `["--use-yaml-config"]`
- 需要确保 config.yaml 中 `crawler.output_mode: duckdb`
- 当前 config.yaml 中没有此配置项

**方案 B**: 直接使用命令行参数 (推荐)
- `["download", "--source", "duckdb", "--year", str(year)]`
- 但需要动态获取年份参数

**方案 C**: 修改脚本支持 `--source sqlite` 或自动检测
- 需要修改 `2.pdf_batch_converter.py`
- 影响范围较大

**选定方案**: 修改 WebUI 使用 `--use-yaml-config`，并在 config.yaml 中添加 `output_mode: duckdb`。

### 影响范围

1. `webui/pages/3_任务管理.py`: 修改 args
2. `config.yaml`: 添加 `output_mode: duckdb` (如尚未存在)
3. `config.yaml.example`: 更新模板以包含该配置

## 实现方案

1. 修改 `webui/pages/3_任务管理.py`:
   - 将 `converter` 的 args 从 `["--use-config"]` 改为 `["--use-yaml-config"]`

2. 检查/更新 `config.yaml`:
   - 在 `crawler` 配置下添加 `output_mode: duckdb`

3. 更新 `config.yaml.example`:
   - 添加 `output_mode` 配置项说明

4. 验证:
   - 启动 WebUI，确认待下载数量显示正确
   - 启动 converter 任务，确认能读取 SQLite 中的待下载任务
