---
task_id: webui-enhancement
type: FEAT
complexity: S
current_phase: COMPLETED
completed_phases: [P0, P0.B, P1, P2.G, P2.R, P2.A, I.G, I.R]
branch: feature/streamlit-webui

next_action: NONE
next_action_prompt: ""

completed_at: 2026-01-11

blocked: false
blocked_reason: ""

created_at: 2026-01-11
updated_at: 2026-01-11
---

# WebUI 增强计划

> 基于已完成的 WebUI MVP，继续完善两个高优功能

## 问题定义 (P0)

**类型**: FEAT-S (标准功能开发)

**背景**: WebUI MVP 已完成，但存在两个关键问题：
1. 任务状态存储在 `st.session_state`，页面刷新或服务重启后丢失
2. 当前只能全量爬取，无法按需选择特定公司/年份的年报

**范围**:

### 做什么

#### 子任务 1: 任务状态持久化 (P1 优先级)
- 使用 PID 文件 (`data/run/<task>.pid`) 持久化任务进程信息
- 使用 `psutil` 检测进程是否存活，支持服务重启后恢复状态
- 支持任务的启动/恢复/停止操作
- 页面刷新后仍能正确显示运行中的任务状态

#### 子任务 2: 智能选股与年报探索器 (P2 优先级)
- 新增 "年报探索器" 页面
- 支持按股票代码/公司名称/行业/年份搜索年报
- 基于 CNINFO API 或已入库的 companies 表数据
- 用户可勾选目标年报，一键触发定向处理流程
- 替代纯全量爬取模式，支持增量按需处理

### 不做什么
- 不实现复杂的任务队列/调度系统（保持 subprocess 简单模型）
- 不实现 Docker 化部署
- 不实现用户认证

**完成标准**:

| ID | Given | When | Then |
|----|-------|------|------|
| E1 | 爬取任务运行中 | 刷新页面 | 状态仍显示"运行中"，PID 文件存在 |
| E2 | 服务重启 | 访问任务管理页 | 能检测到之前启动的任务（如仍在运行） |
| E3 | 任务已结束 | 访问任务管理页 | 状态显示"已完成"或"错误"，PID 文件被清理 |
| E4 | companies 表有数据 | 访问年报探索器页 | 能搜索并展示公司列表 |
| E5 | 用户勾选 3 家公司 | 点击"处理选中年报" | 仅下载/转换/提取这 3 家的年报 |
| E6 | 输入股票代码 600519 | 搜索 | 展示贵州茅台相关年报列表 |

---

## 技术调研 (P1)

### 1. 现有实现分析

当前 `webui/components/task_runner.py` 使用 `st.session_state` 存储 `TaskState` 对象：
- `TaskState` 包含 `process: subprocess.Popen | None` 和 `log_file: Path | None`
- 问题：`subprocess.Popen` 对象无法序列化，页面刷新后丢失

### 2. psutil 进程检测

`psutil` 已安装 (v5.9.8)，核心 API：
- `psutil.pid_exists(pid)` - 检查 PID 是否存在
- `psutil.Process(pid)` - 获取进程对象
- `process.is_running()` - 检查进程是否运行中
- `process.cmdline()` - 获取命令行参数（用于验证是否为我们启动的进程）
- `process.terminate()` / `process.kill()` - 终止进程

### 3. PID 文件管理方案

**目录结构**:
```
data/run/
├── crawler.pid      # 内容: {"pid": 12345, "started_at": "2026-01-11T10:30:00", "log_file": "logs/webui/crawler.log"}
├── converter.pid
└── extractor.pid
```

**PID 文件格式** (JSON):
```json
{
  "pid": 12345,
  "started_at": "2026-01-11T10:30:00Z",
  "log_file": "logs/webui/crawler.log",
  "script": "1.report_link_crawler.py",
  "args": ["--use-config"]
}
```

**生命周期**:
1. **启动时**: 创建 PID 文件，写入进程信息
2. **状态检查时**: 读取 PID 文件 → `psutil.pid_exists()` → 验证 cmdline
3. **停止时**: 终止进程 → 删除 PID 文件
4. **进程自然结束时**: 状态检查发现进程不存在 → 清理 PID 文件

### 4. 边界情况处理

| 场景 | 处理方式 |
|------|----------|
| PID 文件存在但进程不存在 | 清理 PID 文件，返回 "stopped" |
| PID 存在但 cmdline 不匹配 | PID 被复用，清理文件，返回 "stopped" |
| 启动失败 | 不创建 PID 文件，返回错误 |
| 文件权限问题 | 使用 try-except 捕获，降级为内存状态 |

### 5. 影响范围

需修改文件：
- `webui/components/task_runner.py` - 核心逻辑改造
- `webui/pages/3_任务管理.py` - 移除 session_state 初始化
- `requirements.txt` - 添加 psutil（已安装，需显式声明）

### 6. 年报探索器数据源

查询 companies 表和 reports 表：
- `companies`: stock_code, short_name, full_name, plate, trade, trade_name
- `reports`: stock_code, year, download_status, convert_status, extract_status

支持的搜索维度：
- 股票代码（精确/模糊）
- 公司名称（模糊）
- 行业 (trade_name)
- 年份范围
- 处理状态（待下载/待转换/待提取/已完成）

---

## 设计规格书 (P2)

> Gemini 生成初稿，Claude 审核完善

### 1. 文件结构变更

```
webui/
├── components/
│   ├── db_utils.py             # MODIFIED: 新增查询函数
│   ├── task_runner.py          # MODIFIED: 重构任务管理逻辑
│   └── pid_manager.py          # NEW: PID 文件管理器
├── pages/
│   ├── 3_任务管理.py           # MODIFIED: 适配新的 task_runner
│   └── 4_年报浏览器.py         # NEW: 年报探索与处理页面
data/
└── run/                        # NEW: 存放 PID 文件的目录
```

### 2. PID 管理器 (`components/pid_manager.py`)

```python
from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Literal

import psutil

TaskName = Literal["crawler", "converter", "extractor"]

RUN_DIR = Path("data/run")

TASK_SCRIPTS = {
    "crawler": "1.report_link_crawler.py",
    "converter": "2.pdf_batch_converter.py",
    "extractor": "mda_extractor.py",
}


@dataclass
class PIDInfo:
    """Holds the metadata of a running process."""
    pid: int
    script: str
    args: list[str]
    log_file: str
    started_at: str  # ISO format

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2, ensure_ascii=False)

    @classmethod
    def from_path(cls, path: Path) -> "PIDInfo | None":
        if not path.exists():
            return None
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            return cls(**data)
        except (json.JSONDecodeError, TypeError, OSError, KeyError):
            path.unlink(missing_ok=True)
            return None


class PIDManager:
    """Manages task lifecycle via PID files."""

    def __init__(self, task_name: TaskName):
        self.task_name = task_name
        self.pid_file = RUN_DIR / f"{task_name}.pid"
        RUN_DIR.mkdir(parents=True, exist_ok=True)

    def write(self, pid_info: PIDInfo) -> None:
        with open(self.pid_file, "w", encoding="utf-8") as f:
            f.write(pid_info.to_json())

    def read(self) -> PIDInfo | None:
        return PIDInfo.from_path(self.pid_file)

    def delete(self) -> None:
        self.pid_file.unlink(missing_ok=True)

    def get_process(self) -> psutil.Process | None:
        """
        Gets the psutil.Process object if the process is alive and valid.
        Validates process identity by checking cmdline contains the expected script.
        """
        pid_info = self.read()
        if not pid_info:
            return None

        try:
            if not psutil.pid_exists(pid_info.pid):
                self.delete()
                return None

            proc = psutil.Process(pid_info.pid)

            # Validate: cmdline should contain our script name
            cmdline = " ".join(proc.cmdline()).lower()
            expected_script = pid_info.script.lower()
            if expected_script not in cmdline:
                self.delete()  # PID was reused by another process
                return None

            return proc

        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            self.delete()
            return None

    def get_exit_code(self) -> int | None:
        """Returns the exit code if process has terminated, else None."""
        pid_info = self.read()
        if not pid_info:
            return None
        try:
            proc = psutil.Process(pid_info.pid)
            # If we can get the process and it's still running, return None
            if proc.is_running() and proc.status() != psutil.STATUS_ZOMBIE:
                return None
            # Zombie process - get return code
            return proc.returncode
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return None
```

### 3. 任务运行器改造 (`components/task_runner.py`)

```python
from __future__ import annotations

import subprocess
from datetime import datetime, timezone
from pathlib import Path

import psutil
import streamlit as st

from .pid_manager import PIDManager, PIDInfo, TaskName, TASK_SCRIPTS

TASK_LABELS = {
    "crawler": "爬取链接",
    "converter": "下载转换",
    "extractor": "提取 MDA",
}

LOG_DIR = Path("logs/webui")


def start_task(task_name: TaskName, extra_args: list[str] | None = None) -> bool:
    """Starts a background task and creates a PID file."""
    manager = PIDManager(task_name)

    # Check if already running
    if manager.get_process() is not None:
        st.warning(f"任务 '{TASK_LABELS[task_name]}' 已在运行中。")
        return False

    # Ensure directories exist
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / f"{task_name}.log"

    script = TASK_SCRIPTS[task_name]
    script_path = Path(script)
    if not script_path.exists():
        st.error(f"脚本文件不存在: {script_path}")
        return False

    cmd = ["python", str(script_path)]
    if extra_args:
        cmd.extend(extra_args)

    try:
        with open(log_file, "w", encoding="utf-8") as f:
            process = subprocess.Popen(
                cmd,
                stdout=f,
                stderr=subprocess.STDOUT,
                cwd=Path.cwd(),
                text=True,
                encoding="utf-8",
            )

        pid_info = PIDInfo(
            pid=process.pid,
            script=script,
            args=extra_args or [],
            log_file=str(log_file),
            started_at=datetime.now(timezone.utc).isoformat(),
        )
        manager.write(pid_info)

        st.success(f"任务 '{TASK_LABELS[task_name]}' 已启动。")
        return True

    except (OSError, subprocess.SubprocessError) as e:
        st.error(f"启动任务失败: {e}")
        return False


def stop_task(task_name: TaskName) -> bool:
    """Stops a background task using info from its PID file."""
    manager = PIDManager(task_name)
    proc = manager.get_process()

    if proc is None:
        st.info(f"任务 '{TASK_LABELS[task_name]}' 未在运行。")
        manager.delete()
        return False

    try:
        proc.terminate()
        proc.wait(timeout=5)
        st.success(f"任务 '{TASK_LABELS[task_name]}' 已停止。")
    except psutil.TimeoutExpired:
        proc.kill()
        st.warning("任务未能优雅停止，已强制终止。")
    except psutil.Error as e:
        st.error(f"停止任务时出错: {e}")
        return False
    finally:
        manager.delete()

    return True


def get_task_status(task_name: TaskName) -> str:
    """Gets the status: 'stopped', 'running', 'completed', or 'error'."""
    manager = PIDManager(task_name)
    pid_info = manager.read()

    if pid_info is None:
        return "stopped"

    proc = manager.get_process()
    if proc is None:
        # PID file exists but process is gone - check log for exit status
        manager.delete()
        return "stopped"

    # Process exists and is running
    try:
        status = proc.status()
        if status == psutil.STATUS_ZOMBIE:
            manager.delete()
            return "error"
        return "running"
    except psutil.Error:
        manager.delete()
        return "stopped"


def read_log(task_name: TaskName, tail_lines: int = 100) -> str:
    """Reads the tail end of a task's log file."""
    log_path = LOG_DIR / f"{task_name}.log"

    # Also check PID file for custom log path
    manager = PIDManager(task_name)
    pid_info = manager.read()
    if pid_info and pid_info.log_file:
        log_path = Path(pid_info.log_file)

    if not log_path.exists():
        return "日志文件不存在。"

    try:
        with open(log_path, encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
            return "".join(lines[-tail_lines:])
    except OSError as e:
        return f"读取日志文件失败: {e}"
```

### 4. 年报浏览器页面 (`pages/4_年报浏览器.py`)

**布局设计**:

```
┌─────────────────────────────────────────────────────────────────┐
│ 智能选股与年报浏览器                                              │
├───────────────────────┬─────────────────────────────────────────┤
│ [侧边栏: 筛选条件]    │ 共找到 1,234 条记录                       │
│                       ├─────────────────────────────────────────┤
│ 搜索公司: [_______]   │ ☑ │ 代码   │ 名称   │ 年份 │ 下载 │ ...│
│                       │ ☐ │ 600519 │ 贵州茅台│ 2023 │  ✓  │    │
│ 行业:                 │ ☑ │ 000001 │ 平安银行│ 2023 │  ✓  │    │
│ [✓ 银行] [✓ 证券]    │ ☐ │ 600036 │ 招商银行│ 2022 │ 待处理│   │
│                       │                                         │
│ 年份: [2020]--[2023] │                                         │
│                       ├─────────────────────────────────────────┤
│ 下载状态: [全部 ▼]   │ 已选择 2 条                               │
│ 转换状态: [全部 ▼]   │ [添加到处理队列] [立即下载转换] [提取MDA]│
│ 提取状态: [全部 ▼]   │                                         │
│                       │                                         │
│ [查询]                │                                         │
└───────────────────────┴─────────────────────────────────────────┘
```

### 5. db_utils.py 新增查询函数

```python
@st.cache_data(ttl=300)
def get_filter_options(_conn: duckdb.DuckDBPyConnection) -> dict:
    """Gets unique values for filter dropdowns."""
    if _conn is None:
        return {"trades": [], "plates": [], "min_year": 2010, "max_year": 2024}
    try:
        trades = _conn.execute(
            "SELECT DISTINCT trade_name FROM companies WHERE trade_name IS NOT NULL ORDER BY trade_name"
        ).df()["trade_name"].tolist()

        plates = _conn.execute(
            "SELECT DISTINCT plate FROM companies WHERE plate IS NOT NULL ORDER BY plate"
        ).df()["plate"].tolist()

        years = _conn.execute("SELECT MIN(year), MAX(year) FROM reports").fetchone()

        return {
            "trades": trades,
            "plates": plates,
            "min_year": years[0] or 2010,
            "max_year": years[1] or 2024,
        }
    except duckdb.Error:
        return {"trades": [], "plates": [], "min_year": 2010, "max_year": 2024}


@st.cache_data(ttl=10)
def search_reports(
    _conn: duckdb.DuckDBPyConnection,
    query: str | None = None,
    trades: list[str] | None = None,
    years: tuple[int, int] | None = None,
    download_status: str | None = None,
    convert_status: str | None = None,
    extract_status: str | None = None,
    limit: int = 500,
) -> pd.DataFrame:
    """Searches and filters reports based on multiple criteria."""
    if _conn is None:
        return pd.DataFrame()

    where_clauses = []
    params = []

    if query:
        where_clauses.append("(c.stock_code ILIKE ? OR c.short_name ILIKE ?)")
        params.extend([f"%{query}%", f"%{query}%"])

    if trades:
        placeholders = ",".join(["?"] * len(trades))
        where_clauses.append(f"c.trade_name IN ({placeholders})")
        params.extend(trades)

    if years:
        where_clauses.append("r.year BETWEEN ? AND ?")
        params.extend(years)

    for status_field, status_value in [
        ("download_status", download_status),
        ("convert_status", convert_status),
        ("extract_status", extract_status),
    ]:
        if status_value and status_value != "全部":
            where_clauses.append(f"r.{status_field} = ?")
            params.append(status_value)

    where_str = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    sql = f"""
        SELECT
            c.stock_code,
            c.short_name,
            r.year,
            r.download_status,
            r.convert_status,
            r.extract_status,
            c.plate,
            c.trade_name
        FROM reports r
        JOIN companies c ON r.stock_code = c.stock_code
        {where_str}
        ORDER BY r.year DESC, c.stock_code
        LIMIT {limit}
    """

    try:
        return _conn.execute(sql, params).df()
    except duckdb.Error as e:
        st.error(f"查询报告失败: {e}")
        return pd.DataFrame()


def update_report_status_batch(
    conn: duckdb.DuckDBPyConnection,
    stock_codes_years: list[tuple[str, int]],
    status_field: str,
    new_status: str,
) -> int:
    """Batch update report status for selected records."""
    if not stock_codes_years:
        return 0

    # Build parameterized query
    placeholders = ",".join(["(?, ?)"] * len(stock_codes_years))
    params = []
    for code, year in stock_codes_years:
        params.extend([code, year])
    params.append(new_status)

    sql = f"""
        UPDATE reports
        SET {status_field} = ?
        WHERE (stock_code, year) IN ({placeholders})
    """

    try:
        conn.execute(sql, [new_status] + params[:-1])
        return len(stock_codes_years)
    except duckdb.Error:
        return 0
```

### 6. 验收标准

| ID | Given | When | Then |
|----|-------|------|------|
| E1 | 任务未运行 | 点击"启动" | 状态显示"运行中"，PID 文件创建 |
| E2 | 任务运行中 | 刷新页面 | 状态仍显示"运行中" |
| E3 | 任务运行中 | 重启 Streamlit 服务 | 状态仍显示"运行中" |
| E4 | 任务运行中 | 点击"停止" | 状态显示"已停止"，PID 文件删除 |
| E5 | PID 文件存在但进程不存在 | 访问页面 | 状态显示"已停止"，PID 文件自动清理 |
| E6 | companies 表有数据 | 访问年报浏览器 | 显示行业/年份筛选选项 |
| E7 | 输入"600519" | 点击查询 | 仅显示贵州茅台年报 |
| E8 | 选择行业"银行"，年份 2022-2023 | 查询 | 显示银行业 2022-2023 年报 |
| E9 | 勾选 3 条记录 | 点击"添加到处理队列" | 记录状态更新为 pending |
