"""Background task runner for WebUI with PID-based persistence."""

from __future__ import annotations

import subprocess
from datetime import UTC, datetime
from pathlib import Path

import psutil
import streamlit as st

from .pid_manager import TASK_SCRIPTS, PIDInfo, PIDManager, TaskName

TASK_LABELS: dict[str, str] = {
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
            started_at=datetime.now(UTC).isoformat(),
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
        # PID file exists but process is gone
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
