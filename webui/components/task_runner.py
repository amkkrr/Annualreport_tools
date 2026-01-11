"""Background task runner for WebUI."""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import streamlit as st

TaskName = Literal["crawler", "converter", "extractor"]

TASK_SCRIPTS: dict[str, str] = {
    "crawler": "1.report_link_crawler.py",
    "converter": "2.pdf_batch_converter.py",
    "extractor": "mda_extractor.py",
}

TASK_LABELS: dict[str, str] = {
    "crawler": "爬取链接",
    "converter": "下载转换",
    "extractor": "提取 MDA",
}

LOG_DIR = Path("logs/webui")


@dataclass
class TaskState:
    """Holds the state of a background task."""

    process: subprocess.Popen | None = None
    log_file: Path | None = None


def get_task_state(task_name: TaskName) -> TaskState:
    """Retrieves or initializes the state for a given task."""
    if "task_states" not in st.session_state:
        st.session_state.task_states = {name: TaskState() for name in TASK_SCRIPTS}
    return st.session_state.task_states[task_name]


def start_task(task_name: TaskName, extra_args: list[str] | None = None) -> bool:
    """Starts a background task and updates its state. Returns True on success."""
    state = get_task_state(task_name)

    # Check if already running
    if state.process and state.process.poll() is None:
        st.warning(f"任务 '{TASK_LABELS.get(task_name, task_name)}' 已在运行中。")
        return False

    # Ensure log directory exists
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / f"{task_name}.log"
    state.log_file = log_file

    # Check script exists
    script_path = Path(TASK_SCRIPTS[task_name])
    if not script_path.exists():
        st.error(f"脚本文件不存在: {script_path}")
        return False

    # Build command
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
        state.process = process
        st.success(f"任务 '{TASK_LABELS.get(task_name, task_name)}' 已启动。")
        return True
    except (OSError, subprocess.SubprocessError) as e:
        st.error(f"启动任务失败: {e}")
        return False


def stop_task(task_name: TaskName) -> bool:
    """Stops a background task. Returns True on success."""
    state = get_task_state(task_name)

    if state.process is None or state.process.poll() is not None:
        st.info(f"任务 '{TASK_LABELS.get(task_name, task_name)}' 未在运行。")
        state.process = None
        return False

    try:
        state.process.terminate()
        state.process.wait(timeout=5)
        st.success(f"任务 '{TASK_LABELS.get(task_name, task_name)}' 已停止。")
    except subprocess.TimeoutExpired:
        state.process.kill()
        st.warning("任务未能优雅停止，已强制终止。")
    except Exception as e:
        st.error(f"停止任务时出错: {e}")
        return False
    finally:
        state.process = None

    return True


def get_task_status(task_name: TaskName) -> str:
    """Gets the status of a task: 'stopped', 'running', 'completed', or 'error'."""
    state = get_task_state(task_name)

    if state.process is None:
        return "stopped"

    exit_code = state.process.poll()
    if exit_code is None:
        return "running"
    if exit_code == 0:
        state.process = None
        return "completed"

    return "error"


def read_log(task_name: TaskName, tail_lines: int = 100) -> str:
    """Reads the tail end of a task's log file."""
    state = get_task_state(task_name)

    if state.log_file is None or not state.log_file.exists():
        return "日志文件不存在。"

    try:
        with open(state.log_file, encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
            return "".join(lines[-tail_lines:])
    except OSError as e:
        return f"读取日志文件失败: {e}"
