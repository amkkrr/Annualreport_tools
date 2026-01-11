"""任务管理页面 - 启动、停止和监控后台任务。"""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from webui.components import task_runner

st.title("任务管理")

# Task Control Panel
st.subheader("任务控制面板")

col1, col2, col3 = st.columns(3)

tasks_meta = {
    "crawler": {"label": "爬取链接", "column": col1, "args": ["--use-config"]},
    "converter": {"label": "下载转换", "column": col2, "args": ["--use-config"]},
    "extractor": {"label": "提取 MDA", "column": col3, "args": ["--use-config"]},
}

for task_key, meta in tasks_meta.items():
    with meta["column"]:
        with st.container(border=True):
            status = task_runner.get_task_status(task_key)
            status_map = {
                "running": ("green", "运行中"),
                "stopped": ("gray", "已停止"),
                "completed": ("blue", "已完成"),
                "error": ("red", "错误"),
            }
            color, text = status_map.get(status, ("gray", "未知"))

            st.markdown(f"**{meta['label']}**")
            st.markdown(f"状态: :{color}[{text}]")

            c1, c2 = st.columns(2)
            with c1:
                if st.button("启动", key=f"start_{task_key}", use_container_width=True):
                    task_runner.start_task(task_key, extra_args=meta["args"])
                    st.rerun()

            with c2:
                if st.button("停止", key=f"stop_{task_key}", use_container_width=True):
                    task_runner.stop_task(task_key)
                    st.rerun()

st.divider()

# Log Viewer
st.subheader("任务日志")

# Auto-refresh toggle
auto_refresh = st.checkbox("自动刷新日志 (每 5 秒)", value=False)

# Log tabs
log_tabs = st.tabs([meta["label"] for meta in tasks_meta.values()])

for i, (task_key, meta) in enumerate(tasks_meta.items()):
    with log_tabs[i]:
        log_content = task_runner.read_log(task_key)
        st.code(log_content if log_content else "暂无日志", language="log")

# Manual refresh button
if st.button("刷新日志"):
    st.rerun()

# Auto-refresh using rerun
if auto_refresh:
    import time

    time.sleep(5)
    st.rerun()
