"""任务管理页面 - 启动、停止和监控后台任务。"""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from webui.components import db_utils, task_runner

st.title("任务管理")

# Task Control Panel
st.subheader("任务控制面板")

# Get counts
counts = db_utils.get_counts()
pending_downloads = counts.get("pending_downloads", 0)
pending_converts = counts.get("pending_converts", 0)
pending_extractions = counts.get("pending_extractions", 0)

col1, col2, col3 = st.columns(3)

tasks_meta = {
    "crawler": {
        "label": "爬取链接",
        "column": col1,
        "args": ["--use-config"],
        "queues": [],
    },
    "converter": {
        "label": "下载转换",
        "column": col2,
        "args": ["--use-yaml-config"],
        "queues": [
            ("待下载", pending_downloads),
            ("待转换", pending_converts),
        ],
    },
    "extractor": {
        "label": "提取 MDA",
        "column": col3,
        "args": ["--use-config"],
        "queues": [
            ("待提取", pending_extractions),
        ],
    },
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

            if meta["queues"]:
                for q_label, q_count in meta["queues"]:
                    st.markdown(f"{q_label}: {q_count} 条")
            else:
                st.markdown("&nbsp;")  # 占位保持高度一致

            c1, c2 = st.columns(2)

            # 针对爬虫任务增加年份选择
            extra_args = list(meta["args"])
            if task_key == "crawler":
                years = st.multiselect(
                    "选择年份", options=list(range(2004, 2026)), default=[2024], key="crawler_years"
                )
                if years:
                    extra_args.extend(["--year"] + [str(y) for y in years])

            with c1:
                if st.button("启动", key=f"start_{task_key}", use_container_width=True):
                    task_runner.start_task(task_key, extra_args=extra_args)
                    st.rerun()

            with c2:
                if st.button("停止", key=f"stop_{task_key}", use_container_width=True):
                    task_runner.stop_task(task_key)
                    st.rerun()

st.divider()

# Log Viewer
st.subheader("任务日志")

# Log tabs
log_tabs = st.tabs([meta["label"] for meta in tasks_meta.values()])

for i, (task_key, meta) in enumerate(tasks_meta.items()):
    with log_tabs[i]:
        log_placeholder = st.empty()
        log_content = task_runner.read_log(task_key)
        log_placeholder.code(log_content if log_content else "暂无日志", language="log")

# Auto-refresh using loop and st.empty
if st.checkbox("开启实时日志刷新", value=False):
    import time

    while True:
        for i, (task_key, meta) in enumerate(tasks_meta.items()):
            # Note: We can't easily update placeholders in tabs from a loop outside the for i loop
            # without keeping track of all placeholders.
            # Simplest way in Streamlit is to rerun or use a more complex state.
            # However, to avoid full page flicker, we can just use the previous st.rerun approach
            # but maybe with a slightly better UI or just stick to it if the user wants auto-refresh.
            # Actually, st.empty() inside the tab only works during the initial render.
            pass
        time.sleep(5)
        st.rerun()
else:
    if st.button("刷新日志"):
        st.rerun()
