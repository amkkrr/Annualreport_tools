"""监控仪表盘页面 - 展示数据处理进度和队列状态。"""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from webui.components import db_utils

st.title("监控仪表盘")

# Refresh button
if st.button("刷新数据"):
    st.cache_data.clear()
    st.rerun()

# Key Metrics
counts = db_utils.get_counts()
st.subheader("关键指标")
col1, col2, col3, col4 = st.columns(4)
col1.metric("待下载", f"{counts.get('pending_downloads', 0):,}")
col2.metric("待转换", f"{counts.get('pending_converts', 0):,}")
col3.metric("待审核 MDA", f"{counts.get('mda_needs_review', 0):,}")
col4.metric("已完成提取", f"{counts.get('total_extracted', 0):,}")

st.divider()

# Annual Progress
st.subheader("年度处理进度")
progress_df = db_utils.get_reports_progress()
if not progress_df.empty:
    st.dataframe(progress_df, use_container_width=True)
else:
    st.info("暂无年度进度数据。")

st.divider()

# Pending Queues
st.subheader("待处理队列")
st.caption("最多显示 100 条记录")

tab1, tab2, tab3 = st.tabs(["待下载队列", "待转换队列", "待审核 MDA"])

with tab1:
    df = db_utils.get_pending_downloads()
    if not df.empty:
        st.dataframe(df, use_container_width=True, height=350)
    else:
        st.success("没有待下载的任务。")

with tab2:
    df = db_utils.get_pending_converts()
    if not df.empty:
        st.dataframe(df, use_container_width=True, height=350)
    else:
        st.success("没有待转换的任务。")

with tab3:
    df = db_utils.get_mda_needs_review()
    if not df.empty:
        st.dataframe(df, use_container_width=True, height=350)
    else:
        st.success("没有待审核的 MDA。")
