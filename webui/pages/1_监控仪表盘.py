"""监控仪表盘页面 - 展示数据处理进度和队列状态。"""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from annual_report_mda import db, sqlite_db
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
st.caption("最多显示 100 条记录。勾选记录并使用下方按钮进行批量操作。")

tab1, tab2, tab3 = st.tabs(["待下载队列", "待转换队列", "待审核 MDA"])

with tab1:
    df = db_utils.get_pending_downloads()
    if not df.empty:
        # Add selection column
        df = df.copy()
        df.insert(0, "选择", False)

        edited_df = st.data_editor(
            df,
            column_config={
                "选择": st.column_config.CheckboxColumn("选择", default=False),
                "year": st.column_config.NumberColumn("年份", format="%d"),
            },
            disabled=[c for c in df.columns if c != "选择"],
            hide_index=True,
            use_container_width=True,
            height=350,
            key="editor_pending_downloads",
        )

        selected_rows = edited_df[edited_df["选择"]]
        if not selected_rows.empty:
            c1, c2, _ = st.columns([1, 1, 2])
            with c1:
                if st.button(
                    "批量跳过", key="btn_skip_downloads", help="将选中记录状态设为 skipped"
                ):
                    keys = [
                        (row["stock_code"], int(row["year"])) for _, row in selected_rows.iterrows()
                    ]
                    with sqlite_db.connection_context() as conn:
                        sqlite_db.batch_update_report_status(
                            conn, keys=keys, download_status="skipped"
                        )
                    st.success(f"已跳过 {len(keys)} 条记录")
                    st.cache_data.clear()
                    st.rerun()
            with c2:
                if st.button("批量重置", key="btn_reset_downloads", help="重新标记为 pending"):
                    keys = [
                        (row["stock_code"], int(row["year"])) for _, row in selected_rows.iterrows()
                    ]
                    with sqlite_db.connection_context() as conn:
                        sqlite_db.batch_update_report_status(
                            conn, keys=keys, download_status="pending"
                        )
                    st.success(f"已重置 {len(keys)} 条记录")
                    st.cache_data.clear()
                    st.rerun()
    else:
        st.success("没有待下载的任务。")

with tab2:
    df = db_utils.get_pending_converts()
    if not df.empty:
        # Add selection column
        df = df.copy()
        df.insert(0, "选择", False)

        edited_df = st.data_editor(
            df,
            column_config={
                "选择": st.column_config.CheckboxColumn("选择", default=False),
                "year": st.column_config.NumberColumn("年份", format="%d"),
            },
            disabled=[c for c in df.columns if c != "选择"],
            hide_index=True,
            use_container_width=True,
            height=350,
            key="editor_pending_converts",
        )

        selected_rows = edited_df[edited_df["选择"]]
        if not selected_rows.empty:
            c1, c2, _ = st.columns([1, 1, 2])
            with c1:
                if st.button(
                    "批量跳过", key="btn_skip_converts", help="将选中记录状态设为 skipped"
                ):
                    keys = [
                        (row["stock_code"], int(row["year"])) for _, row in selected_rows.iterrows()
                    ]
                    with sqlite_db.connection_context() as conn:
                        sqlite_db.batch_update_report_status(
                            conn, keys=keys, convert_status="skipped"
                        )
                    st.success(f"已跳过 {len(keys)} 条记录")
                    st.cache_data.clear()
                    st.rerun()
            with c2:
                if st.button("批量重置", key="btn_reset_converts", help="重新标记为 pending"):
                    keys = [
                        (row["stock_code"], int(row["year"])) for _, row in selected_rows.iterrows()
                    ]
                    with sqlite_db.connection_context() as conn:
                        sqlite_db.batch_update_report_status(
                            conn, keys=keys, convert_status="pending"
                        )
                    st.success(f"已重置 {len(keys)} 条记录")
                    st.cache_data.clear()
                    st.rerun()
    else:
        st.success("没有待转换的任务。")

with tab3:
    df = db_utils.get_mda_needs_review()
    if not df.empty:
        # Add selection column
        df = df.copy()
        df.insert(0, "选择", False)

        edited_df = st.data_editor(
            df,
            column_config={
                "选择": st.column_config.CheckboxColumn("选择", default=False),
                "year": st.column_config.NumberColumn("年份", format="%d"),
            },
            disabled=[c for c in df.columns if c != "选择"],
            hide_index=True,
            use_container_width=True,
            height=350,
            key="editor_mda_review",
        )

        selected_rows = edited_df[edited_df["选择"]]
        if not selected_rows.empty:
            c1, _ = st.columns([1, 3])
            with c1:
                if st.button("批量通过审核", key="btn_approve_mda"):
                    keys = [
                        (row["stock_code"], int(row["year"])) for _, row in selected_rows.iterrows()
                    ]
                    import duckdb

                    from annual_report_mda.db import DEFAULT_DUCKDB_PATH

                    conn = duckdb.connect(database=str(DEFAULT_DUCKDB_PATH), read_only=False)
                    try:
                        db.batch_update_mda_review_status(conn, keys=keys, needs_review=False)
                        st.success(f"已审核通过 {len(keys)} 条 MDA 记录")
                    finally:
                        conn.close()
                    st.cache_data.clear()
                    st.rerun()
    else:
        st.success("没有待审核的 MDA。")

st.divider()
st.subheader("危险区域 (全局操作)")
st.warning("以下操作将影响数据库中所有符合条件的记录，请谨慎操作。")

col1, col2, col3 = st.columns(3)

with col1:
    with st.popover("清理下载队列"):
        st.write("将所有 '待下载' 状态重置为 '已跳过'")
        if st.button("确认清理下载队列", type="primary", key="btn_clear_all_downloads"):
            count = db_utils.clear_all_pending_downloads()
            st.success(f"已清理 {count} 条下载记录")
            st.cache_data.clear()
            st.rerun()

with col2:
    with st.popover("清理转换队列"):
        st.write("将所有 '待转换' 状态重置为 '已跳过'")
        if st.button("确认清理转换队列", type="primary", key="btn_clear_all_converts"):
            count = db_utils.clear_all_pending_converts()
            st.success(f"已清理 {count} 条转换记录")
            st.cache_data.clear()
            st.rerun()

with col3:
    with st.popover("重置失败记录"):
        phase = st.selectbox(
            "选择阶段",
            ["download", "convert", "extract"],
            format_func=lambda x: {"download": "下载", "convert": "转换", "extract": "提取"}.get(
                x, x
            ),
        )
        if st.button("确认重置失败记录", type="primary", key="btn_reset_all_failed"):
            count = db_utils.reset_all_failed(phase)
            st.success(f"已重置 {count} 条失败记录")
            st.cache_data.clear()
            st.rerun()
