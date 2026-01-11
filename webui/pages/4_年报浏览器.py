"""年报浏览器页面 - 智能选股与年报探索器。"""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from webui.components import db_utils

st.set_page_config(page_title="年报浏览器", page_icon="📚", layout="wide")
st.title("智能选股与年报浏览器")

# Get database connection
conn = db_utils.get_connection()

if conn is None:
    st.error("无法连接到数据库，请确认数据库文件存在。")
    st.stop()

# Get filter options
filter_options = db_utils.get_filter_options(conn)

# =============================================================================
# Sidebar: Filters
# =============================================================================

with st.sidebar:
    st.header("筛选条件")

    # Company search
    search_query = st.text_input(
        "搜索公司",
        placeholder="输入股票代码或公司名称",
        help="支持模糊搜索，如：600519 或 茅台",
    )

    # Industry filter
    selected_trades = st.multiselect(
        "选择行业",
        options=filter_options["trades"],
        default=[],
        help="可多选，留空表示不限制",
    )

    # Year range
    min_year = filter_options["min_year"]
    max_year = filter_options["max_year"]
    if min_year and max_year and min_year <= max_year:
        year_range = st.slider(
            "年份范围",
            min_value=min_year,
            max_value=max_year,
            value=(min_year, max_year),
        )
    else:
        year_range = (2020, 2024)
        st.info("无法获取年份范围，使用默认值")

    st.divider()
    st.subheader("处理状态筛选")

    status_options = ["全部", "pending", "success", "failed"]

    download_status = st.selectbox(
        "下载状态",
        options=status_options,
        index=0,
    )

    convert_status = st.selectbox(
        "转换状态",
        options=status_options,
        index=0,
    )

    extract_status = st.selectbox(
        "提取状态",
        options=status_options,
        index=0,
    )

    st.divider()

    # Query button
    query_btn = st.button("查询", type="primary", use_container_width=True)

# =============================================================================
# Main Content: Results
# =============================================================================

# Initialize session state for selected rows
if "selected_reports" not in st.session_state:
    st.session_state.selected_reports = set()

# Perform search
df = db_utils.search_reports(
    conn,
    query=search_query if search_query else None,
    trades=selected_trades if selected_trades else None,
    years=year_range,
    download_status=download_status,
    convert_status=convert_status,
    extract_status=extract_status,
)

# Results summary
col1, col2, col3 = st.columns([2, 1, 1])
with col1:
    st.metric("查询结果", f"{len(df)} 条记录")

if df.empty:
    st.info("未找到符合条件的记录。请调整筛选条件后重试。")
    st.stop()

# Add selection column
df = df.copy()
df.insert(0, "选择", False)

# Status display mapping
status_display = {
    "pending": "⏳ 待处理",
    "success": "✅ 成功",
    "failed": "❌ 失败",
    None: "—",
}


def format_status(val):
    return status_display.get(val, str(val))


# Display editable dataframe
st.subheader("年报列表")

edited_df = st.data_editor(
    df,
    column_config={
        "选择": st.column_config.CheckboxColumn(
            "选择",
            help="勾选要处理的年报",
            default=False,
        ),
        "stock_code": st.column_config.TextColumn("股票代码", width="small"),
        "short_name": st.column_config.TextColumn("公司名称", width="medium"),
        "year": st.column_config.NumberColumn("年份", format="%d", width="small"),
        "download_status": st.column_config.TextColumn("下载状态", width="small"),
        "convert_status": st.column_config.TextColumn("转换状态", width="small"),
        "extract_status": st.column_config.TextColumn("提取状态", width="small"),
        "plate": st.column_config.TextColumn("板块", width="small"),
        "trade_name": st.column_config.TextColumn("行业", width="medium"),
    },
    disabled=[
        "stock_code",
        "short_name",
        "year",
        "download_status",
        "convert_status",
        "extract_status",
        "plate",
        "trade_name",
    ],
    hide_index=True,
    use_container_width=True,
    key="report_editor",
)

# Get selected rows
selected_rows = edited_df[edited_df["选择"]]
selected_count = len(selected_rows)

# =============================================================================
# Batch Operations
# =============================================================================

st.divider()
st.subheader("批量操作")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("已选择", f"{selected_count} 条")

with col2:
    if st.button(
        "重置下载状态",
        disabled=selected_count == 0,
        help="将选中记录的下载状态重置为 pending",
        use_container_width=True,
    ):
        if selected_count > 0:
            write_conn = db_utils.get_write_connection()
            if write_conn:
                try:
                    for _, row in selected_rows.iterrows():
                        write_conn.execute(
                            "UPDATE reports SET download_status = 'pending' WHERE stock_code = ? AND year = ?",
                            [row["stock_code"], row["year"]],
                        )
                    st.success(f"已重置 {selected_count} 条记录的下载状态")
                    st.cache_data.clear()
                    st.rerun()
                finally:
                    write_conn.close()

with col3:
    if st.button(
        "重置转换状态",
        disabled=selected_count == 0,
        help="将选中记录的转换状态重置为 pending",
        use_container_width=True,
    ):
        if selected_count > 0:
            write_conn = db_utils.get_write_connection()
            if write_conn:
                try:
                    for _, row in selected_rows.iterrows():
                        write_conn.execute(
                            "UPDATE reports SET convert_status = 'pending' WHERE stock_code = ? AND year = ?",
                            [row["stock_code"], row["year"]],
                        )
                    st.success(f"已重置 {selected_count} 条记录的转换状态")
                    st.cache_data.clear()
                    st.rerun()
                finally:
                    write_conn.close()

with col4:
    if st.button(
        "重置提取状态",
        disabled=selected_count == 0,
        help="将选中记录的提取状态重置为 pending",
        use_container_width=True,
    ):
        if selected_count > 0:
            write_conn = db_utils.get_write_connection()
            if write_conn:
                try:
                    for _, row in selected_rows.iterrows():
                        write_conn.execute(
                            "UPDATE reports SET extract_status = 'pending' WHERE stock_code = ? AND year = ?",
                            [row["stock_code"], row["year"]],
                        )
                    st.success(f"已重置 {selected_count} 条记录的提取状态")
                    st.cache_data.clear()
                    st.rerun()
                finally:
                    write_conn.close()

# Tips
st.info(
    """
    **使用说明**:
    1. 使用左侧筛选条件查找目标年报
    2. 勾选要处理的记录
    3. 使用批量操作按钮重置状态为 pending
    4. 前往"任务管理"页面启动对应任务，系统会自动处理 pending 状态的记录
    """
)
