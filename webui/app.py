"""Streamlit WebUI - Main Entry Point."""

from __future__ import annotations

import streamlit as st

# Set page config - must be the first Streamlit command
st.set_page_config(
    page_title="年报分析工具箱",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        "Get Help": "https://github.com/JeffDing/AnnualReport-Tools",
        "Report a bug": "https://github.com/JeffDing/AnnualReport-Tools/issues",
        "About": "# 年报分析工具箱 \n\n一个用于自动化下载、转换和分析年报的集成工具。",
    },
)

# Main page content
st.title("欢迎使用年报分析工具箱")
st.markdown(
    """
    这是一个基于 Streamlit 构建的 WebUI，旨在简化年报分析工作流。

    **请从左侧侧边栏选择一个页面开始:**

    - **监控仪表盘**: 查看数据处理进度、队列状态和关键指标。
    - **配置管理**: 编辑和管理系统的配置文件 `config.yaml`。
    - **任务管理**: 启动、停止和监控后台处理任务（如爬虫、转换、提取）。

    ---

    ### 快速开始

    1. 确保已安装依赖: `pip install -r webui/requirements.txt`
    2. 确保数据库存在: `data/annual_reports.duckdb`
    3. 确保配置文件存在: `config.yaml`

    ### 项目链接
    - **源代码:** [GitHub Repository](https://github.com/JeffDing/AnnualReport-Tools)
"""
)
