"""配置管理页面 - 编辑和保存 config.yaml。"""

from __future__ import annotations

import datetime
import sys
from pathlib import Path

import streamlit as st

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from webui.components import config_editor

st.title("配置管理")

# Load existing config
config = config_editor.load_config()

with st.form(key="config_form"):
    st.subheader("爬虫配置")

    # Target Years
    current_year = datetime.date.today().year
    all_years = list(range(2010, current_year + 1))
    default_years = config.get("crawler", {}).get("years", [current_year - 1])
    # Ensure default_years are in all_years
    default_years = [y for y in default_years if y in all_years]
    target_years = st.multiselect(
        "目标年份",
        options=all_years,
        default=default_years,
        help="选择要爬取年报的年份",
    )

    # Market Plates
    plate_options = {
        "沪市主板": "shmb",
        "深市主板": "szmb",
        "创业板": "cyb",
        "科创板": "kcb",
    }
    default_plates = config.get("crawler", {}).get("plates", ["shmb", "szmb"])
    default_plates_keys = [k for k, v in plate_options.items() if v in default_plates]
    target_plates_keys = st.multiselect(
        "目标板块", options=list(plate_options.keys()), default=default_plates_keys
    )
    target_plates = [plate_options[k] for k in target_plates_keys]

    # Exclude keywords
    default_exclude = config.get("crawler", {}).get(
        "exclude_keywords", ["摘要", "半年报", "季报", "英文"]
    )
    default_exclude_text = "\n".join(default_exclude)
    exclude_keywords_text = st.text_area(
        "排除关键词 (每行一个)",
        value=default_exclude_text,
        height=120,
        help="报告标题中包含这些词的将被忽略",
    )

    st.divider()
    st.subheader("下载与转换配置")

    # Downloader processes
    default_dl_proc = config.get("downloader", {}).get("processes", 4)
    downloader_processes = st.number_input(
        "下载并发进程数", min_value=1, max_value=16, value=default_dl_proc, step=1
    )

    # Converter processes
    default_conv_proc = config.get("converter", {}).get("processes", 4)
    converter_processes = st.number_input(
        "转换并发进程数", min_value=1, max_value=16, value=default_conv_proc, step=1
    )

    st.divider()
    st.subheader("MDA 提取配置")

    # Extractor processes
    default_ext_proc = config.get("extractor", {}).get("processes", 2)
    extractor_processes = st.number_input(
        "MDA 提取并发进程数", min_value=1, max_value=8, value=default_ext_proc, step=1
    )

    # Incremental mode
    default_incremental = config.get("extractor", {}).get("incremental", True)
    incremental_mode = st.checkbox(
        "增量模式", value=default_incremental, help="仅处理尚未提取或提取失败的报告"
    )

    # Form submission
    submitted = st.form_submit_button("保存配置")
    if submitted:
        new_config = {
            "crawler": {
                "years": target_years,
                "plates": target_plates,
                "exclude_keywords": [
                    kw.strip() for kw in exclude_keywords_text.split("\n") if kw.strip()
                ],
            },
            "downloader": {"processes": downloader_processes},
            "converter": {"processes": converter_processes},
            "extractor": {
                "processes": extractor_processes,
                "incremental": incremental_mode,
            },
        }

        # Preserve other top-level keys from original config
        for key, value in config.items():
            if key not in new_config:
                new_config[key] = value

        if config_editor.save_config(new_config):
            st.success("配置已成功保存到 `config.yaml`！")
