"""Configuration file editor for WebUI."""

from __future__ import annotations

from pathlib import Path

import streamlit as st
import yaml

DEFAULT_CONFIG_PATH = Path("config.yaml")


def load_config(config_path: Path = DEFAULT_CONFIG_PATH) -> dict:
    """Loads the config.yaml file."""
    try:
        if config_path.exists():
            with open(config_path, encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
        else:
            st.warning(f"配置文件不存在于 `{config_path}`，将使用空配置。")
            return {}
    except (OSError, yaml.YAMLError) as e:
        st.error(f"读取配置文件失败: {e}")
        return {}


def save_config(config_data: dict, config_path: Path = DEFAULT_CONFIG_PATH) -> bool:
    """Saves the config data to config.yaml. Returns True on success."""
    try:
        with open(config_path, "w", encoding="utf-8") as f:
            yaml.dump(config_data, f, allow_unicode=True, sort_keys=False, default_flow_style=False)
        return True
    except (OSError, yaml.YAMLError) as e:
        st.error(f"保存配置文件失败: {e}")
        return False
