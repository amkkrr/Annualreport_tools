"""WebUI Components Package."""

from .config_editor import load_config, save_config
from .db_utils import (
    get_counts,
    get_duckdb_connection,
    get_filter_options,
    get_mda_needs_review,
    get_pending_converts,
    get_pending_downloads,
    get_reports_progress,
    get_sqlite_connection,
    search_reports,
)
from .task_runner import (
    get_task_status,
    read_log,
    start_task,
    stop_task,
)

__all__ = [
    # Database connections
    "get_sqlite_connection",
    "get_duckdb_connection",
    # Query functions
    "get_reports_progress",
    "get_pending_downloads",
    "get_pending_converts",
    "get_mda_needs_review",
    "get_counts",
    "get_filter_options",
    "search_reports",
    # Config management
    "load_config",
    "save_config",
    # Task management
    "start_task",
    "stop_task",
    "get_task_status",
    "read_log",
]
