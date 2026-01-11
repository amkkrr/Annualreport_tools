from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path

try:
    from dotenv import load_dotenv  # type: ignore
except Exception:  # pragma: no cover
    load_dotenv = None  # type: ignore[assignment]

try:
    from rich.logging import RichHandler  # type: ignore
except Exception:  # pragma: no cover
    RichHandler = None  # type: ignore[misc,assignment]


@dataclass(frozen=True)
class RuntimeConfig:
    db_path: Path = Path("data/annual_reports.duckdb")
    log_level: str = "INFO"


# Module-level flag to track if logging has been configured
_logging_configured = False


def configure_logging(
    level: str = "INFO",
    log_dir: Path | str | None = None,
    log_file_prefix: str = "app",
    max_bytes: int = 10 * 1024 * 1024,
    backup_count: int = 7,
    enable_console: bool = True,
    enable_file: bool = False,
    file_format: str = "%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    date_format: str = "%Y-%m-%d %H:%M:%S",
    console_rich: bool = True,
) -> None:
    """Configure logging with optional console (Rich) and file handlers.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR).
        log_dir: Directory for log files. Required if enable_file is True.
        log_file_prefix: Prefix for log file names (e.g., "app" -> "app.log").
        max_bytes: Maximum size of a single log file before rotation (default: 10MB).
        backup_count: Number of backup files to keep (default: 7).
        enable_console: Whether to enable console output (default: True).
        enable_file: Whether to enable file logging (default: False).
        file_format: Log format string for file handler.
        date_format: Date format string for timestamps.
        console_rich: Whether to use RichHandler for console output (default: True).
    """
    global _logging_configured

    root = logging.getLogger()

    # Clear existing handlers to avoid duplication
    if _logging_configured:
        root.handlers.clear()

    root.setLevel(level)
    handlers: list[logging.Handler] = []

    # Console handler
    if enable_console:
        if console_rich and RichHandler is not None:
            console_handler: logging.Handler = RichHandler(
                rich_tracebacks=True,
                tracebacks_show_locals=False,
                show_path=False,
            )
        else:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(logging.Formatter(file_format, datefmt=date_format))
        console_handler.setLevel(level)
        handlers.append(console_handler)

    # File handler with rotation
    if enable_file:
        if log_dir is None:
            log_dir = Path("logs")
        log_dir_path = Path(log_dir)
        log_dir_path.mkdir(parents=True, exist_ok=True)

        log_file = log_dir_path / f"{log_file_prefix}.log"
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
        file_handler.setFormatter(logging.Formatter(file_format, datefmt=date_format))
        file_handler.setLevel(level)
        handlers.append(file_handler)

    # Apply handlers
    for handler in handlers:
        root.addHandler(handler)

    _logging_configured = True  # noqa: F841 - modifies global via declaration above


def configure_logging_from_config(
    log_level: str = "INFO",
    logging_config: object | None = None,
) -> None:
    """Configure logging from a LoggingConfig object.

    This is a convenience wrapper that accepts a config object.

    Args:
        log_level: Log level from project.log_level.
        logging_config: LoggingConfig object (from config_manager).
    """
    if logging_config is None:
        configure_logging(level=log_level)
        return

    # Access attributes dynamically to avoid circular import
    configure_logging(
        level=log_level,
        log_dir=getattr(logging_config, "log_dir", None),
        log_file_prefix=getattr(logging_config, "file_prefix", "app"),
        max_bytes=getattr(logging_config, "max_bytes", 10 * 1024 * 1024),
        backup_count=getattr(logging_config, "backup_count", 7),
        enable_console=getattr(logging_config, "enable_console", True),
        enable_file=getattr(logging_config, "enable_file", False),
        file_format=getattr(
            logging_config,
            "file_format",
            "%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        ),
        date_format=getattr(logging_config, "date_format", "%Y-%m-%d %H:%M:%S"),
        console_rich=getattr(logging_config, "console_rich", True),
    )


def load_dotenv_if_present(dotenv_path: str | Path = ".env") -> bool:
    if load_dotenv is None:
        return False
    path = Path(dotenv_path)
    if not path.exists():
        return False
    return bool(load_dotenv(dotenv_path=str(path), override=False))


def utc_now() -> datetime:
    return datetime.now(UTC)


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()


def ensure_parent_dir(path: str | Path) -> None:
    Path(path).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)


def to_int(value: str) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
