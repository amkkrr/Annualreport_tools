from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
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


def configure_logging(level: str = "INFO") -> None:
    root = logging.getLogger()

    if RichHandler is not None:
        for handler in root.handlers:
            if isinstance(handler, RichHandler):
                root.setLevel(level)
                return

        logging.basicConfig(
            level=level,
            format="%(message)s",
            datefmt="[%X]",
            handlers=[
                RichHandler(
                    rich_tracebacks=True,
                    tracebacks_show_locals=False,
                    show_path=False,
                )
            ],
        )
        return

    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
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
