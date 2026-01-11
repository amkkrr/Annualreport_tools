"""PID file manager for persistent task state tracking."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal

import psutil

TaskName = Literal["crawler", "converter", "extractor"]

RUN_DIR = Path("data/run")

TASK_SCRIPTS: dict[str, str] = {
    "crawler": "1.report_link_crawler.py",
    "converter": "2.pdf_batch_converter.py",
    "extractor": "mda_extractor.py",
}


@dataclass
class PIDInfo:
    """Holds the metadata of a running process."""

    pid: int
    script: str
    args: list[str]
    log_file: str
    started_at: str  # ISO format

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2, ensure_ascii=False)

    @classmethod
    def from_path(cls, path: Path) -> PIDInfo | None:
        if not path.exists():
            return None
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            return cls(**data)
        except (json.JSONDecodeError, TypeError, OSError, KeyError):
            path.unlink(missing_ok=True)
            return None


class PIDManager:
    """Manages task lifecycle via PID files."""

    def __init__(self, task_name: TaskName):
        self.task_name = task_name
        self.pid_file = RUN_DIR / f"{task_name}.pid"
        RUN_DIR.mkdir(parents=True, exist_ok=True)

    def write(self, pid_info: PIDInfo) -> None:
        with open(self.pid_file, "w", encoding="utf-8") as f:
            f.write(pid_info.to_json())

    def read(self) -> PIDInfo | None:
        return PIDInfo.from_path(self.pid_file)

    def delete(self) -> None:
        self.pid_file.unlink(missing_ok=True)

    def get_process(self) -> psutil.Process | None:
        """
        Gets the psutil.Process object if the process is alive and valid.
        Validates process identity by checking cmdline contains the expected script.
        """
        pid_info = self.read()
        if not pid_info:
            return None

        try:
            if not psutil.pid_exists(pid_info.pid):
                self.delete()
                return None

            proc = psutil.Process(pid_info.pid)

            # Validate: cmdline should contain our script name
            cmdline = " ".join(proc.cmdline()).lower()
            expected_script = pid_info.script.lower()
            if expected_script not in cmdline:
                self.delete()  # PID was reused by another process
                return None

            return proc

        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            self.delete()
            return None
