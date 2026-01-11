from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

_PAGE_MARKER_RE = re.compile(r"^\s*(?:=+|-+)\s*Page\s*(\d+)\s*(?:=+|-+)\s*$", re.IGNORECASE)


@dataclass(frozen=True)
class TextLoadResult:
    pages: list[str]
    page_numbers: list[int | None]
    had_page_breaks: bool
    page_break_kind: str  # "form_feed" | "page_marker" | "none"
    encoding_used: str


def load_text_file(path: str | Path) -> tuple[str, str]:
    path = Path(path)

    raw = path.read_bytes()
    for encoding in ("utf-8", "utf-8-sig", "gb18030", "gbk", "latin-1"):
        try:
            return raw.decode(encoding), encoding
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="ignore"), "utf-8(ignore)"


def clean_text(text: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = text.replace("\u00a0", " ")
    return text


def split_pages(text: str) -> TextLoadResult:
    text = clean_text(text)

    if "\f" in text:
        parts = [p.strip("\n") for p in text.split("\f")]
        pages = [p for p in parts if p.strip()]
        if not pages:
            pages = [""]
        return TextLoadResult(
            pages=pages,
            page_numbers=[None for _ in pages],
            had_page_breaks=len(pages) > 1,
            page_break_kind="form_feed",
            encoding_used="",
        )

    lines = text.split("\n")
    pages: list[str] = []
    page_numbers: list[int | None] = []

    current: list[str] = []
    current_page_number: int | None = None
    saw_marker = False

    for line in lines:
        match = _PAGE_MARKER_RE.match(line)
        if match:
            if current or saw_marker:
                pages.append("\n".join(current).strip("\n"))
                page_numbers.append(current_page_number)
                current = []
                current_page_number = None
            current_page_number = int(match.group(1))
            saw_marker = True
            continue

        current.append(line)

    pages.append("\n".join(current).strip("\n"))
    page_numbers.append(current_page_number)

    if saw_marker:
        pages = [p for p in pages if p.strip() or len(pages) == 1]
        if len(page_numbers) != len(pages):
            # best-effort realignment if empty pages got removed
            page_numbers = page_numbers[: len(pages)]
        return TextLoadResult(
            pages=pages,
            page_numbers=page_numbers,
            had_page_breaks=len(pages) > 1,
            page_break_kind="page_marker",
            encoding_used="",
        )

    return TextLoadResult(
        pages=[text],
        page_numbers=[None],
        had_page_breaks=False,
        page_break_kind="none",
        encoding_used="",
    )


def load_pages(path: str | Path) -> TextLoadResult:
    text, encoding = load_text_file(path)
    result = split_pages(text)
    return TextLoadResult(
        pages=result.pages,
        page_numbers=result.page_numbers,
        had_page_breaks=result.had_page_breaks,
        page_break_kind=result.page_break_kind,
        encoding_used=encoding,
    )
