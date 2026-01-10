from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Optional, Sequence

from .scorer import MDA_PATTERNS, MDA_TITLES, NEXT_TITLES, ScoreDetail, calculate_mda_score


MAX_PAGES_DEFAULT = 15
MAX_CHARS_DEFAULT = 120_000


_TOC_DOTLINE_RE = re.compile(r"[.…\.·]{2,}\s*\d+\s*$")
_HEADING_MAX_LEN = 60
# 引用词：如果行包含这些词，则不是真正的章节标题
_REFERENCE_WORDS = ("参见", "详见", "见本", "请参阅", "详情请见", "参考", "参阅")
# 章节级标题模式：匹配 "第X节" 格式，应优先于普通标题
_SECTION_LEVEL_RE = re.compile(r"^第[一二三四五六七八九十百零\d]+[章节部分]")


@dataclass(frozen=True)
class TocHit:
    printed_page_start: int
    printed_page_end: Optional[int]
    page_index_start: int
    page_index_end: Optional[int]


@dataclass(frozen=True)
class ExtractionResult:
    mda_raw: str
    score: float
    score_detail: ScoreDetail

    page_index_start: int
    page_index_end: int
    page_count: int

    printed_page_start: Optional[int]
    printed_page_end: Optional[int]

    hit_start: str
    hit_end: Optional[str]

    is_truncated: bool
    truncation_reason: Optional[str]

    used_rule_type: str  # "generic" | "custom" | "toc"

    quality_flags: list[str]
    quality_detail: dict[str, Any]


def _is_toc_page(text: str) -> bool:
    if not text:
        return False

    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    dot_lines = sum(1 for ln in lines if _TOC_DOTLINE_RE.search(ln))
    has_toc_keyword = ("目 录" in text) or ("目录" in text)

    if has_toc_keyword and dot_lines >= 2:
        return True

    if len(lines) < 8:
        return False

    return dot_lines >= 5


def _looks_like_heading(line: str) -> bool:
    s = line.strip()
    if not s:
        return False
    if len(s) > _HEADING_MAX_LEN:
        return False
    if _TOC_DOTLINE_RE.search(s):
        return False
    if re.fullmatch(r"\d+", s):
        return False
    # 排除包含引用词的行（这些是对章节的引用，不是章节标题本身）
    if any(ref in s for ref in _REFERENCE_WORDS):
        return False
    return True


def _compile_title_regex(titles: Sequence[str]) -> re.Pattern[str]:
    escaped = [re.escape(t) for t in titles if t]
    if not escaped:
        return re.compile(r"^$")  # never match
    return re.compile("|".join(escaped))


def _find_heading_hits(
    pages: Sequence[str],
    *,
    title_regex: re.Pattern[str],
    pattern_regexes: Sequence[re.Pattern[str]],
) -> list[tuple[int, int, str]]:
    hits: list[tuple[int, int, str]] = []
    skip_toc_pages = len(pages) > 1

    for page_index, page_text in enumerate(pages):
        if skip_toc_pages and _is_toc_page(page_text):
            continue
        for line_index, line in enumerate(page_text.splitlines()):
            if not _looks_like_heading(line):
                continue
            if any(r.search(line) for r in pattern_regexes) or title_regex.search(line):
                hits.append((page_index, line_index, line.strip()))
    return hits


def _find_end_hits(
    pages: Sequence[str],
    *,
    start_page_index: int,
    start_line_index: int,
    end_titles: Sequence[str],
    max_page_index_exclusive: int,
) -> list[tuple[int, int, str]]:
    end_title_re = _compile_title_regex(end_titles)
    hits: list[tuple[int, int, str]] = []

    for page_index in range(start_page_index, min(len(pages), max_page_index_exclusive)):
        lines = pages[page_index].splitlines()
        begin = start_line_index + 1 if page_index == start_page_index else 0
        for line_index in range(begin, len(lines)):
            line = lines[line_index]
            if not _looks_like_heading(line):
                continue
            if end_title_re.search(line):
                # 额外检查：结束标记应该在行首或是章节级标题格式
                stripped = line.strip()
                # 如果是 "第X节" 格式，接受
                if _SECTION_LEVEL_RE.match(stripped):
                    hits.append((page_index, line_index, stripped))
                    continue
                # 检查结束标记是否在行首
                for title in end_titles:
                    if stripped.startswith(title):
                        hits.append((page_index, line_index, stripped))
                        break
    return hits


def _extract_between(
    pages: Sequence[str],
    *,
    start_page_index: int,
    start_line_index: int,
    end_page_index: Optional[int],
    end_line_index: Optional[int],
) -> tuple[str, int, int]:
    if end_page_index is None:
        end_page_index = len(pages) - 1
        end_line_index = None

    selected_pages: list[str] = []

    for page_index in range(start_page_index, end_page_index + 1):
        lines = pages[page_index].splitlines()

        if page_index == start_page_index:
            lines = lines[start_line_index:]

        if page_index == end_page_index and end_line_index is not None:
            lines = lines[:end_line_index]

        selected_pages.append("\n".join(lines).strip("\n"))

    text = "\n".join(p for p in selected_pages if p.strip())
    page_index_start = start_page_index
    page_index_end = end_page_index + 1  # 开区间 end
    return text, page_index_start, page_index_end


def _apply_limits(
    pages: Sequence[str],
    *,
    start_page_index: int,
    start_line_index: int,
    max_pages: int,
    max_chars: int,
    end_page_index: Optional[int],
    end_line_index: Optional[int],
) -> tuple[str, int, int, bool, Optional[str]]:
    is_truncated = False
    truncation_reason: Optional[str] = None

    page_limit_end_exclusive = min(len(pages), start_page_index + max_pages)

    limited_end_page_index = end_page_index
    limited_end_line_index = end_line_index

    if limited_end_page_index is None:
        if len(pages) > page_limit_end_exclusive:
            limited_end_page_index = page_limit_end_exclusive - 1
            limited_end_line_index = None
            is_truncated = True
            truncation_reason = "max_pages"
        else:
            limited_end_page_index = len(pages) - 1
            limited_end_line_index = None
            is_truncated = True
            truncation_reason = "end_not_found"
    else:
        if limited_end_page_index + 1 > page_limit_end_exclusive:
            limited_end_page_index = page_limit_end_exclusive - 1
            limited_end_line_index = None
            is_truncated = True
            truncation_reason = "max_pages"

    text, page_index_start, page_index_end = _extract_between(
        pages,
        start_page_index=start_page_index,
        start_line_index=start_line_index,
        end_page_index=limited_end_page_index,
        end_line_index=limited_end_line_index,
    )

    if len(text) > max_chars:
        text = text[:max_chars]
        if not is_truncated:
            is_truncated = True
            truncation_reason = "max_chars"

    return text, page_index_start, page_index_end, is_truncated, truncation_reason


def _compute_quality(text: str, *, page_break_kind: str) -> tuple[list[str], dict[str, Any]]:
    flags: list[str] = []
    detail: dict[str, Any] = {"page_break_kind": page_break_kind, "char_count": len(text)}

    if page_break_kind == "none":
        flags.append("FLAG_PAGE_BOUNDARY_MISSING")

    if len(text) < 1000 or len(text) > 50_000:
        flags.append("FLAG_LENGTH_ABNORMAL")

    anchor_words = ["收入", "利润", "同比"]
    anchor_hit = sum(1 for w in anchor_words if w in text)
    detail["anchor_hit_count"] = anchor_hit
    if anchor_hit < 2:
        flags.append("FLAG_CONTENT_MISMATCH")

    tail = text[-500:] if len(text) > 500 else text
    if any(x in tail for x in ["监事会", "审计报告"]):
        flags.append("FLAG_TAIL_OVERLAP")

    return flags, detail


def _try_extract_with_custom_rule(
    pages: Sequence[str],
    *,
    start_pattern: str,
    end_pattern: Optional[str],
    max_pages: int,
    max_chars: int,
    page_break_kind: str,
) -> Optional[ExtractionResult]:
    start_re = re.compile(re.escape(start_pattern))
    end_re = re.compile(re.escape(end_pattern)) if end_pattern else None

    for page_index, page_text in enumerate(pages):
        lines = page_text.splitlines()
        for line_index, line in enumerate(lines):
            if not _looks_like_heading(line):
                continue
            if not start_re.search(line):
                continue

            end_page_index: Optional[int] = None
            end_line_index: Optional[int] = None
            hit_end: Optional[str] = None

            if end_re is not None:
                for p in range(page_index, min(len(pages), page_index + max_pages)):
                    p_lines = pages[p].splitlines()
                    begin = line_index + 1 if p == page_index else 0
                    for li in range(begin, len(p_lines)):
                        cand = p_lines[li]
                        if not _looks_like_heading(cand):
                            continue
                        if end_re.search(cand):
                            end_page_index = p
                            end_line_index = li
                            hit_end = cand.strip()
                            break
                    if end_page_index is not None:
                        break

            text, page_index_start, page_index_end, is_truncated, truncation_reason = _apply_limits(
                pages,
                start_page_index=page_index,
                start_line_index=line_index,
                max_pages=max_pages,
                max_chars=max_chars,
                end_page_index=end_page_index,
                end_line_index=end_line_index,
            )
            score, score_detail = calculate_mda_score(text)
            flags, detail = _compute_quality(text, page_break_kind=page_break_kind)

            return ExtractionResult(
                mda_raw=text,
                score=score,
                score_detail=score_detail,
                page_index_start=page_index_start,
                page_index_end=page_index_end,
                page_count=page_index_end - page_index_start,
                printed_page_start=None,
                printed_page_end=None,
                hit_start=line.strip(),
                hit_end=hit_end,
                is_truncated=is_truncated,
                truncation_reason=truncation_reason,
                used_rule_type="custom",
                quality_flags=flags,
                quality_detail=detail,
            )

    return None


def parse_toc_for_page_range(
    pages: Sequence[str],
    *,
    page_numbers: Optional[Sequence[Optional[int]]] = None,
    scan_pages: int = 15,
) -> Optional[TocHit]:
    if not pages:
        return None

    title_re = _compile_title_regex(MDA_TITLES)
    next_re = _compile_title_regex(NEXT_TITLES)

    toc_pages = []
    for i in range(min(len(pages), scan_pages)):
        if _is_toc_page(pages[i]):
            toc_pages.append(pages[i])

    if not toc_pages:
        return None

    toc_text = "\n".join(toc_pages)
    lines = [ln.strip() for ln in toc_text.splitlines() if ln.strip()]

    printed_start: Optional[int] = None
    printed_end: Optional[int] = None

    for ln in lines:
        if not _TOC_DOTLINE_RE.search(ln):
            continue
        if printed_start is None and title_re.search(ln):
            m = re.search(r"(\d+)\s*$", ln)
            if m:
                printed_start = int(m.group(1))
                continue
        if printed_start is not None and printed_end is None and next_re.search(ln):
            m = re.search(r"(\d+)\s*$", ln)
            if m:
                printed_end = int(m.group(1))
                break

    if printed_start is None:
        return None

    if not page_numbers:
        return None

    def _map_printed_to_index(pn: int) -> Optional[int]:
        for idx, printed in enumerate(page_numbers):
            if printed == pn:
                return idx
        return None

    page_index_start = _map_printed_to_index(printed_start)
    if page_index_start is None:
        return None

    page_index_end = _map_printed_to_index(printed_end) if printed_end is not None else None

    return TocHit(
        printed_page_start=printed_start,
        printed_page_end=printed_end,
        page_index_start=page_index_start,
        page_index_end=page_index_end,
    )


def extract_mda_from_pages(
    pages: Sequence[str],
    *,
    max_pages: int = MAX_PAGES_DEFAULT,
    max_chars: int = MAX_CHARS_DEFAULT,
    page_break_kind: str = "none",
) -> Optional[ExtractionResult]:
    if not pages:
        return None

    pattern_regexes = [re.compile(p) for p in MDA_PATTERNS]
    title_re = _compile_title_regex(MDA_TITLES)

    start_hits = _find_heading_hits(pages, title_regex=title_re, pattern_regexes=pattern_regexes)
    if not start_hits:
        return None

    best_start = None
    best_anchor_score = -1.0

    for page_index, line_index, line_text in start_hits:
        snippet_text, _, _, _, _ = _apply_limits(
            pages,
            start_page_index=page_index,
            start_line_index=line_index,
            max_pages=2,
            max_chars=2000,
            end_page_index=None,
            end_line_index=None,
        )
        score, _ = calculate_mda_score(snippet_text)
        # 给章节级标题（如"第四节"）加权，优先选择章节级别的标题
        if _SECTION_LEVEL_RE.match(line_text.strip()):
            score += 0.5
        if score > best_anchor_score:
            best_anchor_score = score
            best_start = (page_index, line_index, line_text)

    if best_start is None:
        return None

    start_page_index, start_line_index, hit_start = best_start

    max_page_index_exclusive = min(len(pages), start_page_index + max_pages)

    end_hits = _find_end_hits(
        pages,
        start_page_index=start_page_index,
        start_line_index=start_line_index,
        end_titles=NEXT_TITLES,
        max_page_index_exclusive=max_page_index_exclusive,
    )

    end_page_index: Optional[int] = None
    end_line_index: Optional[int] = None
    hit_end: Optional[str] = None

    if end_hits:
        end_page_index, end_line_index, hit_end = end_hits[0]

    text, page_index_start, page_index_end, is_truncated, truncation_reason = _apply_limits(
        pages,
        start_page_index=start_page_index,
        start_line_index=start_line_index,
        max_pages=max_pages,
        max_chars=max_chars,
        end_page_index=end_page_index,
        end_line_index=end_line_index,
    )

    score, score_detail = calculate_mda_score(text)
    flags, detail = _compute_quality(text, page_break_kind=page_break_kind)

    return ExtractionResult(
        mda_raw=text,
        score=score,
        score_detail=score_detail,
        page_index_start=page_index_start,
        page_index_end=page_index_end,
        page_count=page_index_end - page_index_start,
        printed_page_start=None,
        printed_page_end=None,
        hit_start=hit_start,
        hit_end=hit_end,
        is_truncated=is_truncated,
        truncation_reason=truncation_reason,
        used_rule_type="generic",
        quality_flags=flags,
        quality_detail=detail,
    )


def extract_mda_iterative(
    pages: Sequence[str],
    *,
    page_numbers: Optional[Sequence[Optional[int]]] = None,
    page_break_kind: str = "none",
    max_pages: int = MAX_PAGES_DEFAULT,
    max_chars: int = MAX_CHARS_DEFAULT,
    custom_start_pattern: Optional[str] = None,
    custom_end_pattern: Optional[str] = None,
) -> Optional[ExtractionResult]:
    candidates: list[ExtractionResult] = []

    if custom_start_pattern:
        custom = _try_extract_with_custom_rule(
            pages,
            start_pattern=custom_start_pattern,
            end_pattern=custom_end_pattern,
            max_pages=max_pages,
            max_chars=max_chars,
            page_break_kind=page_break_kind,
        )
        if custom is not None:
            candidates.append(custom)

    toc = parse_toc_for_page_range(pages, page_numbers=page_numbers)
    if toc is not None and toc.page_index_end is not None:
        text, page_index_start, page_index_end = _extract_between(
            pages,
            start_page_index=toc.page_index_start,
            start_line_index=0,
            end_page_index=toc.page_index_end - 1,
            end_line_index=None,
        )
        limited_text = text[:max_chars]
        score, score_detail = calculate_mda_score(limited_text)
        flags, detail = _compute_quality(limited_text, page_break_kind=page_break_kind)
        candidates.append(
            ExtractionResult(
                mda_raw=limited_text,
                score=score,
                score_detail=score_detail,
                page_index_start=page_index_start,
                page_index_end=page_index_end,
                page_count=page_index_end - page_index_start,
                printed_page_start=toc.printed_page_start,
                printed_page_end=toc.printed_page_end,
                hit_start="toc",
                hit_end=None,
                is_truncated=len(text) > max_chars,
                truncation_reason="max_chars" if len(text) > max_chars else None,
                used_rule_type="toc",
                quality_flags=flags,
                quality_detail=detail,
            )
        )

    body = extract_mda_from_pages(
        pages,
        max_pages=max_pages,
        max_chars=max_chars,
        page_break_kind=page_break_kind,
    )
    if body is not None:
        candidates.append(body)

    if not candidates:
        return None

    # 交叉校验 TOC 和正文结果
    toc_result = next((c for c in candidates if c.used_rule_type == "toc"), None)
    body_result = next((c for c in candidates if c.used_rule_type == "generic"), None)

    extra_flags: list[str] = []
    if toc_result is not None and body_result is not None:
        # 比较页码范围差异
        toc_start = toc_result.page_index_start
        body_start = body_result.page_index_start
        page_diff = abs(toc_start - body_start)
        if page_diff > 2:
            extra_flags.append("FLAG_TOC_MISMATCH")

    candidates.sort(key=lambda c: (c.score, len(c.mda_raw)), reverse=True)
    best = candidates[0]
    if best.score <= 0.4:
        return None

    # 如果有额外的质量标记，创建新的结果对象
    if extra_flags:
        merged_flags = list(best.quality_flags) + extra_flags
        merged_detail = dict(best.quality_detail)
        if toc_result and body_result:
            merged_detail["toc_body_page_diff"] = abs(
                toc_result.page_index_start - body_result.page_index_start
            )
        best = ExtractionResult(
            mda_raw=best.mda_raw,
            score=best.score,
            score_detail=best.score_detail,
            page_index_start=best.page_index_start,
            page_index_end=best.page_index_end,
            page_count=best.page_count,
            printed_page_start=best.printed_page_start,
            printed_page_end=best.printed_page_end,
            hit_start=best.hit_start,
            hit_end=best.hit_end,
            is_truncated=best.is_truncated,
            truncation_reason=best.truncation_reason,
            used_rule_type=best.used_rule_type,
            quality_flags=merged_flags,
            quality_detail=merged_detail,
        )

    return best