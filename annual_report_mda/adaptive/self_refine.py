"""
Self-Refine 循环 - 迭代改进提取质量
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from annual_report_mda.llm.client import LLMClient, LLMJSONParseError
from annual_report_mda.llm.prompts import (
    EVALUATE_EXTRACTION_PROMPT,
    SELF_REFINE_PROMPT,
    SYSTEM_PROMPT,
)

if TYPE_CHECKING:
    from annual_report_mda.strategies import ExtractionResult


_LOG = logging.getLogger(__name__)


@dataclass
class RefineResult:
    """Self-Refine 结果"""

    success: bool
    extraction: ExtractionResult | None
    iterations: int
    final_score: float
    history: list[dict] = field(default_factory=list)


class SelfRefineLoop:
    """Self-Refine 循环控制器"""

    def __init__(
        self,
        llm_client: LLMClient,
        max_iterations: int = 3,
        score_threshold: float = 70.0,
    ):
        self.llm = llm_client
        self.max_iterations = max_iterations
        self.score_threshold = score_threshold

    async def refine(
        self,
        pages: list[str],
        stock_code: str,
        year: int,
        *,
        initial_extraction: ExtractionResult | None = None,
        industry: str | None = None,
    ) -> RefineResult:
        """
        执行 Self-Refine 循环。

        流程:
        1. 初始提取（如果未提供）
        2. LLM 评估质量
        3. 如果评分 >= 阈值，成功返回
        4. 否则，LLM 生成改进建议
        5. 使用改进后的 pattern 重新提取
        6. 重复 2-5，最多 max_iterations 次
        """
        from annual_report_mda.strategies import extract_mda_iterative

        history = []
        current_extraction = initial_extraction
        current_start_pattern = None
        current_end_pattern = None

        loop = asyncio.get_event_loop()

        for iteration in range(self.max_iterations):
            # 1. 如果没有提取结果，执行初始提取
            if current_extraction is None:
                current_extraction = await loop.run_in_executor(
                    None,
                    lambda sp=current_start_pattern, ep=current_end_pattern: extract_mda_iterative(
                        pages,
                        custom_start_pattern=sp,
                        custom_end_pattern=ep,
                    ),
                )

            if current_extraction is None:
                _LOG.warning("提取失败，无法进行 refine")
                return RefineResult(
                    success=False,
                    extraction=None,
                    iterations=iteration + 1,
                    final_score=0.0,
                    history=history,
                )

            # 2. LLM 评估
            try:
                evaluation = await self._evaluate(current_extraction, stock_code, year)
            except LLMJSONParseError as e:
                _LOG.warning(f"评估 JSON 解析失败: {e}")
                evaluation = {"total_score": 0, "issues": ["评估失败"], "suggestions": []}

            score = evaluation.get("total_score", 0)

            history.append(
                {
                    "iteration": iteration + 1,
                    "score": score,
                    "issues": evaluation.get("issues", []),
                    "start_pattern": current_start_pattern,
                    "end_pattern": current_end_pattern,
                }
            )

            # 3. 检查是否通过
            if score >= self.score_threshold:
                _LOG.info(f"Refine 成功: iteration={iteration + 1}, score={score}")
                return RefineResult(
                    success=True,
                    extraction=current_extraction,
                    iterations=iteration + 1,
                    final_score=score,
                    history=history,
                )

            # 4. 生成改进建议
            if iteration < self.max_iterations - 1:
                try:
                    refinement = await self._get_refinement(
                        current_extraction,
                        evaluation,
                        pages,
                        stock_code,
                        year,
                    )
                    current_start_pattern = refinement.get("refined_start_pattern")
                    current_end_pattern = refinement.get("refined_end_pattern")
                    current_extraction = None  # 重置，下一轮重新提取

                except LLMJSONParseError as e:
                    _LOG.warning(f"Refine JSON 解析失败，终止迭代: {e}")
                    break

        # 达到最大迭代次数
        return RefineResult(
            success=False,
            extraction=current_extraction,
            iterations=self.max_iterations,
            final_score=history[-1]["score"] if history else 0.0,
            history=history,
        )

    async def _evaluate(
        self,
        extraction: ExtractionResult,
        stock_code: str,
        year: int,
    ) -> dict:
        """使用 LLM 评估提取质量"""
        prompt = EVALUATE_EXTRACTION_PROMPT.format(
            stock_code=stock_code,
            year=year,
            char_count=len(extraction.mda_raw),
            used_rule_type=extraction.used_rule_type,
            mda_text_preview=extraction.mda_raw[:5000],
        )

        return await self.llm.complete_with_json(
            prompt,
            system=SYSTEM_PROMPT,
            temperature=0.3,
        )

    async def _get_refinement(
        self,
        extraction: ExtractionResult,
        evaluation: dict,
        pages: list[str],
        stock_code: str,
        year: int,
    ) -> dict:
        """使用 LLM 生成改进建议"""
        context = self._get_context_snippet(pages, extraction)

        prompt = SELF_REFINE_PROMPT.format(
            current_start=extraction.hit_start or "未知",
            current_end=extraction.hit_end or "未知",
            char_count=len(extraction.mda_raw),
            quality_score=evaluation.get("total_score", 0),
            evaluation_feedback="\n".join(f"- {issue}" for issue in evaluation.get("issues", [])),
            problem_diagnosis="\n".join(f"- {s}" for s in evaluation.get("suggestions", [])),
            context_snippet=context,
        )

        return await self.llm.complete_with_json(
            prompt,
            system=SYSTEM_PROMPT,
            temperature=0.5,
        )

    def _get_context_snippet(
        self,
        pages: list[str],
        extraction: ExtractionResult,
    ) -> str:
        """获取提取边界附近的上下文"""
        start_idx = max(0, (extraction.page_index_start or 0) - 1)
        end_idx = min(len(pages), (extraction.page_index_end or len(pages)) + 1)

        snippet_parts = []

        # 开头部分
        for i in range(start_idx, min(start_idx + 2, len(pages))):
            snippet_parts.append(f"=== 第 {i + 1} 页 ===\n{pages[i][:1000]}")

        if end_idx - start_idx > 4:
            snippet_parts.append("... [中间内容省略] ...")

        # 结尾部分
        for i in range(max(start_idx + 2, end_idx - 2), end_idx):
            if i < len(pages):
                snippet_parts.append(f"=== 第 {i + 1} 页 ===\n{pages[i][-1000:]}")

        return "\n\n".join(snippet_parts)
