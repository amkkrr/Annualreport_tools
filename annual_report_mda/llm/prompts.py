"""
LLM Prompt 模板
"""
from __future__ import annotations


SYSTEM_PROMPT = """你是一个专业的中国上市公司年报分析助手。你的任务是帮助识别和提取年报中的"管理层讨论与分析"(MD&A) 章节。

你需要遵循以下原则:
1. 精确识别章节边界，不遗漏重要内容
2. 排除目录页、财务报表等无关内容
3. 输出严格的 JSON 格式，便于程序解析
"""


ANALYZE_TOC_PROMPT = """分析以下年报目录结构，识别"管理层讨论与分析"(MD&A) 章节的边界。

## 年报信息
- 股票代码: {stock_code}
- 年份: {year}
- 行业: {industry}

## 目录内容
{toc_content}

## 任务
1. 找出 MD&A 章节的起始标题（通常是"第X节 管理层讨论与分析"或"第X节 董事会报告"）
2. 找出下一章节的标题作为结束边界
3. 提取目录中标注的页码范围

## 输出格式 (JSON)
```json
{{
  "start_pattern": "章节标题正则表达式",
  "end_pattern": "下一章节标题正则表达式",
  "toc_start_page": 起始页码,
  "toc_end_page": 结束页码,
  "confidence": 0.0-1.0,
  "reasoning": "判断依据"
}}
```
"""


SELF_REFINE_PROMPT = """你之前的 MD&A 提取结果存在问题，请根据反馈进行改进。

## 当前提取结果
- 起始标题: {current_start}
- 结束标题: {current_end}
- 提取文本长度: {char_count} 字符
- 质量评分: {quality_score}/100

## 评估反馈
{evaluation_feedback}

## 问题诊断
{problem_diagnosis}

## 年报原文片段 (前后文)
{context_snippet}

## 任务
根据以上反馈，修正起始和结束标题模式。

## 输出格式 (JSON)
```json
{{
  "refined_start_pattern": "修正后的起始标题",
  "refined_end_pattern": "修正后的结束标题",
  "changes_made": "做了哪些修改",
  "expected_improvement": "预期改进效果"
}}
```
"""


EVALUATE_EXTRACTION_PROMPT = """评估以下 MD&A 提取结果的质量。

## 提取结果
- 股票代码: {stock_code}
- 年份: {year}
- 字符数: {char_count}
- 使用策略: {used_rule_type}

## 提取文本 (前 5000 字符)
{mda_text_preview}

## 评估维度
1. **完整性** (0-30分): MD&A 内容是否完整，有无明显遗漏
2. **准确性** (0-30分): 边界是否准确，有无包含无关内容（如财务报表、目录页）
3. **清洁度** (0-20分): 有无噪音（表格残留、页眉页脚、乱码）
4. **结构性** (0-20分): 是否保持原文结构，段落分隔是否合理

## 输出格式 (JSON)
```json
{{
  "scores": {{
    "completeness": N,
    "accuracy": N,
    "cleanliness": N,
    "structure": N
  }},
  "total_score": N,
  "issues": ["问题1", "问题2"],
  "suggestions": ["改进建议1", "改进建议2"],
  "pass": true/false
}}
```
"""


FEW_SHOT_TEMPLATE = """以下是相似年报的成功提取案例：

{examples}

请参考以上案例，为当前年报生成提取规则。
"""


def format_few_shot_examples(samples: list[dict]) -> str:
    """格式化 few-shot 示例"""
    if not samples:
        return ""

    lines = []
    for i, sample in enumerate(samples, 1):
        lines.append(f"### 案例 {i}: {sample.get('stock_code', 'N/A')} ({sample.get('year', 'N/A')})")
        lines.append(f"- 行业: {sample.get('industry', 'N/A')}")
        lines.append(f"- 起始标题: `{sample.get('start_pattern', 'N/A')}`")
        lines.append(f"- 结束标题: `{sample.get('end_pattern', 'N/A')}`")
        lines.append(f"- 提取字数: {sample.get('char_count', 'N/A')}")
        lines.append(f"- 质量评分: {sample.get('quality_score', 'N/A')}")
        lines.append("")

    return "\n".join(lines)
