#!/usr/bin/env python3
"""
LLM 辅助标注脚本 - 使用 claude CLI 识别 MD&A 边界

用法:
    python scripts/llm_annotate.py --input samples.json --output data/golden_set_draft.json
"""
import json
import subprocess
import sys
from pathlib import Path
from typing import Optional
import argparse
from datetime import datetime


class LLMCallError(Exception):
    """LLM 调用失败异常"""
    pass


def annotate_sample(
    txt_path: str,
    stock_code: str,
    year: int,
    model: str = "claude-sonnet",
    max_chars: int = 80000,
    timeout: int = 120
) -> dict:
    """
    使用 LLM 识别 MD&A 边界。

    Args:
        txt_path: 年报 TXT 文件路径
        stock_code: 股票代码
        year: 年份
        model: LLM 模型标识
        max_chars: 最大字符数（避免超出 token 限制）
        timeout: LLM 调用超时时间（秒）

    Returns:
        {
            "start_marker": str,
            "end_marker": str,
            "start_char_offset": int,
            "end_char_offset": int,
            "confidence": float,
            "reasoning": str
        }

    Raises:
        FileNotFoundError: TXT 文件不存在
        LLMCallError: LLM 调用失败
    """
    txt_path_obj = Path(txt_path)

    # 1. 边界检查
    if not txt_path_obj.exists():
        raise FileNotFoundError(f"TXT 文件不存在: {txt_path}")

    # 2. 读取文件
    content = txt_path_obj.read_text(encoding="utf-8")

    # 3. 截取关键部分（避免超出 token 限制）
    truncated = content[:max_chars]

    # 4. 构建 Prompt
    prompt = f"""分析以下年报文本，识别"管理层讨论与分析"(MD&A) 章节的边界。

## 年报信息
- 股票代码: {stock_code}
- 年份: {year}

## 年报内容 (前 {max_chars} 字符)
{truncated}

## 任务
找出 MD&A 章节的:
1. 起始标记（章节标题）
2. 结束标记（下一章节标题）
3. 起始字符位置（0-indexed，在完整文本中的位置）
4. 结束字符位置

## 输出格式 (JSON)
请严格按照以下 JSON 格式输出，不要包含其他文字：
{{"start_marker": "...", "end_marker": "...", "start_char_offset": N, "end_char_offset": M, "confidence": 0.0-1.0, "reasoning": "..."}}
"""

    # 5. 调用 claude CLI（通过标准输入传递 prompt）
    try:
        result = subprocess.run(
            ["claude"],
            input=prompt,
            capture_output=True,
            text=True,
            timeout=timeout
        )
    except subprocess.TimeoutExpired:
        raise LLMCallError(f"LLM 调用超时 ({timeout}s)")
    except FileNotFoundError:
        raise LLMCallError("claude CLI 未找到，请确认已安装并在 PATH 中")

    if result.returncode != 0:
        raise LLMCallError(f"claude CLI 执行失败: {result.stderr}")

    # 6. 解析返回
    try:
        # 提取 JSON（可能被包裹在 markdown 代码块中）
        output = result.stdout.strip()

        # 尝试提取 JSON 代码块
        if "```json" in output:
            start_idx = output.find("```json") + 7
            end_idx = output.find("```", start_idx)
            output = output[start_idx:end_idx].strip()
        elif "```" in output:
            start_idx = output.find("```") + 3
            end_idx = output.find("```", start_idx)
            output = output[start_idx:end_idx].strip()

        return json.loads(output)
    except json.JSONDecodeError as e:
        raise LLMCallError(f"LLM 返回的 JSON 无法解析: {e}\n输出内容:\n{result.stdout}")


def batch_annotate(
    sample_list: list[dict],
    output_path: str = "data/golden_set_draft.json",
    concurrency: int = 1,
    model: str = "claude-sonnet"
) -> None:
    """
    批量标注样本列表。

    Args:
        sample_list: [{"txt_path": str, "stock_code": str, "year": int}, ...]
        output_path: 输出 draft JSON 路径
        concurrency: 并发数 (默认 1，避免 API 限流)
        model: LLM 模型标识
    """
    output_path_obj = Path(output_path)
    output_path_obj.parent.mkdir(parents=True, exist_ok=True)

    results = {
        "version": "1.0",
        "created_at": datetime.now().isoformat(),
        "model": model,
        "samples": []
    }

    total = len(sample_list)

    for idx, sample in enumerate(sample_list, 1):
        txt_path = sample["txt_path"]
        stock_code = sample["stock_code"]
        year = sample["year"]

        print(f"[{idx}/{total}] 标注 {stock_code} ({year})...")

        try:
            annotation = annotate_sample(txt_path, stock_code, year, model)

            # 构建样本记录
            sample_record = {
                "id": f"GS-{idx:03d}",
                "stock_code": stock_code,
                "company_name": sample.get("company_name", ""),
                "year": year,
                "source_txt_path": txt_path,
                "golden_boundary": {
                    "start_marker": annotation["start_marker"],
                    "end_marker": annotation["end_marker"],
                    "start_char_offset": annotation["start_char_offset"],
                    "end_char_offset": annotation["end_char_offset"],
                    "char_count": annotation["end_char_offset"] - annotation["start_char_offset"]
                },
                "annotation": {
                    "method": "llm_assisted",
                    "llm_model": model,
                    "confidence": annotation.get("confidence", 0.0),
                    "reasoning": annotation.get("reasoning", ""),
                    "human_verified": False,
                    "annotated_at": datetime.now().isoformat()
                }
            }

            results["samples"].append(sample_record)
            print(f"  ✓ 成功 (confidence: {annotation.get('confidence', 0.0):.2f})")

        except (FileNotFoundError, LLMCallError) as e:
            print(f"  ✗ 失败: {e}")
            # 记录失败但继续
            results["samples"].append({
                "id": f"GS-{idx:03d}",
                "stock_code": stock_code,
                "year": year,
                "source_txt_path": txt_path,
                "error": str(e)
            })

    # 保存结果
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    success_count = sum(1 for s in results["samples"] if "error" not in s)
    print(f"\n完成: {success_count}/{total} 成功")
    print(f"输出: {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="使用 LLM 辅助标注 MD&A 边界",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
    # 单个样本
    python scripts/llm_annotate.py \\
        --txt outputs/annual_reports/txt/601012_2013_2012年年度报告.txt \\
        --stock-code 601012 \\
        --year 2012

    # 批量标注
    python scripts/llm_annotate.py \\
        --input samples.json \\
        --output data/golden_set_draft.json
        """
    )

    # 单个样本模式
    parser.add_argument("--txt", type=str, help="单个 TXT 文件路径")
    parser.add_argument("--stock-code", type=str, help="股票代码")
    parser.add_argument("--year", type=int, help="年份")

    # 批量模式
    parser.add_argument("--input", type=str, help="批量样本列表 JSON 文件")
    parser.add_argument("--output", type=str, default="data/golden_set_draft.json",
                        help="输出路径 (默认: data/golden_set_draft.json)")

    # 通用参数
    parser.add_argument("--model", type=str, default="claude-sonnet",
                        help="LLM 模型标识 (默认: claude-sonnet)")
    parser.add_argument("--concurrency", type=int, default=1,
                        help="批量模式并发数 (默认: 1)")

    args = parser.parse_args()

    # 单个样本模式
    if args.txt and args.stock_code and args.year:
        try:
            result = annotate_sample(args.txt, args.stock_code, args.year, args.model)
            print(json.dumps(result, ensure_ascii=False, indent=2))
        except (FileNotFoundError, LLMCallError) as e:
            print(f"错误: {e}", file=sys.stderr)
            sys.exit(1)

    # 批量模式
    elif args.input:
        input_path = Path(args.input)
        if not input_path.exists():
            print(f"错误: 输入文件不存在: {args.input}", file=sys.stderr)
            sys.exit(1)

        with open(input_path, encoding="utf-8") as f:
            sample_list = json.load(f)

        batch_annotate(sample_list, args.output, args.concurrency, args.model)

    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
