#!/usr/bin/env python3
"""
评估脚本 - 评估 MD&A 提取质量

用法:
    python scripts/evaluate_extraction.py --golden data/golden_set.json --source duckdb
"""
import json
import subprocess
import sys
from pathlib import Path
from typing import Optional
import argparse
from datetime import datetime
import duckdb


class LLMCallError(Exception):
    """LLM 调用失败异常"""
    pass


def call_llm_judge(
    golden_sample: dict,
    extracted_text: str,
    model: str = "claude-sonnet",
    timeout: int = 120
) -> dict:
    """
    调用 LLM 评估提取质量。

    Args:
        golden_sample: 黄金集样本（含 source_txt_path）
        extracted_text: 提取器输出的 MD&A 文本
        model: LLM 模型标识
        timeout: 超时时间（秒）

    Returns:
        {
            "total_score": int,  # 0-100
            "completeness": {"score": int, "reason": str},
            "accuracy": {"score": int, "reason": str},
            "boundary": {"score": int, "reason": str},
            "noise_control": {"score": int, "reason": str},
            "suggestions": list[str]
        }

    Raises:
        LLMCallError: LLM 调用失败
    """
    # 读取原文（用于对比）
    source_path = Path(golden_sample["source_txt_path"])
    if not source_path.exists():
        raise FileNotFoundError(f"原文文件不存在: {source_path}")

    original_text = source_path.read_text(encoding="utf-8")

    # 构建评估 Prompt
    prompt = f"""你是 MD&A 提取质量评估专家。

## 任务
评估提取的 MD&A 文本质量。

## 输入信息
**原文路径**: {source_path}
**股票代码**: {golden_sample.get('stock_code', 'N/A')}
**年份**: {golden_sample.get('year', 'N/A')}

**原文片段** (前 5000 字符):
{original_text[:5000]}

**提取的 MD&A 文本** (前 5000 字符):
{extracted_text[:5000]}

## 评估维度（各 25 分，满分 100）
1. **完整性 (Completeness)**: MD&A 核心内容是否完整？是否遗漏重要章节？
2. **准确性 (Accuracy)**: 是否包含非 MD&A 内容（如财务报表/其他章节）？
3. **边界准确 (Boundary)**: 起止位置是否正确？
4. **噪音控制 (Noise Control)**: 页眉页脚/表格碎片是否去除？

## 输出格式 (JSON)
请严格按照以下 JSON 格式输出，不要包含其他文字：
{{
  "total_score": 85,
  "completeness": {{"score": 22, "reason": "..."}},
  "accuracy": {{"score": 25, "reason": "..."}},
  "boundary": {{"score": 18, "reason": "..."}},
  "noise_control": {{"score": 20, "reason": "..."}},
  "suggestions": ["...", "..."]
}}
"""

    # 调用 claude CLI（通过标准输入传递 prompt）
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

    # 解析返回
    try:
        output = result.stdout.strip()

        # 提取 JSON
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


def evaluate_single(
    golden_sample: dict,
    extracted_result: dict,
    use_llm_judge: bool = True,
    model: str = "claude-sonnet"
) -> dict:
    """
    评估单个样本的提取质量。

    Args:
        golden_sample: 黄金集中的样本定义
        extracted_result: 提取器输出的结果（包含 start_char_offset, end_char_offset, text）
        use_llm_judge: 是否使用 LLM 评估（否则仅用规则评估）
        model: LLM 模型标识

    Returns:
        {
            "sample_id": str,
            "llm_evaluation": {...} | None,
            "rule_evaluation": {
                "boundary_match": bool,
                "char_overlap_ratio": float
            },
            "final_score": float
        }
    """
    sample_id = golden_sample["id"]

    # 1. 规则评估（边界匹配）
    golden_start = golden_sample["golden_boundary"]["start_char_offset"]
    golden_end = golden_sample["golden_boundary"]["end_char_offset"]

    extracted_start = extracted_result.get("start_char_offset", 0)
    extracted_end = extracted_result.get("end_char_offset", 0)

    start_diff = abs(extracted_start - golden_start)
    end_diff = abs(extracted_end - golden_end)

    # 容差: 200 字符
    TOLERANCE = 200
    boundary_match = start_diff <= TOLERANCE and end_diff <= TOLERANCE

    # 计算重叠率
    overlap_start = max(golden_start, extracted_start)
    overlap_end = min(golden_end, extracted_end)
    overlap_len = max(0, overlap_end - overlap_start)
    golden_len = golden_end - golden_start
    overlap_ratio = overlap_len / golden_len if golden_len > 0 else 0

    rule_eval = {
        "boundary_match": boundary_match,
        "char_overlap_ratio": overlap_ratio,
        "start_offset_diff": extracted_start - golden_start,
        "end_offset_diff": extracted_end - golden_end
    }

    # 2. LLM 评估（可选）
    llm_eval = None
    if use_llm_judge:
        try:
            extracted_text = extracted_result.get("text", "")
            llm_eval = call_llm_judge(golden_sample, extracted_text, model)
        except (FileNotFoundError, LLMCallError) as e:
            print(f"  警告: LLM 评估失败 ({e})，仅使用规则评估")

    # 3. 综合评分
    if llm_eval:
        final_score = llm_eval["total_score"]
    else:
        # 规则评分: 重叠率 * 100
        final_score = overlap_ratio * 100

    return {
        "sample_id": sample_id,
        "extracted_boundary": {
            "start_char_offset": extracted_start,
            "end_char_offset": extracted_end
        },
        "llm_evaluation": llm_eval,
        "rule_evaluation": rule_eval,
        "boundary_match": {
            "start_offset_diff": rule_eval["start_offset_diff"],
            "end_offset_diff": rule_eval["end_offset_diff"],
            "is_acceptable": boundary_match
        },
        "final_score": final_score
    }


def run_evaluation(
    golden_set_path: str = "data/golden_set.json",
    extraction_source: str = "duckdb",
    output_path: str = "data/evaluation_results.json",
    use_llm_judge: bool = True,
    model: str = "claude-sonnet"
) -> dict:
    """
    运行完整评估流程。

    Args:
        golden_set_path: 黄金数据集路径
        extraction_source: 提取结果来源 ("duckdb" | "json")
        output_path: 评估结果输出路径
        use_llm_judge: 是否使用 LLM 评估
        model: LLM 模型标识

    Returns:
        汇总统计 {precision, recall, f1, avg_score}
    """
    # 1. 加载黄金数据集
    golden_path = Path(golden_set_path)
    if not golden_path.exists():
        raise FileNotFoundError(f"黄金数据集不存在: {golden_set_path}")

    with open(golden_path, encoding="utf-8") as f:
        golden_set = json.load(f)

    # 2. 加载提取结果
    if extraction_source == "duckdb":
        # 从 DuckDB 加载
        db_path = "data/annual_reports.duckdb"
        conn = duckdb.connect(db_path, read_only=True)

        # 查询所有提取结果
        query = """
        SELECT
            stock_code,
            year,
            mda_raw as text,
            page_index_start,
            page_index_end,
            printed_page_start,
            printed_page_end,
            source_path
        FROM mda_text
        """
        extraction_results = conn.execute(query).fetchall()
        conn.close()

        # 转换为字典（以 stock_code-year 为 key）
        extracted_dict = {}
        for row in extraction_results:
            key = f"{row[0]}-{row[1]}"
            # 注意：DuckDB 结果没有 char_offset，需要从原文计算
            extracted_dict[key] = {
                "stock_code": row[0],
                "year": row[1],
                "text": row[2],
                "start_char_offset": 0,  # 需要实际计算
                "end_char_offset": len(row[2]) if row[2] else 0
            }

    else:
        # 从 JSON 文件加载
        with open(extraction_source, encoding="utf-8") as f:
            extracted_list = json.load(f)

        extracted_dict = {
            f"{e['stock_code']}-{e['year']}": e
            for e in extracted_list
        }

    # 3. 逐样本评估
    results = {
        "evaluation_id": f"EVAL-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
        "evaluated_at": datetime.now().isoformat(),
        "extractor_version": "0.3.0",  # 可从配置读取
        "use_llm_judge": use_llm_judge,
        "model": model if use_llm_judge else None,
        "results": []
    }

    total = len(golden_set["samples"])
    scores = []

    for idx, golden_sample in enumerate(golden_set["samples"], 1):
        if "error" in golden_sample:
            # 跳过标注失败的样本
            continue

        sample_id = golden_sample["id"]
        stock_code = golden_sample["stock_code"]
        year = golden_sample["year"]
        key = f"{stock_code}-{year}"

        print(f"[{idx}/{total}] 评估 {sample_id} ({stock_code}-{year})...")

        # 查找提取结果
        if key not in extracted_dict:
            print(f"  ✗ 未找到提取结果")
            results["results"].append({
                "sample_id": sample_id,
                "error": "未找到提取结果"
            })
            continue

        extracted_result = extracted_dict[key]

        try:
            eval_result = evaluate_single(
                golden_sample,
                extracted_result,
                use_llm_judge,
                model
            )
            results["results"].append(eval_result)
            scores.append(eval_result["final_score"])
            print(f"  ✓ 评分: {eval_result['final_score']:.1f}")

        except Exception as e:
            print(f"  ✗ 评估失败: {e}")
            results["results"].append({
                "sample_id": sample_id,
                "error": str(e)
            })

    # 4. 计算汇总统计
    if scores:
        avg_score = sum(scores) / len(scores)
        # 简化的 Precision/Recall 计算（基于边界匹配）
        acceptable_count = sum(
            1 for r in results["results"]
            if "boundary_match" in r and r["boundary_match"]["is_acceptable"]
        )
        precision = recall = acceptable_count / len(scores)
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0
    else:
        avg_score = precision = recall = f1 = 0

    results["summary"] = {
        "total_samples": total,
        "evaluated_samples": len(scores),
        "avg_score": round(avg_score, 2),
        "precision": round(precision, 3),
        "recall": round(recall, 3),
        "f1": round(f1, 3)
    }

    # 5. 保存结果
    output_path_obj = Path(output_path)
    output_path_obj.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path_obj, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"\n评估完成:")
    print(f"  总样本: {total}")
    print(f"  成功评估: {len(scores)}")
    print(f"  平均分: {avg_score:.2f}")
    print(f"  Precision: {precision:.3f}")
    print(f"  Recall: {recall:.3f}")
    print(f"  F1: {f1:.3f}")
    print(f"\n结果已保存至: {output_path}")

    return results["summary"]


def print_summary(result_path: str):
    """打印评估摘要"""
    with open(result_path, encoding="utf-8") as f:
        data = json.load(f)

    summary = data.get("summary", {})
    print("=== 评估摘要 ===")
    print(f"评估ID: {data.get('evaluation_id')}")
    print(f"评估时间: {data.get('evaluated_at')}")
    print(f"总样本: {summary.get('total_samples', 0)}")
    print(f"成功评估: {summary.get('evaluated_samples', 0)}")
    print(f"平均分: {summary.get('avg_score', 0):.2f}")
    print(f"Precision: {summary.get('precision', 0):.3f}")
    print(f"Recall: {summary.get('recall', 0):.3f}")
    print(f"F1: {summary.get('f1', 0):.3f}")


def main():
    parser = argparse.ArgumentParser(
        description="评估 MD&A 提取质量",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
    # 运行评估
    python scripts/evaluate_extraction.py \\
        --golden data/golden_set.json \\
        --source duckdb \\
        --use-llm-judge

    # 查看摘要
    python scripts/evaluate_extraction.py --summary data/evaluation_results.json
        """
    )

    parser.add_argument("--golden", type=str, default="data/golden_set.json",
                        help="黄金数据集路径")
    parser.add_argument("--source", type=str, default="duckdb",
                        help="提取结果来源 (duckdb | JSON文件路径)")
    parser.add_argument("--output", type=str, default="data/evaluation_results.json",
                        help="评估结果输出路径")
    parser.add_argument("--use-llm-judge", action="store_true",
                        help="启用 LLM 评估（默认仅规则评估）")
    parser.add_argument("--model", type=str, default="claude-sonnet",
                        help="LLM 模型标识")
    parser.add_argument("--summary", type=str,
                        help="打印评估结果摘要")

    args = parser.parse_args()

    if args.summary:
        print_summary(args.summary)
        return

    try:
        run_evaluation(
            golden_set_path=args.golden,
            extraction_source=args.source,
            output_path=args.output,
            use_llm_judge=args.use_llm_judge,
            model=args.model
        )
    except Exception as e:
        print(f"错误: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
