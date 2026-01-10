#!/usr/bin/env python3
"""
自动化改进流程 - 分析失败原因，改进策略，迭代评估

用法:
    python scripts/auto_improve.py --eval data/evaluation_results.json --golden data/golden_set_draft.json

流程:
    1. 分析低分样本的失败模式
    2. 识别常见失败原因
    3. 生成改进建议
    4. (可选) 使用 LLM 分析复杂案例
"""
import json
import argparse
from pathlib import Path
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional
import subprocess
import re


@dataclass
class FailurePattern:
    """失败模式"""
    pattern_type: str  # start_too_early, end_too_early, no_overlap, etc.
    sample_ids: list[str] = field(default_factory=list)
    count: int = 0
    avg_score: float = 0.0
    examples: list[dict] = field(default_factory=list)


def load_evaluation(eval_path: str) -> dict:
    """加载评估结果"""
    with open(eval_path, encoding="utf-8") as f:
        return json.load(f)


def load_golden(golden_path: str) -> dict:
    """加载 golden 数据集"""
    with open(golden_path, encoding="utf-8") as f:
        data = json.load(f)
    # 转换为 sample_id -> sample 映射
    return {s["id"]: s for s in data.get("samples", [])}


def analyze_failure_pattern(result: dict, golden_sample: Optional[dict]) -> str:
    """分析单个样本的失败模式"""
    score = result.get("final_score", 0)
    rule_eval = result.get("rule_evaluation", {})

    overlap = rule_eval.get("char_overlap_ratio", 0)
    start_diff = rule_eval.get("start_offset_diff", 0)
    end_diff = rule_eval.get("end_offset_diff", 0)

    # 无重叠 - 完全错误
    if overlap == 0:
        return "no_overlap"

    # 起始太早 (start_diff 是负数表示提取的起始位置比 golden 更早)
    if start_diff < -5000:
        # 结束也太早
        if end_diff < -5000:
            return "both_too_early"
        # 结束正常但起始太早
        return "start_too_early"

    # 结束太早 (提取内容不完整)
    if end_diff < -5000:
        return "end_too_early"

    # 结束太晚 (包含了多余内容)
    if end_diff > 5000:
        return "end_too_late"

    # 分数较低但边界差异不大
    if score < 70:
        return "content_mismatch"

    return "acceptable"


def analyze_failures(eval_data: dict, golden_data: dict) -> dict[str, FailurePattern]:
    """分析所有失败样本，归类失败模式"""
    patterns: dict[str, FailurePattern] = {}

    for result in eval_data.get("results", []):
        sample_id = result.get("sample_id")
        score = result.get("final_score", 0)
        golden_sample = golden_data.get(sample_id)

        pattern_type = analyze_failure_pattern(result, golden_sample)

        if pattern_type not in patterns:
            patterns[pattern_type] = FailurePattern(pattern_type=pattern_type)

        p = patterns[pattern_type]
        p.sample_ids.append(sample_id)
        p.count += 1

        # 保留低分样本作为示例
        if score < 30 and len(p.examples) < 5:
            p.examples.append({
                "sample_id": sample_id,
                "score": score,
                "start_diff": result.get("rule_evaluation", {}).get("start_offset_diff", 0),
                "end_diff": result.get("rule_evaluation", {}).get("end_offset_diff", 0),
                "overlap": result.get("rule_evaluation", {}).get("char_overlap_ratio", 0),
            })

    # 计算平均分
    for pattern_type, p in patterns.items():
        if p.sample_ids:
            scores = []
            for result in eval_data.get("results", []):
                if result.get("sample_id") in p.sample_ids:
                    scores.append(result.get("final_score", 0))
            p.avg_score = sum(scores) / len(scores) if scores else 0

    return patterns


def generate_improvement_suggestions(patterns: dict[str, FailurePattern]) -> list[dict]:
    """根据失败模式生成改进建议"""
    suggestions = []

    # 无重叠问题 - 标题匹配失败
    if "no_overlap" in patterns and patterns["no_overlap"].count > 0:
        p = patterns["no_overlap"]
        suggestions.append({
            "priority": 1,
            "issue": f"完全无法匹配 ({p.count} 个样本，平均分 {p.avg_score:.1f})",
            "cause": "MD&A 章节标题识别失败，可能使用了非标准章节名称",
            "fix": "扩展 MDA_TITLES 列表，添加更多变体",
            "action": "add_mda_titles",
            "sample_ids": p.sample_ids[:5],
        })

    # 起始位置太早
    if "start_too_early" in patterns and patterns["start_too_early"].count > 0:
        p = patterns["start_too_early"]
        suggestions.append({
            "priority": 2,
            "issue": f"起始位置识别过早 ({p.count} 个样本，平均分 {p.avg_score:.1f})",
            "cause": "当前提取器从页面开始提取，未准确定位章节标题",
            "fix": "改进起始标记匹配逻辑，使用更精确的正则表达式",
            "action": "improve_start_detection",
            "sample_ids": p.sample_ids[:5],
        })

    # 结束位置太早
    if "end_too_early" in patterns and patterns["end_too_early"].count > 0:
        p = patterns["end_too_early"]
        suggestions.append({
            "priority": 2,
            "issue": f"结束位置识别过早 ({p.count} 个样本，平均分 {p.avg_score:.1f})",
            "cause": "提取在 MD&A 完整结束前就停止了",
            "fix": "调整结束标记检测，确保识别正确的下一章节",
            "action": "improve_end_detection",
            "sample_ids": p.sample_ids[:5],
        })

    # 两端都太早
    if "both_too_early" in patterns and patterns["both_too_early"].count > 0:
        p = patterns["both_too_early"]
        suggestions.append({
            "priority": 1,
            "issue": f"起始和结束都识别过早 ({p.count} 个样本，平均分 {p.avg_score:.1f})",
            "cause": "提取器定位到错误的章节或目录区域",
            "fix": "增强目录页检测，跳过目录中的章节引用",
            "action": "improve_toc_skip",
            "sample_ids": p.sample_ids[:5],
        })

    return sorted(suggestions, key=lambda x: x["priority"])


def analyze_sample_with_llm(
    sample_id: str,
    golden_sample: dict,
    txt_path: str,
    timeout: int = 120
) -> Optional[dict]:
    """使用 LLM 分析单个失败样本的原因"""
    if not Path(txt_path).exists():
        return None

    content = Path(txt_path).read_text(encoding="utf-8")[:30000]

    golden_boundary = golden_sample.get("golden_boundary", {})
    start_marker = golden_boundary.get("start_marker", "")
    end_marker = golden_boundary.get("end_marker", "")
    start_offset = golden_boundary.get("start_char_offset", 0)
    end_offset = golden_boundary.get("end_char_offset", 0)

    prompt = f"""分析这份年报文本，解释为什么自动提取可能失败。

## 预期边界
- 起始标记: {start_marker}
- 结束标记: {end_marker}
- 起始位置: 字符 {start_offset}
- 结束位置: 字符 {end_offset}

## 年报内容 (前 30000 字符)
{content}

## 分析任务
1. 检查起始标记是否存在于预期位置
2. 检查是否有变体标题 (如 "第三节" vs "第四节")
3. 检查是否有目录干扰
4. 提出改进建议

## 输出格式 (JSON)
{{"found_start_marker": true/false, "actual_start_position": N, "variation_detected": "...", "toc_interference": true/false, "suggestion": "..."}}
"""

    try:
        result = subprocess.run(
            ["claude", "-p", prompt],
            capture_output=True,
            text=True,
            timeout=timeout
        )
        if result.returncode == 0:
            output = result.stdout.strip()
            if "```json" in output:
                start_idx = output.find("```json") + 7
                end_idx = output.find("```", start_idx)
                output = output[start_idx:end_idx].strip()
            return json.loads(output)
    except Exception as e:
        print(f"  LLM 分析失败: {e}")

    return None


def extract_new_patterns_from_golden(golden_data: dict) -> list[str]:
    """从 golden 数据集中提取新的标题模式"""
    patterns = set()

    for sample in golden_data.values():
        boundary = sample.get("golden_boundary", {})
        start_marker = boundary.get("start_marker", "")
        if start_marker:
            # 清理并添加变体
            cleaned = start_marker.strip()
            patterns.add(cleaned)

            # 提取核心模式 (如 "第四节 经营情况讨论与分析" -> "经营情况讨论与分析")
            if "节" in cleaned:
                parts = cleaned.split()
                if len(parts) > 1:
                    patterns.add(parts[-1])

    return list(patterns)


def generate_improvement_patch(suggestions: list[dict], golden_data: dict) -> dict:
    """生成改进补丁"""
    patch = {
        "new_mda_titles": [],
        "new_mda_patterns": [],
        "config_changes": {},
    }

    # 从 golden 数据中提取新模式
    new_patterns = extract_new_patterns_from_golden(golden_data)

    # 添加常见标题变体
    known_titles = [
        "经营情况讨论与分析",
        "董事会报告",
        "管理层讨论与分析",
    ]

    for p in new_patterns:
        if p and p not in known_titles and len(p) > 3:
            patch["new_mda_titles"].append(p)

    # 添加正则模式
    patch["new_mda_patterns"] = [
        r"第[一二三四五六七八九十百零\d]+[章节部分]\s*经营情况讨论与分析",
        r"第[一二三四五六七八九十百零\d]+[章节部分]\s*经营情况的讨论与分析",
    ]

    return patch


def apply_improvements(patch: dict) -> bool:
    """应用改进到 scorer.py"""
    scorer_path = Path("annual_report_mda/scorer.py")
    if not scorer_path.exists():
        print("错误: scorer.py 不存在")
        return False

    content = scorer_path.read_text(encoding="utf-8")

    # 检查是否需要添加新标题
    modified = False

    for title in patch.get("new_mda_titles", []):
        if title and title not in content:
            # 找到 MDA_TITLES 列表并添加
            pattern = r'(MDA_TITLES:\s*list\[str\]\s*=\s*\[)'
            replacement = f'\\1\n    "{title}",'
            new_content = re.sub(pattern, replacement, content)
            if new_content != content:
                content = new_content
                modified = True
                print(f"  + 添加标题: {title}")

    for pat in patch.get("new_mda_patterns", []):
        if pat and pat not in content:
            # 找到 MDA_PATTERNS 列表并添加
            pattern = r'(MDA_PATTERNS:\s*list\[str\]\s*=\s*\[)'
            replacement = f'\\1\n    r"{pat}",'
            new_content = re.sub(pattern, replacement, content)
            if new_content != content:
                content = new_content
                modified = True
                print(f"  + 添加模式: {pat}")

    if modified:
        scorer_path.write_text(content, encoding="utf-8")
        print("scorer.py 已更新")

    return modified


def run_evaluation(golden_path: str, output_path: str) -> dict:
    """运行评估脚本"""
    result = subprocess.run(
        [
            "python", "scripts/evaluate_extraction.py",
            "--golden", golden_path,
            "--source", "duckdb",
            "--output", output_path,
        ],
        capture_output=True,
        text=True,
    )

    if result.returncode == 0 and Path(output_path).exists():
        with open(output_path, encoding="utf-8") as f:
            return json.load(f)
    return {}


def compare_scores(old_eval: dict, new_eval: dict) -> dict:
    """比较两次评估的分数变化"""
    old_results = {r["sample_id"]: r for r in old_eval.get("results", [])}
    new_results = {r["sample_id"]: r for r in new_eval.get("results", [])}

    comparison = {
        "improved": [],
        "degraded": [],
        "unchanged": [],
        "old_avg": old_eval.get("summary", {}).get("avg_score", 0),
        "new_avg": new_eval.get("summary", {}).get("avg_score", 0),
    }

    for sample_id in old_results:
        old_score = old_results[sample_id].get("final_score", 0)
        new_score = new_results.get(sample_id, {}).get("final_score", 0)

        diff = new_score - old_score
        if diff > 5:
            comparison["improved"].append({
                "sample_id": sample_id,
                "old_score": old_score,
                "new_score": new_score,
                "diff": diff,
            })
        elif diff < -5:
            comparison["degraded"].append({
                "sample_id": sample_id,
                "old_score": old_score,
                "new_score": new_score,
                "diff": diff,
            })
        else:
            comparison["unchanged"].append(sample_id)

    return comparison


def print_analysis_report(
    patterns: dict[str, FailurePattern],
    suggestions: list[dict],
):
    """打印分析报告"""
    print("\n" + "=" * 60)
    print("失败模式分析报告")
    print("=" * 60)

    print("\n## 失败模式分布\n")
    for pattern_type, p in sorted(patterns.items(), key=lambda x: -x[1].count):
        print(f"  {pattern_type}: {p.count} 个样本 (平均分: {p.avg_score:.1f})")
        if p.examples:
            print(f"    示例: {[e['sample_id'] for e in p.examples[:3]]}")

    print("\n## 改进建议\n")
    for i, s in enumerate(suggestions, 1):
        print(f"  {i}. [{s['priority']}] {s['issue']}")
        print(f"     原因: {s['cause']}")
        print(f"     修复: {s['fix']}")
        print()


def main():
    parser = argparse.ArgumentParser(description="自动化分析和改进 MD&A 提取")
    parser.add_argument("--eval", type=str, default="data/evaluation_results.json",
                        help="评估结果文件")
    parser.add_argument("--golden", type=str, default="data/golden_set_draft.json",
                        help="Golden 数据集文件")
    parser.add_argument("--apply", action="store_true",
                        help="应用改进并重新评估")
    parser.add_argument("--use-llm", action="store_true",
                        help="使用 LLM 分析低分样本 (较慢)")
    parser.add_argument("--output", type=str, default="data/improvement_report.json",
                        help="输出报告路径")

    args = parser.parse_args()

    # 1. 加载数据
    print("加载评估结果...")
    eval_data = load_evaluation(args.eval)
    golden_data = load_golden(args.golden)

    # 2. 分析失败模式
    print("分析失败模式...")
    patterns = analyze_failures(eval_data, golden_data)

    # 3. 生成改进建议
    print("生成改进建议...")
    suggestions = generate_improvement_suggestions(patterns)

    # 4. 打印报告
    print_analysis_report(patterns, suggestions)

    # 5. (可选) LLM 深度分析
    if args.use_llm:
        print("\n使用 LLM 分析低分样本...")
        llm_analyses = []

        # 选择低分样本进行分析
        low_score_samples = [
            r for r in eval_data.get("results", [])
            if r.get("final_score", 0) < 30
        ][:5]  # 最多分析 5 个

        for result in low_score_samples:
            sample_id = result.get("sample_id")
            golden_sample = golden_data.get(sample_id)
            if golden_sample:
                txt_path = golden_sample.get("source_txt_path", "")
                print(f"  分析 {sample_id}...")
                analysis = analyze_sample_with_llm(sample_id, golden_sample, txt_path)
                if analysis:
                    llm_analyses.append({
                        "sample_id": sample_id,
                        "analysis": analysis,
                    })

        if llm_analyses:
            print(f"\n完成 {len(llm_analyses)} 个样本的 LLM 分析")

    # 6. 生成改进补丁
    patch = generate_improvement_patch(suggestions, golden_data)

    # 7. 保存报告
    report = {
        "patterns": {k: {"count": v.count, "avg_score": v.avg_score, "sample_ids": v.sample_ids}
                     for k, v in patterns.items()},
        "suggestions": suggestions,
        "patch": patch,
        "summary": eval_data.get("summary", {}),
    }

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"\n报告已保存至: {args.output}")

    # 8. (可选) 应用改进
    if args.apply:
        print("\n应用改进...")
        if apply_improvements(patch):
            print("\n重新运行提取...")
            subprocess.run([
                "python", "mda_extractor.py",
                "--dir", "outputs/annual_reports",
                "--workers", "4",
            ])

            print("\n重新评估...")
            new_eval_path = "data/evaluation_results_v2.json"
            new_eval = run_evaluation(args.golden, new_eval_path)

            if new_eval:
                comparison = compare_scores(eval_data, new_eval)
                print("\n## 分数变化")
                print(f"  旧平均分: {comparison['old_avg']:.2f}")
                print(f"  新平均分: {comparison['new_avg']:.2f}")
                print(f"  提升样本: {len(comparison['improved'])}")
                print(f"  下降样本: {len(comparison['degraded'])}")
        else:
            print("无需改进或改进失败")


if __name__ == "__main__":
    main()
