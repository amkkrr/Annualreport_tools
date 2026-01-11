#!/usr/bin/env python3
"""
修复 golden_set_draft.json 中的字符偏移量

LLM 标注时提供的偏移量是估计值，需要根据实际标记位置重新计算。
"""

import json
from pathlib import Path


def find_marker_positions(text: str, marker: str) -> list[tuple[int, bool, bool]]:
    """
    查找标记在文本中的所有位置。
    支持空白字符的灵活匹配。

    Returns:
        [(position, is_toc, is_reference), ...]
    """

    # 引用词：如果标记前面有这些词，说明是引用而不是真正的章节标题
    REFERENCE_PREFIXES = (
        "参见",
        "详见",
        "见本",
        "请参阅",
        "详情请见",
        "参考",
        "参阅",
        "查阅",
        "请查阅",
        "敬请查阅",
    )

    positions = []

    # 方法1: 精确匹配
    start = 0
    while True:
        pos = text.find(marker, start)
        if pos == -1:
            break
        positions.append((pos, None, None))  # 暂不判断 TOC/reference
        start = pos + 1

    # 方法2: 无空格版本匹配
    marker_no_space = marker.replace(" ", "")
    start = 0
    while True:
        pos = text.find(marker_no_space, start)
        if pos == -1:
            break
        if pos not in [p for p, _, _ in positions]:
            positions.append((pos, None, None))
        start = pos + 1

    # 对每个位置判断是否为 TOC 或引用
    result = []
    for pos, _, _ in positions:
        after = text[pos : pos + len(marker) + 150]

        has_dotline = (
            "..." in after or "…" in after or after.count(".") > 10 or after.count("·") > 5
        )

        context = text[max(0, pos - 200) : pos + 200]
        dot_density = context.count("...") + context.count("…") + context.count("·") * 0.5

        is_toc = has_dotline or dot_density > 10

        # 检查是否是引用（标记前面有引用词）
        before = text[max(0, pos - 20) : pos]
        is_reference = any(ref in before for ref in REFERENCE_PREFIXES)

        result.append((pos, is_toc, is_reference))

    return sorted(result, key=lambda x: x[0])


def fix_sample_offsets(sample: dict) -> dict:
    """修复单个样本的偏移量"""
    if "error" in sample:
        return sample

    txt_path = sample.get("source_txt_path", "")
    if not txt_path or not Path(txt_path).exists():
        print(f"  警告: 文件不存在 {txt_path}")
        return sample

    try:
        content = Path(txt_path).read_text(encoding="utf-8")
    except Exception as e:
        print(f"  警告: 读取文件失败 {e}")
        return sample

    boundary = sample.get("golden_boundary", {})
    start_marker = boundary.get("start_marker", "")
    end_marker = boundary.get("end_marker", "")
    original_start = boundary.get("start_char_offset", 0)
    original_end = boundary.get("end_char_offset", 0)

    # 查找起始标记位置
    start_positions = find_marker_positions(content, start_marker)
    if not start_positions:
        print(f"  警告: 未找到起始标记 '{start_marker[:30]}'")
        return sample

    # 优先选择非 TOC 且非引用的位置
    valid_starts = [pos for pos, is_toc, is_ref in start_positions if not is_toc and not is_ref]
    if valid_starts:
        new_start = valid_starts[0]
    else:
        # 退而求其次，选择非 TOC 位置
        non_toc_starts = [pos for pos, is_toc, is_ref in start_positions if not is_toc]
        if non_toc_starts:
            new_start = non_toc_starts[0]
        else:
            new_start = start_positions[0][0]

    # 查找结束标记位置 (从 new_start 之后搜索)
    new_end = original_end
    if end_marker:
        end_positions = find_marker_positions(content[new_start:], end_marker)
        if end_positions:
            # 取第一个非 TOC 且非引用的位置
            valid_ends = [pos for pos, is_toc, is_ref in end_positions if not is_toc and not is_ref]
            if valid_ends:
                new_end = new_start + valid_ends[0]
            else:
                non_toc_ends = [pos for pos, is_toc, is_ref in end_positions if not is_toc]
                if non_toc_ends:
                    new_end = new_start + non_toc_ends[0]
                else:
                    new_end = new_start + end_positions[0][0]

    # 更新偏移量
    changed = (new_start != original_start) or (new_end != original_end)
    if changed:
        sample["golden_boundary"]["start_char_offset"] = new_start
        sample["golden_boundary"]["end_char_offset"] = new_end
        sample["golden_boundary"]["char_count"] = new_end - new_start
        sample["annotation"]["offset_corrected"] = True
        sample["annotation"]["original_start"] = original_start
        sample["annotation"]["original_end"] = original_end

    return sample


def main():
    import argparse

    parser = argparse.ArgumentParser(description="修复 golden 数据集的偏移量")
    parser.add_argument("--input", default="data/golden_set_draft.json", help="输入文件")
    parser.add_argument("--output", default="data/golden_set_fixed.json", help="输出文件")

    args = parser.parse_args()

    # 加载数据
    with open(args.input, encoding="utf-8") as f:
        data = json.load(f)

    print(f"处理 {len(data['samples'])} 个样本...")

    # 修复偏移量
    fixed_count = 0
    for i, sample in enumerate(data["samples"], 1):
        sample_id = sample.get("id", f"#{i}")
        original_start = sample.get("golden_boundary", {}).get("start_char_offset", 0)

        fixed_sample = fix_sample_offsets(sample)

        new_start = fixed_sample.get("golden_boundary", {}).get("start_char_offset", 0)
        if new_start != original_start:
            print(f"  [{sample_id}] 起始偏移: {original_start} -> {new_start}")
            fixed_count += 1

        data["samples"][i - 1] = fixed_sample

    # 保存结果
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"\n完成: 修复了 {fixed_count} 个样本")
    print(f"输出: {args.output}")


if __name__ == "__main__":
    main()
