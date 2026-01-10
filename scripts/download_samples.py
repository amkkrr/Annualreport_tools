#!/usr/bin/env python3
"""
下载选定的样本 PDF 并转换为 TXT

用法:
    python scripts/download_samples.py --input data/download_samples.json
"""
import json
import sys
from pathlib import Path
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import pdfplumber


def download_pdf(url: str, output_path: Path, timeout: int = 30) -> bool:
    """下载 PDF 文件"""
    if output_path.exists():
        return True

    output_path.parent.mkdir(parents=True, exist_ok=True)

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

    try:
        resp = requests.get(url, headers=headers, timeout=timeout, stream=True)
        resp.raise_for_status()

        with open(output_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
        return True
    except Exception as e:
        print(f"  下载失败: {e}")
        return False


def convert_pdf_to_txt(pdf_path: Path, txt_path: Path) -> bool:
    """将 PDF 转换为 TXT"""
    if txt_path.exists():
        return True

    txt_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        text_parts = []
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    text_parts.append(text)

        if not text_parts:
            print(f"  警告: PDF 无文本内容")
            return False

        txt_path.write_text("\n\n".join(text_parts), encoding="utf-8")
        return True
    except Exception as e:
        print(f"  转换失败: {e}")
        return False


def process_sample(sample: dict, pdf_root: str, txt_root: str) -> dict:
    """处理单个样本：下载 + 转换"""
    stock_code = sample["stock_code"]
    year = sample["year"]
    company_name = sample.get("company_name", "")
    url = sample["pdf_url"]

    # 构建文件名
    if company_name:
        filename = f"{stock_code}_{company_name}_{year}"
    else:
        filename = f"{stock_code}_{year}"

    # 清理文件名中的非法字符
    filename = "".join(c for c in filename if c.isalnum() or c in "_-")

    pdf_path = Path(pdf_root) / str(year) / "pdf" / f"{filename}.pdf"
    txt_path = Path(txt_root) / str(year) / "txt" / f"{filename}.txt"

    result = {
        "stock_code": stock_code,
        "year": year,
        "company_name": company_name,
        "pdf_path": str(pdf_path),
        "txt_path": str(txt_path),
        "download_ok": False,
        "convert_ok": False
    }

    # 下载
    if download_pdf(url, pdf_path):
        result["download_ok"] = True

        # 转换
        if convert_pdf_to_txt(pdf_path, txt_path):
            result["convert_ok"] = True

    return result


def main():
    parser = argparse.ArgumentParser(description="下载选定的样本 PDF 并转换为 TXT")
    parser.add_argument("--input", type=str, default="data/download_samples.json",
                        help="样本列表 JSON 文件")
    parser.add_argument("--output-root", type=str, default="outputs/annual_reports",
                        help="输出根目录")
    parser.add_argument("--workers", type=int, default=4,
                        help="并发下载数")
    parser.add_argument("--result", type=str, default="data/download_results.json",
                        help="结果输出文件")

    args = parser.parse_args()

    # 加载样本
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"错误: 样本文件不存在: {args.input}")
        sys.exit(1)

    with open(input_path, encoding="utf-8") as f:
        data = json.load(f)

    samples = data.get("samples", [])
    if not samples:
        print("错误: 样本列表为空")
        sys.exit(1)

    print(f"共 {len(samples)} 个样本待处理")

    # 处理样本
    results = []
    success_count = 0

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(
                process_sample, sample, args.output_root, args.output_root
            ): sample
            for sample in samples
        }

        for i, future in enumerate(as_completed(futures), 1):
            sample = futures[future]
            try:
                result = future.result()
                results.append(result)

                status = "✓" if result["convert_ok"] else "✗"
                if result["convert_ok"]:
                    success_count += 1

                print(f"[{i}/{len(samples)}] {status} {sample['stock_code']}-{sample['year']}")
            except Exception as e:
                print(f"[{i}/{len(samples)}] ✗ {sample['stock_code']}-{sample['year']}: {e}")
                results.append({
                    "stock_code": sample["stock_code"],
                    "year": sample["year"],
                    "error": str(e)
                })

    # 保存结果
    result_path = Path(args.result)
    result_path.parent.mkdir(parents=True, exist_ok=True)

    with open(result_path, "w", encoding="utf-8") as f:
        json.dump({
            "total": len(samples),
            "success": success_count,
            "results": results
        }, f, ensure_ascii=False, indent=2)

    print(f"\n完成: {success_count}/{len(samples)} 成功")
    print(f"结果已保存至: {args.result}")


if __name__ == "__main__":
    main()
