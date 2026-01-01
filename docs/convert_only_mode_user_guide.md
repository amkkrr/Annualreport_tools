# 纯转换模式（Convert-Only Mode）用户使用指南

本指南适用于：你已经有一批 PDF（不依赖 Excel 元数据/下载环节），希望直接批量转换为 TXT。

对应脚本：`2.pdf_batch_converter.py`

## 1. 环境要求

- Python 3.10+
- 依赖安装（与下载模式共用）：

```bash
pip install -r requirements.txt

# 推荐安装备用解析库（提高转换成功率）
pip install PyPDF2 pdfminer.six
```

## 2. 目录准备（推荐相对路径）

建议将 PDF 放在当前目录下的子目录中，例如：

```
./annual_reports/pdf/
```

输出 TXT 推荐放在当前目录下的 `./outputs/...` 子目录中，例如：

```
./outputs/annual_reports/txt/
```

## 3. 配置与运行

### 3.1 命令行方式（推荐）

无参数运行会输出帮助：

```bash
python3 2.pdf_batch_converter.py
```

执行纯转换模式：

```bash
python3 2.pdf_batch_converter.py convert-only --pdf-dir annual_reports/pdf --txt-dir outputs/annual_reports/txt
```

递归扫描：

```bash
python3 2.pdf_batch_converter.py convert-only --pdf-dir annual_reports/pdf --txt-dir outputs/annual_reports/txt --recursive
```

强制覆盖：

```bash
python3 2.pdf_batch_converter.py convert-only --pdf-dir annual_reports/pdf --txt-dir outputs/annual_reports/txt --force
```

输出到 PDF 同目录（将 `--txt-dir` 设为空字符串）：

```bash
python3 2.pdf_batch_converter.py convert-only --pdf-dir annual_reports/pdf --txt-dir ""
```

### 3.2 使用脚本底部配置（兼容旧用法）

打开 `2.pdf_batch_converter.py`，在底部“配置区域”修改参数，然后用 `--use-config` 运行：

```python
RUN_MODE = "convert_only"

PDF_SOURCE_DIR = "annual_reports/pdf"
TXT_OUTPUT_DIR = "outputs/annual_reports/txt"  # 设为 None 则输出到PDF同目录
RECURSIVE_SCAN = False
FORCE_OVERWRITE = False
FILE_PATTERN = "*.pdf"
PROCESSES = None
DELETE_PDF = False
```

运行：

```bash
python3 2.pdf_batch_converter.py --use-config
```

运行：

```bash
python3 2.pdf_batch_converter.py
```

## 4. 行为规则（与规格书一致）

### 4.1 输出路径规则

- `TXT_OUTPUT_DIR is None`：输出到 PDF 同目录。
- `TXT_OUTPUT_DIR is not None`：
  - `RECURSIVE_SCAN=False`：输出到 `TXT_OUTPUT_DIR/原文件名.txt`
  - `RECURSIVE_SCAN=True`：输出到 `TXT_OUTPUT_DIR/<相对PDF_SOURCE_DIR的子路径>/原文件名.txt`（保留相对目录结构，避免同名覆盖）

### 4.2 跳过/覆盖

- 默认跳过：目标 TXT 已存在且“非空”时跳过（断点续跑友好）。
- 若 TXT 存在但为 0 字节：视为无效，会重新转换并覆盖。
- 强制覆盖：设置 `FORCE_OVERWRITE=True`，无论 TXT 是否存在/是否有效都重新转换。
- 写入采用临时文件并在成功后替换，避免失败时留下半成品 TXT。

### 4.3 file_pattern（大小写）

- `FILE_PATTERN` 为 glob 风格（近似 `*.pdf`、`*年报*.pdf`）。
- 扩展名匹配大小写不敏感：`.pdf` 与 `.PDF` 都会被识别为 PDF。

### 4.4 delete_pdf（删除源PDF）

- `DELETE_PDF=True` 时，仅在“转换成功且 TXT 写入成功并通过最小有效性检查（非空）”后删除源 PDF。
- 若因“已存在且有效 TXT”而跳过，不会删除源 PDF。

## 5. 常见问题

### Q1：为什么显示“跳过已存在(有效)”？
说明目标 TXT 已存在且非空。若需要重跑，请设置 `FORCE_OVERWRITE=True`。

### Q2：为什么转换失败？
可能原因：PDF 为扫描件/图片型、PDF损坏、或解析库兼容性不足。建议安装 `PyPDF2` 与 `pdfminer.six` 后重试。

### Q3：磁盘空间不足怎么办？
脚本检测到 `Errno 28 No space left on device` 会终止处理。请释放磁盘空间后重跑（会跳过已成功且有效的 TXT）。
