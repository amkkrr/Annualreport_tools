# PDF纯转换模式 规格书

## 1. 功能概述

### 1.1 背景
当前 `2.pdf_batch_converter.py` 必须依赖Excel元数据文件才能运行。用户可能已有PDF文件（来自其他渠道或历史下载），需要直接转换为TXT。

### 1.2 目标
新增"纯转换模式"（Convert-Only Mode），支持直接扫描指定目录的PDF文件进行批量转换，无需Excel文件。

---

## 2. 功能需求

### 2.1 核心功能

| 功能 | 描述 |
|------|------|
| 目录扫描 | 扫描指定目录下的所有PDF文件 |
| 递归扫描 | 可选支持递归扫描子目录 |
| 批量转换 | 使用现有多库回退机制转换PDF→TXT |
| 多进程 | 复用现有多进程框架 |
| 跳过已转换 | 默认若TXT已存在且有效则跳过（支持强制覆盖） |

### 2.2 输入输出

```
输入：
├── ./pdf_dir/
│   ├── 600519_贵州茅台_2023.pdf
│   ├── 000001_平安银行_2022.pdf
│   └── 随意命名.pdf              ← 支持任意文件名

输出：
├── ./outputs/txt_dir/
│   ├── 600519_贵州茅台_2023.txt
│   ├── 000001_平安银行_2022.txt
│   └── 随意命名.txt              ← 保持原文件名
```

### 2.3 文件名策略

| 策略 | 说明 |
|------|------|
| 保持原名 | `原文件名.pdf` → `原文件名.txt` |
| 输出目录 | 可指定独立输出目录，或与PDF同目录 |

### 2.3.1 路径基准（相对路径）

- 规格中所有目录路径均推荐使用相对路径；相对路径以脚本运行时的当前工作目录（`cwd`）为基准解析。
- 推荐将输出产物写入当前目录下的子目录（例如 `./outputs/...`），便于归档与避免污染源目录。

### 2.4 输出路径规则（含递归与冲突）

- `txt_dir is None`：输出到PDF同目录（天然保留目录结构）。
- `txt_dir is not None`：
  - `recursive=False`：输出到 `txt_dir/原文件名.txt`
  - `recursive=True`：输出到 `txt_dir/<相对pdf_dir的子路径>/原文件名.txt`（保留相对目录结构，避免不同子目录同名PDF覆盖）

示例（递归 + 独立输出目录）：

```
输入：
├── pdf_dir/
│   ├── A/report.pdf
│   └── B/report.pdf

输出（保留相对子目录）：
├── txt_dir/
│   ├── A/report.txt
│   └── B/report.txt
```

### 2.5 跳过/覆盖策略（TXT有效性）

- 默认跳过：当目标TXT已存在且满足最小有效性检查（至少“非空”）时，跳过该PDF。
- TXT异常处理：若TXT存在但为 0 字节（或写入失败导致明显异常），视为“无效”，应重新转换并覆盖。
- 强制覆盖：提供 `force=True` 时，无论TXT是否存在/是否有效，均重新转换并覆盖写入。

---

## 3. 配置设计

### 3.1 新增配置类

```python
@dataclass(frozen=True)
class ConvertOnlyConfig:
    """纯转换模式配置类。"""
    pdf_dir: str              # PDF源目录
    txt_dir: Optional[str] = None  # TXT输出目录（None则与PDF同目录；不建议空字符串）
    recursive: bool = False   # 是否递归扫描子目录
    delete_pdf: bool = False  # 转换后是否删除PDF
    force: bool = False       # 是否强制覆盖（忽略“已存在TXT则跳过”）
    processes: Optional[int] = None  # 进程数
    file_pattern: str = "*.pdf"      # 文件匹配模式
```

### 3.2 配置区域扩展

```python
# ==================== 模式选择 ====================
RUN_MODE = "convert_only"  # "download" | "convert_only"

# ==================== 纯转换模式配置 ====================
PDF_SOURCE_DIR = "annual_reports/pdf"              # PDF源目录（相对路径以cwd为基准）
TXT_OUTPUT_DIR = "outputs/annual_reports/txt"      # TXT输出目录；None则与PDF同目录
RECURSIVE_SCAN = False                             # 递归扫描
FORCE_OVERWRITE = False                            # 强制覆盖（忽略“已存在TXT则跳过”）
FILE_PATTERN = "*.pdf"                             # 文件匹配模式（大小写不敏感识别.pdf/.PDF）
PROCESSES = None                                   # 进程数（None=自动）
```

### 3.3 file_pattern 与大小写

- `file_pattern` 使用 glob 语义（例如 `*.pdf`、`*年报*.pdf`）。
- 默认匹配应对扩展名大小写不敏感：`a.pdf` 与 `a.PDF` 均应被识别为PDF并纳入扫描/转换范围。

---

### 3.4 delete_pdf 的严格条件

- 仅当“转换成功”且“目标TXT写入成功并通过最小有效性检查（至少非空）”时，才允许删除源PDF。
- 若因“已存在且有效TXT而跳过”，不得删除源PDF。
- 若转换失败/写入失败/生成空TXT，不得删除源PDF。

---

## 4. 类设计

### 4.1 新增类

```
ConvertOnlyResult
├── status: str  # "success" | "skipped" | "failed"
├── pdf_path: Path
├── txt_path: Path
├── backend: Optional[str]  # 使用的转换后端（如 "pdfplumber"），失败/跳过可为None
└── error: Optional[str]    # 失败原因（字符串化异常）

ConvertOnlyProcessor
├── __init__(config: ConvertOnlyConfig)
├── _scan_pdf_files() -> List[Path]        # 扫描PDF文件
├── _convert_single(pdf_path: Path) -> ConvertOnlyResult  # 转换单个文件（含跳过/失败信息）
└── run() -> None                           # 执行批量转换
```

### 4.2 复用现有组件

| 组件 | 复用方式 |
|------|----------|
| `PDFConverter._convert_pdf_to_txt()` | 直接调用 |
| `PDFConverter._convert_with_pdfplumber()` | 直接调用 |
| `PDFConverter._convert_with_pypdf2()` | 直接调用 |
| `PDFConverter._convert_with_pdfminer()` | 直接调用 |
| 多进程框架 | 复用 `Pool` + `map/imap_unordered` 模式（worker 返回 `ConvertOnlyResult`，主进程汇总统计） |

### 4.3 多进程契约与可序列化边界

- 每个 worker 在子进程内独立创建/使用 `PDFConverter`（避免跨进程序列化与资源共享问题）。
- worker 的输入为 `pdf_path`（及必要的只读配置），输出为可序列化的 `ConvertOnlyResult`。
- 主进程负责：
  - 汇总 `success/skipped/failed` 数量；
  - 对失败项输出 `pdf_path + error`；
  - 最终打印统一汇总行，避免多进程日志互相穿插影响可读性。

---

## 5. 使用方式

### 5.1 命令行示例

```bash
# 修改脚本配置后运行
python 2.pdf_batch_converter.py
```

### 5.2 配置示例

```python
# 模式1：转换单个目录
RUN_MODE = "convert_only"
PDF_SOURCE_DIR = "annual_reports/pdf"
TXT_OUTPUT_DIR = "outputs/annual_reports/txt"

# 模式2：递归转换多级目录
RUN_MODE = "convert_only"
PDF_SOURCE_DIR = "年报文件"
TXT_OUTPUT_DIR = None  # 输出到PDF同目录
RECURSIVE_SCAN = True
```

---

## 6. 日志输出

```
============================================================
纯转换模式启动
PDF源目录: annual_reports/pdf
TXT输出目录: outputs/annual_reports/txt
递归扫描: False
============================================================
扫描到 1520 个PDF文件
使用 8 个进程处理
转换成功 (pdfplumber): outputs/annual_reports/txt/600519_贵州茅台_2023.txt
跳过已存在(有效): outputs/annual_reports/txt/000001_平安银行_2022.txt
============================================================
处理完成: 成功 1518/1520, 跳过 200, 失败 2
============================================================
```

---

## 7. 错误处理

| 错误类型 | 处理方式 |
|----------|----------|
| PDF目录不存在/不可读 | 报错退出 |
| 输出目录不可创建/不可写 | 报错退出 |
| 单个PDF转换失败/文件损坏 | 记录失败原因，继续处理其他文件 |
| 磁盘空间不足（如 `OSError: [Errno 28] No space left on device`） | 记录日志并终止处理（避免产生不完整TXT与误删） |

---

## 8. 兼容性

- 与现有"下载+转换"模式互不影响
- 通过 `RUN_MODE` 参数切换模式
- 共享PDF转换核心逻辑，保持一致性

---

## 9. 实现优先级

| 优先级 | 功能 |
|--------|------|
| P0 | 基础目录扫描 + 批量转换 |
| P0 | 多进程支持 |
| P0 | 跳过已转换文件 |
| P1 | 递归扫描子目录 |
| P2 | 自定义文件匹配模式 |
