
基于**已完成文本抽取**的 A 股年报 MD&A 章节提取工具，纯 CPU 运行，无需 GPU。

> 说明：PDF→文本 的抽取环节由上游工具完成；本工具只负责**在已抽取的文本中定位并截取**「管理层讨论与分析」（MD&A）章节。

---

## 1. 目标与约束

### 目标

- 从年报**已抽取文本**中提取「管理层讨论与分析」（MD&A）章节全文
- 输出结构化文本，存入 `annual_reports.duckdb`

### 约束

- **纯 CPU 环境**：不依赖 GPU 或深度学习模型
- **依赖上游文本质量**：本工具不负责 OCR；若上游输出为空/严重缺失，则视为失败（记录原因，需 OCR 才能处理）
- **批量处理**：支持 1000+ 文件的并发处理（并发调度与 DuckDB 写入由现成代理负责，见第 5.3）

---

## 2. 依赖库

```bash
pip install rich duckdb
```

| 库             | 用途                                  |
| -------------- | ------------------------------------- |
| **pdfplumber** | PDF 文本提取（比 pypdf 表格处理更好） |
| **rich**       | 进度条与日志                          |
| **duckdb**     | 结果存储                              |

---

## 3. 核心逻辑：章节定位

A 股年报格式不统一，MD&A 章节名称多样。采用 **TOC 优先 + 正文扫描 + 密度验证** 的多轮迭代策略。

### 3.1 章节名模式

```python
# MD&A 章节可能的标题
MDA_TITLES = [
    # 标准命名
    '董事会报告',
    '董事局报告',
    '经营情况讨论与分析',
    '经营层讨论与分析',
    '管理层讨论与分析',
    '管理层分析与讨论',
    '董事会工作报告',
    '董事局工作报告',
    # 语序变体
    '经营分析与讨论',
    '讨论与分析',
    # 港股/B股风格
    '业务回顾',
    '业务回顾与展望',
    # 简写/异体
    '董事会报告书',
    '董事会工作汇报',
    # 英文混排
    'Management Discussion and Analysis',
    'MD&A',
]

# 带序号前缀的正则模式（用于匹配 "第三节 管理层讨论与分析" 等）
MDA_PATTERNS = [
    r'第[一二三四五六七八九十百零\d]+[章节部分]\s*管理层讨论与分析',
    r'第[一二三四五六七八九十百零\d]+[章节部分]\s*董事会报告',
    r'[一二三四五六七八九十百零\d]+[、\.]\s*董事会报告',
    r'[一二三四五六七八九十百零\d]+[、\.]\s*管理层讨论与分析',
]

# 下一章节（MD&A 结束标志）
NEXT_TITLES = [
    '监事会报告',
    '监事会工作报告',
    '重要事项',
    '公司治理',
    '财务报告',
    '审计报告',
]
```

### 3.2 提取算法（迭代增强版）

```python
def calculate_mda_score(text: str) -> float:
    """
    CPU 质检核心：计算文本像 MD&A 的程度 (0.0 - 1.0)
    依据：长度适中、包含关键指标词、不包含大量无意义字符
    """
    if not text or len(text) < 500: return 0.0
    
    # 关键词特征
    keywords = ['主营业务', '收入', '同比', '毛利率', '现金流', '行业', '展望']
    hit_count = sum(1 for k in keywords if k in text)
    
    # 负面特征（避免提取到目录或表格堆砌区）
    dots_count = text.count('...') + text.count('…')
    
    score = (hit_count / len(keywords)) * 0.8
    if dots_count < 10: score += 0.2
    return score

def extract_mda_iterative(pages_text: Sequence[str]):
    """
    迭代提取策略：
    Strategy 1: 尝试解析目录页获取页码范围（最准）
    Strategy 2: 标准正则前后匹配（通用）
    Strategy 3: 放宽正则匹配（召回）
    """
    candidates = []

    # --- Strategy 1: TOC Parsing (伪代码示意) ---
    # 扫描前 15 页，寻找 "管理层...15" 结构的行，解析出 start_page 和 end_page
    toc_hit = parse_toc_for_page_range(pages_text[:15]) 
    if toc_hit:
        text = extract_by_pages(pages_text, toc_hit.start, toc_hit.end)
        candidates.append({"src": "toc", "text": text, "score": calculate_mda_score(text)})

    # --- Strategy 2: Body Scan (标准模式) ---
    body_hit = extract_mda_from_pages(pages_text) # 原有的正则逻辑
    if body_hit:
        candidates.append({"src": "body_std", "text": body_hit['mda_raw'], "score": calculate_mda_score(body_hit['mda_raw'])})

    # --- 决策择优 ---
    # 按分数排序，返回最佳结果
    candidates.sort(key=lambda x: x['score'], reverse=True)
    
    if candidates and candidates[0]['score'] > 0.4:
        return candidates[0]
    
    return None # 均失败，标记需人工或 LLM 处理
```

### 3.3 目录页与页眉误判（实现要点）

- **目录页（TOC）误命中**是最常见的误判来源：目录里必然出现“董事会报告/管理层讨论与分析/财务报告”等标题。
- 建议默认启用 `is_toc_page()` 过滤；对“疑似目录页但不确定”的情况，可采用更强特征：
  - 出现“……/....”引导线 + 多个页码
  - 同页出现大量“第X节/第X章/……”
- **页眉/页脚重复标题**可能导致 start 提前或 end 过早触发；可作为后续增强：
  - 基于 `extract_words()` 的位置约束（标题更靠上且字号更大）
  - 或对跨页重复行做简单去重（先收集每页首行/末行的高频文本）

---

## 4. 数据结构

### 4.1 输入

```
data/annual_reports/
├── 000778/
│   ├── 2022.pdf
│   ├── 2023.pdf
│   └── 2024.pdf
└── 600519/
    └── ...
```

### 4.2 输出（DuckDB 表）

```sql
-- 存储提取结果（建议加入可观测字段，便于复核与质检）
CREATE TABLE mda_text (
    stock_code VARCHAR,
    year INTEGER,
    mda_raw TEXT,
    char_count INTEGER,

    start_page INTEGER,
    end_page INTEGER,
    page_count INTEGER,

    hit_start VARCHAR,          -- 命中的标题/正则（含 hit_kind）
    hit_end VARCHAR,
    is_truncated BOOLEAN,       -- 是否触发 max_pages 截断

    pdf_path VARCHAR,
    pdf_sha256 VARCHAR,         -- 用于增量/复跑校验（可选）
    extracted_at TIMESTAMP,
    used_rule_type VARCHAR,    -- 'generic' 或 'custom'
    PRIMARY KEY (stock_code, year)
);

-- 存储自适应规则（学习到的知识）
CREATE TABLE extraction_rules (
    stock_code VARCHAR,
    start_pattern VARCHAR,     -- 学习到的起始标题，如 "第三节 董事会报告"
    end_pattern VARCHAR,       -- 学习到的结束标题
    rule_source VARCHAR,       -- 'llm_learned' 或 'manual'
    updated_at TIMESTAMP,
    PRIMARY KEY (stock_code)
);
```

---

## 5. CLI 接口

```bash
# 单文件测试（输入为“已抽取文本”，需包含页边界信息）
python mda_extractor.py --pages data/annual_reports_text/000778/2023.pages.json

# 批量提取（指定目录：目录下存放已抽取文本）
python mda_extractor.py --dir data/annual_reports_text/ --workers 4

# 开启自适应学习模式（需要配置 LLM API）
python mda_extractor.py --dir data/annual_reports_text/ --learn --api-key "sk-..."
```

### 参数说明

| 参数            | 说明                                                |
| --------------- | --------------------------------------------------- |
| `--pages`       | 单文件模式：逐页文本（示例为 JSON，格式约定见实现） |
| `--dir`         | 批量模式：扫描目录下所有已抽取文本文件              |
| `--workers`     | 并发数，默认 4                                      |
| `--incremental` | 增量模式，跳过已存在的记录                          |
| `--dry-run`     | 仅统计，不写入数据库                                |
| `--db`          | 数据库路径，默认 `data/annual_reports.duckdb`       |

---

## 6. 错误处理

| 错误类型                 | 处理方式                                        |
| ------------------------ | ----------------------------------------------- |
| 文本文件无法打开/解析    | 记录日志，跳过                                  |
| 未找到 MD&A 章节         | 标记 `mda_raw = NULL`，人工复核                 |
| 提取文本过短 (<500 字符) | 标记为失败：可能是扫描件导致上游抽取为空/不完整 |
| 编码错误                 | 尝试多种编码， fallback 到空值                  |

### 错误日志表

```sql
CREATE TABLE extraction_errors (
    stock_code VARCHAR,
    year INTEGER,
    pdf_path VARCHAR,
    error_type VARCHAR,
    error_message TEXT,
    created_at TIMESTAMP
);
```

---

## 7. 质量检查

提取完成后， system 应执行自动化的三层校验逻辑，对结果打标（Flag），而非直接删除。

### 7.1 自动化校验层级

| 层级            | 方法       | 逻辑描述                                         | 动作                         |
| :-------------- | :--------- | :----------------------------------------------- | :--------------------------- |
| **L1 物理校验** | 长度检测   | `len(text) < 1000` 或 `len(text) > 50000`        | 标记 `FLAG_LENGTH_ABNORMAL`  |
| **L2 内容校验** | 关键词锚点 | 文本中不含 `['收入', '利润', '同比']` 中任意两个 | 标记 `FLAG_CONTENT_MISMATCH` |
| **L2 内容校验** | 越界检测   | 文本末尾 500 字出现 `['监事会', '审计报告']`     | 标记 `FLAG_TAIL_OVERLAP`     |
| **L3 时序校验** | 同比突变   | `(今年字数 - 去年字数) / 去年字数 > 50%`         | 标记 `FLAG_YOY_CHANGE_HIGH`  |

### 7.2 质检 SQL 脚本

```sql
-- 检查成功率 (L1 & L2)
SELECT 
    COUNT(*) AS total,
    SUM(CASE WHEN quality_flag IS NULL THEN 1 ELSE 0 END) AS perfect_clean,
    ROUND(SUM(CASE WHEN quality_flag IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS clean_rate
FROM mda_text;

-- 检查异常短文本 (L1)
SELECT stock_code, year, char_count
FROM mda_text
WHERE char_count < 2000
ORDER BY char_count;

-- 检查缺失年份
SELECT stock_code, COUNT(*) AS years
FROM mda_text
GROUP BY stock_code
HAVING years < 2;
```

---

## 8. 实施步骤

- [ ] 创建 `mda_extractor.py` 入口文件
- [ ] 实现 `extract_mda()` 核心函数
- [ ] 添加 Rich 进度条
- [ ] 用 3-5 只熟悉股票做单体测试
- [ ] 批量运行并检查成功率
- [ ] 针对失败案例针对失败案例调整章节名模式

---

## 9. 已知局限

<aside> ⚠️

**不支持的情况**

- 上游未做 OCR 的扫描件（通常会导致抽取文本为空或极短，本工具将其标记为失败）
- 部分老年报格式混乱（2015 年前）
- 章节名非标准命名的年报 </aside>



**优化方向**

若成功率 < 90%，可考虑：

1. 增加章节名模式
2. 基于页码目录定位
3. 对失败案例用 LLM 辅助识别章节边界 

---

## 10. 进阶：自适应迭代与学习机制

为了应对复杂情况，系统采用 **"CPU 快迭代 + LLM 慢学习"** 的分层架构。

### 10.1 层级 1：CPU 启发式迭代 (Heuristic Loop)

在无需外网和 GPU 的情况下，通过多轮尝试提高召回率：

1.  **基于 TOC 的跳跃 (Map & Jump)**:
    *   优先扫描前 20 页寻找目录。
    *   正则匹配 `(管理层讨论|董事会报告).{2,50}(\d+)$`。
    *   若成功提取页码 $P_{start}$ 和下一章页码 $P_{end}$，直接切片 `pages[P_{start}:P_{end}]`。
    *   *优势*：完全避开正文标题的异体字干扰。

2.  **锚点评分竞价 (Anchor Scoring)**:
    *   若全文发现多个“管理层讨论与分析”标题（例如目录出现一次，正文出现一次，页眉出现多次）。
    *   分别以这些位置为起点向后截取 2000 字。
    *   计算**信息熵**或**关键词密度**（Keywords: 营收、同比、风险）。
    *   保留得分最高的那个作为起点。

3.  **动态边界回退 (Boundary Fallback)**:
    *   若找不到标准的“结束标题”（如“重要事项”），则启用**备用结束符**（如“五、”、“第五节”）。
    *   若仍失败，则基于长度截断（Start + 15 pages），并标记 `warning: fuzzy_end`。
### 10.2 层级 2：LLM 辅助规则生成 (Adaptive Learning)

仅当 CPU 策略全部失败（Result is None 或 Score < 0.3）时，触发“慢思考”机制。
1.  **Fast Path (CPU)**: 尝试使用通用正则或已缓存的 `extraction_rules` 进行提取。
2. **Evaluation**: 检查提取结果（是否为空？字符数是否 < 500？）。
3. **Slow Path (LLM)**: 若提取失败，仅提取 PDF 前 20 页（含目录）的纯文本。
4. **Learning**: 调用 LLM (如 DeepSeek/Qwen/GPT-4o-mini) 分析目录结构。
5. **Caching**: 将 LLM 识别出的 `start_pattern` 和 `end_pattern` 存入 DuckDB。
6. **Retry**: 使用新规则重新运行 CPU 提取。

### 10.3 LLM Prompt 示例

```python
PROMPT_TEMPLATE = """
你是一个金融文档分析师。以下是某 A 股上市公司年报的前 20 页文本（包含目录）。
请分析并提取“管理层讨论与分析”章节（或“董事会报告”、“经营情况讨论”）的：
1. 精确起始标题（Start Title）
2. 精确结束标题（End Title，即下一章的标题）

文档片段：
{text_content}

请仅以 JSON 格式返回，格式如下：
{{
    "found": true,
    "start_pattern": "第四节 经营情况讨论与分析",
    "end_pattern": "第五节 重要事项"
}}
如果找不到目录或相关章节，返回 "found": false。
"""
```

### 10.4 成本控制

- **只传目录**：不传整个 PDF，只传前 20 页 or TOC 页面，Token 消耗极低。
- **一次学习，永久使用**：规则按 `stock_code` 缓存。只要该公司不换排版风格，后续年份无需再次调用 LLM。