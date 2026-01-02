
基于**已完成文本抽取**的 A 股年报 MD&A 章节提取工具，纯 CPU 运行，无需 GPU。

> 说明：PDF→文本 的抽取环节由上游工具完成；本工具只负责**在已抽取的文本中定位并截取**「管理层讨论与分析」（MD&A）章节。

---

## 1. 目标与约束

### 目标

- 从年报**已抽取文本**中提取「管理层讨论与分析」（MD&A）章节全文
- 输出结构化文本，存入 `data/annual_reports.duckdb`

### 约束

- **纯 CPU 环境**：不依赖 GPU 或深度学习模型
- **依赖上游文本质量**：本工具不负责 OCR；若上游输出为空/严重缺失，则视为失败（记录原因，需 OCR 才能处理）
- **批量处理**：支持 1000+ 文件的并发处理（并发调度与 DuckDB 写入由现成代理负责，见第 5.3）

---

## 2. 依赖库

```bash
pip install rich duckdb python-dotenv
```

| 库                | 用途                                       |
| ----------------- | ------------------------------------------ |
| **rich**          | 进度条与日志                               |
| **duckdb**        | 结果存储                                   |
| **python-dotenv** | 从 `.env` 加载 LLM 密钥/配置（可选但推荐） |

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
MAX_PAGES_DEFAULT = 15
MAX_CHARS_DEFAULT = 120_000

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
    # 扫描前 15 页，寻找 "管理层...15" 结构的行；若解析到目录页码，需先映射到 page_index_start/page_index_end
    toc_hit = parse_toc_for_page_range(pages_text[:15]) 
    if toc_hit:
        text = extract_by_pages(
            pages_text,
            toc_hit.page_index_start,
            toc_hit.page_index_end,
            max_pages=MAX_PAGES_DEFAULT,
            max_chars=MAX_CHARS_DEFAULT,
        )
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
  - 基于“跨页重复行”去重：统计每页首/末 1-2 行的高频文本，抽取时对这些行做过滤（不依赖 PDF 版面信息）
  - 对“像标题”的命中加约束：独立一行/前缀序号（如“第X节”）/长度上限/非目录引导线

### 3.4 规则优先级与冲突裁决（确定性约束）

为保证同一份报告在不同运行中产出一致结果，需在实现中写死以下优先级与兜底：

1. **起始边界优先级**：`extraction_rules`（匹配到且通过目录/正文判定） > TOC 命中 > 正文标题命中 > 放宽正则。
2. **结束边界优先级**：TOC 的“下一章标题” > `NEXT_TITLES`（需“像标题”）> 备用结束符（如“第X节/第X章”）> 截断。
3. **标题命中需满足“像标题”**（避免目录与页眉误触发）：尽量要求独立一行、长度上限、可带序号前缀、非引导线（`……`/`...`）密集行。
4. **截断规则**：当 end 未找到或超过阈值时，以 `max_pages/max_chars` 截断，并写入 `is_truncated=true` 与 `truncation_reason`；截断后仍需执行质检评分，失败则标记质量 Flag（不静默当成功）。

---

## 4. 数据结构

### 4.1 输入

本工具只消费上游产出的**单文件文本**（`*.txt`），不直接解析 PDF。

```
data/annual_reports_text/
├── 000778/
│   ├── 2022.txt
│   ├── 2023.txt
│   └── 2024.txt
└── 600519/
    └── ...
```

#### `*.txt` 格式约定（最低可实现契约）

上游输出为单个文本文件，但必须携带“页边界”信息，便于 TOC 优先策略按页切片与落库页范围字段。

推荐的页分隔符（按优先级）：

1. **ASCII Form Feed**：`\f`（大多数 PDF→Text 工具会在换页处插入）
2. 明确分隔行：如 `===== Page 12 =====` / `--- Page 12 ---`（需全量一致）

实现上，工具应按上述规则将 `*.txt` 解析为 `pages_text: list[str]`，并以 `page_index`（0-based）作为唯一可信页序。
若无法识别页分隔符，则将全文视为单页（`page_index=0`），并强制关闭 TOC→页范围切片（仅保留正文扫描与长度阈值兜底），同时写入质量 Flag（例如 `FLAG_PAGE_BOUNDARY_MISSING`）。

页码语义约定：

- `page_index`：由 `*.txt` 切分出的页序下标（从 0 开始），用于所有切片与落库的**唯一可信页序**。
- `source_sha256`：建议对输入 `*.txt` 文件内容计算，作为幂等键与增量判定基础。
- `printed_page_number`：目录解析出的“印刷页码/书页页码”，年报常与 PDF 物理页不一致；由于输入为纯文本，通常只能通过目录行解析得到（可为空）。
- TOC 策略若解析到印刷页码：必须先做“印刷页码 → page_index”的映射；映射失败则降级为正文扫描（不得盲切片）。

### 4.2 输出（DuckDB 表）

```sql
-- 存储提取结果（建议加入可观测字段，便于复核与质检）
CREATE TABLE mda_text (
    stock_code VARCHAR,
    year INTEGER,
    mda_raw TEXT,
    char_count INTEGER,

    -- 页范围语义：均为 pages[].page_index（0-based），end 为“开区间”（含 start，不含 end）
    page_index_start INTEGER,
    page_index_end INTEGER,
    page_count INTEGER,
    -- 可选：目录解析出的印刷页码（若可靠）
    printed_page_start INTEGER,
    printed_page_end INTEGER,

    hit_start VARCHAR,          -- 命中的标题/正则（含 hit_kind）
    hit_end VARCHAR,
    is_truncated BOOLEAN,       -- 是否触发 max_pages 截断
    truncation_reason VARCHAR,  -- 'max_pages' | 'max_chars' | 'end_not_found' | NULL

    -- 质量标注：数组形态，允许多个 Flag
    quality_flags JSON,         -- e.g. ["FLAG_LENGTH_ABNORMAL","FLAG_TAIL_OVERLAP"]
    quality_detail JSON,

    source_path VARCHAR,
    source_sha256 VARCHAR,      -- 用于增量/复跑校验
    extractor_version VARCHAR,
    extracted_at TIMESTAMP,
    used_rule_type VARCHAR,    -- 'generic' 或 'custom'
    PRIMARY KEY (stock_code, year, source_sha256)
);

-- 存储自适应规则（学习到的知识）
CREATE TABLE extraction_rules (
    stock_code VARCHAR,
    year INTEGER,
    report_signature VARCHAR,   -- 可选：目录章节序列/标题序列哈希，用于版式变体区分
    start_pattern VARCHAR,     -- 学习到的起始标题，如 "第三节 董事会报告"
    end_pattern VARCHAR,       -- 学习到的结束标题
    rule_source VARCHAR,       -- 'llm_learned' 或 'manual'
    updated_at TIMESTAMP,
    PRIMARY KEY (stock_code, year)
);
```

---

## 5. CLI 接口

```bash
# 单文件测试（输入为“已抽取文本”，需包含页边界信息）
python mda_extractor.py --text data/annual_reports_text/000778/2023.txt

# 批量提取（指定目录：目录下存放已抽取文本）
python mda_extractor.py --dir data/annual_reports_text/ --workers 4

# 开启自适应学习模式（需要配置 LLM API；密钥放在 .env 或环境变量中，避免命令行明文）
python mda_extractor.py --dir data/annual_reports_text/ --learn
```

### 参数说明

| 参数            | 说明                                                               |
| --------------- | ------------------------------------------------------------------ |
| `--text`        | 单文件模式：上游抽取的单个 `*.txt`（需可切分出页边界，约定见 4.1） |
| `--dir`         | 批量模式：扫描目录下所有 `*.txt`                                   |
| `--workers`     | 并发数，默认 4                                                     |
| `--incremental` | 增量模式：仅当同一份输入（`source_sha256`）已成功入库时跳过        |
| `--dry-run`     | 仅统计，不写入数据库                                               |
| `--db`          | 数据库路径，默认 `data/annual_reports.duckdb`                      |

### LLM 配置（可选，推荐使用 `.env`）

- 不使用 `--api-key` 等命令行明文参数，避免 shell history / 进程列表泄露密钥。
- 推荐在项目根目录放置 `.env`（权限建议 600），由 `python-dotenv` 读取。

示例：

```bash
cat > .env <<'EOF'
LLM_PROVIDER=deepseek
LLM_API_KEY=sk-...
LLM_MODEL=deepseek-chat
LLM_TIMEOUT_SEC=30
LLM_MAX_RETRIES=2
EOF
```

### 增量与幂等（`--incremental` 口径）

- 同一份输入以 `(stock_code, year, source_sha256)` 作为幂等键；重复运行应对该键做 upsert（结果一致、无重复行）。
- `--incremental`：当且仅当该幂等键已存在且“上一轮成功”（例如 `mda_raw` 非空且 `char_count >= 500`）时跳过；若存在但失败/为空则允许重跑覆盖。
- 若同一 `stock_code+year` 出现不同 `source_sha256`（更正版/不同来源）：应插入为新版本，不覆盖旧版本，便于审计与回溯。

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
    source_path VARCHAR,
    source_sha256 VARCHAR,
    error_type VARCHAR,
    error_message TEXT,
    provider VARCHAR,
    http_status INTEGER,
    trace_id VARCHAR,
    created_at TIMESTAMP
);
```

---

## 7. 质量检查

提取完成后， system 应执行自动化的三层校验逻辑，对结果打标（Flag），而非直接删除。

### 7.1 自动化校验层级

| 层级            | 方法       | 逻辑描述                                         | 动作                              |
| :-------------- | :--------- | :----------------------------------------------- | :-------------------------------- |
| **L1 物理校验** | 格式检测   | 无法识别页分隔符，导致 `pages_text` 只能视为单页 | 标记 `FLAG_PAGE_BOUNDARY_MISSING` |
| **L1 物理校验** | 长度检测   | `len(text) < 1000` 或 `len(text) > 50000`        | 标记 `FLAG_LENGTH_ABNORMAL`       |
| **L2 内容校验** | 关键词锚点 | 文本中不含 `['收入', '利润', '同比']` 中任意两个 | 标记 `FLAG_CONTENT_MISMATCH`      |
| **L2 内容校验** | 越界检测   | 文本末尾 500 字出现 `['监事会', '审计报告']`     | 标记 `FLAG_TAIL_OVERLAP`          |
| **L3 时序校验** | 同比突变   | `(今年字数 - 去年字数) / 去年字数 > 50%`         | 标记 `FLAG_YOY_CHANGE_HIGH`       |

### 7.2 质检 SQL 脚本

```sql
-- 以“同公司同年最新一条”为准（避免多版本 source_sha256 干扰统计）
CREATE OR REPLACE VIEW mda_text_latest AS
SELECT *
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY stock_code, year ORDER BY extracted_at DESC) AS rn
    FROM mda_text
) t
WHERE rn = 1;

-- 检查成功率 (L1 & L2)
SELECT 
    COUNT(*) AS total,
    SUM(CASE WHEN quality_flags IS NULL THEN 1 ELSE 0 END) AS perfect_clean,
    ROUND(SUM(CASE WHEN quality_flags IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS clean_rate
FROM mda_text_latest;

-- 检查异常短文本 (L1)
SELECT stock_code, year, char_count
FROM mda_text_latest
WHERE char_count < 2000
ORDER BY char_count;

-- 检查缺失年份
SELECT stock_code, COUNT(DISTINCT year) AS years
FROM mda_text_latest
GROUP BY stock_code
HAVING years < 2;
```

---

## 8. MVP 行动计划与编码清单

### 8.1 Phase 1: 基础设施与核心脚手架 (Day 1)
目标：跑通 "Hello World"，建立输入输出管道。

- [ ] **项目初始化**
    - [ ] 创建核心包目录 `annual_report_mda/` (用于存放具体逻辑，保持根目录整洁)
    - [ ] 编写 `requirements.txt` (rich, duckdb, python-dotenv)
    - [ ] 创建 `annual_report_mda/utils.py` (日志配置, 数据库连接单例)
- [ ] **数据层实现**
    - [ ] 编写 `annual_report_mda/db.py`: 实现 `init_db()` 创建 DuckDB 表 (`mda_text`, `extraction_rules`, `extraction_errors`)
    - [ ] 编写 `annual_report_mda/data_manager.py`: 处理数据的 upsert 和幂等检查 (`source_sha256`)
- [ ] **CLI 入口**
    - [ ] 创建根目录入口文件 `mda_extractor.py` (作为调用入口，不带序号)
    - [ ] 使用 `argparse` 实现命令行参数解析 (`--text`, `--dir`, `--workers`, `--incremental`, `--db`)
    - [ ] 搭建多进程框架 (`ProcessPoolExecutor`)

### 8.2 Phase 2: 核心提取逻辑 (Day 1-2)
目标：实现 CPU 提取算法，完成“定位+截取+评分”闭环。

- [ ] **文本预处理**
    - [ ] 实现 `TextLoader` 类: 读取 `*.txt`，识别页边界符 (`\f`, `=== Page X ===`)，生成 `List[str]` 页列表
    - [ ] 实现 `clean_text()`: 基础清洗
- [ ] **特征工程**
    - [ ] 实现 `calculate_mda_score(text)`: 关键词密度评分
    - [ ] 定义 `MDA_TITLES` 和 `MDA_PATTERNS` 常量
- [ ] **提取策略实现**
    - [ ] 实现 `Strategy 1: TOC Parsing`: 扫描前 15 页，正则匹配目录行，解析页码映射
    - [ ] 实现 `Strategy 2: Body Scan`: 正则搜索正文标题 (Start/End Patterns)
    - [ ] 实现 `extract_mda_iterative()`: 整合策略，加入择优逻辑
- [ ] **结果封装**
    - [ ] 构造提取结果对象，包含 `mda_raw`, `page_range`, `score` 等

### 8.3 Phase 3: 质量控制与容错 (Day 2)
目标：增加结果的可靠性，处理异常情况。

- [ ] **质检模块**
    - [ ] 实现 L1 校验: 长度检测 (<500字), 页边界丢失标记
    - [ ] 实现 L2 校验: 关键词缺失检测
    - [ ] 实现截断处理: `MAX_PAGES`/`MAX_CHARS` 限制逻辑
- [ ] **错误处理**
    - [ ] 全局异常捕获装饰器
    - [ ] 错误日志写入 DuckDB `extraction_errors` 表
- [ ] **自适应学习 (基础版)**
    - [ ] 预留 `ExtractionRules` 查询接口 (暂不接入 LLM，仅查表)

### 8.4 Phase 4: 测试与验证 (Day 3)
目标：验证准确率，准备交付。

- [ ] **单元测试**
    - [ ] 构造 Mock 文本数据 (含/不含目录，含/不含页分隔符)
    - [ ] 测试 `calculate_mda_score`
    - [ ] 测试 `extract_mda_iterative`
- [ ] **集成测试**
    - [ ] 选取 5-10 个真实样本进行端到端跑测
    - [ ] 验证 `--incremental` 增量逻辑
    - [ ] 验证 DuckDB 数据落盘正确性
- [ ] **文档**
    - [ ] 更新 `README.md` 使用说明
    - [ ] 编写简单的开发者指南

---

## 9. 已知局限


**不支持的情况**

- 上游未做 OCR 的扫描件（通常会导致抽取文本为空或极短，本工具将其标记为失败）
- 部分老年报格式混乱（2015 年前）
- 章节名非标准命名的年报 


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
    *   若成功提取到“印刷页码” $P_{start}/P_{end}$：先映射到 `page_index_start/page_index_end`（基于 `printed_page_number` 或可用的映射表）；映射失败则降级为正文扫描。
    *   若映射成功：切片 `pages[page_index_start:page_index_end]`。
    *   *优势*：完全避开正文标题的异体字干扰。

2.  **锚点评分竞价 (Anchor Scoring)**:
    *   若全文发现多个“管理层讨论与分析”标题（例如目录出现一次，正文出现一次，页眉出现多次）。
    *   分别以这些位置为起点向后截取 2000 字。
    *   计算**信息熵**或**关键词密度**（Keywords: 营收、同比、风险）。
    *   保留得分最高的那个作为起点。

3.  **动态边界回退 (Boundary Fallback)**:
    *   若找不到标准的“结束标题”（如“重要事项”），则启用**备用结束符**（如“五、”、“第五节”）。
    *   若仍失败，则基于阈值截断（`max_pages/max_chars`），并落库 `is_truncated=true` 与 `truncation_reason='end_not_found'`。
### 10.2 层级 2：LLM 辅助规则生成 (Adaptive Learning)

仅当 CPU 策略全部失败（Result is None 或 Score < 0.3）时，触发“慢思考”机制。
1.  **Fast Path (CPU)**: 尝试使用通用正则或已缓存的 `extraction_rules` 进行提取。
2. **Evaluation**: 检查提取结果（是否为空？字符数是否 < 500？）。
3. **Slow Path (LLM)**: 若提取失败，仅提取解析后的前 20 页（含目录）的纯文本片段（来自输入 `*.txt` 的页切分结果）。
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

### 10.4 LLM 安全、合规与失败降级（最低要求）

- **密钥管理**：仅支持环境变量/`.env`/交互式输入读取；禁止将密钥写入日志与命令行参数。
- **调用参数**：至少支持 `timeout`、`max_retries`、并发上限与失败熔断；LLM 失败不得阻塞主流程（返回“需人工复核/CPU 兜底结果”）。
- **记录最小化**：`extraction_rules` 仅存 `start_pattern/end_pattern` 等规则；不得落库保存发送给 LLM 的原始全文片段；错误表仅记录 provider/状态码/trace_id 等诊断信息。

### 10.5 成本控制

- **只传目录**：不传整个 PDF，只传前 20 页 or TOC 页面，Token 消耗极低。
- **学习缓存可控**：规则至少按 `stock_code+year` 缓存；若启用 `report_signature`，可进一步区分“同公司不同版式”，避免跨年误用规则。
