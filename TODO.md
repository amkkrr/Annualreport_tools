# 项目后续开发计划 (To-Do List)

基于项目现状分析、技术债文档及功能清单，整理如下后续开发计划。本计划旨在提升工具的易用性、稳定性和分析深度。

## 优先级说明

| 标记 | 含义 | 说明 |
|------|------|------|
| `P0` | 阻塞性 | 影响后续所有工作，必须最先完成 |
| `P1` | 高优先级 | 核心功能，尽快完成 |
| `P2` | 中优先级 | 重要但非紧急 |
| `P3` | 低优先级 | 锦上添花，可延后 |

---

## Phase 1: 架构治理 — 配置标准化与数据底座

> 目标：从"文件驱动"转型为"数据驱动"，将 DuckDB 作为全流程（爬虫->下载->提取）的核心枢纽。

### 1.1 统一配置管理

- [x] `P0` 创建 `config.yaml` 模板，集中管理年份、路径、数据库、关键词配置（完成于 2026-01-07）
- [x] `P0` 使用 Pydantic 定义配置类，替代手写验证层（一步实现加载+校验）（完成于 2026-01-07）
- [x] `P1` 改造 `mda_extractor.py` 以支持读取 `config.yaml`（完成于 2026-01-07）
- [x] `P2` 对危险配置增加校验（如输出路径不能为根目录、不能覆盖源文件）（完成于 2026-01-07）

### 1.2 DuckDB 核心化（替代 Excel）

- [x] `P0` 扩展 `annual_report_mda/db.py`，新增全生命周期管理表：（完成于 2026-01-08）
    - `reports`: 存储元数据（URL、发布日期）、下载状态、转换状态、本地路径
    - `companies`: 存储公司基本信息
- [x] `P1` 改造 `1.report_link_crawler.py`：爬取结果直接写入 `reports` 表（支持增量）（完成于 2026-01-08）
- [x] `P1` 改造 `2.pdf_batch_converter.py`：从 `reports` 表读取待处理任务，完成后更新状态（完成于 2026-01-08）

### 1.3 迁移与兼容

- [x] `P0` 编写一次性迁移脚本：`res/AnnualReport_links_2004_2023.xlsx` → DuckDB（完成于 2026-01-08）
- [x] `P1` 保留 Excel 读取兼容层，支持 `--legacy` 模式，便于回滚（完成于 2026-01-08）

### 📍 里程碑 M1: 配置统一 + DuckDB 写入跑通 ✅ (验收通过 2026-01-08)

> 验收报告: [plans/M1_verification_report.md](plans/M1_verification_report.md)

#### 功能验收（必须通过）

| ID | 验收项 | Given | When | Then | 状态 |
|----|--------|-------|------|------|------|
| M1-01 | 配置加载 | `config.yaml` 存在且格式正确 | 任意模块启动时读取配置 | 无报错，配置值正确注入到各模块 | ✅ |
| M1-02 | 配置校验 | `config.yaml` 中路径字段为空或非法 | 模块启动 | 抛出 `ValidationError`，提示具体字段名 | ✅ |
| M1-03 | 爬虫写入 | DuckDB `reports` 表为空 | 执行爬虫爬取 2023 年某行业数据 | `reports` 表新增记录，含 `stock_code`, `url`, `publish_date` | ✅ |
| M1-04 | 增量爬取 | `reports` 表已有 100 条记录 | 再次执行爬虫 | 仅新增不重复的记录，原有记录不变 | ✅ |
| M1-05 | 数据迁移 | Excel 含 N 条记录 | 执行 `python scripts/migrate_excel_to_duckdb.py` | DuckDB 含 N 条记录，必填字段无空值 | ✅ |
| M1-06 | 兼容模式 | 仅有 Excel 文件，无 DuckDB | 使用 `--legacy` 参数运行下载模块 | 功能正常，从 Excel 读取数据 | ✅ |

#### 质量验收（建议通过）

| ID | 验收项 | 指标 | 状态 |
|----|--------|------|------|
| M1-Q1 | 迁移性能 | 5 万条数据迁移耗时 < 60s | ⏳ 待验证 |
| M1-Q2 | 迁移幂等 | 重复执行迁移脚本不产生重复数据 | ✅ |
| M1-Q3 | 配置热加载 | 修改 `config.yaml` 后无需重启即可生效（可选） | ➖ 不适用 |

#### 测试命令

```bash
# 运行 M1 相关测试
pytest tests/test_m1_*.py -v

# 手动验证迁移
python scripts/migrate_excel_to_duckdb.py --dry-run  # 预览
python scripts/migrate_excel_to_duckdb.py            # 执行
duckdb data/annual_reports.duckdb "SELECT COUNT(*) FROM reports"
```

---

## Phase 2: 核心算法 — 结构化提取与质量控制

> 目标：从"正则截取"升级为"结构化解析"，大幅提升提取准确率。

### 2.1 黄金数据集准备（LLM 辅助）

- [ ] `P0` **LLM 初标 + 人工抽检**：使用 Claude/GPT 初步识别 MD&A 边界，人工仅审核/修正 10-20 份高不确定性样本
- [ ] `P1` **LLM-as-Judge 评估器**：用强模型（如 Claude Opus）直接评估提取质量，输出完整性/准确性评分 + 理由
- [ ] `P1` 编写评估脚本，支持 LLM 评分 + 关键特征校验（标志性标题存在、财务表格排除、长度合理）

### 2.2 精准提取策略升级

- [ ] `P1` **目录页解析 (TOC Parsing)**：引入正则提取目录页中的页码范围，作为定位的强校验
- [ ] `P2` **PDF 书签 (Outlines)**：尝试读取 PDF 内置书签定位章节（作为加分项，非主策略）
- [ ] `P2` **字段切分**：尝试将 MD&A 自动切分为"经营回顾"与"未来展望"两个子字段

### 2.3 质量评分闭环

- [ ] `P1` 升级评分器 `scorer.py`，增加负向特征检测（表格残留、页眉干扰、乱码比例等）
- [ ] `P1` 建立反馈机制：数据库记录 `quality_score`，对低分样本标记 `needs_review`

### 📍 里程碑 M2: 全流程 DuckDB 驱动 + 质量评估体系上线 ⏳

> 验收报告: 待创建

#### 功能验收（必须通过）

| ID | 验收项 | Given | When | Then | 状态 |
|----|--------|-------|------|------|------|
| M2-01 | 全流程驱动 | `reports` 表有待下载记录 | 执行下载->转换->提取流程 | 各阶段状态正确更新（`download_status`, `convert_status`, `extract_status`） | ⏳ |
| M2-02 | 断点续传 | 流程中途中断 | 重新执行 | 从中断处继续，已完成的不重复处理 | ⏳ |
| M2-03 | 黄金集评估 | 黄金数据集含 50 份标注 | 执行评估脚本 | 输出 Precision / Recall / F1 报告 | ⏳ |
| M2-04 | 质量评分 | 提取完成 | 查询数据库 | `quality_score` 字段有值（0-100） | ⏳ |
| M2-05 | 低分标记 | `quality_score < 60` | 查询 `needs_review` 字段 | 值为 `true` | ⏳ |
| M2-06 | 负向检测 | 提取文本含表格残留（如连续数字行） | 评分器运行 | 扣分并在 `score_detail` 记录原因 | ⏳ |

#### 质量验收（建议通过）

| ID | 验收项 | 指标 | 状态 |
|----|--------|------|------|
| M2-Q1 | 提取准确率 | 黄金集 F1 ≥ 0.85 | ⏳ |
| M2-Q2 | 提取性能 | 单份年报提取耗时 < 5s（不含 PDF 加载） | ⏳ |
| M2-Q3 | 评分一致性 | 相同文本多次评分结果一致 | ⏳ |

#### 测试命令

```bash
# 运行 M2 相关测试
pytest tests/test_m2_*.py -v

# 评估黄金集
python scripts/evaluate_extraction.py --gold-set data/golden_set.json

# 查询低分样本
duckdb data/annual_reports.duckdb "SELECT stock_code, year, quality_score FROM mda WHERE needs_review = true"
```

---

## Phase 2.5: MD&A 提取器增强 — mda_extractor.py

> 目标：完善 MD&A 提取器的测试覆盖、质检能力和 LLM 自适应学习功能。

### 2.5.1 端到端测试验证（高优先）

- [ ] `P1` 编写 `calculate_mda_score` 单元测试，覆盖各评分维度
- [ ] `P1` 编写 `extract_mda_iterative` 单元测试，验证迭代策略逻辑
- [ ] `P1` 创建集成测试：端到端跑测 + 增量逻辑验证
- [ ] `P1` 准备 Mock 数据集：含/不含目录、含/不含页分隔符的样本

### 2.5.2 质检增强

- [ ] `P1` 实现 L3 时序校验（FLAG_YOY_CHANGE_HIGH），检测年际变化异常
- [ ] `P1` 创建质检 SQL 视图 `mda_text_latest`，简化查询流程

### 2.5.3 文档与分支管理

- [ ] `P1` 更新主 README.md，补充 `mda_extractor.py` 使用说明
- [ ] `P2` 编写开发者指南，说明策略扩展方式
- [ ] `P1` 合并 `feature/mda-extractor-spec` 分支到 `main`（或在 main 继续开发）

### 2.5.4 LLM 自适应学习（规格书 §10）

> 核心思路：通过上下文工程实现等效"学习"能力，无需微调模型。

**架构：提取主循环**
```
输入年报 → 检索相似成功案例 → 构建 prompt
    ↓
多策略提取 → LLM-as-Judge 评分
    ↓
评分 ≥ 阈值?
  ├─ 是 → 存入成功样本库 → 输出
  └─ 否 → Self-Refine → 重试(最多N次)
             ↓
        仍失败 → 记录失败模式 → 人工队列
```

**子任务：**

- [ ] `P1` **Self-Refine 模式**：提取 → 自我评估（遗漏/噪音检测）→ 修正重提取
- [ ] `P2` **动态 Few-shot 样本库**：成功案例向量化存储，新任务时检索相似行业/年份案例作为示例
- [ ] `P2` **策略权重自适应**：记录各策略成功率，动态调整选择权重
- [ ] `P2` **失败模式学习**：分析失败原因 → 生成排除规则 → 加入负面 prompt
- [ ] `P2` 实现 LLM API 客户端，支持 DeepSeek/Qwen/GPT-4o-mini/Claude
- [ ] `P2` 设计 Prompt 模板：分析目录结构，提取 start/end pattern
- [ ] `P2` 实现规则写入 `extraction_rules` 表的逻辑
- [ ] `P2` 添加 `--learn` 参数支持，启用自适应学习模式
- [ ] `P2` 实现失败熔断与降级机制（API 调用失败时回退到 CPU 策略）

### 📍 里程碑 M2.5: MD&A 提取器测试与质检完备 ⏳

> 验收报告: 待创建

#### 功能验收（必须通过）

| ID | 验收项 | Given | When | Then | 状态 |
|----|--------|-------|------|------|------|
| M2.5-01 | 评分单测 | 测试用例准备完毕 | 执行 `pytest tests/test_scorer.py` | 所有测试通过，覆盖率 ≥ 80% | ⏳ |
| M2.5-02 | 提取单测 | Mock 数据准备完毕 | 执行 `pytest tests/test_extractor.py` | 含/不含目录样本均正确处理 | ⏳ |
| M2.5-03 | 集成测试 | 测试数据库初始化 | 执行端到端测试 | 增量逻辑正确，已处理文件不重复提取 | ⏳ |
| M2.5-04 | 时序校验 | 同一公司连续两年 MDA 文本 | 执行质检 | 正确识别 FLAG_YOY_CHANGE_HIGH | ⏳ |
| M2.5-05 | README 更新 | 文档修改完成 | 阅读 README.md | 包含 mda_extractor.py 的完整使用说明 | ⏳ |

#### 质量验收（建议通过）

| ID | 验收项 | 指标 | 状态 |
|----|--------|------|------|
| M2.5-Q1 | 单测覆盖率 | `annual_report_mda/` 模块覆盖率 ≥ 70% | ⏳ |
| M2.5-Q2 | 测试执行时间 | 全部单测运行 < 30s | ⏳ |
| M2.5-Q3 | 文档完整性 | README 包含快速开始、参数说明、输出格式 | ⏳ |

#### 测试命令

```bash
# 运行 M2.5 相关测试
pytest tests/test_mda_*.py -v

# 运行评分器单测
pytest tests/test_scorer.py -v --cov=annual_report_mda.scorer

# 运行提取器单测
pytest tests/test_extractor.py -v --cov=annual_report_mda.strategies

# 端到端集成测试
pytest tests/test_mda_integration.py -v
```

---

## Phase 3: 分析赋能 — NLP 深度挖掘

> 目标：基于清洗后的高质量数据，进行深度价值挖掘。

### 3.1 下游分析模块

- [ ] `P1` 创建 `4.advanced_analysis.py`，直接从 DuckDB 读取已清洗文本
- [ ] `P1` **情感分析 (Sentiment)**：使用金融领域情感词典（如知网 HowNet 金融词典）计算积极/消极得分
- [ ] `P2` **相似度分析 (Similarity)**：计算同一公司连续两年的文本相似度，量化战略变化
- [ ] `P2` **主题模型 (LDA)**：自动发现年报中的潜在主题聚类

### 📍 里程碑 M3: NLP 分析模块可用 ⏳

> 验收报告: 待创建

#### 功能验收（必须通过）

| ID | 验收项 | Given | When | Then | 状态 |
|----|--------|-------|------|------|------|
| M3-01 | 情感分析 | MDA 表有已清洗文本 | 执行情感分析 | 输出 `sentiment_score`（-1 到 1），结果写入 DuckDB | ⏳ |
| M3-02 | 情感词典 | 使用金融领域词典 | 分析含"亏损"、"增长"的文本 | "亏损"为负向，"增长"为正向 | ⏳ |
| M3-03 | 相似度分析 | 同一公司有 2022、2023 两年数据 | 执行相似度分析 | 输出相似度分数（0-1），支持指定公司和年份 | ⏳ |
| M3-04 | LDA 主题 | MDA 表有 ≥ 100 份文本 | 执行 LDA 分析 | 输出 Top-10 主题词列表及文档-主题分布矩阵 | ⏳ |
| M3-05 | 结果持久化 | 分析完成 | 查询数据库 | `sentiment_score`, `similarity_score` 字段有值 | ⏳ |

#### 质量验收（建议通过）

| ID | 验收项 | 指标 | 状态 |
|----|--------|------|------|
| M3-Q1 | 情感分析性能 | 1000 份年报情感分析耗时 < 60s | ⏳ |
| M3-Q2 | 主题可解释性 | Top-10 主题中 ≥ 7 个可被人类理解（主观评估） | ⏳ |
| M3-Q3 | 相似度合理性 | 同公司相邻年份相似度 > 跨公司相似度均值 | ⏳ |

#### 测试命令

```bash
# 运行 M3 相关测试
pytest tests/test_m3_*.py -v

# 情感分析
python 4.advanced_analysis.py sentiment --year 2023 --output results/sentiment_2023.csv

# 相似度分析
python 4.advanced_analysis.py similarity --stock-code 600519 --years 2022,2023

# LDA 主题分析
python 4.advanced_analysis.py lda --n-topics 10 --output results/lda_topics.json
```

---

## Phase 4: 工程化与自动化

> 目标：提升代码质量、可维护性和运维便利性。

### 4.1 CI/CD 流水线建设

- [ ] `P1` 配置 GitHub Actions，实现代码提交后的自动 Lint 检查（`ruff` / `black`）
- [ ] `P1` 编写核心功能的单元测试（爬虫解析逻辑、PDF 清洗逻辑、评分器）
- [ ] `P2` 集成测试覆盖率报告（`pytest-cov`）

### 4.2 日志系统完善

- [ ] `P1` 引入 Python 标准 `logging` 模块，替代 `print` 输出
- [ ] `P2` 统一使用 `RichHandler` 美化终端输出
- [ ] `P2` 实现日志文件轮转（`RotatingFileHandler`），按天或按大小保存

### 4.3 界面与交互

- [ ] `P3` 调研 Streamlit 开发参数配置与监控界面（WebUI）

### 📍 里程碑 M4: 工程化基础设施完备 ⏳

> 验收报告: 待创建

#### 功能验收（必须通过）

| ID | 验收项 | Given | When | Then | 状态 |
|----|--------|-------|------|------|------|
| M4-01 | Lint 自动化 | 提交含格式问题的代码 | Push 到 GitHub | Actions 运行失败，输出具体问题行号 | ⏳ |
| M4-02 | 测试自动化 | 提交代码 | Push 到 GitHub | 自动运行 `pytest`，失败时阻止合并 | ⏳ |
| M4-03 | 日志输出 | 任意模块运行 | 查看终端输出 | 使用统一格式（时间戳 + 级别 + 模块名 + 消息） | ⏳ |
| M4-04 | 日志文件 | 模块运行超过 1 天 | 检查日志目录 | 按天生成独立日志文件（如 `app_2024-01-15.log`） | ⏳ |
| M4-05 | 日志轮转 | 日志文件超过 10MB | 检查日志目录 | 自动归档旧日志，保留最近 7 个文件 | ⏳ |

#### 质量验收（建议通过）

| ID | 验收项 | 指标 | 状态 |
|----|--------|------|------|
| M4-Q1 | 测试覆盖率 | 核心模块覆盖率 ≥ 60% | ⏳ |
| M4-Q2 | CI 耗时 | 完整 CI 流程 < 5 分钟 | ⏳ |
| M4-Q3 | Lint 规则 | 零 `ruff` 警告（忽略规则需在 `pyproject.toml` 显式声明） | ⏳ |

#### 测试命令

```bash
# 本地运行 Lint
ruff check . --fix
black . --check

# 本地运行测试 + 覆盖率
pytest --cov=annual_report_mda --cov-report=html

# 验证日志轮转配置
python -c "from annual_report_mda.logging_config import setup_logging; setup_logging()"
ls -la logs/
```

---

## Phase 5: 长期规划

> 目标：为未来扩展预留接口。

### 5.1 云端集成

- [ ] `P3` 支持对象存储（AWS S3 / 阿里云 OSS），将 PDF/TXT 文件存入云端

### 5.2 API 服务化

- [ ] `P3` 评估使用 FastAPI 将核心功能封装为 RESTful API，支持外部系统调用

---

## 里程碑总览

| 里程碑 | 核心交付物 | 依赖 | 验收用例数 | 状态 |
|--------|-----------|------|-----------|------|
| **M1** | 配置统一 + DuckDB 写入跑通 | - | 6 功能 + 3 质量 | ✅ 通过 |
| **M2** | 全流程 DuckDB 驱动 + 质量评估体系 | M1 | 6 功能 + 3 质量 | ⏳ 待验收 |
| **M2.5** | MD&A 提取器测试与质检完备 | M2 | 5 功能 + 3 质量 | ⏳ 待验收 |
| **M3** | NLP 分析模块可用 | M2.5 | 5 功能 + 3 质量 | ⏳ 待验收 |
| **M4** | CI/CD + 日志系统 | 可与 M1-M3 并行 | 5 功能 + 3 质量 | ⏳ 待验收 |

```
M1 ──────► M2 ──────► M2.5 ──────► M3
                        ↑
M4 (可并行) ────────────┘
```

---

## 验收标准制定原则

本文档验收标准遵循以下原则：

1. **SMART 原则**：具体（Specific）、可度量（Measurable）、可达成（Achievable）、相关（Relevant）
2. **Given-When-Then 格式**：明确前置条件、触发动作、预期结果
3. **分层验收**：功能验收（必须通过）+ 质量验收（建议通过）
4. **可执行化**：每个里程碑附带测试命令，验收标准可直接转化为自动化测试
