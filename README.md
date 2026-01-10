<div align="center">
  <img src="https://raw.githubusercontent.com/legeling/Annualreport_tools/main/imgs/icon.svg" width="96" alt="Annualreport Tools Icon" />
  <h1>Annualreport_tools · 年报工具集</h1>
  <p>快速抓取巨潮资讯年报、批量下载PDF、转换为TXT，并进行关键词分析。</p>
  <p>
    <a href="https://github.com/legeling/Annualreport_tools/stargazers"><img src="https://img.shields.io/github/stars/legeling/Annualreport_tools?style=flat-square" alt="GitHub Stars"/></a>
    <a href="https://github.com/legeling/Annualreport_tools/network/members"><img src="https://img.shields.io/github/forks/legeling/Annualreport_tools?style=flat-square" alt="GitHub Forks"/></a>
    <a href="https://github.com/legeling/Annualreport_tools/watchers"><img src="https://img.shields.io/github/watchers/legeling/Annualreport_tools?style=flat-square" alt="GitHub Watchers"/></a>
    <img src="https://img.shields.io/badge/Python-3.10%2B-3776AB?style=flat-square&logo=python&logoColor=white" alt="Python Version"/>
    <a href="https://github.com/legeling/Annualreport_tools/issues"><img src="https://img.shields.io/github/issues/legeling/Annualreport_tools?style=flat-square" alt="GitHub Issues"/></a>
  </p>
</div>

<p align="center">
  <a href="./docs/README.en.md">English</a> ·
  <a href="./docs/README.zh.md">简体中文</a>
</p>

---

## 📌 仓库说明

> **注意：** 本仓库是 [legeling/Annualreport_tools](https://github.com/legeling/Annualreport_tools) 的一个分支（Fork）。原仓库提供了完整的年报数据采集和分析工具链，本分支在此基础上进行了个性化调整和优化。
>
> - **上游仓库（原仓库）：** [https://github.com/legeling/Annualreport_tools](https://github.com/legeling/Annualreport_tools)
> - **贡献方式：** 欢迎通过 Pull Request 向上游仓库贡献代码和改进建议

---

## 免责声明

**重要提示：**

- 本项目**仅供学习研究使用**，请勿用于任何违法违规的爬虫行为、商业转售或其他违反法律法规的活动。
- 请**优先使用已整理好的网盘数据集**（`./res/AnnualReport_links_2004_2023.xlsx`），该文件已包含下载好的年报链接。避免频繁访问巨潮资讯服务器，尊重源站资源与相关监管要求。
- **限速至关重要**：爬虫实现了按天分片的机制以最小化服务器负载。请不要修改代码以增加请求频率。
- 您对使用这些脚本触发的任何数据收集行为**负全部责任**。作者不对滥用行为承担任何责任。
- 使用本工具集即表示您已阅读并同意本免责声明。

## 核心功能

1. **report_link_crawler.py** – 按板块/行业分段查询巨潮资讯，在速率限制下保持稳定。
2. **pdf_batch_converter.py** – 具有MIME验证的鲁棒PDF下载器 + 转换为TXT。
3. **text_analysis.py** – 多进程关键词分析器 + Excel导出。
4. **text_analysis_universal.py** – 接受任意TXT目录的轻量级分析器。
5. **mda_extractor.py** – 从年报文本中提取「管理层讨论与分析」(MD&A) 章节，支持多策略迭代提取。
6. **资源文件（`/res`）** – 精选的年报主表和文档图标资源。
7. **文档文件夹** – 存储在`docs/`下的双语文档，方便切换。

## 快速开始

1. 安装依赖：`pip install -r requirements.txt`
2. 运行 `1.report_link_crawler.py`（或复用 `./res/AnnualReport_links_2004_2023.xlsx`）准备年报链接。
3. 执行 `2.pdf_batch_converter.py` 下载PDF并转换为TXT；可选择之后删除原始PDF。
4. 启动 `3.text_analysis.py`（多进程）或 `text_analysis_universal.py` 生成Excel中的关键词总计和总词数。
5. 查看 [Wiki](https://github.com/legeling/Annualreport_tools/wiki) 或 `docs/` 获取特定语言的详细教程。

## 模块概览

| 脚本/资源                                 | 说明                                      |
| ----------------------------------------- | ----------------------------------------- |
| `1.report_link_crawler.py`                | 带板块/行业过滤器和重试逻辑的巨潮资讯爬虫，支持 DuckDB/Excel 双模式 |
| `2.pdf_batch_converter.py`                | 批量下载 + pdfplumber转换，带文件验证，支持 DuckDB 任务队列     |
| `3.text_analysis.py`                      | 多进程关键词分析，Excel导出               |
| `text_analysis_universal.py`              | 适用于任意TXT文件夹的轻量级分析器         |
| `mda_extractor.py`                        | MD&A 章节提取器，支持批量处理与增量模式   |
| `scripts/migrate_excel_to_duckdb.py`      | Excel → DuckDB 数据迁移脚本               |
| `scripts/llm_annotate.py`                 | LLM 辅助标注工具，生成黄金数据集          |
| `scripts/evaluate_extraction.py`          | 提取质量评估，支持规则+LLM 双模式         |
| `scripts/auto_improve.py`                 | 自动分析失败模式并生成改进建议            |
| `./res/AnnualReport_links_2004_2023.xlsx` | 涵盖2004-2023年的精选主表                 |

## 脚本索引（旧版编号）

1. `1.report_link_crawler.py`（原 `1.年报链接抓取.py`）
2. `2.pdf_batch_converter.py`（原 `2.PDF转码.py`）
3. `3.text_analysis.py`（原 `3.文本分析.py`）
4. `text_analysis_universal.py`（原 `文本分析-universal.py`）
5. `mda_extractor.py` — MD&A 章节提取器（新增）

## MD&A 提取器使用说明

`mda_extractor.py` 用于从已转换的年报文本 (`*.txt`) 中提取「管理层讨论与分析」章节，采用 **TOC 解析 + 正文扫描 + 质量评分** 的多策略迭代方法。

### 基本用法

```bash
# 单文件模式
python mda_extractor.py --text data/annual_reports_text/000778/2023.txt

# 批量模式（递归扫描目录）
python mda_extractor.py --dir data/annual_reports_text/ --workers 4

# 增量模式（跳过已成功入库的文件）
python mda_extractor.py --dir data/annual_reports_text/ --incremental

# 仅统计，不写入数据库
python mda_extractor.py --dir data/annual_reports_text/ --dry-run
```

### 参数说明

| 参数            | 说明                                          |
| --------------- | --------------------------------------------- |
| `--text`        | 单文件模式：指定单个 `*.txt` 文件             |
| `--dir`         | 批量模式：递归扫描目录下所有 `*.txt`          |
| `--db`          | 数据库路径，默认 `data/annual_reports.duckdb` |
| `--workers`     | 并发进程数，默认 4                            |
| `--incremental` | 增量模式：已成功入库的文件自动跳过            |
| `--dry-run`     | 仅执行提取，不写入数据库                      |
| `--max-pages`   | 最大页数截断，默认 15                         |
| `--max-chars`   | 最大字符截断，默认 120,000                    |
| `--stock-code`  | 手动指定股票代码（覆盖自动解析）              |
| `--year`        | 手动指定年份（覆盖自动解析）                  |
| `--log-level`   | 日志级别：DEBUG / INFO / WARNING / ERROR      |

### 输入要求

- 输入文件为上游 `pdf_batch_converter.py` 产出的 `*.txt`
- 文件名或目录结构需包含 6 位股票代码和 4 位年份，如 `600519_贵州茅台_2023.txt` 或 `000778/2023.txt`
- 推荐使用 `\f` (Form Feed) 或 `=== Page X ===` 作为页分隔符

### 输出

提取结果存入 DuckDB 数据库 `mda_text` 表，包含：
- `mda_raw`: 提取的 MD&A 原文
- `char_count`: 字符数
- `page_index_start/end`: 页范围
- `quality_flags`: 质量标记（如 `FLAG_LENGTH_ABNORMAL`）
- `source_sha256`: 源文件哈希（用于增量判定）

## 依赖要求

```bash
pip install -r requirements.txt
```

## 多语言文档

- [docs/README.en.md](./docs/README.en.md) — English（完整版本）
- [docs/README.zh.md](./docs/README.zh.md) — 简体中文版本

## 📚 技术文档

为了方便开发者更好地理解和参与本项目，我们提供了详细的架构与设计文档：

### 架构与设计
- **[系统架构图 (System Architecture)](./docs/system_architecture.md)**：数据流向与模块交互图解。
- **[架构预览 (Preview Architecture)](./docs/preview_architecture.md)**：架构设计预览与演进说明。
- **[功能清单 (Feature List)](./docs/feature_list.md)**：详细的功能点梳理。
- **[依赖关系图 (Dependency Graph)](./docs/dependency_graph.md)**：外部库与服务依赖分析。
- **[技术债与优化清单 (Technical Debt)](./docs/technical_debt.md)**：已知问题与未来改进计划。

### 模块文档
- **[PDF 批量转换器手册](./docs/pdf_batch_converter_manual.md)**：pdf_batch_converter.py 使用指南。
- **[纯转换模式用户指南](./docs/convert_only_mode_user_guide.md)**：convert 子命令的使用说明。
- **[纯转换模式维护文档](./docs/convert_only_mode_maintenance.md)**：convert 模式的开发维护指南。
- **[文本分析评审](./docs/text_analysis_review.md)**：文本分析模块的代码评审报告。

### 规格与审计
- **[MD&A 提取器规格书](./docs/annual_report_mda_extractor_spec.md)**：MD&A 提取器的完整技术规格。
- **[MD&A 提取器规格审计](./docs/annual_report_mda_extractor_spec_audit.md)**：MD&A 提取器规格书的审计报告。

## 更新日志

| 日期       | 亮点                                                          |
| ---------- | ------------------------------------------------------------- |
| 2026/01/10 | **MD&A 提取策略优化**：平均分从 68.86 提升至 96.51，修复引用词误匹配、结束标记误识别等问题 |
| 2026/01/10 | 新增黄金数据集工具链：LLM 辅助标注、评估脚本、自动改进分析    |
| 2026/01/08 | **DuckDB 核心化 (M1.2) 完成**：爬虫/转换器支持双模式，新增迁移脚本和单元测试 |
| 2026/01/08 | 文档同步：补充 MD&A 规格书索引，完善注册表子模块列表          |
| 2026/01/07 | 新增统一配置管理 (config_manager)，支持 YAML + Pydantic 验证  |
| 2026/01/07 | 新增 M2.5 里程碑（MD&A 提取器测试与质检），扩充技术文档索引   |
| 2026/01/07 | 添加 MD&A 提取器使用说明、数据库 schema.json                  |
| 2026/01/02 | 新增 MD&A 提取器模块（annual_report_mda 包）                  |
| 2026/01/02 | 添加技术文档：系统架构图、功能清单、依赖关系图、技术债清单    |
| 2026/01/01 | 重构 PDF 批量转换器，支持 argparse 命令行接口                 |
| 2026/01/01 | 添加纯转换模式用户指南、维护文档                              |
| 2025/11/21 | 代码优化：添加类型提示，改进错误处理，增强所有脚本的鲁棒性    |
| 2025/11/21 | README切换为英文默认 + 免责声明，多进程分析器，添加docs文件夹 |
| 2025/03/15 | 添加requirements文件，下载器现在支持其他公告                  |
| 2024/10/13 | 修复爬虫结果中缺失公司的问题                                  |
| 2024/02/14 | 上传主表，改进可读性                                          |
| 2024/01/04 | 改进关键词准确性，添加通用分析器                              |
| 2023/05/25 | 全面重构，参数化工作流                                        |
| 2023/04/20 | 初始提交                                                      |

## TODO

> 详细开发计划与验收标准请参阅 [TODO.md](./TODO.md)

- [x] **M1 配置统一 + DuckDB 底座** — 统一配置管理，DuckDB 替代 Excel 作为数据枢纽（完成于 2026-01-08）
- [ ] **M2 结构化提取 + 质量评估** — 提升 MD&A 提取准确率，建立质量评分闭环
- [ ] **M2.5 MD&A 提取器测试与质检** — 端到端测试、质检增强、文档完善
- [ ] **M3 NLP 深度分析** — 情感分析、相似度分析、LDA 主题模型
- [ ] **M4 工程化基础设施** — CI/CD 流水线、日志系统、测试覆盖
- [ ] **M5 长期规划** — 云端存储、API 服务化
- [x] 双语文档 & 项目指标

## 贡献

欢迎提交Issues和PRs！与社区分享功能想法、bug报告或最佳实践。


