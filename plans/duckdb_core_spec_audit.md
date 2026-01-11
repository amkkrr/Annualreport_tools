# DuckDB 核心化规格书审计报告

本文档对 `docs/duckdb_core_spec.md` 所描述的 DuckDB 核心化方案进行审计，评估其完备性、一致性与可落地性，并输出问题清单与建议修改点。

---

## 1. 文档整体评价

- **定位明确**：将 DuckDB 作为年报数据全生命周期的核心存储，替代 Excel 驱动模式的目标清晰，有配套的数据模型、模块改造、迁移脚本与验收标准。
- **结构合理**：章节划分基本覆盖从数据模型、模块改造、迁移脚本、配置、验收到风险的关键环节，方便实现与验收。
- **工程可行性较高**：以 `reports` 作为任务驱动表，结合状态字段与索引，可以支持增量爬取、断点续传和幂等迁移。

主要问题集中在以下几个方面：

1. **数据模型与状态机细节不完整**（尤其是提取阶段 `extract_*` 相关字段缺失）。
2. **部分 DuckDB 相关说明与命令存在概念/实现层面的错误或混用 SQLite 概念**。
3. **若干伪代码与函数设计存在不一致或缺少分支处理的问题**。
4. **部分关键约束、幂等与并发场景未在规格层面讲清楚（仅在实现上暗示）**。

下文将分模块详细审计。

---

## 2. 结构与范围审计

### 2.1 覆盖范围

已覆盖：

- 年报数据的**核心数据模型**：`companies`、`reports` 和已有 `mda_text` 的关系。
- 爬虫、下载/转换模块在 **DuckDB 驱动模式** 下的职责和伪代码。
- 从历史 Excel 到 DuckDB 的 **迁移脚本** 设计。
- 配置文件 (`config.yaml`) 扩展与 **兼容模式** (`--legacy` / `--source excel`)。
- 基本的功能与质量 **验收标准** 与测试命令。
- 高层次的 **风险与缓解** 列表。

缺失或薄弱的范围：

- **提取器模块**（抽取到 `mda_text`）：只在关系图和状态字段中提到 `extract_status`，但缺少：
  - 从 `reports` 读取待提取任务的查询接口。
  - 提取完成后写入 `mda_text` 的模式（尤其是多版本/多来源的关系）。
  - 提取阶段的错误字段与重试策略。
- **并发写入与多进程处理的规范**：有“最佳实践”段落，但存在技术性错误（见第 6 章）。
- **统一的错误编码或错误分类规范**：目前仅有 download/convert 的 error 文本字段，缺少统一结构或约束。

建议：后续补一份专门针对“提取器 + mda_text 表”的规格（可与本规格并列），并对并发与错误模型给出更系统的规范。

---

## 3. 数据模型审计

### 3.1 `companies` 表设计

优点：

- 以 `stock_code` 作为主键，保证公司维度表的简单性。
- `short_name` + `full_name` 结构合理，满足日常展示和检索。
- 预留了 `plate`、`trade`、`trade_name` 字段，便于按板块和行业分析。
- `first_seen_at` 与 `updated_at` 时间戳字段可以支持后续审计和数据修正。

发现的问题与建议：

1. **字段约束不够明确**
   - 当前仅 `short_name` 标记为 `NOT NULL`，其他如 `plate`、`trade` 理论上也属于“业务必需但可能缺失”字段。
   - 建议在规格中明确哪些字段是“逻辑必填但可以晚到”（例如：短期内允许为 NULL，但期望最终补全）。

2. **缺少唯一性约束的说明**
   - 虽然 `stock_code` 为主键，但缺少对 `short_name` 可能重复的讨论（存在同名公司、历史更名等场景）。
   - 建议在文档中明确：`short_name` 不保证唯一，仅做展示与模糊搜索；不要在其上做业务主键。

3. **行业/板块编码规范未定义**
   - `plate`、`trade`、`trade_name` 的编码来源、枚举范围和更新策略未明确。
   - 若这些字段后续参与统计与过滤，建议：
     - 约定来源（例如：巨潮/交易所/内部映射表）。
     - 给出典型示例或说明是否会有多值情况。

### 3.2 `reports` 表设计

优点：

- `(stock_code, year)` 作为主键，满足“每家公司每个年份一个年报”的简化假设，利于任务管理。
- 将下载/转换/提取拆分为三个状态字段，并配套索引，利于按状态聚合与调度。
- 记录 `pdf_path`、`txt_path`、文件大小与哈希信息，有利于存档与数据一致性检查。
- 记录 `crawled_at`、`downloaded_at`、`converted_at` 和 `updated_at`，为审计和重放提供基础信息。
- `source` 字段可区分数据来源（例如 cninfo、手工或迁移），对合并多来源数据很重要。

主要问题：

1. **缺少提取阶段的错误与重试字段（重要）**

   表定义中仅有：

   - `download_error`、`convert_error`
   - `download_retries`、`convert_retries`

   但没有：

   - `extract_error`
   - `extract_retries`
   - `extracted_at`（与 `downloaded_at`、`converted_at` 对齐）

   同时，在“状态机设计”中描述了 `extract_status` 及其转移关系，但没有对应字段存储提取错误原因与重试次数。这会导致：

   - 无法区分“提取任务失败但尚未超过最大重试次数”与“已放弃”的状态。
   - 无法统计“提取阶段的失败原因分布”和“重试效率”。

   **建议（强烈）**：为提取阶段补齐：

   - `extract_error VARCHAR`
   - `extract_retries INTEGER DEFAULT 0`
   - `extracted_at TIMESTAMP`

   并在后续接口（如提取器模块的更新函数）中同步设计。

2. **多版本年报场景未覆盖（视业务而定）**

   当前主键 `(stock_code, year)` 假设“每年一份年报”。现实中可能存在：

   - 原始年报 + 更正公告 + 修订版。
   - 不同市场板块或不同语言版本的年报。

   若后续确实需要区分不同版本，可能需要：

   - 增加 `report_type`（如 `original`/`revised`/`summary`）。
   - 或引入 `version` / `sequence` 字段，将主键扩展为 `(stock_code, year, version)`。

   若短期内明确“不处理多版本，只保留最新或最主要版本”，建议在规格中写明这一业务决策，以免后续产生误用。

3. **`announcement_id` 和 `url` 的唯一性策略未定义**

   - 目前没有对 `announcement_id` 或 `url` 设置唯一约束。
   - 插入逻辑通过 `(stock_code, year)` 去重，若 Excel 或爬虫返回同一公司同一年重复记录（如重复爬取），理论上不会产生重复主键，但可能会 **忽略不同 URL 或不同标题的后续记录**（取决于插入顺序）。

   建议在规格中明确：

   - `announcement_id` 是否唯一？是否需要 `UNIQUE (announcement_id)` 或 `UNIQUE (url)` 约束。
   - 若新数据和旧数据在 `url` 上不一致，是否应当覆盖/更新现有记录，或者仅记录一次。

4. **状态字段未声明 `NOT NULL` 与合法枚举约束**

   三个状态字段：

   - `download_status`
   - `convert_status`
   - `extract_status`

   仅通过默认值和应用层逻辑控制。没有：

   - `NOT NULL` 约束。
   - 枚举约束（例如 CHECK 约束，限制为 `'pending'/'downloading'/'success'/'failed'`）。

   建议从规范角度明确：

   - 所有状态字段都 `NOT NULL`，默认 `'pending'`。
   - 可在规格中建议使用 `CHECK (download_status IN ('pending','downloading','success','failed'))` 等约束（如果 DuckDB 版本支持）。

5. **索引策略可以更明确**

   当前索引：

   - `download_status`
   - `convert_status`
   - `extract_status`

   验收指标中存在：

   - `WHERE download_status = 'pending'`
   - 按 `year` 过滤的查询（如下载器 `get_pending_downloads` 使用 `WHERE year = ?`）。

   若常用查询模式为 `WHERE year = ? AND download_status = 'pending'`，则考虑：

   - 增加组合索引，如 `(year, download_status)`。
   - 或说明当前数据量预期较小，单列索引已足够。

### 3.3 状态机与状态枚举

状态机设计基本合理，但有以下需要补充之处：

1. **最大重试次数未在规格中定义**
   - 状态表中 `"failed"` 的定义依赖“超过最大重试次数”，但配置项（例如 `max_download_retries`、`max_convert_retries`、`max_extract_retries`）没有在规格/配置章节中体现。
   - 建议在配置扩展章节中新增重试相关配置，并在状态机说明处引用。

2. **中间状态恢复策略未定义**
   - 对于进程中断导致的中间态（`downloading`/`converting`/`extracting`），未明确重启后如何处理：
     - 是否有清理逻辑将长时间处于中间状态的记录重置为 `failed` 或 `pending`？
   - 建议明确“中间状态 + 超时”的处理策略，避免任务永久卡死。

3. **提取阶段的状态机缺少配套接口**
   - 虽然给出了状态转换图，但缺少提取阶段：
     - “任务选择 SQL”
     - “状态更新函数签名”
   - 建议在后续扩展中补齐提取器模块的接口规范。

### 3.4 表关系与 `mda_text` 集成

- 关系图中描述了 `mda_text` 的主键为 `(stock_code, year, source_sha256)`。
- 文本中指出 `(stock_code, year)` 应作为外键关联 `reports`，但未给出：
  - 是否在 `mda_text` 中对 `(stock_code, year)` 建立 `FOREIGN KEY` 约束。
  - 若不存在 `reports` 记录时是否允许 `mda_text` 中存在孤儿记录。

建议：

- 在规格中明确 `mda_text` 与 `reports` 的外键关系（是否物理外键）。
- 若考虑性能或迁移成本不加物理外键，也应说明在应用层保证 `(stock_code,year)` 一致性的策略。

---

## 4. 模块改造方案审计

### 4.1 爬虫模块

优点：

- 支持从爬虫直接写入 `reports`，实现“增量爬取 + 幂等插入”。
- `upsert_company` 和 `upsert_report` 的职责划分清晰，有助于复用。
- `_save_to_duckdb` 通过 `new_count` 与 `skip_count` 统计增量情况，有利于观测。

问题与建议：

1. **`upsert_report` 的幂等逻辑在并发场景下存在潜在竞态**

   当前伪代码逻辑为：

   1. `SELECT 1 FROM reports WHERE stock_code = ? AND year = ?`
   2. 若无结果则 `INSERT INTO reports (...)`

   若存在多进程或多线程同时插入同一 `(stock_code, year)`，会存在：

   - 两个进程都看不到记录，然后都尝试插入，可能导致主键冲突。

   建议：

   - 要么在规格中声明：写入为单进程/单连接模式，避免并发插入。
   - 要么将 `upsert_report` 也改为 `INSERT ... ON CONFLICT DO NOTHING`（前提是确认 DuckDB 对 `ON CONFLICT DO NOTHING` 的支持情况，并在规格中写清）。

2. **公司信息更新策略需要说明**

   - `upsert_company` 仅更新 `short_name`、`plate`、`trade`，对 `trade_name` 处理未提及。
   - 建议说明：
     - `trade_name` 是否来自同一数据源。
     - 若两个数据源提供不同的 `short_name` 或 `plate`，以谁为准。

3. **`_save_to_duckdb` 对异常场景缺少说明**

   - 未提及当某条记录写入失败（例如违反约束或连接异常）时的处理策略：
     - 是否跳过记录、记录日志并继续。
     - 是否整体事务回滚。

   建议在规格中补充“一条记录写入失败时的默认处理策略”。

### 4.2 下载与转换模块

优点：

- 抽象出 `DuckDBDrivenProcessor`，将任务获取和状态更新集中在数据库接口中，结构清晰。
- 通过 `get_pending_downloads` + `update_download_status` + `update_convert_status` 实现断点续传。
- 在 `process_single` 中优先检查 TXT 是否存在，减少重复下载和转换。

重要问题：

1. **`update_convert_status` 未处理 `"converting"` 状态（逻辑缺失，严重）**

   - 伪代码中定义的 `update_convert_status` 仅处理 `success` 与 `failed` 两种状态分支。
   - 但在 `process_single` 中存在调用：

     ```python
     update_convert_status(..., status="converting")
     ```

   - 这意味着：
     - 传入 `"converting"` 时函数不做任何 UPDATE 操作。
     - 数据库中的 `convert_status` 将直接从 `pending` 或 `failed` 跳到 `success` 或 `failed`，缺失 “处理中” 中间状态，且与状态机图不一致。

   建议（必须修改）：

   - 在规格中补齐 `status == "converting"` 分支的 SQL 更新语句。
   - 或者修改调用方逻辑，不再在处理中阶段写入 `"converting"`（不推荐，建议保留中间状态）。

2. **`get_pending_downloads` 的状态过滤范围有限**

   - `status` 仅允许 `"pending" / "failed" / "all"`。
   - 若后续希望支持针对 `"downloading"` 或 `"success"` 的巡检任务，需要追加状态选项。
   - 目前与 CLI 参数 `choices=["pending", "failed", "all"]` 是一致的，可以暂视为满足需求。

   建议：

   - 在规格中明确下载器命令的使用方式：默认只处理 `pending` 和 `failed`。
   - 若未来要增加更多状态筛选，在本规格中预留扩展说明。

3. **文件命名与路径策略未与爬虫/迁移模块统一描述**

   - `process_single` 中构建文件名 `f"{stock_code}_{company_name}_{year}"`。
   - 如果迁移脚本和爬虫模块在生成 `pdf_path`/`txt_path` 时使用不同的命名规则，可能导致：
     - 数据库中的 `pdf_path`/`txt_path` 与实际文件路径不一致。
   - 目前规格未说明“统一的路径命名约定”。

   建议：

   - 在规格或单独的“存储规范”中给出统一的命名模板，并要求所有模块共用。

4. **错误信息结构不统一**

   - `update_download_status` / `update_convert_status` 仅接收 `error: str`。
   - 未说明：
     - 错误字符串格式（自由文本 / `[错误码] 错误信息`）。
     - 是否考虑错误码枚举以便统计。

   建议：

   - 在规格中定义错误信息的标准格式，至少在团队内部约定。

---

## 5. 迁移脚本审计

优点：

- 支持 `--dry-run` 预览模式，方便在迁移前评估数据规模和年限范围。
- 通过 `upsert_company` 与 `upsert_report` 实现幂等迁移，避免重复数据。
- 提供 `--batch-size` 控制提交频率，考虑了性能与事务大小。

发现的问题与建议：

1. **`new_companies` 变量未使用**

   - 代码中定义 `new_companies = 0`，但未更新或输出该统计信息。
   - 建议：
     - 要么去掉该变量；
     - 要么在插入新公司时进行计数并在日志中输出。

2. **Excel 列名兼容性仅在“风险”中提到，未在脚本中体现**

   - 风险表中提到：“Excel 列名变更 → 迁移脚本失败 → 增加列名兼容映射”。
   - 但脚本实现中只是检查 `required_cols` 是否存在，没有兼容逻辑。
   - 建议：
     - 在规格中定义一份列名映射配置（例如 `excel_columns_alias`），或者在脚本中增加简单的兼容逻辑。

3. **内存占用考虑**

   - `pd.read_excel` 直接一次性读入全文件，对当前数据量可能可接受。
   - 若后续数据量显著增加，可以考虑：
     - 在规格中注明“假定 Excel 数据量在内存可接受范围内”。
     - 或给出分批读取的建议（例如：分 sheet 或按年份拆分）。

4. **异常处理与回滚策略未定义**

   - 迁移过程中遇到单行异常（类型错误、约束冲突）时的预期行为未说明：
     - 是否整个迁移事务回滚？
     - 还是跳过错误行继续？
   - 建议在规格中明确默认策略，并鼓励脚本实现时记录详细日志。

---

## 6. 配置与兼容模式审计

### 6.1 配置扩展

优点：

- `database.path`、`crawler.output.mode`、`downloader.source.mode` 等配置项清晰，方便切换行为。
- 通过 `filter_status` 控制下载器处理范围，可以实现“仅处理 pending / failed”。

建议补充：

1. **最大重试次数参数**

   - 建议在 `downloader` 或全局配置中增加：
     - `max_download_retries`
     - `max_convert_retries`
     - `max_extract_retries`
   - 并在状态机与错误策略中引用这些配置。

2. **路径规范配置**

   - 建议集中定义 PDF/TXT 根目录与文件命名模板，而不是在代码中硬编码。

### 6.2 兼容模式

- `--legacy` / `--source excel` 的设计基本清晰：保留 Excel 驱动能力以支持回滚与过渡。
- 建议在文档中明确两个参数的优先级关系（例如：当两者同时出现时的行为）。

---

## 7. 测试与验收标准审计

优点：

- 功能验收表（Given/When/Then）明确了关键路径（表创建、爬虫写入、增量、公司同步、下载读取、状态更新、断点续传）的预期行为。
- 质量验收中给出了写入性能、幂等性和索引效率的初步指标。

建议：

1. **增加提取阶段的验收条目**
   - 当前验收项只到“下载/转换”，缺少“提取入 `mda_text`”的验收。
   - 建议增加例如：
     - “提取读取”: 从 `reports` 中按 `extract_status='pending'` 读取任务。
     - “提取写入”: 提取完成后更新 `extract_status` 并写入 `mda_text`。

2. **DuckDB 查询命令有潜在兼容性问题**

   - 测试命令中使用：

     ```sql
     SELECT name FROM sqlite_master WHERE type='table'
     ```

   - 这属于 SQLite 风格，在 DuckDB 中是否长期兼容并非强保证。
   - 建议改为更标准/推荐的方式，如：
     - `SHOW TABLES;`
     - 或查询 `information_schema.tables`。

---

## 8. 并发与 DuckDB 特性审计

第 10.1 节“DuckDB 并发最佳实践”中存在重要问题：

1. **`PRAGMA enable_wal` 属于 SQLite 概念**

   - DuckDB 并不使用 SQLite 的 WAL 机制；`enable_wal` 是典型的 SQLite PRAGMA。
   - 将其写入 DuckDB 规格可能会误导实现者，引入错误假设。

2. **并发写入策略缺少基于 DuckDB 特性的描述**

   - DuckDB 当前对并发写入的支持特性与限制需要参考官方文档，而不是沿用 SQLite 的方案。
   - 建议：
     - 删除或改写 `PRAGMA enable_wal` 相关内容。
     - 明确推荐的模式：例如单写多读、多进程通过任务队列集中写入等。
     - 若要使用多进程并发写入，需要基于 DuckDB 官方文档做专门设计，而不是简单套用 SQLite 模式。

3. **单连接与多连接使用规范需要更详细说明**

   - “单连接模式”是合理的简化方案，但应明确：
     - 在脚本/服务生命周期内复用同一连接。
     - 避免频繁打开/关闭连接造成开销。

---

## 9. 主要问题清单与建议（按优先级）

### 9.1 阻塞级（在落地前必须修正）

1. **提取阶段字段缺失**
   - 在 `reports` 表中补充：
     - `extract_error`
     - `extract_retries`
     - `extracted_at`
   - 同时在后续提取器模块接口中补齐用法。

2. **`update_convert_status` 未处理 `"converting"` 状态**
   - 在函数设计中增加对应分支，保证状态机图与实现一致。

3. **DuckDB 并发部分误用 SQLite 概念**
   - 删除或修正 `PRAGMA enable_wal` 相关内容。
   - 重写“并发最佳实践”章节，基于 DuckDB 官方文档。

### 9.2 重要级（建议本阶段内修正）

1. **最大重试次数配置缺失**
   - 在配置文件与规格中增加重试上限配置，并在状态机定义中引用。

2. **`upsert_report` 的幂等性与并发问题**
   - 明确写入是否单进程。
   - 或改用 `INSERT ... ON CONFLICT DO NOTHING` 风格实现，并在规格中说明。

3. **状态字段与错误字段的约束补充**
   - 为 `download_status`/`convert_status`/`extract_status` 增加 `NOT NULL` 和 CHECK 约束的建议。
   - 为错误字段设计统一格式或错误码规范。

4. **`mda_text` 与 `reports` 的关系规范**
   - 明确是否采用物理外键或应用层约束。
   - 定义多版本/多来源的处理方式。

### 9.3 优化级（可在后续迭代中完善）

1. **索引优化**
   - 结合实际查询模式考虑 `(year, download_status)` 等组合索引。

2. **迁移脚本可观测性与容错**
   - 完善 `new_companies` 统计。
   - 定义异常处理策略。
   - 实现 Excel 列名兼容映射。

3. **命名与路径规范**
   - 提炼出统一的文件命名与路径规则，避免模块间不一致。

4. **测试命令与工具链**
   - 将 `SELECT name FROM sqlite_master` 替换为 DuckDB 推荐用法。
   - 可以增加一些示例 SQL（如按年份统计、按状态聚合）帮助验证数据。

---

## 10. 结论

总体来看，DuckDB 核心化规格书已经为“以 DuckDB 作为年报数据中枢”提供了较完整的蓝图，尤其在 `companies`/`reports` 设计和爬虫、下载器改造上具备较高可实施性。当前的主要缺口集中在：

- 提取阶段字段与接口缺失；
- 少数实现级伪代码与状态机不一致；
- 并发与 DuckDB 特性上存在概念性错误；

在修正上述阻塞与重要问题后，该规格可以作为后续实现与演进的可靠基础。后续建议补一份“提取器与 mda_text 规格书”，使全生命周期闭环设计更加完整。
