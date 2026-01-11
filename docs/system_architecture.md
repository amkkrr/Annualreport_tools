# 系统架构图 (System Architecture)

该项目采用经典的 **管道（Pipeline）架构**，数据流向清晰，分为三个独立但顺序关联的阶段：采集、处理、分析。

```mermaid
graph TD
    subgraph S1 [第一阶段：数据采集]
        User[用户配置] -->|年份/行业/板块| Crawler[1.report_link_crawler.py]
        ExtService[巨潮资讯网 API] <-->|按天分片请求| Crawler
        Crawler -->|增量保存| LinkExcel[Excel 链接表]
    end

    subgraph S2 [第二阶段：文档获取与清洗]
        LinkExcel -->|读取链接| Downloader[2.pdf_batch_converter.py]
        LocalPDF[本地 PDF 目录] -->|纯转换模式| Downloader

        Downloader -->|多进程下载| RawPDF[PDF 文件]
        RawPDF -->|完整性校验| Validator{校验成功?}
        Validator -->|No| Retry[重试下载]
        Validator -->|Yes| Converter[PDF 转 TXT 引擎]

        Converter -->|首选| Backend1[pdfplumber]
        Backend1 -->|失败| Backend2[PyPDF2]
        Backend2 -->|失败| Backend3[pdfminer]

        Backend1 & Backend2 & Backend3 --> RawTXT[TXT 文本文件]
    end

    subgraph S3 [第三阶段：数据分析]
        RawTXT -->|输入| Analyzer[3.text_analysis.py]
        UnivAnalyzer[text_analysis_universal.py] -->|输入| RawTXT

        Dict[jieba 自定义词典] -.->|注入关键词| Analyzer
        Analyzer -->|多进程分词统计| ResultExcel[词频统计结果.xls]
    end
```

## 架构说明

1.  **数据采集层 (Data Collection Layer)**
    *   负责与外部数据源（巨潮资讯网）交互。
    *   核心组件：`1.report_link_crawler.py`
    *   特点：实现按天分片策略以规避 API 限制，支持断点续传（增量保存）。

2.  **数据处理层 (Data Processing Layer)**
    *   负责非结构化数据（PDF）的获取与结构化转换（TXT）。
    *   核心组件：`2.pdf_batch_converter.py`
    *   特点：包含多级转换引擎降级策略，确保最大成功率；支持多进程并发处理以提升效率。

3.  **数据分析层 (Data Analysis Layer)**
    *   负责文本挖掘与指标计算。
    *   核心组件：`3.text_analysis.py`
    *   特点：利用 NLP 技术（分词）提取特定领域关键词，生成可供量化研究的数据报表。
