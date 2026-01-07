# 依赖关系图 (Dependency Graph)

本图展示了项目核心脚本对外部 Python 库、系统服务及网络环境的依赖关系。

```mermaid
graph LR
    subgraph Project [核心脚本]
        Crawler[爬虫脚本<br>1.report_link_crawler.py]
        Converter[转换脚本<br>2.pdf_batch_converter.py]
        Analyzer[分析脚本<br>3.text_analysis.py]
        MDAExtractor[MD&A 提取器<br>mda_extractor.py]
    end

    subgraph ExtService [外部服务]
        CNINFO[巨潮资讯网 API]
    end

    subgraph DataProcess [数据处理库]
        pandas[pandas]
        openpyxl[openpyxl]
        xlwt[xlwt]
    end

    subgraph Network [网络请求]
        requests[requests]
    end

    subgraph PDFEngine [PDF 处理后端]
        pdfplumber[pdfplumber]
        PyPDF2[PyPDF2 (Optional)]
        pdfminer[pdfminer.six (Optional)]
    end

    subgraph NLP [自然语言处理]
        jieba[jieba]
    end

    subgraph Storage [数据存储]
        duckdb[duckdb]
    end

    subgraph Utility [工具库]
        rich[rich]
        dotenv[python-dotenv]
    end

    %% 爬虫依赖
    Crawler --> requests
    Crawler --> openpyxl
    Crawler -.-> CNINFO

    %% 转换依赖
    Converter --> pandas
    Converter --> requests
    Converter --> PDFEngine

    %% 分析依赖
    Analyzer --> jieba
    Analyzer --> xlwt

    %% MD&A 提取器依赖
    MDAExtractor --> duckdb
    MDAExtractor --> rich
    MDAExtractor --> dotenv
```

## 核心依赖说明

| 依赖库           | 版本要求     | 用途                                                      |
| :--------------- | :----------- | :-------------------------------------------------------- |
| **requests**     | `==2.32.3`   | 用于爬虫模块与巨潮资讯网交互，以及转换模块下载 PDF 文件。 |
| **pandas**       | `==2.2.3`    | 用于转换模块读取 Excel 格式的年报链接列表。               |
| **pdfplumber**   | `==0.11.5`   | 首选的 PDF 解析库，用于将 PDF 转换为文本。                |
| **PyPDF2**       | `>=3.0.0`    | 备用 PDF 解析库，当 `pdfplumber` 失败时作为后备方案。     |
| **pdfminer.six** | `>=20221105` | 兜底 PDF 解析库，用于处理前两者均无法解析的复杂 PDF。     |
| **jieba**        | `==0.42.1`   | 中文分词库，用于分析模块提取关键词。                      |
| **openpyxl**     | `==3.1.5`    | 用于爬虫模块将抓取结果写入 `.xlsx` 文件。                 |
| **xlwt**         | `==1.3.0`    | 用于分析模块将词频统计结果写入老版本 `.xls` 文件。        |
| **duckdb**       | `>=0.9.0`    | 用于 MD&A 提取器存储提取结果。                            |
| **rich**         | `>=13.0.0`   | 用于 MD&A 提取器的进度条与日志美化。                      |
| **python-dotenv**| `>=1.0.0`    | 用于 MD&A 提取器从 `.env` 加载配置。                      |