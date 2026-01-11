"""
Pytest fixtures for MD&A extractor tests.
"""

import tempfile
from collections.abc import Generator
from pathlib import Path

import pytest

# 获取 fixtures 目录路径
FIXTURES_DIR = Path(__file__).parent / "fixtures"
MOCK_MDA_DIR = FIXTURES_DIR / "mock_mda"


@pytest.fixture
def mock_mda_dir() -> Path:
    """返回 mock_mda fixtures 目录路径。"""
    return MOCK_MDA_DIR


@pytest.fixture
def mock_with_toc_path() -> Path:
    """返回含目录页的 mock 文件路径。"""
    return MOCK_MDA_DIR / "mock_with_toc.txt"


@pytest.fixture
def mock_no_toc_path() -> Path:
    """返回不含目录页的 mock 文件路径。"""
    return MOCK_MDA_DIR / "mock_no_toc.txt"


@pytest.fixture
def mock_ff_pages_path() -> Path:
    """返回使用 Form Feed 分页的 mock 文件路径。"""
    return MOCK_MDA_DIR / "mock_ff_pages.txt"


@pytest.fixture
def mock_eq_pages_path() -> Path:
    """返回使用 === Page N === 分页的 mock 文件路径。"""
    return MOCK_MDA_DIR / "mock_eq_pages.txt"


@pytest.fixture
def mock_no_pages_path() -> Path:
    """返回无分页符的 mock 文件路径。"""
    return MOCK_MDA_DIR / "mock_no_pages.txt"


@pytest.fixture
def mock_with_toc_text(mock_with_toc_path: Path) -> str:
    """读取含目录页的 mock 文件内容。"""
    return mock_with_toc_path.read_text(encoding="utf-8")


@pytest.fixture
def mock_no_toc_text(mock_no_toc_path: Path) -> str:
    """读取不含目录页的 mock 文件内容。"""
    return mock_no_toc_path.read_text(encoding="utf-8")


@pytest.fixture
def temp_db_path() -> Generator[Path, None, None]:
    """创建临时 DuckDB 数据库文件路径。"""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir) / "test.duckdb"


@pytest.fixture
def temp_txt_file() -> Generator[Path, None, None]:
    """创建临时 TXT 文件。"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False, encoding="utf-8") as f:
        f.write("")
        temp_path = Path(f.name)
    try:
        yield temp_path
    finally:
        if temp_path.exists():
            temp_path.unlink()


@pytest.fixture
def temp_dir() -> Generator[Path, None, None]:
    """创建临时目录。"""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def sample_mda_text() -> str:
    """返回标准的 MD&A 文本样本，用于评分测试。"""
    return (
        """
第三节 管理层讨论与分析

一、报告期内公司所处行业情况

公司主营业务收入同比增长15%，毛利率保持稳定在35%。
行业发展态势良好，市场需求旺盛。

二、主营业务分析

本报告期，公司实现营业收入500亿元，同比增长10%。
归属于上市公司股东的净利润80亿元，同比增长12%。
经营活动产生的现金流量净额充裕。

三、公司未来发展的展望

公司将持续推进技术创新，提升产品竞争力。
行业展望良好，预计市场规模将持续扩大。
"""
        * 10
    )  # 重复以达到足够长度


@pytest.fixture
def short_text() -> str:
    """返回短文本样本（少于 500 字符）。"""
    return "这是一段短文本。"


@pytest.fixture
def text_with_dots() -> str:
    """返回含目录引导线的文本。"""
    return (
        """
目 录

第一节 重要提示 ...................... 1
第二节 公司简介 ...................... 5
第三节 管理层讨论与分析 .............. 10
第四节 公司治理 ...................... 25
第五节 财务报告 ...................... 30
第六节 股份变动 ...................... 35
第七节 优先股相关情况 ................ 40
第八节 董事、监事、高级管理人员 ...... 45
第九节 公司债券相关情况 .............. 50
第十节 财务报告 ...................... 55
"""
        * 50
    )  # 重复以达到足够长度


@pytest.fixture
def text_with_table_residue() -> str:
    """返回含表格残留的文本。"""
    return (
        """
公司主营业务收入情况

123.45
678.90
234.56
789.01
345.67

以上为主要财务数据。
"""
        * 50
    )  # 重复以达到足够长度


@pytest.fixture
def text_with_header_noise() -> str:
    """返回含页眉干扰的文本。"""
    return (
        """
贵州茅台2023年年度报告
公司主营业务收入同比增长10%。
贵州茅台2023年年度报告
毛利率保持稳定。
贵州茅台2023年年度报告
现金流充裕。
贵州茅台2023年年度报告
展望未来发展。
贵州茅台2023年年度报告
行业发展良好。
"""
        * 20
    )  # 重复以达到足够长度
