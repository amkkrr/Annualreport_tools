"""统一配置管理模块 (1.1 Unified Configuration Management)

本模块提供基于 YAML + Pydantic v2 的配置加载、校验和路径归一化功能。

使用方法:
    from annual_report_mda.config_manager import load_config, GlobalConfig

    config = load_config()  # 加载 config.yaml
    config = load_config("custom.yaml")  # 加载指定配置文件
"""

from __future__ import annotations

import logging
import os
import platform
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator

_LOG = logging.getLogger(__name__)


# ============ 路径归一化与安全校验 ============


class PathNormalizer:
    """路径归一化器，负责将相对路径转换为绝对路径并校验安全性。"""

    FORBIDDEN_PREFIXES_UNIX = ["/etc", "/usr", "/bin", "/sbin", "/var", "/root", "/boot"]
    FORBIDDEN_PREFIXES_WIN = ["C:\\Windows", "C:\\Program Files"]
    SOURCE_DIRS = ["annual_report_mda", "tests", ".git"]

    def __init__(self, workspace_root: Path) -> None:
        self.workspace_root = workspace_root.resolve()

    def normalize(self, path: str | Path) -> Path:
        """将路径归一化为绝对路径。"""
        p = Path(path)
        if p.is_absolute():
            return p.resolve()
        return (self.workspace_root / p).resolve()

    def validate_output_path(self, path: Path) -> None:
        """校验输出路径安全性。"""
        resolved = path.resolve()
        path_str = str(resolved)

        if platform.system() != "Windows":
            for prefix in self.FORBIDDEN_PREFIXES_UNIX:
                if path_str.startswith(prefix):
                    raise ValueError(f"禁止写入系统目录: {path_str}")
        else:
            for prefix in self.FORBIDDEN_PREFIXES_WIN:
                if path_str.lower().startswith(prefix.lower()):
                    raise ValueError(f"禁止写入系统目录: {path_str}")

        for src_dir in self.SOURCE_DIRS:
            src_path = (self.workspace_root / src_dir).resolve()
            if resolved == src_path or str(resolved).startswith(str(src_path) + os.sep):
                raise ValueError(f"禁止写入源代码目录: {path_str}")

        if resolved == Path("/").resolve():
            raise ValueError(f"禁止写入根目录: {path_str}")
        if platform.system() == "Windows" and resolved == Path("C:\\").resolve():
            raise ValueError(f"禁止写入根目录: {path_str}")


# ============ 基础配置类 ============


class ProjectConfig(BaseModel):
    """全局项目配置。"""

    workspace_root: Path = Field(default=Path("."))
    log_level: str = Field(default="INFO", pattern=r"^(DEBUG|INFO|WARNING|ERROR)$")


class DatabaseConfig(BaseModel):
    """数据库配置。"""

    path: Path = Field(default=Path("data/annual_reports.duckdb"))


# ============ 爬虫配置 ============


class CrawlerRequestConfig(BaseModel):
    """爬虫专用请求配置。"""

    timeout: int = Field(default=10, ge=1, le=300)
    max_retries: int = Field(default=3, ge=0, le=10)
    retry_delay: int = Field(default=5, ge=1, le=60)


class CrawlerFiltersConfig(BaseModel):
    """爬虫过滤条件配置。"""

    plates: list[str] = Field(default_factory=lambda: ["sz", "sh"])
    trade: str = ""
    exclude_keywords: list[str] = Field(default_factory=list)

    @field_validator("plates")
    @classmethod
    def check_plates(cls, v: list[str]) -> list[str]:
        valid_plates = {"sz", "sh", "szmb", "shmb", "szcy", "shkcp", "bj"}
        for plate in v:
            if plate not in valid_plates:
                raise ValueError(f"Invalid plate '{plate}'. Valid: {valid_plates}")
        return v


class CrawlerOutputConfig(BaseModel):
    """爬虫输出配置。"""

    excel_path_template: str = "res/AnnualReport_links_{year}.xlsx"
    save_interval: int = Field(default=100, ge=1)


class CrawlerConfig(BaseModel):
    """爬虫模块配置。"""

    target_years: list[int] = Field(min_length=1)
    request: CrawlerRequestConfig = Field(default_factory=CrawlerRequestConfig)
    filters: CrawlerFiltersConfig = Field(default_factory=CrawlerFiltersConfig)
    output: CrawlerOutputConfig = Field(default_factory=CrawlerOutputConfig)

    @field_validator("target_years")
    @classmethod
    def check_years(cls, v: list[int]) -> list[int]:
        for year in v:
            if year < 1990 or year > 2100:
                raise ValueError(f"Year {year} is out of reasonable range [1990, 2100]")
        return v


# ============ 下载器配置 ============


class DownloaderRequestConfig(BaseModel):
    """下载器专用请求配置。"""

    timeout: int = Field(default=30, ge=1, le=600)
    max_retries: int = Field(default=3, ge=0, le=10)
    chunk_size: int = Field(default=8192, ge=1024, le=1048576)
    stream: bool = True
    headers: dict[str, str] = Field(default_factory=lambda: {"User-Agent": "Mozilla/5.0"})


class DownloaderPathsConfig(BaseModel):
    """下载器路径配置。"""

    input_excel_template: str = "res/AnnualReport_links_{year}.xlsx"
    pdf_dir_template: str = "outputs/annual_reports/{year}/pdf"
    txt_dir_template: str = "outputs/annual_reports/{year}/txt"


class DownloaderBehaviorConfig(BaseModel):
    """下载器行为配置。"""

    delete_pdf: bool = False
    skip_existing: bool = True
    force_overwrite: bool = False


class DownloaderConfig(BaseModel):
    """下载与转换模块配置。"""

    processes: int | None = Field(default=None, ge=1, le=64)
    paths: DownloaderPathsConfig = Field(default_factory=DownloaderPathsConfig)
    behavior: DownloaderBehaviorConfig = Field(default_factory=DownloaderBehaviorConfig)
    request: DownloaderRequestConfig = Field(default_factory=DownloaderRequestConfig)


# ============ 分析配置 ============


class AnalysisPathsConfig(BaseModel):
    """分析模块路径配置。"""

    output_excel: str = "outputs/analysis/keyword_counts_{timestamp}.xls"
    text_root_dir: str = "outputs/annual_reports"


class AnalysisConfig(BaseModel):
    """文本分析模块配置。"""

    keywords: list[str] = Field(min_length=1)
    paths: AnalysisPathsConfig = Field(default_factory=AnalysisPathsConfig)


# ============ MDA 配置 ============


class MdaLimitsConfig(BaseModel):
    """MDA 提取限制配置。"""

    max_pages: int = Field(default=50, ge=1, le=500)
    max_chars: int = Field(default=200000, ge=1000)


class MdaBehaviorConfig(BaseModel):
    """MDA 提取行为配置。"""

    incremental: bool = True
    workers: int = Field(default=4, ge=1, le=32)


class MdaConfig(BaseModel):
    """MDA 提取模块配置。"""

    limits: MdaLimitsConfig = Field(default_factory=MdaLimitsConfig)
    behavior: MdaBehaviorConfig = Field(default_factory=MdaBehaviorConfig)


# ============ 日志配置 ============


class LoggingConfig(BaseModel):
    """日志配置。"""

    # 终端输出
    enable_console: bool = Field(default=True, description="启用终端输出")
    console_rich: bool = Field(default=True, description="使用 RichHandler 美化输出")

    # 文件日志
    enable_file: bool = Field(default=False, description="启用文件日志")
    log_dir: Path = Field(default=Path("logs"), description="日志目录")
    file_prefix: str = Field(
        default="app",
        pattern=r"^[a-zA-Z][a-zA-Z0-9_-]*$",
        description="日志文件名前缀",
    )

    # 轮转设置
    max_bytes: int = Field(
        default=10 * 1024 * 1024,
        ge=1024,
        le=100 * 1024 * 1024,
        description="单个日志文件最大字节数",
    )
    backup_count: int = Field(
        default=7,
        ge=1,
        le=30,
        description="保留的备份文件数量",
    )

    # 格式设置
    file_format: str = Field(
        default="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        description="文件日志格式",
    )
    date_format: str = Field(default="%Y-%m-%d %H:%M:%S", description="日期格式")


# ============ 全局配置 ============


class GlobalConfig(BaseModel):
    """全局配置根对象。"""

    project: ProjectConfig = Field(default_factory=ProjectConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    crawler: CrawlerConfig
    downloader: DownloaderConfig = Field(default_factory=DownloaderConfig)
    analysis: AnalysisConfig
    mda: MdaConfig = Field(default_factory=MdaConfig)

    @model_validator(mode="after")
    def normalize_paths(self) -> GlobalConfig:
        """在模型验证后执行路径归一化和安全校验。"""
        normalizer = PathNormalizer(self.project.workspace_root)

        # 归一化数据库路径
        object.__setattr__(self.database, "path", normalizer.normalize(self.database.path))

        # 校验输出路径模板安全性
        output_templates = [
            self.downloader.paths.pdf_dir_template,
            self.downloader.paths.txt_dir_template,
        ]
        for template in output_templates:
            test_path = normalizer.normalize(template.replace("{year}", "2023"))
            normalizer.validate_output_path(test_path)

        return self


# ============ 错误格式化 ============


def format_validation_error(e: ValidationError, config_path: str) -> str:
    """格式化验证错误为用户可读信息。"""
    lines = [f"配置文件 {config_path} 校验失败:"]

    for error in e.errors():
        loc = ".".join(str(x) for x in error["loc"])
        msg = error["msg"]
        input_val = error.get("input", "N/A")

        lines.append(f"\n  字段: {loc}")
        lines.append(f"  错误: {msg}")
        lines.append(f"  输入值: {input_val}")

        if "year" in loc.lower() and "range" in msg.lower():
            lines.append("  建议: target_years 应为 1990-2100 之间的年份列表，如 [2022, 2023]")
        elif "keywords" in loc.lower() and "length" in msg.lower():
            lines.append("  建议: keywords 不能为空，请添加至少一个关键词")
        elif "plates" in loc.lower():
            lines.append("  建议: plates 有效值为 sz, sh, szmb, shmb, szcy, shkcp, bj")

    return "\n".join(lines)


# ============ 配置加载 ============


def load_config(path: str = "config.yaml") -> GlobalConfig:
    """加载配置文件。

    Args:
        path: 配置文件路径，默认为 config.yaml

    Returns:
        GlobalConfig 配置对象

    Raises:
        FileNotFoundError: 配置文件不存在
        ValueError: 配置校验失败
    """
    config_path = Path(path)

    if not config_path.exists():
        example_path = Path("config.yaml.example")
        hint = ""
        if example_path.exists():
            hint = f"\n提示: 可复制模板文件创建配置:\n  cp {example_path} {config_path}"
        raise FileNotFoundError(
            f"配置文件不存在: {config_path}{hint}\n或使用 --config 参数指定配置文件路径"
        )

    with open(config_path, encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    if raw is None:
        raise ValueError(f"配置文件为空: {config_path}")

    try:
        return GlobalConfig.model_validate(raw)
    except ValidationError as e:
        raise ValueError(format_validation_error(e, str(config_path))) from e


def load_config_with_fallback() -> GlobalConfig | None:
    """尝试加载配置文件，失败时返回 None 并发出警告。

    此函数用于迁移期间保持向后兼容性。
    """
    try:
        return load_config()
    except FileNotFoundError:
        import warnings

        warnings.warn(
            "未找到 config.yaml，使用内置默认值（已弃用）", DeprecationWarning, stacklevel=2
        )
        return None


def log_config_summary(config: GlobalConfig, logger: logging.Logger) -> None:
    """打印配置摘要。"""
    logger.info("=== 配置加载完成 ===")
    logger.info(f"  workspace_root: {config.project.workspace_root}")
    logger.info(f"  target_years: {config.crawler.target_years}")
    logger.info(f"  workers: {config.mda.behavior.workers}")
    logger.info(f"  log_level: {config.project.log_level}")


# ============ CLI 覆盖 ============


def apply_cli_overrides(config: GlobalConfig, overrides: dict[str, Any]) -> GlobalConfig:
    """将 CLI 参数覆盖到配置对象（不可变更新）。

    Args:
        config: 原始配置对象
        overrides: CLI 覆盖参数字典，键为配置路径，值为新值
            支持的键:
            - "target_years": List[int] -> crawler.target_years
            - "processes": int -> downloader.processes
            - "workers": int -> mda.behavior.workers
            - "timeout": int -> crawler.request.timeout
            - "download_timeout": int -> downloader.request.timeout
            - "log_level": str -> project.log_level
            - "force_overwrite": bool -> downloader.behavior.force_overwrite
            - "incremental": bool -> mda.behavior.incremental

    Returns:
        更新后的配置对象
    """
    if not overrides:
        return config

    updates: dict[str, Any] = {}

    if "target_years" in overrides and overrides["target_years"]:
        crawler_dump = config.crawler.model_dump()
        crawler_dump["target_years"] = overrides["target_years"]
        updates["crawler"] = crawler_dump

    if "processes" in overrides and overrides["processes"] is not None:
        downloader_dump = config.downloader.model_dump()
        downloader_dump["processes"] = overrides["processes"]
        updates["downloader"] = downloader_dump

    if "workers" in overrides and overrides["workers"] is not None:
        mda_dump = config.mda.model_dump()
        mda_dump["behavior"]["workers"] = overrides["workers"]
        updates["mda"] = mda_dump

    if "timeout" in overrides and overrides["timeout"] is not None:
        if "crawler" not in updates:
            updates["crawler"] = config.crawler.model_dump()
        updates["crawler"]["request"]["timeout"] = overrides["timeout"]

    if "download_timeout" in overrides and overrides["download_timeout"] is not None:
        if "downloader" not in updates:
            updates["downloader"] = config.downloader.model_dump()
        updates["downloader"]["request"]["timeout"] = overrides["download_timeout"]

    if "log_level" in overrides and overrides["log_level"]:
        project_dump = config.project.model_dump()
        project_dump["log_level"] = overrides["log_level"]
        updates["project"] = project_dump

    if "force_overwrite" in overrides and overrides["force_overwrite"]:
        if "downloader" not in updates:
            updates["downloader"] = config.downloader.model_dump()
        updates["downloader"]["behavior"]["force_overwrite"] = True

    if "incremental" in overrides and overrides["incremental"]:
        if "mda" not in updates:
            updates["mda"] = config.mda.model_dump()
        updates["mda"]["behavior"]["incremental"] = True

    if updates:
        return config.model_copy(update=updates)
    return config


# ============ 环境变量支持 ============


def get_config_path_from_env() -> str:
    """从环境变量获取配置文件路径。"""
    return os.environ.get("ANNUAL_REPORT_CONFIG_PATH", "config.yaml")
