'''
@Project ：PycharmProjects
@File    ：年报批量下载.py
@IDE     ：PyCharm
@Author  ：lingxiaotian
@Date    ：2023/5/30 11:39
@LastEditTime: 2025/11/21 14:10
'''

from __future__ import annotations

import argparse
import fnmatch
import logging
import os
import re
import sys
from importlib import import_module
import warnings
from dataclasses import dataclass
from multiprocessing import Pool, cpu_count
from pathlib import Path
from typing import Any, Iterable, Optional, Tuple

def _import_or_raise(module_name: str, install_hint: str) -> Any:
    """按需导入第三方依赖，失败时抛出带安装提示的错误。

    设计目标：即使依赖缺失，也能在无参数运行时输出命令行帮助（不在导入阶段崩溃）。
    """
    try:
        return import_module(module_name)
    except ModuleNotFoundError as e:
        raise ModuleNotFoundError(f"缺少依赖 {module_name}，请先安装：{install_hint}") from e

# 抑制pdfplumber的CropBox警告
warnings.filterwarnings('ignore', message='.*CropBox.*')

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


@dataclass(frozen=True)
class ConverterConfig:
    """PDF批量下载转换配置类。"""
    excel_file: str  # Excel表格路径
    pdf_dir: str  # PDF存储目录
    txt_dir: str  # TXT存储目录
    target_year: int  # 目标年份
    delete_pdf: bool = False  # 是否删除转换后的PDF
    max_retries: int = 3  # 下载最大重试次数
    timeout: int = 15  # 请求超时时间（秒）
    chunk_size: int = 8192  # 下载块大小
    processes: Optional[int] = None  # 进程数，None表示自动


class PDFDownloader:
    """PDF下载器类。"""
    
    HEADERS = {
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    }
    
    def __init__(self, timeout: int = 15, chunk_size: int = 8192) -> None:
        self.timeout = timeout
        self.chunk_size = chunk_size
        requests = _import_or_raise("requests", "pip install -r requirements.txt")
        self._requests = requests
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)
    
    def download(self, pdf_url: str, pdf_file_path: str) -> bool:
        """下载PDF文件并验证完整性。
        
        Args:
            pdf_url: PDF下载链接
            pdf_file_path: 保存路径
            
        Returns:
            下载是否成功
        """
        try:
            # 请求PDF文件
            response = self.session.get(pdf_url, stream=True, timeout=self.timeout)
            
            # 检查HTTP状态码
            if response.status_code == 403:
                logging.error(f"403 Forbidden: 服务器禁止访问 {pdf_url}")
                return False
            elif response.status_code != 200:
                logging.error(f"请求失败: {response.status_code} - {response.text[:500]}")
                return False
            
            # 验证Content-Type
            content_type = response.headers.get("Content-Type", "")
            if "pdf" not in content_type.lower():
                logging.error(f"服务器返回的不是 PDF: {content_type}")
                return False
            
            # 写入PDF文件
            with open(pdf_file_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=self.chunk_size):
                    if chunk:
                        f.write(chunk)
            
            # 验证文件完整性
            if not self._verify_pdf(pdf_file_path):
                return False
            
            logging.info(f"PDF 下载成功: {pdf_file_path}")
            return True
            
        except self._requests.exceptions.Timeout:
            logging.error(f"下载超时: {pdf_url}")
            return False
        except self._requests.exceptions.RequestException as e:
            logging.error(f"下载 PDF 文件失败: {e}")
            return False
        except OSError as e:
            logging.error(f"文件写入失败: {e}")
            return False
    
    @staticmethod
    def _verify_pdf(pdf_file_path: str) -> bool:
        """验证PDF文件完整性。"""
        if not os.path.exists(pdf_file_path):
            logging.error(f"文件不存在: {pdf_file_path}")
            return False
        
        if os.path.getsize(pdf_file_path) == 0:
            logging.error(f"下载失败，文件大小为 0 KB: {pdf_file_path}")
            return False
        
        try:
            with open(pdf_file_path, "rb") as f:
                first_bytes = f.read(5)
                if not first_bytes.startswith(b"%PDF"):
                    logging.error(f"下载的文件不是有效的 PDF: {pdf_file_path}")
                    return False
        except OSError as e:
            logging.error(f"文件验证失败: {e}")
            return False
        
        return True

class PDFConverter:
    """PDF转TXT转换器类。"""
    
    # 文件名非法字符正则
    INVALID_CHARS = r'[\\/:*?"<>|]'
    
    def __init__(self, config: ConverterConfig) -> None:
        self.config = config
        self.downloader = PDFDownloader(
            timeout=config.timeout,
            chunk_size=config.chunk_size
        )
        self.text_converter = PDFToTextConverter()
    
    @staticmethod
    def _sanitize_filename(filename: str) -> str:
        """清理文件名中的非法字符。"""
        return re.sub(PDFConverter.INVALID_CHARS, '', filename)
    
    def _download_with_retry(self, pdf_url: str, pdf_file_path: str) -> bool:
        """带重试机制的下载。"""
        for attempt in range(1, self.config.max_retries + 1):
            if self.downloader.download(pdf_url, pdf_file_path):
                return True
            if attempt < self.config.max_retries:
                logging.warning(f"重试下载 ({attempt}/{self.config.max_retries}): {pdf_url}")
        
        logging.error(f"下载失败（已重试 {self.config.max_retries} 次）: {pdf_url}")
        return False
    
    def convert_pdf_to_txt(self, pdf_path: str, txt_path: str) -> PDFConversionResult:
        """将PDF转换为TXT，使用多种库作为备用方案并返回结构化结果。"""
        result = self.text_converter.convert_pdf_to_txt(Path(pdf_path), Path(txt_path))
        if result.success and result.backend:
            logging.info(f"转换成功 ({result.backend}): {txt_path}")
            return result
        if result.error:
            logging.warning(f"PDF转换失败: {pdf_path} - {result.error}")
        else:
            logging.error(f"所有PDF转换方法均失败: {pdf_path}")
        return result
    
    def process_single_file(
        self,
        code: int,
        name: str,
        year: int,
        pdf_url: str
    ) -> bool:
        """处理单个文件的下载和转换。
        
        Args:
            code: 公司代码
            name: 公司简称
            year: 年份
            pdf_url: PDF下载链接
            
        Returns:
            处理是否成功
        """
        # 生成文件名
        base_name = self._sanitize_filename(f"{code:06}_{name}_{year}")
        pdf_file_path = os.path.join(self.config.pdf_dir, f"{base_name}.pdf")
        txt_file_path = os.path.join(self.config.txt_dir, f"{base_name}.txt")
        
        try:
            # 检查TXT是否已存在
            if os.path.exists(txt_file_path):
                logging.info(f"文件已存在，跳过: {base_name}.txt")
                return True
            
            # 下载PDF（如果不存在）
            if not os.path.exists(pdf_file_path):
                if not self._download_with_retry(pdf_url, pdf_file_path):
                    return False
            
            # 转换PDF为TXT
            conversion = self.convert_pdf_to_txt(pdf_file_path, txt_file_path)
            if not conversion.success:
                return False
            
            # 删除PDF（如果配置要求）
            if self.config.delete_pdf and os.path.exists(pdf_file_path):
                try:
                    os.remove(pdf_file_path)
                    logging.info(f"已删除PDF: {pdf_file_path}")
                except OSError as e:
                    logging.warning(f"删除PDF失败: {e}")
            
            return True
            
        except Exception as e:
            logging.error(f"处理文件失败 {code:06}_{name}_{year}: {e}")
            return False


@dataclass(frozen=True)
class PDFConversionResult:
    """单个PDF->TXT转换的结果。"""

    success: bool
    backend: Optional[str] = None
    error: Optional[str] = None


class PDFToTextConverter:
    """纯PDF->TXT转换器（不包含下载逻辑）。"""

    def _convert_with_pdfplumber(self, pdf_path: Path, txt_path: Path) -> PDFConversionResult:
        try:
            pdfplumber = _import_or_raise("pdfplumber", "pip install -r requirements.txt")
        except ModuleNotFoundError as e:
            return PDFConversionResult(success=False, error=str(e))
        try:
            with pdfplumber.open(str(pdf_path)) as pdf:
                with open(txt_path, 'w', encoding='utf-8') as f:
                    for page_num, page in enumerate(pdf.pages, 1):
                        try:
                            text = page.extract_text()
                            if text:
                                f.write(text)
                        except Exception as e:
                            logging.debug(f"pdfplumber提取第 {page_num} 页失败: {e}")
                            continue
            return PDFConversionResult(success=True, backend="pdfplumber")
        except Exception as e:
            return PDFConversionResult(success=False, error=f"pdfplumber: {e}")

    def _convert_with_pypdf2(self, pdf_path: Path, txt_path: Path) -> PDFConversionResult:
        try:
            PdfReader = _import_or_raise("PyPDF2", "pip install PyPDF2").PdfReader
        except ModuleNotFoundError as e:
            return PDFConversionResult(success=False, error=str(e))
        try:
            reader = PdfReader(str(pdf_path))
            with open(txt_path, 'w', encoding='utf-8') as f:
                for page_num, page in enumerate(reader.pages, 1):
                    try:
                        text = page.extract_text()
                        if text:
                            f.write(text)
                    except Exception as e:
                        logging.debug(f"PyPDF2提取第 {page_num} 页失败: {e}")
                        continue
            return PDFConversionResult(success=True, backend="PyPDF2")
        except Exception as e:
            return PDFConversionResult(success=False, error=f"PyPDF2: {e}")

    def _convert_with_pdfminer(self, pdf_path: Path, txt_path: Path) -> PDFConversionResult:
        try:
            pdfminer_extract = _import_or_raise("pdfminer.high_level", "pip install pdfminer.six").extract_text
        except ModuleNotFoundError as e:
            return PDFConversionResult(success=False, error=str(e))
        try:
            text = pdfminer_extract(str(pdf_path))
            with open(txt_path, 'w', encoding='utf-8') as f:
                f.write(text)
            return PDFConversionResult(success=True, backend="pdfminer")
        except Exception as e:
            return PDFConversionResult(success=False, error=f"pdfminer: {e}")

    def convert_pdf_to_txt(self, pdf_path: Path, txt_path: Path) -> PDFConversionResult:
        """将PDF转换为TXT，依次尝试多个后端。"""
        errors: list[str] = []

        txt_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = txt_path.with_name(f"{txt_path.name}.{os.getpid()}.tmp")

        def _cleanup_tmp() -> None:
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
            except OSError:
                pass

        def _commit_tmp(backend_result: PDFConversionResult) -> PDFConversionResult:
            if not tmp_path.exists() or tmp_path.stat().st_size == 0:
                _cleanup_tmp()
                backend_name = backend_result.backend or "unknown"
                return PDFConversionResult(success=False, error=f"{backend_name}: empty output")
            os.replace(tmp_path, txt_path)
            return backend_result

        _cleanup_tmp()
        result = self._convert_with_pdfplumber(pdf_path, tmp_path)
        if result.success:
            return _commit_tmp(result)
        if result.error:
            errors.append(result.error)
        _cleanup_tmp()

        result = self._convert_with_pypdf2(pdf_path, tmp_path)
        if result.success:
            return _commit_tmp(result)
        if result.error:
            errors.append(result.error)
        _cleanup_tmp()

        result = self._convert_with_pdfminer(pdf_path, tmp_path)
        if result.success:
            return _commit_tmp(result)
        if result.error:
            errors.append(result.error)
        _cleanup_tmp()

        error_summary = "; ".join(errors) if errors else "all backends failed"
        return PDFConversionResult(success=False, error=error_summary)


@dataclass(frozen=True)
class ConvertOnlyConfig:
    """纯转换模式配置类。"""

    pdf_dir: str
    txt_dir: Optional[str] = None
    recursive: bool = False
    delete_pdf: bool = False
    force: bool = False
    processes: Optional[int] = None
    file_pattern: str = "*.pdf"


@dataclass(frozen=True)
class ConvertOnlyResult:
    """纯转换模式下单个PDF的处理结果。"""

    status: str  # "success" | "skipped" | "failed"
    pdf_path: Path
    txt_path: Path
    backend: Optional[str] = None
    error: Optional[str] = None


def _matches_file_pattern(path: Path, file_pattern: str) -> bool:
    """使用近似glob语义匹配文件名（大小写不敏感）。"""
    if path.suffix.lower() != ".pdf":
        return False
    return fnmatch.fnmatch(path.name.lower(), file_pattern.lower())


def _scan_pdf_files(pdf_dir: Path, recursive: bool, file_pattern: str) -> list[Path]:
    """扫描PDF文件列表（大小写不敏感匹配）。"""
    if not pdf_dir.exists() or not pdf_dir.is_dir():
        raise FileNotFoundError(f"PDF源目录不存在或不可读: {pdf_dir}")

    pdf_files: list[Path] = []
    if recursive:
        for root, _, files in os.walk(pdf_dir):
            root_path = Path(root)
            for filename in files:
                candidate = root_path / filename
                if not candidate.is_file():
                    continue
                if _matches_file_pattern(candidate, file_pattern):
                    pdf_files.append(candidate)
    else:
        for candidate in pdf_dir.iterdir():
            if not candidate.is_file():
                continue
            if _matches_file_pattern(candidate, file_pattern):
                pdf_files.append(candidate)

    pdf_files.sort()
    return pdf_files


def _resolve_txt_path(pdf_path: Path, pdf_dir: Path, txt_dir: Optional[Path], recursive: bool) -> Path:
    """根据规格书规则计算目标TXT路径。"""
    if txt_dir is None:
        return pdf_path.with_suffix(".txt")
    if not recursive:
        return (txt_dir / pdf_path.name).with_suffix(".txt")
    relative_pdf_path = pdf_path.relative_to(pdf_dir)
    return (txt_dir / relative_pdf_path).with_suffix(".txt")


def _is_valid_txt_file(path: Path) -> bool:
    """最小有效性检查：非空文件。"""
    try:
        return path.exists() and path.is_file() and path.stat().st_size > 0
    except OSError:
        return False


def _convert_only_worker(args: Tuple[str, str, Optional[str], bool, bool, str]) -> ConvertOnlyResult:
    """纯转换模式的多进程worker。"""
    pdf_path_str, pdf_dir_str, txt_dir_str, recursive, force, file_pattern = args

    pdf_path = Path(pdf_path_str)
    pdf_dir = Path(pdf_dir_str)
    txt_dir = Path(txt_dir_str) if txt_dir_str is not None else None

    if not _matches_file_pattern(pdf_path, file_pattern):
        target = _resolve_txt_path(pdf_path, pdf_dir, txt_dir, recursive)
        return ConvertOnlyResult(status="skipped", pdf_path=pdf_path, txt_path=target, error="pattern mismatch")

    target_txt_path = _resolve_txt_path(pdf_path, pdf_dir, txt_dir, recursive)

    if (not force) and _is_valid_txt_file(target_txt_path):
        return ConvertOnlyResult(status="skipped", pdf_path=pdf_path, txt_path=target_txt_path)

    try:
        target_txt_path.parent.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        raise OSError(f"输出目录不可创建/不可写: {target_txt_path.parent} ({e})") from e

    converter = PDFToTextConverter()
    try:
        conversion = converter.convert_pdf_to_txt(pdf_path, target_txt_path)
    except OSError as e:
        if getattr(e, "errno", None) == 28:
            raise
        return ConvertOnlyResult(status="failed", pdf_path=pdf_path, txt_path=target_txt_path, error=str(e))
    except Exception as e:
        return ConvertOnlyResult(status="failed", pdf_path=pdf_path, txt_path=target_txt_path, error=str(e))

    if not conversion.success:
        try:
            if target_txt_path.exists() and target_txt_path.stat().st_size == 0:
                target_txt_path.unlink()
        except OSError:
            pass
        return ConvertOnlyResult(
            status="failed",
            pdf_path=pdf_path,
            txt_path=target_txt_path,
            backend=conversion.backend,
            error=conversion.error,
        )

    if not _is_valid_txt_file(target_txt_path):
        return ConvertOnlyResult(
            status="failed",
            pdf_path=pdf_path,
            txt_path=target_txt_path,
            backend=conversion.backend,
            error="TXT写入后为空（最小有效性检查失败）",
        )

    return ConvertOnlyResult(
        status="success",
        pdf_path=pdf_path,
        txt_path=target_txt_path,
        backend=conversion.backend,
    )


class ConvertOnlyProcessor:
    """纯转换模式处理器：扫描目录下PDF并批量转换为TXT。"""

    def __init__(self, config: ConvertOnlyConfig) -> None:
        self.config = config

    def run(self) -> None:
        logging.info("=" * 60)
        logging.info("纯转换模式启动")
        logging.info(f"PDF源目录: {self.config.pdf_dir}")
        logging.info(f"TXT输出目录: {self.config.txt_dir}")
        logging.info(f"递归扫描: {self.config.recursive}")
        logging.info(f"强制覆盖: {self.config.force}")
        logging.info(f"删除PDF: {self.config.delete_pdf}")
        logging.info(f"file_pattern: {self.config.file_pattern}")
        logging.info("=" * 60)

        pdf_dir = Path(self.config.pdf_dir)
        txt_dir = Path(self.config.txt_dir) if self.config.txt_dir is not None else None

        if txt_dir is not None:
            try:
                txt_dir.mkdir(parents=True, exist_ok=True)
            except OSError as e:
                logging.error(f"输出目录不可创建/不可写: {txt_dir} ({e})")
                return

        try:
            pdf_files = _scan_pdf_files(pdf_dir, self.config.recursive, self.config.file_pattern)
        except FileNotFoundError as e:
            logging.error(str(e))
            return

        logging.info(f"扫描到 {len(pdf_files)} 个PDF文件")
        if not pdf_files:
            return

        worker_count = self.config.processes or min(cpu_count(), len(pdf_files))
        logging.info(f"使用 {worker_count} 个进程处理")

        tasks: Iterable[Tuple[str, str, Optional[str], bool, bool, str]] = (
            (str(p), str(pdf_dir), str(txt_dir) if txt_dir is not None else None, self.config.recursive, self.config.force, self.config.file_pattern)
            for p in pdf_files
        )

        counts = {"success": 0, "skipped": 0, "failed": 0}
        failures: list[ConvertOnlyResult] = []

        try:
            with Pool(processes=worker_count) as pool:
                for result in pool.imap_unordered(_convert_only_worker, tasks, chunksize=10):
                    counts[result.status] += 1
                    if result.status == "success":
                        backend = result.backend or "unknown"
                        logging.info(f"转换成功 ({backend}): {result.txt_path}")
                        if self.config.delete_pdf:
                            try:
                                if result.pdf_path.exists():
                                    result.pdf_path.unlink()
                                    logging.info(f"已删除PDF: {result.pdf_path}")
                            except OSError as e:
                                logging.warning(f"删除PDF失败: {result.pdf_path} ({e})")
                    elif result.status == "skipped":
                        logging.info(f"跳过已存在(有效): {result.txt_path}")
                    else:
                        failures.append(result)
                        logging.error(f"转换失败: {result.pdf_path} ({result.error})")
        except OSError as e:
            if getattr(e, "errno", None) == 28:
                logging.error("磁盘空间不足（Errno 28 No space left on device），终止处理。")
                return
            logging.error(f"运行失败: {e}")
            return
        except Exception as e:
            logging.error(f"运行失败: {e}")
            return

        logging.info("=" * 60)
        logging.info(
            f"处理完成: 成功 {counts['success']}/{len(pdf_files)}, 跳过 {counts['skipped']}, 失败 {counts['failed']}"
        )
        logging.info("=" * 60)



def _process_task(args: Tuple) -> bool:
    """多进程任务包装函数。"""
    converter, code, name, year, pdf_url = args
    return converter.process_single_file(code, name, year, pdf_url)


class AnnualReportProcessor:
    """年报批量处理器。"""

    def __init__(
        self,
        config: ConverterConfig,
        db_conn: Optional["duckdb.DuckDBPyConnection"] = None,
    ) -> None:
        self.config = config
        self.converter = PDFConverter(config)
        self.db_conn = db_conn  # 可选的数据库连接，用于 DuckDB 模式

    def _load_excel_data(self) -> Optional[pd.DataFrame]:
        """加载Excel数据。"""
        try:
            pd = _import_or_raise("pandas", "pip install -r requirements.txt")
        except ModuleNotFoundError as e:
            logging.error(str(e))
            return None
        try:
            df = pd.read_excel(self.config.excel_file)
            logging.info(f"成功加载Excel文件: {self.config.excel_file}")
            return df
        except FileNotFoundError:
            logging.error(f"Excel文件不存在: {self.config.excel_file}")
            logging.error("请检查 EXCEL_FILE 路径，或将 RUN_MODE 切换为 \"convert_only\" 使用本地PDF纯转换模式。")
            return None
        except Exception as e:
            logging.error(f"读取Excel失败: {e}")
            return None
    
    def _prepare_directories(self) -> bool:
        """创建必要的目录。"""
        try:
            Path(self.config.pdf_dir).mkdir(parents=True, exist_ok=True)
            Path(self.config.txt_dir).mkdir(parents=True, exist_ok=True)
            logging.info(f"目录准备完成: PDF={self.config.pdf_dir}, TXT={self.config.txt_dir}")
            return True
        except OSError as e:
            logging.error(f"创建目录失败: {e}")
            return False
    
    def _filter_data_by_year(self, df: pd.DataFrame) -> pd.DataFrame:
        """按年份过滤数据。"""
        required_columns = ['公司代码', '公司简称', '年份', '年报链接']
        
        # 检查必需列
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            logging.error(f"Excel缺少必需列: {missing_cols}")
            return pd.DataFrame()
        
        # 过滤年份
        filtered = df[df['年份'].astype(str) == str(self.config.target_year)]
        logging.info(f"找到 {len(filtered)} 条 {self.config.target_year} 年的记录")
        return filtered

    def _load_tasks_from_duckdb(self) -> list:
        """从 DuckDB 加载待处理任务。"""
        if self.db_conn is None:
            raise RuntimeError("未提供数据库连接")

        from annual_report_mda.db import get_pending_downloads

        tasks = get_pending_downloads(self.db_conn, year=self.config.target_year)
        logging.info(f"从 DuckDB 加载 {len(tasks)} 条待下载任务")
        return tasks

    def _update_task_status_in_db(
        self,
        stock_code: str,
        year: int,
        success: bool,
        pdf_path: str | None = None,
        txt_path: str | None = None,
        error: str | None = None,
    ) -> None:
        """更新任务状态到 DuckDB。"""
        if self.db_conn is None:
            return

        from annual_report_mda.db import update_report_status

        if success:
            update_report_status(
                self.db_conn,
                stock_code=stock_code,
                year=year,
                download_status="success",
                convert_status="success",
                pdf_path=pdf_path,
                txt_path=txt_path,
                downloaded_at=True,
                converted_at=True,
            )
        else:
            update_report_status(
                self.db_conn,
                stock_code=stock_code,
                year=year,
                download_status="failed",
                download_error=error,
            )
    
    def run(self) -> bool:
        """执行批量处理流程。

        Returns:
            是否完成了本轮处理（失败会返回 False）。
        """
        logging.info("="*60)
        logging.info("年报批量下载转换程序启动")
        logging.info(f"目标年份: {self.config.target_year}")
        logging.info(f"删除PDF: {self.config.delete_pdf}")
        logging.info(f"数据源: {'DuckDB' if self.db_conn else 'Excel'}")
        logging.info("="*60)

        # 准备目录
        if not self._prepare_directories():
            return False

        # 根据模式加载任务
        if self.db_conn is not None:
            # DuckDB 模式
            db_tasks = self._load_tasks_from_duckdb()
            if not db_tasks:
                logging.warning(f"未找到 {self.config.target_year} 年的待下载任务")
                return True

            # 转换为任务列表格式
            tasks = [
                (self.converter, task['stock_code'], task.get('short_name') or '', task['year'], task['url'])
                for task in db_tasks
            ]
        else:
            # Excel 模式
            df = self._load_excel_data()
            if df is None:
                return False

            filtered_df = self._filter_data_by_year(df)
            if filtered_df.empty:
                logging.warning(f"未找到 {self.config.target_year} 年的数据")
                return True

            tasks = [
                (self.converter, row['公司代码'], row['公司简称'], row['年份'], row['年报链接'])
                for _, row in filtered_df.iterrows()
            ]

        # 多进程处理
        worker_count = self.config.processes or min(cpu_count(), len(tasks))
        logging.info(f"使用 {worker_count} 个进程处理 {len(tasks)} 个文件")

        success_count = 0
        with Pool(processes=worker_count) as pool:
            results = pool.map(_process_task, tasks)
            success_count = sum(results)

            # DuckDB 模式下更新状态（主进程统一写入）
            if self.db_conn is not None:
                for task, success in zip(tasks, results):
                    _, stock_code, short_name, year, _ = task
                    base_name = self.converter._sanitize_filename(f"{int(stock_code):06}_{short_name}_{year}")
                    pdf_path = os.path.join(self.config.pdf_dir, f"{base_name}.pdf")
                    txt_path = os.path.join(self.config.txt_dir, f"{base_name}.txt")
                    self._update_task_status_in_db(
                        stock_code=str(stock_code).zfill(6),
                        year=int(year),
                        success=success,
                        pdf_path=pdf_path if success else None,
                        txt_path=txt_path if success else None,
                        error=None if success else "处理失败",
                    )

        # 输出统计
        logging.info("="*60)
        logging.info(f"处理完成: 成功 {success_count}/{len(tasks)}")
        logging.info("="*60)
        return True


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="2.pdf_batch_converter.py",
        description=(
            "年报工具：下载+转换（download）与纯转换（convert-only）。\n"
            "不带参数运行时默认输出帮助；如需继续使用脚本底部配置，请传 --use-config。"
        ),
        epilog=(
            "示例：\n"
            "  # 查看帮助\n"
            "  python3 2.pdf_batch_converter.py\n"
            "\n"
            "  # 使用 config.yaml 配置文件运行（推荐）\n"
            "  python3 2.pdf_batch_converter.py --use-yaml-config\n"
            "\n"
            "  # 纯转换：将 ./annual_reports/pdf 下的PDF转为TXT，输出到 ./outputs/annual_reports/txt\n"
            "  python3 2.pdf_batch_converter.py convert-only --pdf-dir annual_reports/pdf\n"
            "\n"
            "  # 纯转换：递归扫描子目录\n"
            "  python3 2.pdf_batch_converter.py convert-only --pdf-dir annual_reports/pdf --recursive\n"
            "\n"
            "  # 纯转换：输出到PDF同目录（将 --txt-dir 设为空字符串）\n"
            "  python3 2.pdf_batch_converter.py convert-only --pdf-dir annual_reports/pdf --txt-dir \"\"\n"
            "\n"
            "  # 下载+转换：处理单一年份\n"
            "  python3 2.pdf_batch_converter.py download --excel-file \"你的表.xlsx\" --year 2023\n"
            "\n"
            "  # 下载+转换：批量年份\n"
            "  python3 2.pdf_batch_converter.py download --excel-file \"你的表.xlsx\" --start-year 2022 --end-year 2024\n"
            "\n"
            "  # 兼容旧用法：使用脚本底部配置区域\n"
            "  python3 2.pdf_batch_converter.py --use-config\n"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--use-config",
        action="store_true",
        help='使用脚本底部"配置区域"的参数运行（兼容旧用法）。',
    )
    parser.add_argument(
        "--use-yaml-config",
        action="store_true",
        help="使用 config.yaml 配置文件运行（推荐）。",
    )
    parser.add_argument(
        "--config", "-c",
        default="config.yaml",
        help="指定 YAML 配置文件路径（默认 config.yaml，需配合 --use-yaml-config）。",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="日志级别（默认 INFO）。",
    )

    subparsers = parser.add_subparsers(dest="command")

    download = subparsers.add_parser("download", help="从Excel读取链接，下载PDF并转换为TXT。")
    download.add_argument("--excel-file", help="Excel路径（需包含 公司代码/公司简称/年份/年报链接 列）。使用 --source excel 时必填。")
    download.add_argument("--delete-pdf", action="store_true", help="转换后删除PDF（节省空间）。")
    download.add_argument("--max-retries", type=int, default=3, help="下载失败最大重试次数（默认 3）。")
    download.add_argument("--timeout", type=int, default=15, help="HTTP超时秒数（默认 15）。")
    download.add_argument("--processes", type=int, default=None, help="并行进程数（默认自动）。")

    year_group = download.add_mutually_exclusive_group(required=True)
    year_group.add_argument("--year", type=int, help="处理单一年份。")
    year_group.add_argument("--start-year", type=int, help="批量起始年份（与 --end-year 配对）。")
    download.add_argument("--end-year", type=int, help="批量结束年份（与 --start-year 配对）。")

    download.add_argument(
        "--pdf-dir-template",
        default="outputs/annual_reports/{year}/pdf",
        help="PDF输出目录模板（默认 outputs/annual_reports/{year}/pdf）。",
    )
    download.add_argument(
        "--txt-dir-template",
        default="outputs/annual_reports/{year}/txt",
        help="TXT输出目录模板（默认 outputs/annual_reports/{year}/txt）。",
    )
    download.add_argument(
        "--source",
        choices=["excel", "duckdb"],
        default="excel",
        help="任务数据源：excel（默认）从Excel读取，duckdb 从数据库读取待下载任务。",
    )
    download.add_argument(
        "--db-path",
        default="data/annual_reports.duckdb",
        help="DuckDB 数据库路径（仅当 --source duckdb 时使用，默认 data/annual_reports.duckdb）。",
    )
    download.add_argument(
        "--legacy",
        action="store_true",
        help="兼容模式：强制使用 Excel 数据源（等效 --source excel）。",
    )

    convert_only = subparsers.add_parser("convert-only", help="对本地PDF目录执行批量转换，无需Excel。")
    convert_only.add_argument("--pdf-dir", required=True, help="PDF源目录（相对路径以cwd为基准）。")
    convert_only.add_argument("--txt-dir", default="outputs/annual_reports/txt", help="TXT输出目录（默认 outputs/annual_reports/txt；设为 empty 表示同目录）。")
    convert_only.add_argument("--recursive", action="store_true", help="递归扫描子目录。")
    convert_only.add_argument("--force", action="store_true", help="强制覆盖（忽略已存在有效TXT）。")
    convert_only.add_argument("--delete-pdf", action="store_true", help="转换成功后删除源PDF（安全策略见文档）。")
    convert_only.add_argument("--processes", type=int, default=None, help="并行进程数（默认自动）。")
    convert_only.add_argument("--file-pattern", default="*.pdf", help="文件名匹配模式（默认 *.pdf，大小写不敏感识别.pdf/.PDF）。")

    return parser


def _set_log_level(level: str) -> None:
    logging.getLogger().setLevel(getattr(logging, level))


def _run_download_from_args(args: argparse.Namespace) -> None:
    if args.year is None:
        if args.start_year is None or args.end_year is None:
            raise SystemExit("download 批量模式需要同时指定 --start-year 与 --end-year")
        years = range(args.start_year, args.end_year + 1)
    else:
        years = [args.year]

    # 确定数据源：--legacy 强制使用 Excel
    use_duckdb = (args.source == "duckdb") and (not args.legacy)
    db_conn = None

    # 验证参数
    if not use_duckdb and not args.excel_file:
        raise SystemExit("使用 Excel 数据源时必须指定 --excel-file 参数")

    if use_duckdb:
        from annual_report_mda.db import init_db
        db_conn = init_db(args.db_path)
        logging.info(f"使用 DuckDB 数据源: {args.db_path}")

    for year in years:
        config = ConverterConfig(
            excel_file=args.excel_file if not use_duckdb else "",
            pdf_dir=args.pdf_dir_template.format(year=year),
            txt_dir=args.txt_dir_template.format(year=year),
            target_year=year,
            delete_pdf=args.delete_pdf,
            max_retries=args.max_retries,
            timeout=args.timeout,
            processes=args.processes,
        )
        processor = AnnualReportProcessor(config, db_conn=db_conn)
        ok = processor.run()
        if not ok:
            raise SystemExit(1)
        logging.info(f"{year}年年报处理完毕")

    if db_conn is not None:
        db_conn.close()


def _run_convert_only_from_args(args: argparse.Namespace) -> None:
    txt_dir: Optional[str]
    if args.txt_dir.strip() == "":
        txt_dir = None
    else:
        txt_dir = args.txt_dir

    processor = ConvertOnlyProcessor(
        ConvertOnlyConfig(
            pdf_dir=args.pdf_dir,
            txt_dir=txt_dir,
            recursive=args.recursive,
            delete_pdf=args.delete_pdf,
            force=args.force,
            processes=args.processes,
            file_pattern=args.file_pattern,
        )
    )
    processor.run()


def _run_with_yaml_config(args: argparse.Namespace) -> None:
    """使用 YAML 配置文件运行下载器。"""
    try:
        from annual_report_mda.config_manager import (
            load_config,
            apply_cli_overrides,
            log_config_summary,
        )
    except ImportError as e:
        logging.error(f"无法导入配置管理模块: {e}")
        logging.error("请确保已安装依赖: pip install -r requirements.txt")
        raise SystemExit(1)

    try:
        config = load_config(args.config)
    except FileNotFoundError as e:
        logging.error(str(e))
        raise SystemExit(1)
    except ValueError as e:
        logging.error(str(e))
        raise SystemExit(1)

    if config.project.log_level:
        logging.getLogger().setLevel(getattr(logging, config.project.log_level))

    log_config_summary(config, logging.getLogger())

    downloader_cfg = config.downloader
    crawler_cfg = config.crawler

    # 确定数据源模式
    use_duckdb = getattr(crawler_cfg, "output_mode", "excel") == "duckdb"
    db_conn = None

    if use_duckdb:
        from annual_report_mda.db import init_db
        db_path = config.project.db_path or "data/annual_reports.duckdb"
        db_conn = init_db(db_path)
        logging.info(f"使用 DuckDB 数据源: {db_path}")

    for year in crawler_cfg.target_years:
        excel_path = downloader_cfg.paths.input_excel_template.replace("{year}", str(year))
        pdf_dir = downloader_cfg.paths.pdf_dir_template.replace("{year}", str(year))
        txt_dir = downloader_cfg.paths.txt_dir_template.replace("{year}", str(year))

        legacy_config = ConverterConfig(
            excel_file=excel_path if not use_duckdb else "",
            pdf_dir=pdf_dir,
            txt_dir=txt_dir,
            target_year=year,
            delete_pdf=downloader_cfg.behavior.delete_pdf,
            max_retries=downloader_cfg.request.max_retries,
            timeout=downloader_cfg.request.timeout,
            chunk_size=downloader_cfg.request.chunk_size,
            processes=downloader_cfg.processes,
        )

        processor = AnnualReportProcessor(legacy_config, db_conn=db_conn)
        ok = processor.run()
        if not ok:
            raise SystemExit(1)
        logging.info(f"{year}年年报处理完毕")

    if db_conn is not None:
        db_conn.close()


def _run_with_embedded_config() -> None:
    # ==================== 配置区域 ====================

    # ==================== 模式选择 ====================
    RUN_MODE = "download"  # "download" | "convert_only"

    # Excel表格路径（建议使用绝对路径）
    # 2024年02月14日更新后，此处只需要填写总表的路径，请于网盘或github中获取总表
    EXCEL_FILE = "年报链接_2024【公众号：凌小添】.xlsx"

    # 是否删除转换后的PDF文件（节省磁盘空间）
    DELETE_PDF = False

    # 是否批量处理多个年份
    BATCH_MODE = True

    # 批量模式：年份区间（包含起始和结束年份）
    START_YEAR = 2022
    END_YEAR = 2024

    # 单独模式：指定年份
    SINGLE_YEAR = 2023

    # 下载配置
    MAX_RETRIES = 3  # 最大重试次数
    TIMEOUT = 15  # 请求超时（秒）
    PROCESSES = None  # 进程数（None表示自动）

    # ==================== 纯转换模式配置 ====================
    PDF_SOURCE_DIR = "annual_reports/pdf"
    TXT_OUTPUT_DIR = "outputs/annual_reports/txt"  # None则与PDF同目录
    RECURSIVE_SCAN = False
    FORCE_OVERWRITE = False
    FILE_PATTERN = "*.pdf"

    # ==================== 执行逻辑 ====================

    if RUN_MODE == "convert_only":
        processor = ConvertOnlyProcessor(
            ConvertOnlyConfig(
                pdf_dir=PDF_SOURCE_DIR,
                txt_dir=TXT_OUTPUT_DIR,
                recursive=RECURSIVE_SCAN,
                delete_pdf=DELETE_PDF,
                force=FORCE_OVERWRITE,
                processes=PROCESSES,
                file_pattern=FILE_PATTERN,
            )
        )
        processor.run()
        raise SystemExit(0)

    if BATCH_MODE:
        # 批量处理多个年份
        for year in range(START_YEAR, END_YEAR + 1):
            config = ConverterConfig(
                excel_file=EXCEL_FILE,
                pdf_dir=f'年报文件/{year}/pdf年报',
                txt_dir=f'年报文件/{year}/txt年报',
                target_year=year,
                delete_pdf=DELETE_PDF,
                max_retries=MAX_RETRIES,
                timeout=TIMEOUT,
                processes=PROCESSES
            )

            processor = AnnualReportProcessor(config)
            ok = processor.run()
            if not ok:
                logging.error("批量模式中止：本轮处理失败（通常为Excel路径/权限问题）。")
                raise SystemExit(1)
            logging.info(f"{year}年年报处理完毕")
    else:
        # 处理单独年份
        config = ConverterConfig(
            excel_file=EXCEL_FILE,
            pdf_dir=f'年报文件/{SINGLE_YEAR}/pdf年报',
            txt_dir=f'年报文件/{SINGLE_YEAR}/txt年报',
            target_year=SINGLE_YEAR,
            delete_pdf=DELETE_PDF,
            max_retries=MAX_RETRIES,
            timeout=TIMEOUT,
            processes=PROCESSES
        )

        processor = AnnualReportProcessor(config)
        ok = processor.run()
        if not ok:
            raise SystemExit(1)
        logging.info(f"{SINGLE_YEAR}年年报处理完毕")


def main(argv: list[str]) -> None:
    parser = _build_arg_parser()
    if len(argv) == 1:
        parser.print_help()
        raise SystemExit(0)

    args = parser.parse_args(argv[1:])
    _set_log_level(args.log_level)

    if args.use_yaml_config:
        _run_with_yaml_config(args)
        return

    if args.use_config:
        _run_with_embedded_config()
        return

    if args.command == "download":
        _run_download_from_args(args)
        return

    if args.command == "convert-only":
        _run_convert_only_from_args(args)
        return

    parser.print_help()
    raise SystemExit(2)


if __name__ == '__main__':
    main(sys.argv)
