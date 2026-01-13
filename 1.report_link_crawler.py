#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
@Project ：PycharmProjects
@File    ：巨潮资讯年报2.0.py
@IDE     ：PyCharm
@Author  ：lingxiaotian
@Date    ：2023/5/20 12:38
@LastEditTime: 2025/11/21 14:18
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import openpyxl
import requests

from annual_report_mda.utils import configure_logging

_LOG = logging.getLogger(__name__)

GZH = "【公众号：凌小添】"


@dataclass(frozen=True)
class CrawlerConfig:
    """爬虫配置类。"""

    target_year: int  # 目标年份
    exclude_keywords: list[str]  # 排除关键词列表
    trade: str = ""  # 行业过滤
    plate: str = "sz;sh"  # 板块控制
    max_retries: int = 3  # 最大重试次数
    retry_delay: int = 5  # 重试延迟（秒）
    timeout: int = 10  # 请求超时（秒）
    output_dir: str = "."  # 输出目录
    save_interval: int = 100  # 增量保存间隔（条数）
    strict_year_check: bool = True  # 是否严格检查年份匹配


class CNINFOClient:
    """巨潮资讯API客户端。"""

    BASE_URL = "http://www.cninfo.com.cn/new/hisAnnouncement/query"

    HEADERS = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Host": "www.cninfo.com.cn",
        "Origin": "http://www.cninfo.com.cn",
        "Referer": "http://www.cninfo.com.cn/new/commonUrl/pageOfSearch?url=disclosure/list/search&checkedCategory=category_ndbg_szsh",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
    }

    def __init__(self, config: CrawlerConfig) -> None:
        self.config = config
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)

    def _build_request_data(self, page_num: int, date_range: str) -> dict[str, Any]:
        """构建请求数据。"""
        return {
            "pageNum": page_num,
            "pageSize": 30,
            "column": "szse",
            "tabName": "fulltext",
            "plate": self.config.plate,
            "searchkey": "",
            "secid": "",
            "category": "category_ndbg_szsh",
            "trade": self.config.trade,
            "seDate": date_range,
            "sortName": "code",
            "sortType": "asc",
            "isHLtitle": "false",
        }

    def fetch_page(self, page_num: int, date_range: str) -> dict[str, Any] | None:
        """获取单页数据。

        Args:
            page_num: 页码
            date_range: 日期范围，格式：YYYY-MM-DD~YYYY-MM-DD

        Returns:
            API响应数据，失败返回None
        """
        data = self._build_request_data(page_num, date_range)

        for attempt in range(1, self.config.max_retries + 1):
            try:
                response = self.session.post(self.BASE_URL, data=data, timeout=self.config.timeout)
                response.raise_for_status()
                return response.json()

            except requests.exceptions.Timeout:
                logging.warning(
                    f"请求超时 (尝试 {attempt}/{self.config.max_retries}): {date_range} 第{page_num}页"
                )
            except requests.exceptions.RequestException as e:
                logging.warning(f"网络请求错误 (尝试 {attempt}/{self.config.max_retries}): {e}")
            except ValueError as e:
                logging.warning(f"JSON解析失败 (尝试 {attempt}/{self.config.max_retries}): {e}")

            if attempt < self.config.max_retries:
                time.sleep(self.config.retry_delay)

        logging.error(
            f"获取数据失败（已重试{self.config.max_retries}次）: {date_range} 第{page_num}页"
        )
        return None

    def fetch_all_pages(self, date_range: str) -> list[dict[str, Any]]:
        """获取指定日期范围的所有页面数据。

        Args:
            date_range: 日期范围

        Returns:
            所有公告数据列表
        """
        all_results = []

        # 先获取第一页，确定总页数
        first_page_data = self.fetch_page(1, date_range)
        if not first_page_data:
            return all_results

        total_pages = first_page_data.get("totalpages", 0)
        if total_pages == 0:
            logging.info(f"日期范围 {date_range} 无数据")
            return all_results

        # 处理第一页数据
        announcements = first_page_data.get("announcements")
        if announcements:
            all_results.extend(announcements)

        # 获取剩余页面
        for page_num in range(2, total_pages + 1):
            page_data = self.fetch_page(page_num, date_range)
            if page_data:
                announcements = page_data.get("announcements")
                if announcements:
                    all_results.extend(announcements)

            # 显示进度
            progress = (page_num / total_pages) * 100
            logging.info(
                "日期 %s: %d/%d 页 (%.1f%%)",
                date_range,
                page_num,
                total_pages,
                progress,
            )

        logging.info("日期范围 %s 完成，共获取 %d 条记录", date_range, len(all_results))
        return all_results


class DateRangeGenerator:
    """日期范围生成器。"""

    @staticmethod
    def generate_daily_ranges(year: int, end_date: datetime | None = None) -> list[str]:
        """生成指定年份每一天的日期范围。

        Args:
            year: 年份
            end_date: 截止日期（可选，默认为该年12月31日）

        Returns:
            日期范围列表，格式：["YYYY-MM-DD~YYYY-MM-DD", ...]
        """
        ranges = []
        start_date = datetime(year, 1, 1)
        if end_date is None:
            end_date = datetime(year, 12, 31)

        # 确保 end_date 不会跨年（如果只需要该年的话，但逻辑上由调用者保证更灵活）
        # 这里我们尊重调用者传入的 end_date，只要它 >= start_date

        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            ranges.append(f"{date_str}~{date_str}")
            current_date += timedelta(days=1)

        return ranges

    @staticmethod
    def generate_monthly_ranges(year: int) -> list[str]:
        """生成指定年份每个月的日期范围（备用方案）。

        Args:
            year: 年份

        Returns:
            日期范围列表
        """
        ranges = []
        for month in range(1, 13):
            if month == 12:
                next_month = 1
                next_year = year + 1
            else:
                next_month = month + 1
                next_year = year

            start_date = datetime(year, month, 1)
            end_date = datetime(next_year, next_month, 1) - timedelta(days=1)

            ranges.append(f"{start_date.strftime('%Y-%m-%d')}~{end_date.strftime('%Y-%m-%d')}")

        return ranges


class AnnualReportCrawler:
    """年报爬虫主类。"""

    def __init__(
        self,
        config: CrawlerConfig,
        db_conn: duckdb.DuckDBPyConnection | None = None,
    ) -> None:
        self.config = config
        self.client = CNINFOClient(config)
        self.db_conn = db_conn  # 可选的数据库连接，用于 DuckDB 模式

    def _clean_title(self, title: str) -> str:
        """清理标题。"""
        title = title.strip()
        title = re.sub(r"<.*?>", "", title)  # 移除HTML标签
        title = title.replace("：", "")
        return f"《{title}》"

    def _should_exclude(self, title: str) -> bool:
        """判断是否应该排除该标题。"""
        for keyword in self.config.exclude_keywords:
            if keyword in title:
                return True
        return False

    def _parse_announcement(self, item: dict[str, Any]) -> dict[str, str] | None:
        """解析单条公告数据。

        Returns:
            解析后的数据字典，如果应该排除则返回None
        """
        try:
            company_code = item["secCode"]
            company_name = item["secName"]
            title = self._clean_title(item["announcementTitle"])

            # 检查是否排除
            if self._should_exclude(title):
                return None

            # 提取年份
            year_match = re.search(r"(\d{4})年", title)
            if year_match:
                year = year_match.group(1)
            else:
                year = str(self.config.target_year)

            # 严格年份检查：标题中的年份必须与目标年份一致
            if self.config.strict_year_check:
                if year != str(self.config.target_year):
                    logging.debug(
                        f"年份不匹配，跳过: {title} (期望{self.config.target_year}年，实际{year}年)"
                    )
                    return None

            # 构建完整URL
            adjunct_url = item["adjunctUrl"]
            announcement_url = f"http://static.cninfo.com.cn/{adjunct_url}"

            return {
                "company_code": company_code,
                "company_name": company_name,
                "title": title,
                "year": year,
                "url": announcement_url,
            }
        except (KeyError, AttributeError) as e:
            logging.warning(f"解析公告数据失败: {e}")
            return None

    def _save_to_excel(self, data: list[dict[str, str]], output_path: str) -> None:
        """保存数据到Excel。"""
        workbook = openpyxl.Workbook()
        worksheet = workbook.active
        worksheet.title = "公众号 凌小添"

        # 写入表头
        worksheet.append(["公司代码", "公司简称", "标题", "年份", "年报链接"])

        # 写入数据
        for item in data:
            worksheet.append(
                [
                    item["company_code"],
                    item["company_name"],
                    item["title"],
                    item["year"],
                    item["url"],
                ]
            )

        workbook.save(output_path)
        logging.info(f"Excel文件保存成功: {output_path}")

    def _save_to_duckdb(self, data: list[dict[str, str]]) -> tuple:
        """保存数据到 DuckDB。返回 (新增数, 跳过数)。"""
        if self.db_conn is None:
            raise RuntimeError("未提供数据库连接")

        from annual_report_mda.db import insert_report, upsert_company

        new_count = 0
        skip_count = 0

        for item in data:
            # 先 upsert 公司信息
            upsert_company(
                self.db_conn,
                stock_code=item["company_code"],
                short_name=item["company_name"],
            )

            # 插入年报记录（增量）
            is_new = insert_report(
                self.db_conn,
                stock_code=item["company_code"],
                year=int(item["year"]),
                url=item["url"],
                title=item["title"],
                source="cninfo",
            )

            if is_new:
                new_count += 1
            else:
                skip_count += 1

        return new_count, skip_count

    def run(self) -> None:
        """执行爬取任务。"""
        logging.info("=" * 60)
        logging.info("巨潮资讯年报爬虫启动")
        logging.info(f"目标年份: {self.config.target_year}")
        logging.info(f"板块: {self.config.plate}")
        logging.info(f"行业: {self.config.trade if self.config.trade else '全部'}")
        logging.info(f"排除关键词: {', '.join(self.config.exclude_keywords)}")
        logging.info(f"增量保存间隔: 每{self.config.save_interval}条")
        logging.info(f"严格年份检查: {'开启' if self.config.strict_year_check else '关闭'}")
        logging.info("=" * 60)

        # 生成日期范围（按天）
        # 业务逻辑：抓取 N 年年报，应在 N+1 年检索
        # 优化：不爬取未来日期，且默认止于 N+1 年的 5 月 1 日（A股年报披露基本在4月30日结束）
        search_year = self.config.target_year + 1
        end_of_search = datetime(search_year, 12, 31)

        # 如果是当前年份或未来年份，截断到今天
        today = datetime.now()
        actual_end_date = min(end_of_search, today)

        # 进一步优化：如果是年报，通常 4月30日 就结束了，5月之后大多是冗余请求
        # 默认限制到 5月31日 以覆盖补报和更正，除非当前日期还没到 5月31日
        # 设置截止日期为 min(actual_end_date, 5月31日)
        annual_report_cutoff = datetime(search_year, 5, 31)
        actual_end_date = min(actual_end_date, annual_report_cutoff)

        date_ranges = DateRangeGenerator.generate_daily_ranges(
            search_year, end_date=actual_end_date
        )
        logging.info(
            f"将按 {len(date_ranges)} 个日期范围进行爬取 (截止至 {actual_end_date.strftime('%Y-%m-%d')})"
        )

        # 输出文件路径
        output_filename = f"年报链接_{self.config.target_year}{GZH}.xlsx"
        output_path = Path(self.config.output_dir) / output_filename

        # 爬取和解析数据（边爬边解析）
        parsed_data = []
        total_raw_count = 0
        filtered_count = 0

        for idx, date_range in enumerate(date_ranges, 1):
            logging.info(f"[{idx}/{len(date_ranges)}] 正在爬取: {date_range}")
            results = self.client.fetch_all_pages(date_range)
            total_raw_count += len(results)

            # 立即解析和过滤
            for announcement in results:
                parsed = self._parse_announcement(announcement)
                if parsed:
                    parsed_data.append(parsed)
                else:
                    filtered_count += 1

            # 增量保存 (仅 Excel 模式)
            if self.db_conn is None and len(parsed_data) >= self.config.save_interval:
                self._save_to_excel(parsed_data, str(output_path))
                logging.info(f"增量保存: 已保存 {len(parsed_data)} 条有效记录")

            # 避免请求过快
            if idx < len(date_ranges):
                time.sleep(0.5)

        # 最终保存
        if parsed_data:
            if self.db_conn is not None:
                new_count, skip_count = self._save_to_duckdb(parsed_data)
                logging.info(f"DuckDB 写入完成: 新增 {new_count}, 跳过(已存在) {skip_count}")
            else:
                self._save_to_excel(parsed_data, str(output_path))

        # 统计信息
        logging.info("=" * 60)
        logging.info("爬取完成统计:")
        logging.info(f"  原始记录: {total_raw_count} 条")
        logging.info(f"  过滤记录: {filtered_count} 条")
        logging.info(f"  有效记录: {len(parsed_data)} 条")
        if self.db_conn is None:
            logging.info(f"  保存路径: {output_path}")
        else:
            logging.info("  保存模式: DuckDB")
        logging.info("=" * 60)
        logging.info(f"{self.config.target_year}年年报爬取完成")
        logging.info("=" * 60)


def _build_arg_parser() -> argparse.ArgumentParser:
    """构建命令行参数解析器。"""
    parser = argparse.ArgumentParser(
        prog="1.report_link_crawler.py",
        description="巨潮资讯年报链接爬虫，按天分片爬取，支持板块/行业过滤。",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--use-config",
        action="store_true",
        help="使用 config.yaml 配置文件运行（推荐）。",
    )
    parser.add_argument(
        "--config",
        "-c",
        default="config.yaml",
        help="指定配置文件路径（默认 config.yaml）。",
    )
    parser.add_argument(
        "--year",
        "-y",
        type=int,
        nargs="+",
        help="目标年份（覆盖配置文件），支持多年份如 --year 2022 2023。",
    )
    parser.add_argument(
        "--log-level",
        default=None,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="日志级别（覆盖配置文件）。",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        help="请求超时秒数（覆盖配置文件）。",
    )
    parser.add_argument(
        "--output-mode",
        choices=["excel", "duckdb"],
        default="duckdb",
        help="输出模式: excel (旧版) 或 duckdb (推荐，默认)。",
    )
    parser.add_argument(
        "--legacy",
        action="store_true",
        help="等同于 --output-mode excel，兼容旧版工作流。",
    )
    return parser


def _run_with_config(args: argparse.Namespace) -> None:
    """使用配置文件运行爬虫。"""
    try:
        from annual_report_mda.config_manager import (
            apply_cli_overrides,
            load_config,
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

    overrides = {}
    if args.year:
        overrides["target_years"] = args.year
    if args.log_level:
        overrides["log_level"] = args.log_level
    if args.timeout:
        overrides["timeout"] = args.timeout

    if overrides:
        config = apply_cli_overrides(config, overrides)

    # Configure logging with config settings
    from annual_report_mda.utils import configure_logging_from_config

    configure_logging_from_config(
        log_level=args.log_level or config.project.log_level,
        logging_config=config.logging,
    )

    log_config_summary(config, logging.getLogger())

    # 确定输出模式
    use_duckdb = not args.legacy and args.output_mode == "duckdb"

    # 初始化数据库连接（DuckDB 模式）
    db_conn = None
    if use_duckdb:
        try:
            from annual_report_mda.db import init_db

            db_path = config.database.path
            db_conn = init_db(db_path)
            logging.info(f"DuckDB 模式: 数据将写入 {db_path}")
        except ImportError as e:
            logging.error(f"无法导入数据库模块: {e}")
            raise SystemExit(1)

    crawler_cfg = config.crawler
    for year in crawler_cfg.target_years:
        plates_str = ";".join(crawler_cfg.filters.plates)
        legacy_config = CrawlerConfig(
            target_year=year,
            exclude_keywords=crawler_cfg.filters.exclude_keywords,
            trade=crawler_cfg.filters.trade,
            plate=plates_str,
            max_retries=crawler_cfg.request.max_retries,
            retry_delay=crawler_cfg.request.retry_delay,
            timeout=crawler_cfg.request.timeout,
            output_dir=str(Path(crawler_cfg.output.excel_path_template).parent),
            save_interval=crawler_cfg.output.save_interval,
            strict_year_check=True,
        )

        crawler = AnnualReportCrawler(legacy_config, db_conn=db_conn)
        crawler.run()
        logging.info(f"{year}年处理完成")

    if db_conn is not None:
        db_conn.close()


def _run_with_embedded_config() -> None:
    """使用脚本底部嵌入配置运行（旧版兼容）。"""
    # Configure logging with defaults
    configure_logging(level="INFO")

    # ==================== 配置区域 ====================

    # 目标年份
    TARGET_YEAR = 2024
    # 排除关键词列表（可加入'更正后'、'修订版'等）
    EXCLUDE_KEYWORDS = ["英文", "已取消", "摘要"]

    # 行业过滤（为空则不过滤）
    TRADE = ""

    # 板块控制：深市sz 沪市sh 深主板szmb 沪主板shmb 创业板szcy 科创板shkcp 北交所bj
    PLATE = "sz;sh"

    # 爬虫配置
    MAX_RETRIES = 3
    RETRY_DELAY = 5
    TIMEOUT = 10
    OUTPUT_DIR = "."
    SAVE_INTERVAL = 100
    STRICT_YEAR_CHECK = True

    # 是否批量处理多个年份
    BATCH_MODE = False
    START_YEAR = 2020
    END_YEAR = 2023

    # ==================== 执行逻辑 ====================

    if BATCH_MODE:
        for year in range(START_YEAR, END_YEAR + 1):
            config = CrawlerConfig(
                target_year=year,
                exclude_keywords=EXCLUDE_KEYWORDS,
                trade=TRADE,
                plate=PLATE,
                max_retries=MAX_RETRIES,
                retry_delay=RETRY_DELAY,
                timeout=TIMEOUT,
                output_dir=OUTPUT_DIR,
                save_interval=SAVE_INTERVAL,
                strict_year_check=STRICT_YEAR_CHECK,
            )

            crawler = AnnualReportCrawler(config)
            crawler.run()

            logging.info("%d年处理完成", year)
    else:
        config = CrawlerConfig(
            target_year=TARGET_YEAR,
            exclude_keywords=EXCLUDE_KEYWORDS,
            trade=TRADE,
            plate=PLATE,
            max_retries=MAX_RETRIES,
            retry_delay=RETRY_DELAY,
            timeout=TIMEOUT,
            output_dir=OUTPUT_DIR,
            save_interval=SAVE_INTERVAL,
            strict_year_check=STRICT_YEAR_CHECK,
        )

        crawler = AnnualReportCrawler(config)
        crawler.run()

        logging.info("%d年处理完成", TARGET_YEAR)


def main(argv: list[str]) -> None:
    """主入口函数。"""
    parser = _build_arg_parser()

    if len(argv) == 1:
        logging.info("提示: 无参数运行将使用脚本内置配置。使用 --use-config 启用配置文件模式。")
        logging.info("      使用 --help 查看更多选项。")
        _run_with_embedded_config()
        return

    args = parser.parse_args(argv[1:])

    if args.use_config:
        _run_with_config(args)
    else:
        _run_with_embedded_config()


if __name__ == "__main__":
    main(sys.argv)
