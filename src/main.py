import logging
import os
import sys
import time
import argparse
import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import math

import requests
import schedule
from dotenv import load_dotenv

# --- 配置 ---
load_dotenv()

SATELLITE = os.getenv("SATELLITE", "himawari")
MIN_ZOOM = int(os.getenv("MIN_ZOOM", "1"))
MAX_ZOOM = int(os.getenv("MAX_ZOOM", "7"))
BASE_DOWNLOAD_PATH = Path(os.getenv("DOWNLOAD_PATH", "./data/tiles"))
CHECK_INTERVAL_HOURS = int(os.getenv("CHECK_INTERVAL_HOURS", "3"))
CONCURRENCY = int(os.getenv("CONCURRENCY", "10"))

API_URL = "https://tiles.zoom.earth/times/geocolor.json"
HEADERS = {
    'accept': '*/*',
    'accept-language': 'zh-CN,zh;q=0.9',
    'origin': 'https://zoom.earth',
    'referer': 'https://zoom.earth/',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36'
}

# 使用经过验证的瓦片范围，确保只下载真实存在的瓦片。
HARDCODED_RANGES = {
    "himawari": {
        1: {"x": range(1, 2), "y_ranges": [range(0, 2)]},
        2: {"x": range(2, 4), "y_ranges": [range(0, 1), range(2, 4)]},
        3: {"x": range(2, 6), "y_ranges": [range(0, 1), range(5, 8)]},
        4: {"x": range(4, 12), "y_ranges": [range(0, 1), range(11, 16)]},
        5: {"x": range(11, 20), "y_ranges": [range(23, 32)]},
        6: {"x": range(16, 48), "y_ranges": [range(0, 1), range(47, 64)]},
        7: {"x": range(32, 96), "y_ranges": [range(0, 2), range(95, 128)]},
    }
}

# --- 日志配置 ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# --- 全局变量和锁 ---
# 用于在多线程环境下安全地更新时间戳记录
timestamp_lock = threading.Lock()


def get_tile_ranges_for_satellite(zoom, satellite):
    sat_ranges = HARDCODED_RANGES.get(satellite)
    if not sat_ranges:
        raise ValueError(f"未找到卫星 '{satellite}' 的硬编码范围定义。")
    zoom_ranges = sat_ranges.get(zoom)
    if not zoom_ranges:
        raise ValueError(f"未找到卫星 '{satellite}' 在 zoom={zoom} 的硬编码范围定义。")
    return zoom_ranges["x"], zoom_ranges["y_ranges"]

# --- 核心功能 ---
def fetch_latest_timestamps(satellite: str) -> list[int]:
    try:
        logger.info(f"正在从 {API_URL} 获取最新时间戳...")
        response = requests.get(API_URL, headers=HEADERS, timeout=15)
        response.raise_for_status()
        data = response.json()
        timestamps = data.get(satellite, [])
        logger.info(f"成功获取到卫星 '{satellite}' 的 {len(timestamps)} 个时间点。")
        return sorted(timestamps, reverse=True)
    except requests.exceptions.RequestException as e:
        logger.error(f"获取时间戳失败: {e}")
        return []

def download_tile(satellite: str, timestamp: int, zoom: int, x: int, y: int) -> tuple:
    """下载单个瓦片，如果成功，返回状态和时间戳，否则只返回状态。"""
    dt = datetime.fromtimestamp(timestamp, timezone.utc)
    date_str = dt.strftime("%Y-%m-%d")
    time_str = dt.strftime("%H%M")
    save_path = BASE_DOWNLOAD_PATH / satellite / str(zoom) / str(x) / str(y)
    file_path = save_path / f"{timestamp}.jpg"

    if file_path.exists():
        # 即使文件已存在，我们依然认为这个时间戳是“有效”的
        return "skipped", satellite, timestamp

    save_path.mkdir(parents=True, exist_ok=True)
    url = f"https://tiles.zoom.earth/geocolor/{satellite}/{date_str}/{time_str}/{zoom}/{x}/{y}.jpg"
    temp_file = file_path.with_suffix(".tmp")

    try:
        response = requests.get(url, headers=HEADERS, stream=True, timeout=20)
        response.raise_for_status()
        with open(temp_file, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        if temp_file.stat().st_size < 200:
            logger.warning(f"下载的文件过小，可能为空图，已删除: {url}")
            temp_file.unlink()
            return "failed_small", satellite, None
        temp_file.rename(file_path)
        # 成功下载，返回状态和时间戳
        return "downloaded", satellite, timestamp
    except requests.exceptions.RequestException as e:
        if 'temp_file' in locals() and temp_file.exists():
            temp_file.unlink()
        if e.response and e.response.status_code == 404:
            logger.debug(f"瓦片不存在 (404): {url}")
        else:
            logger.warning(f"下载失败: {url} | 错误: {e}")
        return "failed", satellite, None

def update_timestamp_record(satellite: str, new_timestamps: set):
    """
    读取、更新并写回指定卫星的时间戳记录文件。
    这是一个原子操作，通过文件锁确保数据一致性。
    """
    if not new_timestamps:
        return

    # 文件路径位于 /data/tiles/himawari/timestamps.json
    record_file = BASE_DOWNLOAD_PATH / satellite / "timestamps.json"
    record_file.parent.mkdir(parents=True, exist_ok=True)

    with timestamp_lock:
        try:
            if record_file.exists():
                with open(record_file, 'r') as f:
                    existing_timestamps = set(json.load(f))
            else:
                existing_timestamps = set()

            updated_count = len(new_timestamps - existing_timestamps)
            if updated_count > 0:
                all_timestamps = sorted(list(existing_timestamps.union(new_timestamps)))
                with open(record_file, 'w') as f:
                    json.dump(all_timestamps, f, indent=2)
                logger.info(f"时间戳记录文件 '{record_file.name}' 已更新，新增 {updated_count} 个时间点。")
            else:
                logger.info("没有新的时间戳需要记录。")

        except (IOError, json.JSONDecodeError) as e:
            logger.error(f"读写时间戳记录文件失败: {e}")

def run_download_job(is_debug_run: bool = False, debug_zoom: int = None):
    if is_debug_run:
        logger.info("=============================================")
        logger.info(f"开始执行调试任务，卫星: {SATELLITE}, Zoom: {debug_zoom}")
        zoom_levels = [debug_zoom]
    else:
        logger.info("=============================================")
        logger.info(f"开始执行下载任务，卫星: {SATELLITE}, Zooms: {MIN_ZOOM}-{MAX_ZOOM}")
        zoom_levels = range(MIN_ZOOM, MAX_ZOOM + 1)

    timestamps = fetch_latest_timestamps(SATELLITE)
    if not timestamps:
        logger.warning("未获取到任何时间戳，本次任务结束。")
        return

    if is_debug_run and timestamps:
        timestamps = timestamps[:1]
        logger.info(f"调试模式：仅使用最新时间戳: {timestamps[0]}")

    tasks = []
    for zoom in zoom_levels:
        try:
            x_range, y_ranges_list = get_tile_ranges_for_satellite(zoom, SATELLITE)
            logger.info(f"Zoom {zoom}: 使用硬编码范围 X: {list(x_range)}, Y 范围列表: {[list(r) for r in y_ranges_list]}")
            for ts in timestamps:
                for x in x_range:
                    for y_range in y_ranges_list:
                        for y in y_range:
                            tasks.append((SATELLITE, ts, zoom, x, y))
        except ValueError as e:
            logger.error(f"无法为 Zoom {zoom} 生成任务: {e}")
            continue

    logger.info(f"共计生成 {len(tasks)} 个潜在下载任务。")
    if not tasks:
        return

    stats = {"downloaded": 0, "skipped": 0, "failed": 0, "failed_small": 0}
    # 用于收集本次任务中所有成功下载或已存在的瓦片对应的时间戳
    successful_timestamps = set()

    with ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
        futures = [executor.submit(download_tile, *task) for task in tasks]
        for future in as_completed(futures):
            try:
                status, satellite, timestamp = future.result()
                stats[status] += 1
                if timestamp: # 如果返回了有效的时间戳
                    successful_timestamps.add(timestamp)
            except Exception as exc:
                logger.error(f"一个下载任务产生未知异常: {exc}")
                stats["failed"] += 1

    logger.info("任务执行完毕。统计:")
    logger.info(f"  - 新下载: {stats['downloaded']}")
    logger.info(f"  - 已跳过: {stats['skipped']}")
    logger.info(f"  - 下载失败: {stats['failed'] + stats['failed_small']}")
    
    # 在所有下载任务完成后，更新时间戳记录文件
    update_timestamp_record(SATELLITE, successful_timestamps)
    
    logger.info("=============================================\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Zoom Earth Tile Downloader")
    parser.add_argument(
        "mode",
        nargs='?',
        default="service",
        choices=["run-once", "service"],
        help="运行模式: 'run-once' (用于调试，只运行一次) 或 'service' (作为服务持续运行)."
    )
    parser.add_argument(
        "--zoom",
        type=int,
        default=7,
        help="在 'run-once' 模式下，指定要测试的 zoom 等级。"
    )
    args = parser.parse_args()

    if args.mode == "run-once":
        logger.info(f"以 'run-once' 模式启动，任务将仅执行一次。")
        run_download_job(is_debug_run=True, debug_zoom=args.zoom)
    else: # mode == "service"
        logger.info(f"服务启动，将每隔 {CHECK_INTERVAL_HOURS} 小时执行一次任务。")
        run_download_job()
        schedule.every(CHECK_INTERVAL_HOURS).hours.do(run_download_job)
        while True:
            schedule.run_pending()
            time.sleep(60)