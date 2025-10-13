import logging
import os
import sys
import time
import argparse
import json
import threading
from datetime import datetime, timezone, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import math

import requests
import schedule
from dotenv import load_dotenv

# --- 配置 ---
load_dotenv()

SATELLITE = os.getenv("SATELLITE", "himawari")
MIN_ZOOM = int(os.getenv("MIN_ZOOM", "4"))
MAX_ZOOM = int(os.getenv("MAX_ZOOM", "7"))
BASE_DOWNLOAD_PATH = Path(os.getenv("DOWNLOAD_PATH", "./data/tiles"))
CHECK_INTERVAL_HOURS = int(os.getenv("CHECK_INTERVAL_HOURS", "3"))
CONCURRENCY = int(os.getenv("CONCURRENCY", "10"))
# 数据清理配置，单位为天
CLEANUP_DAYS = int(os.getenv("CLEANUP_DAYS", "30"))


API_URL = "https://tiles.zoom.earth/times/geocolor.json"
HEADERS = {
    'accept': '*/*',
    'accept-language': 'zh-CN,zh;q=0.9',
    'origin': 'https://zoom.earth',
    'referer': 'https://zoom.earth/',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36'
}

HARDCODED_RANGES = {
    "himawari": {
        3: {"x": range(2, 6), "y_ranges": [range(0, 1), range(5, 8)]},
        4: {"x": range(4, 12), "y_ranges": [range(0, 1), range(11, 16)]},
        5: {"x": range(11, 20), "y_ranges": [range(23, 32)]},
        6: {"x": range(16, 48), "y_ranges": [range(0, 1), range(47, 64)]},
        7: {"x": range(32, 96), "y_ranges": [range(0, 2), range(95, 128)]},
    }
    ## 仅用于中国地图
    # "himawari": {
    #     3: {"x": range(2, 4), "y_ranges": [range(0, 1), range(5, 8)]},
    #     4: {"x": range(4, 8), "y_ranges": [range(0, 1), range(11, 16)]},
    #     5: {"x": range(11, 16), "y_ranges": [range(23, 32)]},
    #     6: {"x": range(16, 32), "y_ranges": [range(0, 1), range(47, 64)]},
    #     7: {"x": range(32, 64), "y_ranges": [range(0, 2), range(95, 128)]},
    # }
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

timestamp_lock = threading.Lock()
progress_lock = threading.Lock()
processed_count = 0

def get_tile_ranges_for_satellite(zoom, satellite):
    sat_ranges = HARDCODED_RANGES.get(satellite)
    if not sat_ranges:
        raise ValueError(f"未找到卫星 '{satellite}' 的硬编码范围定义。")
    zoom_ranges = sat_ranges.get(zoom)
    if not zoom_ranges:
        raise ValueError(f"未找到卫星 '{satellite}' 在 zoom={zoom} 的硬编码范围定义。")
    return zoom_ranges["x"], zoom_ranges["y_ranges"]

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
    dt = datetime.fromtimestamp(timestamp, timezone.utc)
    date_str = dt.strftime("%Y-%m-%d")
    time_str = dt.strftime("%H%M")
    save_path = BASE_DOWNLOAD_PATH / satellite / str(zoom) / str(x) / str(y)
    file_path = save_path / f"{timestamp}.jpg"
    if file_path.exists():
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
            temp_file.unlink()
            return "failed_small", satellite, None
        temp_file.rename(file_path)
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
    if not new_timestamps:
        return
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

def get_local_timestamps(satellite: str) -> set:
    """从本地文件加载已记录的时间戳"""
    record_file = BASE_DOWNLOAD_PATH / satellite / "timestamps.json"
    if not record_file.exists():
        return set()
    try:
        with open(record_file, 'r') as f:
            return set(json.load(f))
    except (IOError, json.JSONDecodeError) as e:
        logger.warning(f"读取本地时间戳记录失败: {e}, 将作为首次运行处理。")
        return set()

def run_download_task(is_debug_run: bool = False, debug_zoom: int = None):
    """执行瓦片下载的核心逻辑"""
    global processed_count
    
    if is_debug_run:
        logger.info("--- 开始执行调试下载任务 ---")
        logger.info(f"卫星: {SATELLITE}, Zoom: {debug_zoom}")
        zoom_levels = [debug_zoom]
    else:
        logger.info("--- 开始执行增量下载任务 ---")
        logger.info(f"卫星: {SATELLITE}, Zooms: {MIN_ZOOM}-{MAX_ZOOM}")
        zoom_levels = range(MIN_ZOOM, MAX_ZOOM + 1)

    remote_timestamps = set(fetch_latest_timestamps(SATELLITE))
    if not remote_timestamps:
        logger.warning("未获取到任何远程时间戳，下载任务结束。")
        return

    if is_debug_run:
        timestamps_to_process = {sorted(list(remote_timestamps), reverse=True)[0]} if remote_timestamps else set()
        logger.info(f"调试模式：仅使用最新时间戳: {list(timestamps_to_process)[0] if timestamps_to_process else 'N/A'}")
    else:
        local_timestamps = get_local_timestamps(SATELLITE)
        timestamps_to_process = remote_timestamps - local_timestamps
        logger.info(f"本地已记录 {len(local_timestamps)} 个时间戳，发现 {len(timestamps_to_process)} 个新时间戳需要处理。")

    if not timestamps_to_process:
        logger.info("所有时间戳均已处理，无需下载。下载任务结束。")
        return
    
    tasks = []
    for zoom in zoom_levels:
        try:
            x_range, y_ranges_list = get_tile_ranges_for_satellite(zoom, SATELLITE)
            logger.info(f"Zoom {zoom}: 使用硬编码范围 X: {list(x_range)}, Y 范围列表: {[list(r) for r in y_ranges_list]}")
            for ts in timestamps_to_process:
                for x in x_range:
                    for y_range in y_ranges_list:
                        for y in y_range:
                            tasks.append((SATELLITE, ts, zoom, x, y))
        except ValueError as e:
            logger.error(f"无法为 Zoom {zoom} 生成任务: {e}")
            continue

    total_tasks = len(tasks)
    logger.info(f"共计为新时间戳生成 {total_tasks} 个潜在下载任务。")
    if not tasks:
        return

    stats = {"downloaded": 0, "skipped": 0, "failed": 0, "failed_small": 0}
    successful_timestamps = set()
    processed_count = 0 

    with ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
        futures = [executor.submit(download_tile, *task) for task in tasks]
        for future in as_completed(futures):
            with progress_lock:
                processed_count += 1
                if processed_count % 500 == 0 or processed_count == total_tasks:
                    logger.info(f"下载进度: {processed_count} / {total_tasks} ({(processed_count/total_tasks)*100:.1f}%)")
            try:
                status, _, timestamp = future.result()
                stats[status] += 1
                if timestamp:
                    successful_timestamps.add(timestamp)
            except Exception as exc:
                logger.error(f"一个下载任务产生未知异常: {exc}")
                stats["failed"] += 1

    logger.info("下载任务执行完毕。统计:")
    logger.info(f"  - 新下载: {stats['downloaded']}")
    logger.info(f"  - 已跳过: {stats['skipped']}")
    logger.info(f"  - 下载失败: {stats['failed'] + stats['failed_small']}")
    
    update_timestamp_record(SATELLITE, successful_timestamps)
    logger.info("--- 增量下载任务结束 ---")

def prune_empty_dirs(path: Path) -> int:
    """递归删除空目录并返回删除的数量"""
    if not path.is_dir() or path == BASE_DOWNLOAD_PATH or path == (BASE_DOWNLOAD_PATH / SATELLITE):
        return 0
    
    deleted_count = 0
    try:
        if not any(path.iterdir()):
            path.rmdir()
            logger.debug(f"已删除空目录: {path}")
            deleted_count += 1
            # 递归到父目录
            deleted_count += prune_empty_dirs(path.parent)
    except OSError as e:
        logger.warning(f"删除目录失败 {path}: {e}")
    return deleted_count

def run_cleanup_task():
    """清理旧的瓦片数据和时间戳记录"""
    logger.info("--- 开始执行数据清理任务 ---")
    logger.info(f"将清理早于 {CLEANUP_DAYS} 天的数据...")
    
    cutoff_dt = datetime.now(timezone.utc) - timedelta(days=CLEANUP_DAYS)
    cutoff_timestamp = int(cutoff_dt.timestamp())
    logger.info(f"清理时间戳阈值: {cutoff_timestamp} ({cutoff_dt.strftime('%Y-%m-%d %H:%M:%S %Z')})")

    data_root = BASE_DOWNLOAD_PATH / SATELLITE
    if not data_root.exists():
        logger.info("数据目录不存在，无需清理。")
        return

    # 1. 清理旧的瓦片文件
    deleted_files_count = 0
    deleted_dirs_count = 0
    checked_files = 0
    all_files = list(data_root.rglob("*.jpg"))
    total_files = len(all_files)
    logger.info(f"发现 {total_files} 个瓦片文件待检查...")
    
    for file_path in all_files:
        checked_files += 1
        if checked_files % 5000 == 0:
            logger.info(f"已检查 {checked_files}/{total_files} 个文件...")
        try:
            file_ts = int(file_path.stem)
            if file_ts < cutoff_timestamp:
                parent_dir = file_path.parent
                file_path.unlink()
                deleted_files_count += 1
                # 尝试清理可能变为空的父目录
                deleted_dirs_count += prune_empty_dirs(parent_dir)
        except (ValueError, FileNotFoundError):
            logger.warning(f"无法从文件名解析时间戳或文件已消失: {file_path}")
            continue

    logger.info(f"文件清理完成。共删除 {deleted_files_count} 个旧瓦片文件，清理了 {deleted_dirs_count} 个空目录。")

    # 2. 清理 timestamps.json 文件
    record_file = data_root / "timestamps.json"
    if not record_file.exists():
        logger.info("时间戳记录文件不存在，无需清理。")
        logger.info("--- 数据清理任务结束 ---")
        return
        
    with timestamp_lock:
        try:
            with open(record_file, 'r') as f:
                timestamps = json.load(f)
            
            original_count = len(timestamps)
            kept_timestamps = [ts for ts in timestamps if ts >= cutoff_timestamp]
            new_count = len(kept_timestamps)
            
            if new_count < original_count:
                with open(record_file, 'w') as f:
                    json.dump(sorted(kept_timestamps), f, indent=2)
                logger.info(f"时间戳记录文件已更新：从 {original_count} 个条目清理至 {new_count} 个，移除了 {original_count - new_count} 个旧条目。")
            else:
                logger.info("时间戳记录文件中没有需要清理的旧条目。")
        except (IOError, json.JSONDecodeError) as e:
            logger.error(f"读写时间戳记录文件以进行清理时失败: {e}")

    logger.info("--- 数据清理任务结束 ---")

def run_job_cycle(is_debug_run: bool = False, debug_zoom: int = None):
    """
    运行一个完整的任务周期，包括下载和清理。
    在调试模式下，仅运行下载任务。
    """
    logger.info("=============================================")
    logger.info(f"开始执行任务周期...")
    
    # 步骤 1: 执行下载任务
    run_download_task(is_debug_run=is_debug_run, debug_zoom=debug_zoom)

    # 步骤 2: 执行清理任务 (仅在非调试模式下)
    if not is_debug_run:
        run_cleanup_task()
    else:
        logger.info("处于调试模式，跳过数据清理任务。")

    logger.info(f"任务周期执行完毕。")
    logger.info("=============================================\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Zoom Earth Tile Downloader and Manager")
    parser.add_argument(
        "mode",
        nargs='?',
        default="service",
        choices=["run-once", "service"],
        help="运行模式: 'run-once' (用于调试下载，只运行一次且不清理) 或 'service' (作为服务持续运行，包含下载和清理)."
    )
    parser.add_argument(
        "--zoom",
        type=int,
        default=3,
        help="在 'run-once' 模式下，指定要测试的 zoom 等级。"
    )
    args = parser.parse_args()

    if args.mode == "run-once":
        logger.info(f"以 'run-once' 模式启动，下载任务将仅执行一次。")
        # 调试模式只运行下载，不运行清理
        run_download_task(is_debug_run=True, debug_zoom=args.zoom)
    else: # mode == "service"
        logger.info(f"服务启动，将每隔 {CHECK_INTERVAL_HOURS} 小时执行一次完整任务周期（下载+清理）。")
        # 立即执行一次完整周期
        run_job_cycle()
        # 然后设置定时任务
        schedule.every(CHECK_INTERVAL_HOURS).hours.do(run_job_cycle)
        while True:
            schedule.run_pending()
            time.sleep(60)