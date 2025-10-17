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

import requests
import schedule
from dotenv import load_dotenv

class Config:
    """应用配置类"""
    def __init__(self):
        load_dotenv()
        self.SATELLITE = os.getenv("SATELLITE", "himawari")
        self.MIN_ZOOM = int(os.getenv("MIN_ZOOM", "4"))
        self.MAX_ZOOM = int(os.getenv("MAX_ZOOM", "7"))
        self.BASE_DOWNLOAD_PATH = Path(os.getenv("DOWNLOAD_PATH", "./data/tiles"))
        self.CHECK_INTERVAL_MINUTES = int(os.getenv("CHECK_INTERVAL_MINUTES", "10"))
        self.CONCURRENCY = int(os.getenv("CONCURRENCY", "10"))
        self.CLEANUP_DAYS = int(os.getenv("CLEANUP_DAYS", "30"))
        self.API_URL = "https://tiles.zoom.earth/times/geocolor.json"
        self.HEADERS = {
            'accept': '*/*',
            'accept-language': 'zh-CN,zh;q=0.9',
            'origin': 'https://zoom.earth',
            'referer': 'https://zoom.earth/',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36'
        }
        self.HARDCODED_RANGES = {
            "himawari": {
                3: {"x": range(2, 6), "y_ranges": [range(0, 1), range(5, 8)]},
                4: {"x": range(4, 12), "y_ranges": [range(0, 1), range(11, 16)]},
                5: {"x": range(11, 20), "y_ranges": [range(23, 32)]},
                6: {"x": range(16, 48), "y_ranges": [range(0, 1), range(47, 64)]},
                7: {"x": range(32, 64), "y_ranges": [range(0, 2), range(95, 128)]},
            }
        }

# --- [!] 优化点: 将应用的状态（如锁和计数器）也封装起来 ---
class AppState:
    """应用运行时状态类"""
    def __init__(self):
        self.timestamp_lock = threading.Lock()
        self.progress_lock = threading.Lock()
        self.processed_count = 0

    def reset_progress(self):
        """重置下载进度计数器"""
        with self.progress_lock:
            self.processed_count = 0

    def increment_progress(self):
        """线程安全地增加进度计数"""
        with self.progress_lock:
            self.processed_count += 1
            return self.processed_count

# --- 全局实例 ---
config = Config()
app_state = AppState()

# --- 日志配置 ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def get_tile_ranges_for_satellite(zoom: int, satellite: str) -> tuple[range, list[range]]:
    """根据卫星和缩放级别获取瓦片坐标范围"""
    sat_ranges = config.HARDCODED_RANGES.get(satellite)
    if not sat_ranges:
        raise ValueError(f"未找到卫星 '{satellite}' 的硬编码范围定义。")
    zoom_ranges = sat_ranges.get(zoom)
    if not zoom_ranges:
        raise ValueError(f"未找到卫星 '{satellite}' 在 zoom={zoom} 的硬编码范围定义。")
    return zoom_ranges["x"], zoom_ranges["y_ranges"]

def fetch_latest_timestamps(satellite: str) -> set[int]:
    """从API获取最新的时间戳集合"""
    try:
        logger.info(f"正在从 {config.API_URL} 获取最新时间戳...")
        response = requests.get(config.API_URL, headers=config.HEADERS, timeout=15)
        response.raise_for_status()
        data = response.json()
        timestamps = data.get(satellite, [])
        logger.info(f"成功获取到卫星 '{satellite}' 的 {len(timestamps)} 个时间点。")
        return set(timestamps)
    except requests.exceptions.RequestException as e:
        logger.error(f"获取时间戳失败: {e}")
        return set()

def download_tile(satellite: str, timestamp: int, zoom: int, x: int, y: int) -> tuple[str, str, int | None]:
    """下载单个瓦片文件"""
    # [!] 优化点: 路径拼接更清晰
    save_path = config.BASE_DOWNLOAD_PATH / satellite / str(zoom) / str(x) / str(y)
    file_path = save_path / f"{timestamp}.jpg"
    
    if file_path.exists():
        return "skipped", satellite, timestamp

    save_path.mkdir(parents=True, exist_ok=True)
    dt = datetime.fromtimestamp(timestamp, timezone.utc)
    # [!] 优化点: URL格式化更易读
    url = (
        f"https://tiles.zoom.earth/geocolor/{satellite}/"
        f"{dt.strftime('%Y-%m-%d')}/{dt.strftime('%H%M')}/"
        f"{zoom}/{x}/{y}.jpg"
    )
    temp_file = file_path.with_suffix(".tmp")
    
    try:
        response = requests.get(url, headers=config.HEADERS, stream=True, timeout=20)
        response.raise_for_status()
        with open(temp_file, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        # [!] 优化点: 文件大小检查常量化
        MIN_FILE_SIZE_BYTES = 200
        if temp_file.stat().st_size < MIN_FILE_SIZE_BYTES:
            temp_file.unlink()
            return "failed_small", satellite, None
        temp_file.rename(file_path)
        return "downloaded", satellite, timestamp
    except requests.exceptions.RequestException as e:
        if temp_file.exists():
            temp_file.unlink()
        if hasattr(e, 'response') and e.response and e.response.status_code == 404:
            logger.debug(f"瓦片不存在 (404): {url}")
        else:
            logger.warning(f"下载失败: {url} | 错误: {e}")
        return "failed", satellite, None

def update_timestamp_record(satellite: str, new_timestamps: set):
    """将新的时间戳更新到本地记录文件"""
    if not new_timestamps:
        return
    record_file = config.BASE_DOWNLOAD_PATH / satellite / "timestamps.json"
    record_file.parent.mkdir(parents=True, exist_ok=True)
    
    with app_state.timestamp_lock:
        try:
            existing_timestamps = get_local_timestamps(satellite, read_from_file=True)
            
            # [!] 优化点: 逻辑更清晰
            combined_timestamps = existing_timestamps.union(new_timestamps)
            if len(combined_timestamps) > len(existing_timestamps):
                with open(record_file, 'w') as f:
                    json.dump(sorted(list(combined_timestamps)), f, indent=2)
                added_count = len(combined_timestamps) - len(existing_timestamps)
                logger.info(f"时间戳记录文件 '{record_file.name}' 已更新，新增 {added_count} 个时间点。")
            else:
                logger.info("没有新的时间戳需要记录。")
        except (IOError, json.JSONDecodeError) as e:
            logger.error(f"读写时间戳记录文件失败: {e}")

def get_local_timestamps(satellite: str, read_from_file: bool = True) -> set:
    """从本地文件加载已记录的时间戳"""
    if not read_from_file: # 允许在update函数中复用，避免重复加锁
        return set()
    record_file = config.BASE_DOWNLOAD_PATH / satellite / "timestamps.json"
    if not record_file.exists():
        return set()
    try:
        with open(record_file, 'r') as f:
            return set(json.load(f))
    except (IOError, json.JSONDecodeError) as e:
        logger.warning(f"读取本地时间戳记录失败: {e}, 将作为首次运行处理。")
        return set()

def run_download_task(is_debug_run: bool = False, debug_zoom: int | None = None):
    """执行瓦片下载的核心逻辑，包含重试机制"""
    task_name = "调试下载" if is_debug_run else "增量下载"
    logger.info(f"--- 开始执行{task_name}任务 ---")
    
    if is_debug_run:
        logger.info(f"卫星: {config.SATELLITE}, Zoom: {debug_zoom}")
        zoom_levels = [debug_zoom] if debug_zoom is not None else []
    else:
        logger.info(f"卫星: {config.SATELLITE}, Zooms: {config.MIN_ZOOM}-{config.MAX_ZOOM}")
        zoom_levels = range(config.MIN_ZOOM, config.MAX_ZOOM + 1)

    remote_timestamps = fetch_latest_timestamps(config.SATELLITE)
    if not remote_timestamps:
        logger.warning("未获取到任何远程时间戳，下载任务结束。")
        return

    if is_debug_run:
        if remote_timestamps:
            latest_ts = sorted(list(remote_timestamps), reverse=True)[0]
            timestamps_to_process = {latest_ts}
            logger.info(f"调试模式：仅使用最新时间戳: {latest_ts}")
        else:
            timestamps_to_process = set()
    else:
        local_timestamps = get_local_timestamps(config.SATELLITE)
        timestamps_to_process = remote_timestamps - local_timestamps
        logger.info(f"本地已记录 {len(local_timestamps)} 个时间戳，发现 {len(timestamps_to_process)} 个新时间戳需要处理。")

    if not timestamps_to_process:
        logger.info("所有时间戳均已处理，无需下载。")
        logger.info(f"--- {task_name}任务结束 ---")
        return
    
    initial_tasks = []
    for zoom in zoom_levels:
        try:
            x_range, y_ranges_list = get_tile_ranges_for_satellite(zoom, config.SATELLITE)
            for ts in timestamps_to_process:
                for x in x_range:
                    for y_range in y_ranges_list:
                        for y in y_range:
                            initial_tasks.append((config.SATELLITE, ts, zoom, x, y))
        except ValueError as e:
            logger.error(f"无法为 Zoom {zoom} 生成任务: {e}")

    if not initial_tasks:
        logger.info("未生成任何下载任务。")
        logger.info(f"--- {task_name}任务结束 ---")
        return
    
    logger.info(f"共计为新时间戳生成 {len(initial_tasks)} 个潜在下载任务。")
    
    stats = {"downloaded": 0, "skipped": 0, "failed": 0, "failed_small": 0}
    successful_timestamps = set()
    tasks_to_process = initial_tasks
    
    MAX_DOWNLOAD_ATTEMPTS = 3 # 1次初始尝试 + 2次重试

    for attempt in range(1, MAX_DOWNLOAD_ATTEMPTS + 1):
        if not tasks_to_process:
            break # 如果没有需要处理的任务了，提前退出循环

        is_retry = attempt > 1
        attempt_name = f"重试 {attempt - 1}/{MAX_DOWNLOAD_ATTEMPTS - 1}" if is_retry else f"尝试 {attempt}/{MAX_DOWNLOAD_ATTEMPTS}"
        logger.info(f"--- 开始第 {attempt} 轮下载 ({attempt_name}) ---")
        logger.info(f"本轮需要处理 {len(tasks_to_process)} 个任务。")
        
        failed_in_this_attempt = []
        app_state.reset_progress()
        total_in_this_attempt = len(tasks_to_process)

        with ThreadPoolExecutor(max_workers=config.CONCURRENCY) as executor:
            futures = {executor.submit(download_tile, *task): task for task in tasks_to_process}
            for future in as_completed(futures):
                current_count = app_state.increment_progress()
                if current_count % 500 == 0 or current_count == total_in_this_attempt:
                    progress = (current_count / total_in_this_attempt) * 100
                    logger.info(f"本轮下载进度: {current_count} / {total_in_this_attempt} ({progress:.1f}%)")
                
                try:
                    status, _, timestamp = future.result()
                    if status == "failed":
                        failed_in_this_attempt.append(futures[future])
                    else:
                        stats[status] += 1
                        if timestamp:
                            successful_timestamps.add(timestamp)
                except Exception as exc:
                    task_info = futures[future]
                    logger.error(f"任务 {task_info} 产生未知异常: {exc}")
                    failed_in_this_attempt.append(task_info)
        
        tasks_to_process = failed_in_this_attempt
        if tasks_to_process:
            logger.warning(f"第 {attempt} 轮下载完成，有 {len(tasks_to_process)} 个任务失败，将进行重试。")
        else:
            logger.info(f"第 {attempt} 轮下载成功完成，没有失败的任务。")

    # 循环结束后，仍然在 tasks_to_process 列表中的任务是最终失败的
    if tasks_to_process:
        stats["failed"] = len(tasks_to_process)
        logger.error(f"经过 {MAX_DOWNLOAD_ATTEMPTS} 次尝试后，仍有 {stats['failed']} 个任务最终下载失败。")

    logger.info("下载任务执行完毕。最终统计:")
    logger.info(f"  - 新下载: {stats['downloaded']}")
    logger.info(f"  - 已跳过: {stats['skipped']}")
    logger.info(f"  - 最终失败 (网络/错误): {stats['failed']}")
    logger.info(f"  - 下载失败 (文件过小): {stats['failed_small']}")
    
    update_timestamp_record(config.SATELLITE, successful_timestamps)
    logger.info(f"--- {task_name}任务结束 ---")

def prune_empty_dirs(path: Path) -> int:
    """递归删除空目录并返回删除的数量"""
    # [!] 优化点: 简化路径检查
    if not path.is_dir() or not str(path).startswith(str(config.BASE_DOWNLOAD_PATH / config.SATELLITE)):
        return 0
    
    deleted_count = 0
    try:
        # [!] 优化点: 使用 `try...except StopIteration` 替代 `any`，对于大目录可能更高效
        if next(path.iterdir(), None) is None:
            path.rmdir()
            logger.debug(f"已删除空目录: {path}")
            deleted_count += 1
            deleted_count += prune_empty_dirs(path.parent)
    except OSError as e:
        logger.warning(f"删除目录失败 {path}: {e}")
    return deleted_count

def run_cleanup_task():
    """清理旧的瓦片数据和时间戳记录"""
    logger.info("--- 开始执行数据清理任务 ---")
    logger.info(f"将清理早于 {config.CLEANUP_DAYS} 天的数据...")
    
    cutoff_dt = datetime.now(timezone.utc) - timedelta(days=config.CLEANUP_DAYS)
    cutoff_timestamp = int(cutoff_dt.timestamp())
    logger.info(f"清理时间戳阈值: {cutoff_timestamp} ({cutoff_dt.strftime('%Y-%m-%d %H:%M:%S %Z')})")

    data_root = config.BASE_DOWNLOAD_PATH / config.SATELLITE
    if not data_root.exists():
        logger.info("数据目录不存在，无需清理。")
        return

    # 1. 清理旧的瓦片文件
    logger.info("开始扫描并清理旧瓦片文件...")
    deleted_files_count = 0
    unique_parent_dirs_to_check = set()
    # [!] 优化点: rglob 是一个生成器，直接迭代比先转为list更节省内存
    for file_path in data_root.rglob("*.jpg"):
        try:
            if int(file_path.stem) < cutoff_timestamp:
                unique_parent_dirs_to_check.add(file_path.parent)
                file_path.unlink()
                deleted_files_count += 1
        except (ValueError, FileNotFoundError):
            logger.warning(f"无法从文件名解析时间戳或文件已消失: {file_path}")
    logger.info(f"文件扫描完成。共删除 {deleted_files_count} 个旧瓦片文件。")
    
    # [!] 优化点: 仅在删除文件后，对受影响的目录进行空目录检查
    logger.info("开始清理空目录...")
    deleted_dirs_count = 0
    for parent_dir in unique_parent_dirs_to_check:
        deleted_dirs_count += prune_empty_dirs(parent_dir)
    logger.info(f"空目录清理完成，共移除 {deleted_dirs_count} 个目录。")

    # 2. 清理 timestamps.json 文件
    record_file = data_root / "timestamps.json"
    if not record_file.exists():
        logger.info("时间戳记录文件不存在，无需清理。")
    else:
        with app_state.timestamp_lock:
            try:
                with open(record_file, 'r') as f:
                    timestamps = json.load(f)
                
                original_count = len(timestamps)
                kept_timestamps = [ts for ts in timestamps if ts >= cutoff_timestamp]
                
                if len(kept_timestamps) < original_count:
                    with open(record_file, 'w') as f:
                        json.dump(sorted(kept_timestamps), f, indent=2)
                    removed_count = original_count - len(kept_timestamps)
                    logger.info(f"时间戳记录文件已更新：移除了 {removed_count} 个旧条目。")
                else:
                    logger.info("时间戳记录文件中没有需要清理的旧条目。")
            except (IOError, json.JSONDecodeError) as e:
                logger.error(f"读写时间戳记录文件以进行清理时失败: {e}")

    logger.info("--- 数据清理任务结束 ---")

def run_job_cycle():
    """运行一个完整的任务周期（下载+清理）"""
    logger.info("=============================================")
    logger.info(f"开始新一轮任务周期...")
    
    run_download_task()
    run_cleanup_task()

    logger.info(f"本轮任务周期执行完毕。")
    logger.info("=============================================\n")

def main():
    """程序主入口，处理命令行参数和启动模式"""
    parser = argparse.ArgumentParser(description="Zoom Earth Tile Downloader and Manager")
    parser.add_argument(
        "mode",
        nargs='?',
        default="service",
        choices=["run-once", "service"],
        help="运行模式: 'run-once' (用于调试下载) 或 'service' (作为服务持续运行)。"
    )
    parser.add_argument(
        "--zoom",
        type=int,
        help="在 'run-once' 模式下，指定要测试的 zoom 等级。"
    )
    args = parser.parse_args()

    if args.mode == "run-once":
        if args.zoom is None:
            logger.error("'run-once' 模式下必须通过 --zoom 参数指定一个缩放级别。")
            sys.exit(1)
        logger.info(f"以 'run-once' 模式启动，将仅执行一次调试下载任务。")
        run_download_task(is_debug_run=True, debug_zoom=args.zoom)
    else: # mode == "service"
        logger.info(f"服务启动，将每隔 {config.CHECK_INTERVAL_MINUTES} 分钟执行一次完整任务周期。")
        run_job_cycle()
        schedule.every(config.CHECK_INTERVAL_MINUTES).minutes.do(run_job_cycle)
        while True:
            schedule.run_pending()
            time.sleep(60)

if __name__ == "__main__":
    main()