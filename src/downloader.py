# src/downloader.py
import time
import os
from pathlib import Path
from queue import Queue, Empty
import threading
from threading import Thread, Event
from typing import List, Tuple, Dict

import requests
from loguru import logger

# 禁用所有代理设置
os.environ['NO_PROXY'] = '*'
os.environ['HTTP_PROXY'] = ''
os.environ['HTTPS_PROXY'] = ''
os.environ['http_proxy'] = ''
os.environ['https_proxy'] = ''

from .tile_math import TileMath
from .providers import ProviderManager, TileProvider

# 路径转换函数：处理Windows路径和Linux路径

def convert_path(output_dir: str) -> Path:
    """
    转换路径，支持Windows路径和Linux路径，在WSL2环境中自动转换
    
    Args:
        output_dir: 输入的路径，可以是Windows路径（如D:\codes）或Linux路径（如/mnt/d/codes）
        
    Returns:
        Path: 转换后的Path对象
    """
    # 检查是否为Windows路径（包含盘符和反斜杠）
    if len(output_dir) > 1 and output_dir[1] == ':' and ('\\' in output_dir or '/' in output_dir):
        # 转换Windows路径到WSL2路径
        # 将盘符转换为/mnt/[小写盘符]
        drive_letter = output_dir[0].lower()
        # 替换反斜杠为正斜杠
        wsl_path = output_dir[2:].replace('\\', '/')
        # 构建完整的WSL路径
        full_path = f"/mnt/{drive_letter}/{wsl_path.lstrip('/')}"
        logger.info(f"转换Windows路径到WSL2路径: {output_dir} -> {full_path}")
        return Path(full_path)
    else:
        # 直接返回Linux路径
        return Path(output_dir)


logger.add("tile_downloader.log", rotation="10 MB")


class TileDownloader:
    """
    核心下载器：负责接收 (x, y, z) 任务，并发下载
    """

    def __init__(
        self,
        provider_name: str,
        output_dir: str = "tiles",
        max_threads: int = 8,  # 增加默认线程数到8
        retries: int = 2,      # 减少重试次数到2
        delay: float = 0.05,   # 减少延迟到0.05秒
        timeout: int = 10,
        is_tms: bool = False,
        progress_callback: callable = None
    ):
        try:
            self.provider: TileProvider = ProviderManager.get_provider(provider_name)
            self.provider.is_tms = is_tms
            self.is_tms = is_tms
            
            # 转换输出目录路径，支持Windows路径和Linux路径
            self.output_dir = convert_path(output_dir)
            
            self.max_threads = max_threads
            self.retries = retries
            self.delay = delay
            self.timeout = timeout

            self.task_queue: Queue = Queue()
            self.stop_event = Event()

            self.downloaded_count = 0
            self.failed_count = 0
            self.skipped_count = 0
            self.total_tasks = 0
            self.total_bytes = 0  # 添加total_bytes属性，用于统计下载的总字节数
            self.progress_callback = progress_callback

            # 创建输出目录
            self.output_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"初始化下载器: provider={provider_name}, threads={max_threads}, output_dir={self.output_dir}")
        except Exception as e:
            logger.error(f"初始化下载器失败: {e}")
            raise

    def add_task(self, x: int, y: int, zoom: int):
        self.task_queue.put((x, y, zoom))
        logger.debug(f"添加任务: z={zoom}, x={x}, y={y}")

    def add_tasks(self, tiles: List[Tuple[int, int, int]]):
        for x, y, z in tiles:
            self.add_task(x, y, z)

    def add_tasks_for_bbox(
        self,
        west: float,
        south: float,
        east: float,
        north: float,
        min_zoom: int,
        max_zoom: int,
    ):
        """
        根据经纬度范围添加任务，支持多个缩放级别
        """
        logger.info(
            f"计算 bbox 瓦片: west={west}, south={south}, east={east}, north={north}, "
            f"min_zoom={min_zoom}, max_zoom={max_zoom}, is_tms={self.is_tms}"
        )

        total_tiles = 0
        for zoom in range(min_zoom, max_zoom + 1):
            xy_tiles = TileMath.calculate_tiles_in_bbox(
                west, south, east, north, zoom, is_tms=self.is_tms
            )
            tile_count = len(xy_tiles)
            total_tiles += tile_count
            logger.info(f"缩放级别 {zoom}: 找到 {tile_count} 个瓦片")
            for x, y in xy_tiles:
                self.add_task(x, y, zoom)
        
        self.total_tasks = total_tiles
        logger.info(f"总计: {total_tiles} 个瓦片任务")

    def start(self):
        """
        开始下载任务
        """
        try:
            logger.info(f"开始下载，线程数={self.max_threads}，provider={self.provider.name}")
            
            # 计算实际需要的线程数
            # 基础线程数：不超过配置的最大线程数、不超过任务数、不超过系统CPU核心数*2
            cpu_cores = os.cpu_count() or 4
            base_threads = min(
                self.max_threads, 
                self.task_queue.qsize(), 
                cpu_cores * 2,  # 根据CPU核心数动态调整，充分利用系统资源
                32  # 限制最大线程数为32，避免资源耗尽
            )
            # 确保至少有1个线程
            actual_threads = max(1, base_threads)
            
            logger.info(f"实际使用线程数: {actual_threads}, CPU核心数: {cpu_cores}")
            
            threads = []
            
            # 创建线程
            for i in range(actual_threads):
                t = Thread(target=self._worker, name=f"Downloader-{i+1}", daemon=True)
                t.start()
                threads.append(t)
                time.sleep(0.05)  # 线程启动延迟，避免同时请求过多

            # 等待任务队列清空
            self.task_queue.join()
            
            # 停止事件
            self.stop_event.set()

            # 等待所有线程结束
            for t in threads:
                t.join(timeout=5)  # 设置超时，避免线程永久阻塞

            logger.info(
                f"结束下载: 成功={self.downloaded_count}, 失败={self.failed_count}, "
                f"跳过={self.skipped_count}, 总计={self.total_tasks}"
            )
            
            # 下载完成后调用进度回调
            if self.progress_callback:
                self.progress_callback(self.downloaded_count, self.total_tasks, self.total_bytes)
        except Exception as e:
            logger.error(f"下载过程中发生异常: {e}")
            self.stop_event.set()
            raise

    def _worker(self):
        """
        工作线程，处理下载任务
        """
        thread_name = threading.current_thread().name
        logger.info(f"{thread_name} 启动")
        
        while not self.stop_event.is_set():
            try:
                # 获取任务，设置超时
                task = self.task_queue.get(timeout=1)
                if task is None:
                    self._update_progress()
                    self.task_queue.task_done()
                    continue
                
                x, y, z = task
                logger.debug(f"{thread_name} 处理任务: z={z}, x={x}, y={y}")
                
                try:
                    # zoom 范围检查
                    if not (self.provider.min_zoom <= z <= self.provider.max_zoom):
                        logger.warning(
                            f"{thread_name} - zoom {z} 超出 [{self.provider.min_zoom}, {self.provider.max_zoom}]，跳过"
                        )
                        self.skipped_count += 1
                        self._update_progress()
                        self.task_queue.task_done()
                        continue

                    # 检查文件是否已存在
                    file_path = self.provider.get_tile_path(x, y, z, self.output_dir)
                    if file_path.exists():
                        logger.debug(f"{thread_name} - [跳过] 已存在: {file_path}")
                        self.skipped_count += 1
                        self._update_progress()
                        self.task_queue.task_done()
                        continue

                    # 创建父目录
                    file_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    # 获取下载URL
                    url = self.provider.get_tile_url(x, y, z)
                    
                    ok = False
                    for attempt in range(self.retries):
                        try:
                            logger.debug(f"{thread_name} - 下载: {url} (尝试 {attempt+1}/{self.retries})")
                            
                            # 禁用代理，直接连接
                            resp = requests.get(
                                url, 
                                timeout=self.timeout, 
                                stream=True, 
                                proxies={'http': None, 'https': None},
                                headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
                            )
                            
                            if resp.status_code == 200:
                                # 验证响应内容
                                content_type = resp.headers.get('Content-Type', '')
                                if not ('image' in content_type or 'jpeg' in content_type or 'png' in content_type):
                                    logger.warning(f"{thread_name} - 非图片响应: {url}, Content-Type: {content_type}")
                                    continue
                                
                                # 写入文件
                                total_bytes = 0
                                with open(file_path, "wb") as f:
                                    for chunk in resp.iter_content(8192):
                                        if chunk:
                                            total_bytes += len(chunk)
                                            f.write(chunk)
                                
                                logger.info(f"{thread_name} - 下载成功: {file_path} ({total_bytes} 字节)")
                                ok = True
                                self.downloaded_count += 1
                                self.total_bytes += total_bytes  # 更新总字节数
                                break
                            else:
                                logger.warning(
                                    f"{thread_name} - [HTTP {resp.status_code}] {url}"
                                )
                                if resp.status_code in [403, 404]:
                                    # 403禁止访问或404不存在，直接跳过
                                    logger.warning(f"{thread_name} - 永久错误，停止重试: {url}")
                                    break
                        except requests.exceptions.ConnectionError as e:
                            logger.error(f"{thread_name} - 连接错误 {url} - {e}")
                        except requests.exceptions.Timeout as e:
                            logger.error(f"{thread_name} - 超时错误 {url} - {e}")
                        except requests.exceptions.RequestException as e:
                            logger.error(f"{thread_name} - 请求错误 {url} - {e}")
                        except IOError as e:
                            logger.error(f"{thread_name} - 文件写入错误 {file_path} - {e}")
                        except Exception as e:
                            logger.error(f"{thread_name} - 未知错误 {url} - {e}")

                        # 指数退避重试
                        retry_delay = self.delay * (2 ** attempt) * (0.5 + 0.5 * (hash(url) % 2))  # 使用URL哈希值作为随机因子
                        time.sleep(retry_delay)

                    if not ok:
                        logger.error(f"{thread_name} - 下载失败: {url}")
                        self.failed_count += 1

                    # 任务完成后的延迟
                    time.sleep(self.delay)
                except Exception as e:
                    logger.error(f"{thread_name} - 任务处理错误: z={z}, x={x}, y={y} - {e}")
                    self.failed_count += 1
                finally:
                    # 更新进度
                    self._update_progress()
                    # 确保任务标记为完成
                    self.task_queue.task_done()
                    
            except Empty:
                # 无任务，检查是否需要停止
                if self.stop_event.is_set():
                    break
                time.sleep(0.1)
            except Exception as e:
                logger.error(f"{thread_name} - 线程内部异常: {e}")
                # 确保线程不会因异常退出
                time.sleep(0.5)
        
        logger.info(f"{thread_name} 结束")
    
    def _update_progress(self):
        """
        更新下载进度
        """
        if self.progress_callback and self.total_tasks > 0:
            # 使用已处理的任务数作为下载进度
            processed = self.downloaded_count + self.failed_count + self.skipped_count
            self.progress_callback(processed, self.total_tasks, self.total_bytes)

    def get_statistics(self) -> Dict[str, int]:
        return {
            "downloaded": self.downloaded_count,
            "failed": self.failed_count,
            "skipped": self.skipped_count,
            "total": self.downloaded_count + self.failed_count + self.skipped_count,
        }


class BatchDownloader:
    """
    批量下载工具：提供高级接口
    """

    @staticmethod
    def download_single_tile(
        provider_name: str,
        lat: float,
        lon: float,
        zoom: int,
        output_dir: str = "tiles",
        is_tms: bool = False,
    ) -> Dict[str, int]:
        x, y = TileMath.latlon_to_tile(lat, lon, zoom, is_tms=is_tms)
        dl = TileDownloader(
            provider_name, output_dir, max_threads=1, is_tms=is_tms
        )
        dl.add_task(x, y, zoom)
        dl.start()
        return dl.get_statistics()

    @staticmethod
    def download_bbox(
        provider_name: str,
        west: float,
        south: float,
        east: float,
        north: float,
        min_zoom: int,
        max_zoom: int,
        output_dir: str = "tiles",
        max_threads: int = 4,
        is_tms: bool = False,
    ) -> Dict[str, int]:
        """
        下载矩形区域瓦片
        """
        dl = TileDownloader(
            provider_name, output_dir, max_threads=max_threads, is_tms=is_tms
        )
        dl.add_tasks_for_bbox(west, south, east, north, min_zoom, max_zoom)
        dl.start()
        return dl.get_statistics()
