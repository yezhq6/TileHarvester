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


logger.add("logs/tile_downloader.log", rotation="10 MB")


class TileDownloader:
    """
    核心下载器：负责接收 (x, y, z) 任务，并发下载
    """

    def __init__(
        self,
        provider_name: str,
        output_dir: str = "tiles",
        max_threads: int = 8,  # 增加默认线程数到8
        retries: int = 3,      # 减少重试次数到2
        delay: float = 0.05,   # 减少延迟到0.05秒
        timeout: int = 10,
        is_tms: bool = False,
        progress_callback: callable = None,
        enable_resume: bool = True  # 是否启用断点续传
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
            self.enable_resume = enable_resume

            self.task_queue: Queue = Queue()
            self.stop_event = Event()

            self.downloaded_count = 0
            self.failed_count = 0
            self.skipped_count = 0
            self.total_tasks = 0
            self.total_bytes = 0  # 添加total_bytes属性，用于统计下载的总字节数
            self.progress_callback = progress_callback
            
            # 进度持久化相关
            self.progress_file = self.output_dir / f".{provider_name}_progress.json"
            self.processed_tiles = set()  # 已处理的瓦片集合，格式为(x,y,z)

            # 创建输出目录
            self.output_dir.mkdir(parents=True, exist_ok=True)
            
            # 加载已下载的进度
            if self.enable_resume:
                self._load_progress()
            
            logger.info(f"初始化下载器: provider={provider_name}, threads={max_threads}, output_dir={self.output_dir}")
            logger.info(f"断点续传: {'已启用' if self.enable_resume else '已禁用'}")
        except Exception as e:
            logger.error(f"初始化下载器失败: {e}")
            raise

    def _load_progress(self):
        """
        从文件加载已下载的进度
        """
        import json
        if self.progress_file.exists():
            try:
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    progress = json.load(f)
                
                # 只加载已处理的瓦片列表，计数在每次运行时重新开始
                processed_tiles = progress.get('processed_tiles', [])
                self.processed_tiles = set(tuple(tile) for tile in processed_tiles)
                
                # 加载total_tasks和total_bytes，用于统计
                self.total_tasks = progress.get('total_tasks', 0)
                self.total_bytes = progress.get('total_bytes', 0)
                
                logger.info(f"已加载进度: 已处理瓦片={len(self.processed_tiles)}, 总任务数={self.total_tasks}, 总字节数={self.total_bytes}")
            except Exception as e:
                logger.error(f"加载进度失败: {e}")
        else:
            logger.info(f"未找到进度文件: {self.progress_file}")
    
    def _save_progress(self):
        """
        将当前进度保存到文件
        """
        import json
        try:
            progress = {
                'downloaded_count': self.downloaded_count,
                'failed_count': self.failed_count,
                'skipped_count': self.skipped_count,
                'total_tasks': self.total_tasks,
                'total_bytes': self.total_bytes,
                'processed_tiles': list(self.processed_tiles),
                'timestamp': time.time()
            }
            
            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump(progress, f, ensure_ascii=False, indent=2)
            
            logger.debug(f"进度已保存到: {self.progress_file}")
        except Exception as e:
            logger.error(f"保存进度失败: {e}")
    
    def _is_tile_processed(self, x: int, y: int, z: int) -> bool:
        """
        检查瓦片是否已处理
        
        Args:
            x: 瓦片x坐标
            y: 瓦片y坐标
            z: 缩放级别
            
        Returns:
            bool: 是否已处理
        """
        return (x, y, z) in self.processed_tiles
    
    def _mark_tile_processed(self, x: int, y: int, z: int, status: str):
        """
        标记瓦片为已处理
        
        Args:
            x: 瓦片x坐标
            y: 瓦片y坐标
            z: 缩放级别
            status: 处理状态，'success'、'failed'或'skipped'
        """
        tile_key = (x, y, z)
        
        # 只有新处理的瓦片才更新计数
        if tile_key not in self.processed_tiles:
            self.processed_tiles.add(tile_key)
            
            # 更新计数
            if status == 'success':
                self.downloaded_count += 1
            elif status == 'failed':
                self.failed_count += 1
            elif status == 'skipped':
                self.skipped_count += 1
        else:
            # 已处理的瓦片只增加跳过计数
            if status == 'skipped':
                self.skipped_count += 1
    
    def add_task(self, x: int, y: int, zoom: int):
        """
        添加任务到队列，启用断点续传时跳过已处理的瓦片
        """
        # 启用断点续传且瓦片已处理，则跳过
        if self.enable_resume and self._is_tile_processed(x, y, zoom):
            logger.debug(f"[跳过已处理] 任务: z={zoom}, x={x}, y={y}")
            self.skipped_count += 1
            return
        
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
        batch_size: int = 10000  # 每批处理的任务数量，默认10000个
    ):
        """
        根据经纬度范围添加任务，支持多个缩放级别，分批处理避免内存占用过高
        
        Args:
            west: 西边界经度
            south: 南边界纬度
            east: 东边界经度
            north: 北边界纬度
            min_zoom: 最小缩放级别
            max_zoom: 最大缩放级别
            batch_size: 每批处理的任务数量，默认10000个
        """
        logger.info(
            f"计算 bbox 瓦片: west={west}, south={south}, east={east}, north={north}, "
            f"min_zoom={min_zoom}, max_zoom={max_zoom}, is_tms={self.is_tms}"
        )

        # 先计算所有瓦片任务
        all_tiles = []
        total_tiles = 0
        
        for zoom in range(min_zoom, max_zoom + 1):
            xy_tiles = TileMath.calculate_tiles_in_bbox(
                west, south, east, north, zoom, is_tms=self.is_tms
            )
            tile_count = len(xy_tiles)
            total_tiles += tile_count
            logger.info(f"缩放级别 {zoom}: 找到 {tile_count} 个瓦片")
            
            # 将当前缩放级别的瓦片添加到总列表中
            for x, y in xy_tiles:
                all_tiles.append((x, y, zoom))
        
        self.total_tasks = total_tiles
        logger.info(f"总计: {total_tiles} 个瓦片任务，将按每批 {batch_size} 个进行处理")
        
        # 分批添加任务到队列
        for i in range(0, total_tiles, batch_size):
            batch = all_tiles[i:i+batch_size]
            logger.debug(f"添加第 {i//batch_size + 1} 批任务，共 {len(batch)} 个瓦片")
            
            for x, y, z in batch:
                self.add_task(x, y, z)
            
            # 每批添加后短暂休眠，避免内存占用过高
            if i + batch_size < total_tiles:
                time.sleep(0.1)

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
        
        processed_in_batch = 0  # 用于定期保存进度的计数器
        
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
                        self._mark_tile_processed(x, y, z, 'skipped')
                        self._update_progress()
                        self.task_queue.task_done()
                        processed_in_batch += 1
                        continue

                    # 检查文件是否已存在
                    file_path = self.provider.get_tile_path(x, y, z, self.output_dir)
                    if file_path.exists():
                        logger.debug(f"{thread_name} - [跳过] 已存在: {file_path}")
                        self._mark_tile_processed(x, y, z, 'skipped')
                        self._update_progress()
                        self.task_queue.task_done()
                        processed_in_batch += 1
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
                                self.total_bytes += total_bytes  # 更新总字节数
                                self._mark_tile_processed(x, y, z, 'success')
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

                        # 指数退避重试，增加最大延迟限制
                        retry_delay = min(
                            self.delay * (2 ** attempt) * (0.5 + 0.5 * (hash(url) % 2)),
                            60  # 最大延迟60秒
                        )
                        time.sleep(retry_delay)

                    if not ok:
                        logger.error(f"{thread_name} - 下载失败: {url}")
                        self._mark_tile_processed(x, y, z, 'failed')

                    # 任务完成后的延迟
                    time.sleep(self.delay)
                except Exception as e:
                    logger.error(f"{thread_name} - 任务处理错误: z={z}, x={x}, y={y} - {e}")
                    self._mark_tile_processed(x, y, z, 'failed')
                    self._update_progress()
                    self.task_queue.task_done()
                    processed_in_batch += 1
                else:
                    # 更新进度
                    self._update_progress()
                    # 标记任务完成 - 只调用一次
                    self.task_queue.task_done()
                    processed_in_batch += 1
                
                # 每处理100个瓦片保存一次进度
                if self.enable_resume and processed_in_batch >= 100:
                    self._save_progress()
                    processed_in_batch = 0
                    
            except Empty:
                # 无任务，检查是否需要停止
                if self.stop_event.is_set():
                    break
                time.sleep(0.1)
            except Exception as e:
                logger.error(f"{thread_name} - 线程内部异常: {e}")
                # 确保线程不会因异常退出
                time.sleep(0.5)
        
        # 线程结束前保存进度
        if self.enable_resume:
            self._save_progress()
        
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
        enable_resume: bool = True,
        batch_size: int = 10000
    ) -> Dict[str, int]:
        """
        下载矩形区域瓦片
        
        Args:
            provider_name: 瓦片提供商名称
            west: 西边界经度
            south: 南边界纬度
            east: 东边界经度
            north: 北边界纬度
            min_zoom: 最小缩放级别
            max_zoom: 最大缩放级别
            output_dir: 输出目录
            max_threads: 最大线程数
            is_tms: 是否使用TMS坐标系
            enable_resume: 是否启用断点续传
            batch_size: 每批处理的任务数量
            
        Returns:
            Dict[str, int]: 下载统计信息
        """
        dl = TileDownloader(
            provider_name, 
            output_dir, 
            max_threads=max_threads, 
            is_tms=is_tms,
            enable_resume=enable_resume
        )
        dl.add_tasks_for_bbox(west, south, east, north, min_zoom, max_zoom, batch_size=batch_size)
        dl.start()
        return dl.get_statistics()
