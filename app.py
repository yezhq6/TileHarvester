from flask import Flask, render_template, request, jsonify, Response
import os
import logging
import threading
import time
import json
import queue
from queue import Empty
from pathlib import Path

# 添加src到Python路径
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.downloader import BatchDownloader, TileDownloader
from src.providers import ProviderManager

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

# 配置
app.config['UPLOAD_FOLDER'] = 'tiles'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024
app.config['MAX_THREADS'] = 32  # 限制最大线程数

# 确保瓦片目录存在
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
logger.info(f"应用启动，瓦片存储目录: {app.config['UPLOAD_FOLDER']}")

# 全局下载控制
current_downloader = None
current_downloader_lock = threading.Lock()

# 进度更新事件管理器
progress_queue = queue.Queue()

def update_progress(downloaded, total, total_bytes=0, completed=False, stats=None):
    """更新进度"""
    progress_data = {
        'downloaded': downloaded,
        'total': total,
        'total_bytes': total_bytes,
        'percentage': int((downloaded / total) * 100) if total > 0 else 0,
        'completed': completed,
        'stats': stats
    }
    
    # 将进度数据放入队列
    progress_queue.put(progress_data)

@app.route('/')
def index():
    # 对于自定义提供商，我们不再需要传递提供商列表到模板
    return render_template('index.html')

@app.route('/api/download', methods=['POST'])
def api_download():
    try:
        # 获取请求参数
        if not request.is_json:
            return jsonify({'error': 'Request must be JSON format'}), 400
        
        data = request.get_json()
        
        # 验证必填参数（自定义提供商版本）
        required_fields = ['provider_url', 'north', 'south', 'west', 'east', 'min_zoom', 'max_zoom', 'output_dir']
        for field in required_fields:
            if field not in data:
                return jsonify({'error': f'Missing required field: {field}'}), 400
        
        # 验证并转换参数类型
        try:
            provider_url = data['provider_url']
            north = float(data['north'])
            south = float(data['south'])
            west = float(data['west'])
            east = float(data['east'])
            min_zoom = int(data['min_zoom'])
            max_zoom = int(data['max_zoom'])
            output_dir = data['output_dir']
            threads = int(data.get('threads', 4))
            is_tms = bool(data.get('tms', False))
            subdomains = data.get('subdomains', [])
        except ValueError as e:
            return jsonify({'error': f'Invalid parameter format: {e}'}), 400
        
        # 验证参数范围
        if north <= south:
            return jsonify({'error': 'North must be greater than south'}), 400
        if east <= west:
            return jsonify({'error': 'East must be greater than west'}), 400
        if min_zoom < 0 or max_zoom < 0:
            return jsonify({'error': 'Zoom level must be non-negative'}), 400
        if min_zoom > max_zoom:
            return jsonify({'error': 'Min zoom must be less than or equal to max zoom'}), 400
        
        # 限制线程数
        threads = max(1, min(threads, app.config['MAX_THREADS']))
        
        # 创建自定义瓦片提供商
        provider_name = 'custom'
        try:
            # 清理旧的custom提供商（如果存在）
            if provider_name.lower() in ProviderManager._providers:
                del ProviderManager._providers[provider_name.lower()]
            
            # 创建新的自定义提供商
            ProviderManager.create_custom_provider(
                name=provider_name,
                url_template=provider_url,
                subdomains=subdomains,
                min_zoom=min_zoom,
                max_zoom=max_zoom
            )
            logger.info(f"创建自定义提供商: {provider_name}, URL: {provider_url}")
        except Exception as e:
            logger.error(f"创建自定义提供商失败: {e}")
            return jsonify({'error': f'Failed to create custom provider: {e}'}), 500
        
        logger.info(f"开始下载任务: provider={provider_name}, bbox=[{west},{south},{east},{north}], "
                   f"zoom={min_zoom}-{max_zoom}, threads={threads}, is_tms={is_tms}, output_dir={output_dir}")
        
        try:
            # 定义进度回调函数
            def progress_callback(downloaded, total, total_bytes=0):
                """下载进度回调"""
                update_progress(downloaded, total, total_bytes)
            
            # 直接使用TileDownloader而不是BatchDownloader，以便保存实例
            global current_downloader
            with current_downloader_lock:
                current_downloader = TileDownloader(
                    provider_name,
                    output_dir,
                    max_threads=threads,
                    is_tms=is_tms,
                    progress_callback=progress_callback
                )
                
                # 添加下载任务
                current_downloader.add_tasks_for_bbox(
                    west, south, east, north, min_zoom, max_zoom
                )
                total_tasks = current_downloader.total_tasks
                stats = {
                    'downloaded': 0,
                    'failed': 0,
                    'skipped': 0,
                    'total': total_tasks
                }
            
            # 发送初始进度更新
            update_progress(0, total_tasks)
            
            # 启动下载（在单独的线程中）
            def start_download():
                try:
                    global current_downloader
                    with current_downloader_lock:
                        downloader = current_downloader
                    
                    if downloader:
                        downloader.start()
                        
                        # 下载完成后发送最终进度更新
                        stats = {
                            'downloaded': downloader.downloaded_count,
                            'failed': downloader.failed_count,
                            'skipped': downloader.skipped_count,
                            'total': downloader.total_tasks
                        }
                        update_progress(
                            downloaded=downloader.downloaded_count,
                            total=downloader.total_tasks,
                            completed=True,
                            stats=stats
                        )
                        
                    # 下载完成后清空全局变量
                    with current_downloader_lock:
                        current_downloader = None
                except Exception as e:
                    logger.error(f"下载线程异常: {e}")
                    # 发生异常时也发送完成通知
                    with current_downloader_lock:
                        downloader = current_downloader
                        if downloader:
                            stats = {
                                'downloaded': downloader.downloaded_count,
                                'failed': downloader.failed_count + 1,
                                'skipped': downloader.skipped_count,
                                'total': downloader.total_tasks
                            }
                            update_progress(
                                downloaded=downloader.downloaded_count,
                                total=downloader.total_tasks,
                                completed=True,
                                stats=stats
                            )
                        current_downloader = None
            
            # 启动下载线程
            download_thread = threading.Thread(target=start_download, daemon=True)
            download_thread.start()
            
            logger.info(f"下载任务启动: 总计={stats['total']} 个瓦片")
            
            # 返回初始状态，下载在后台进行
            return jsonify({'success': True, 'stats': stats, 'message': '下载已启动'})
        except Exception as e:
            logger.error(f"下载任务失败: {e}")
            return jsonify({'error': f'Download failed: {e}'}), 500
            
    except Exception as e:
        logger.error(f"API请求处理失败: {e}")
        return jsonify({'error': f'Request processing failed: {e}'}), 500

@app.route('/api/providers')
def api_providers():
    # 获取支持的瓦片提供商及其信息
    providers = []
    for name in ProviderManager.list_providers():
        p = ProviderManager.get_provider(name)
        providers.append({
            'name': name,
            'type': p.provider_type.value,
            'min_zoom': p.min_zoom,
            'max_zoom': p.max_zoom
        })
    return jsonify(providers)

@app.route('/api/progress')
def api_progress():
    """SSE端点，用于推送下载进度"""
    def event_stream():
        try:
            # 持续从队列中获取进度事件并发送
            while True:
                try:
                    # 非阻塞获取队列中的进度数据
                    progress_data = progress_queue.get(timeout=0.1)
                    # 格式化SSE事件
                    event = f"data: {json.dumps(progress_data)}\n\n"
                    yield event
                except queue.Empty:
                    # 如果队列为空，继续等待
                    continue
        except GeneratorExit:
            # 客户端断开连接
            pass
    
    return Response(event_stream(), mimetype='text/event-stream')

@app.route('/api/cancel-download', methods=['POST'])
def api_cancel_download():
    """取消当前下载任务"""
    global current_downloader
    
    with current_downloader_lock:
        if current_downloader:
            # 清空任务队列，立即停止所有下载
            while True:
                try:
                    current_downloader.task_queue.get_nowait()
                    current_downloader.task_queue.task_done()
                except Empty:
                    break
            
            # 触发停止事件
            current_downloader.stop_event.set()
            
            # 清空全局变量
            current_downloader = None
            
            logger.info("下载任务已取消")
            return jsonify({'success': True, 'message': '下载已取消'})

@app.route('/api/pause-download', methods=['POST'])
def api_pause_download():
    """暂停当前下载任务"""
    global current_downloader
    
    with current_downloader_lock:
        if current_downloader:
            current_downloader.pause()
            logger.info("下载任务已暂停")
            return jsonify({'success': True, 'message': '下载已暂停'})
        return jsonify({'success': False, 'message': '没有正在进行的下载任务'})

@app.route('/api/resume-download', methods=['POST'])
def api_resume_download():
    """继续当前下载任务"""
    global current_downloader
    
    with current_downloader_lock:
        if current_downloader:
            current_downloader.resume()
            logger.info("下载任务已恢复")
            return jsonify({'success': True, 'message': '下载已恢复'})
        return jsonify({'success': False, 'message': '没有正在进行的下载任务'})

@app.route('/api/download-status', methods=['GET'])
def api_download_status():
    """获取当前下载状态"""
    global current_downloader
    
    with current_downloader_lock:
        if current_downloader:
            return jsonify({
                'success': True,
                'is_downloading': True,
                'is_paused': current_downloader.is_paused(),
                'statistics': current_downloader.get_statistics()
            })
        return jsonify({
            'success': True,
            'is_downloading': False,
            'is_paused': False,
            'statistics': {'downloaded': 0, 'failed': 0, 'skipped': 0, 'total': 0}
        })

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)