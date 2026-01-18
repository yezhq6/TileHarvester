#!/usr/bin/env python3
"""
测试不同线程数对瓦片下载速度的影响
"""

import time
import shutil
from pathlib import Path
from src.downloader import BatchDownloader

# 测试配置
TEST_REGION = {
    "west": 116.3,
    "south": 39.9,
    "east": 116.5,
    "north": 40.1,
    "min_zoom": 10,
    "max_zoom": 12
}

PROVIDER_NAME = "bing"
OUTPUT_DIR = "E:\\qqg\\tiles"  # 用户指定的输出路径
THREAD_COUNTS = [1, 2, 4, 8, 16, 32]  # 要测试的线程数列表


def clear_existing_tiles():
    """
    清除之前下载的瓦片文件，确保每次测试都是全新的
    """
    try:
        # 转换Windows路径到WSL路径，使用与downloader.py中相同的逻辑
        def convert_path(output_dir: str) -> Path:
            """
            转换路径，支持Windows路径和Linux路径，在WSL2环境中自动转换
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
                return Path(full_path)
            else:
                # 直接返回Linux路径
                return Path(output_dir)
        
        # 转换输出目录路径
        output_path = convert_path(OUTPUT_DIR)
        provider_path = output_path / PROVIDER_NAME
        if provider_path.exists():
            shutil.rmtree(provider_path)
            print(f"已清除之前下载的瓦片: {provider_path}")
        else:
            print(f"没有发现之前下载的瓦片: {provider_path}")
    except Exception as e:
        print(f"清除瓦片时发生错误: {e}")
        import traceback
        traceback.print_exc()


def test_thread_speed(thread_count):
    """
    测试指定线程数的下载速度和稳定性
    
    Args:
        thread_count: 线程数
        
    Returns:
        tuple: (下载时间(秒), stats字典, 瓦片/秒, 字节/秒)
    """
    print(f"\n=== 测试线程数: {thread_count} ===")
    
    start_time = time.time()
    
    try:
        stats = BatchDownloader.download_bbox(
            provider_name=PROVIDER_NAME,
            west=TEST_REGION["west"],
            south=TEST_REGION["south"],
            east=TEST_REGION["east"],
            north=TEST_REGION["north"],
            min_zoom=TEST_REGION["min_zoom"],
            max_zoom=TEST_REGION["max_zoom"],
            output_dir=OUTPUT_DIR,
            max_threads=thread_count,
            is_tms=False
        )
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        downloaded = stats["downloaded"]
        failed = stats["failed"]
        skipped = stats["skipped"]
        total = downloaded + failed + skipped
        
        # 计算速度
        if elapsed_time > 0:
            tiles_per_second = downloaded / elapsed_time
            # 假设平均瓦片大小为10KB（实际会根据提供者不同而变化）
            # 这里只是估算，实际速度以tiles_per_second为准
            bytes_per_second = tiles_per_second * 10 * 1024
        else:
            tiles_per_second = 0
            bytes_per_second = 0
        
        success_rate = (downloaded / total * 100) if total > 0 else 0
        
        print(f"下载时间: {elapsed_time:.2f} 秒")
        print(f"总瓦片数: {total}")
        print(f"成功下载: {downloaded}")
        print(f"下载失败: {failed}")
        print(f"已跳过: {skipped}")
        print(f"成功率: {success_rate:.1f}%")
        print(f"下载速度: {tiles_per_second:.2f} 瓦片/秒")
        print(f"估算带宽: {bytes_per_second / 1024:.2f} KB/s")
        
        return elapsed_time, stats, tiles_per_second, bytes_per_second
        
    except Exception as e:
        print(f"测试失败: {e}")
        return None


def main():
    """
    主测试函数
    """
    print("瓦片下载线程速度与稳定性测试")
    print(f"测试区域: 经度 {TEST_REGION['west']}-{TEST_REGION['east']}, 纬度 {TEST_REGION['south']}-{TEST_REGION['north']}")
    print(f"缩放级别: {TEST_REGION['min_zoom']}-{TEST_REGION['max_zoom']}")
    print(f"瓦片提供商: {PROVIDER_NAME}")
    print(f"输出路径: {OUTPUT_DIR}")
    print(f"测试线程数: {THREAD_COUNTS}")
    print("="*60)
    
    results = []
    
    # 运行所有测试
    for thread_count in THREAD_COUNTS:
        # 每次测试前清除之前的瓦片
        clear_existing_tiles()
        
        # 运行测试
        result = test_thread_speed(thread_count)
        if result:
            results.append((thread_count,) + result)
    
    # 分析结果
    print("\n" + "="*80)
    print("测试结果汇总")
    print("="*80)
    print(f"{'线程数':<8} {'时间(秒)':<12} {'总瓦片':<10} {'成功':<8} {'失败':<8} {'跳过':<8} {'瓦片/秒':<12} {'成功率':<10}")
    print("-"*80)
    
    best_thread = None
    best_speed = 0
    most_stable_thread = None
    highest_success_rate = 0
    
    for thread_count, elapsed, stats, tiles_per_sec, bytes_per_sec in results:
        downloaded = stats["downloaded"]
        failed = stats["failed"]
        skipped = stats["skipped"]
        total = downloaded + failed + skipped
        success_rate = (downloaded / total * 100) if total > 0 else 0
        
        print(f"{thread_count:<8} {elapsed:<12.2f} {total:<10} {downloaded:<8} {failed:<8} {skipped:<8} {tiles_per_sec:<12.2f} {success_rate:<9.1f}%")
        
        # 找出最佳线程数（速度最快）
        if tiles_per_sec > best_speed:
            best_speed = tiles_per_sec
            best_thread = thread_count
        
        # 找出最稳定线程数（成功率最高）
        if success_rate > highest_success_rate:
            highest_success_rate = success_rate
            most_stable_thread = thread_count
    
    print("="*80)
    
    # 推荐最佳线程数
    if best_thread:
        print(f"\n推荐速度最优线程数: {best_thread}")
        print(f"推荐理由: 达到最高下载速度 {best_speed:.2f} 瓦片/秒")
    
    if most_stable_thread:
        print(f"\n推荐稳定性最优线程数: {most_stable_thread}")
        print(f"推荐理由: 达到最高成功率 {highest_success_rate:.1f}%")
    
    print("\n注意事项:")
    print("1. 最佳线程数会受到网络带宽、CPU性能和目标服务器限制的影响")
    print("2. 过多的线程可能导致服务器拒绝连接或触发速率限制")
    print("3. 建议在实际使用中根据网络状况调整线程数")
    print("4. 综合考虑速度和稳定性选择合适的线程数")
    
    return results


if __name__ == "__main__":
    main()
