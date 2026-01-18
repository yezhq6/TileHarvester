#!/usr/bin/env python3
"""
大规模瓦片下载测试脚本
测试改进后的代码在处理大量瓦片时的性能和稳定性
"""

import time
import shutil
from pathlib import Path
from src.downloader import BatchDownloader

# 测试配置
TEST_CONFIG = {
    "provider_name": "bing",
    "output_dir": "E:\\qqg\\tiles_test",
    "max_threads": 16,
    "is_tms": False,
    "enable_resume": True
}

# 测试区域：选择一个大约产生11000个瓦片的区域和缩放级别
# 扩展缩放级别到15级，并调整区域范围以生成约11000个瓦片
TEST_REGION = {
    "west": 120.0,
    "south": 23.0,
    "east": 122.0,
    "north": 25.0,
    "min_zoom": 12,
    "max_zoom": 15
}


def clear_test_dir():
    """
    清除测试目录，确保测试环境干净
    """
    try:
        # 1. 首先检查是否需要转换Windows路径
        output_dir = TEST_CONFIG["output_dir"]
        print(f"原始输出目录: {output_dir}")
        
        # 2. 显式转换Windows路径到WSL2路径
        if isinstance(output_dir, str):
            if output_dir.startswith("E:"):
                # 转换E盘路径到WSL2路径
                output_dir = output_dir.replace("E:", "/mnt/e", 1)
            # 将Windows路径分隔符转换为Linux路径分隔符
            output_dir = output_dir.replace("\\", "/")
        
        output_path = Path(output_dir)
        print(f"转换后输出目录: {output_path}")
        
        # 3. 确保父目录存在
        output_path.parent.mkdir(parents=True, exist_ok=True)
        print(f"父目录存在: {output_path.parent}")
        
        # 4. 彻底删除目录及其内容（如果存在）
        if output_path.exists():
            print(f"删除目录: {output_path}")
            shutil.rmtree(output_path, ignore_errors=True)
        
        # 5. 重新创建空目录
        print(f"创建空目录: {output_path}")
        output_path.mkdir(parents=True, exist_ok=True)
        
        # 6. 验证目录是否为空
        items = list(output_path.iterdir())
        print(f"目录内容: {items}")
        if not items:
            print(f"✅ 测试目录已成功清空: {output_path}")
        else:
            print(f"❌ 测试目录未完全清空: {output_path}")
            
    except Exception as e:
        print(f"清除测试目录失败: {e}")
        import traceback
        traceback.print_exc()


def test_large_scale_download():
    """
    测试大规模瓦片下载
    """
    print("="*60)
    print("大规模瓦片下载测试")
    print("="*60)
    print(f"测试提供商: {TEST_CONFIG['provider_name']}")
    print(f"测试区域: 经度 {TEST_REGION['west']}-{TEST_REGION['east']}, 纬度 {TEST_REGION['south']}-{TEST_REGION['north']}")
    print(f"缩放级别: {TEST_REGION['min_zoom']}-{TEST_REGION['max_zoom']}")
    print(f"线程数: {TEST_CONFIG['max_threads']}")
    print(f"输出路径: {TEST_CONFIG['output_dir']}")
    print(f"断点续传: {'启用' if TEST_CONFIG['enable_resume'] else '禁用'}")
    print("="*60)
    
    # 开始测试
    start_time = time.time()
    
    try:
        # 使用BatchDownloader下载
        stats = BatchDownloader.download_bbox(
            provider_name=TEST_CONFIG["provider_name"],
            west=TEST_REGION["west"],
            south=TEST_REGION["south"],
            east=TEST_REGION["east"],
            north=TEST_REGION["north"],
            min_zoom=TEST_REGION["min_zoom"],
            max_zoom=TEST_REGION["max_zoom"],
            output_dir=TEST_CONFIG["output_dir"],
            max_threads=TEST_CONFIG["max_threads"],
            is_tms=TEST_CONFIG["is_tms"]
        )
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        # 计算性能指标
        total_tiles = stats["downloaded"] + stats["failed"] + stats["skipped"]
        success_rate = (stats["downloaded"] / total_tiles * 100) if total_tiles > 0 else 0
        tiles_per_second = stats["downloaded"] / elapsed_time if elapsed_time > 0 else 0
        
        # 打印测试结果
        print("\n" + "="*60)
        print("测试结果")
        print("="*60)
        print(f"总耗时: {elapsed_time:.2f} 秒")
        print(f"总瓦片数: {total_tiles}")
        print(f"成功下载: {stats['downloaded']}")
        print(f"下载失败: {stats['failed']}")
        print(f"已跳过: {stats['skipped']}")
        print(f"成功率: {success_rate:.1f}%")
        print(f"下载速度: {tiles_per_second:.2f} 瓦片/秒")
        print("="*60)
        
        # 验证下载结果
        if stats["downloaded"] > 0:
            print("✅ 测试成功！大规模瓦片下载正常工作")
            
            # 检查进度文件是否存在
            output_path = Path(TEST_CONFIG["output_dir"])
            progress_file = output_path / f".{TEST_CONFIG['provider_name']}_progress.json"
            if progress_file.exists():
                print(f"✅ 进度文件已创建: {progress_file}")
            else:
                print(f"❌ 进度文件未创建")
            
            return True
        else:
            print("❌ 测试失败！没有成功下载任何瓦片")
            return False
            
    except Exception as e:
        print(f"❌ 测试失败！发生异常: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_resume_download():
    """
    测试断点续传功能
    """
    print("\n" + "="*60)
    print("断点续传测试")
    print("="*60)
    print("第二次运行下载，验证是否能跳过已下载的瓦片")
    
    start_time = time.time()
    
    try:
        # 再次运行下载，应该跳过已下载的瓦片
        stats = BatchDownloader.download_bbox(
            provider_name=TEST_CONFIG["provider_name"],
            west=TEST_REGION["west"],
            south=TEST_REGION["south"],
            east=TEST_REGION["east"],
            north=TEST_REGION["north"],
            min_zoom=TEST_REGION["min_zoom"],
            max_zoom=TEST_REGION["max_zoom"],
            output_dir=TEST_CONFIG["output_dir"],
            max_threads=TEST_CONFIG["max_threads"],
            is_tms=TEST_CONFIG["is_tms"]
        )
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        print(f"总耗时: {elapsed_time:.2f} 秒")
        print(f"成功下载: {stats['downloaded']}")
        print(f"下载失败: {stats['failed']}")
        print(f"已跳过: {stats['skipped']}")
        
        # 验证断点续传功能
        if stats['skipped'] > 0 and stats['downloaded'] == 0:
            print("✅ 断点续传功能正常！所有已下载的瓦片都被跳过")
            return True
        else:
            print("❌ 断点续传功能异常！")
            return False
            
    except Exception as e:
        print(f"❌ 断点续传测试失败！发生异常: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """
    主测试函数
    """
    print("大规模瓦片下载测试开始")
    print("="*80)
    
    # 1. 清除测试目录
    clear_test_dir()
    
    # 2. 运行第一次下载测试
    download_success = test_large_scale_download()
    
    # 3. 仅在第一次下载成功时运行断点续传测试
    resume_success = False
    if download_success:
        resume_success = test_resume_download()
    
    # 4. 清理测试资源
    # clear_test_dir()  # 注释掉，保留测试结果供检查
    
    print("\n" + "="*80)
    print("测试总结")
    print("="*80)
    
    if download_success and resume_success:
        print("✅ 所有测试通过！改进后的代码在大规模瓦片下载时表现良好")
        return 0
    else:
        print("❌ 测试失败！部分功能未通过测试")
        print(f"  - 第一次下载: {'成功' if download_success else '失败'}")
        print(f"  - 断点续传: {'成功' if resume_success else '失败'}")
        return 1


if __name__ == "__main__":
    exit(main())
