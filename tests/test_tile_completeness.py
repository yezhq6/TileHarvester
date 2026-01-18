#!/usr/bin/env python3
"""
瓦片完整性检查测试脚本
检查目标文件夹中下载的瓦片是否完整，计算理论瓦片数与实际瓦片数的差值
"""

import os
import math
from pathlib import Path
from src.tile_math import TileMath

# 测试配置
TEST_CONFIG = {
    "provider_name": "bing",
    "output_dir": "E:/qqg/tiles_test",
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


def convert_path(output_dir):
    """
    转换Windows路径到Linux路径（如果需要）
    """
    if isinstance(output_dir, str):
        if output_dir.startswith("E:"):
            # 转换E盘路径到WSL2路径
            output_dir = output_dir.replace("E:", "/mnt/e", 1)
        # 将Windows路径分隔符转换为Linux路径分隔符
        output_dir = output_dir.replace("\\", "/")
    return Path(output_dir)


def calculate_theoretical_tiles():
    """
    计算理论上应该下载的瓦片数量
    """
    west = TEST_REGION["west"]
    south = TEST_REGION["south"]
    east = TEST_REGION["east"]
    north = TEST_REGION["north"]
    min_zoom = TEST_REGION["min_zoom"]
    max_zoom = TEST_REGION["max_zoom"]
    is_tms = TEST_CONFIG["is_tms"]
    
    total_tiles = 0
    
    for zoom in range(min_zoom, max_zoom + 1):
        tiles = TileMath.calculate_tiles_in_bbox(west, south, east, north, zoom, is_tms)
        tile_count = len(tiles)
        total_tiles += tile_count
        print(f"缩放级别 {zoom}: 理论瓦片数 = {tile_count}")
    
    return total_tiles


def count_actual_tiles():
    """
    统计实际下载的瓦片数量
    """
    # 转换输出目录路径
    output_dir = convert_path(TEST_CONFIG["output_dir"])
    provider_name = TEST_CONFIG["provider_name"]
    
    # 构建完整的瓦片存储路径
    tiles_path = output_dir / provider_name
    
    if not tiles_path.exists():
        print(f"瓦片存储目录不存在: {tiles_path}")
        return 0
    
    total_actual = 0
    
    # 遍历所有缩放级别目录
    for zoom_dir in tiles_path.iterdir():
        if not zoom_dir.is_dir():
            continue
        
        zoom = int(zoom_dir.name)
        zoom_count = 0
        
        # 遍历所有x坐标目录
        for x_dir in zoom_dir.iterdir():
            if not x_dir.is_dir():
                continue
            
            # 遍历所有y坐标文件
            for tile_file in x_dir.iterdir():
                if tile_file.is_file():
                    zoom_count += 1
        
        print(f"缩放级别 {zoom}: 实际瓦片数 = {zoom_count}")
        total_actual += zoom_count
    
    return total_actual


def find_missing_tiles():
    """
    找出缺失的瓦片
    """
    west = TEST_REGION["west"]
    south = TEST_REGION["south"]
    east = TEST_REGION["east"]
    north = TEST_REGION["north"]
    min_zoom = TEST_REGION["min_zoom"]
    max_zoom = TEST_REGION["max_zoom"]
    is_tms = TEST_CONFIG["is_tms"]
    
    # 转换输出目录路径
    output_dir = convert_path(TEST_CONFIG["output_dir"])
    provider_name = TEST_CONFIG["provider_name"]
    tiles_path = output_dir / provider_name
    
    missing_tiles = []
    
    for zoom in range(min_zoom, max_zoom + 1):
        # 使用TileMath类计算瓦片范围
        tiles = TileMath.calculate_tiles_in_bbox(west, south, east, north, zoom, is_tms)
        
        zoom_missing = 0
        
        # 遍历所有理论上应该存在的瓦片
        for tile in tiles:
            x, y = tile
            # 构建瓦片文件路径
            tile_file = tiles_path / str(zoom) / str(x) / f"{y}.jpeg"
            if not tile_file.exists():
                zoom_missing += 1
        
        if zoom_missing > 0:
            print(f"缩放级别 {zoom}: 缺失瓦片数 = {zoom_missing}")
            missing_tiles.append((zoom, zoom_missing))
    
    return missing_tiles


def main():
    """
    主测试函数
    """
    print("="*60)
    print("瓦片完整性检查测试")
    print("="*60)
    print(f"测试提供商: {TEST_CONFIG['provider_name']}")
    print(f"测试区域: 经度 {TEST_REGION['west']}-{TEST_REGION['east']}, 纬度 {TEST_REGION['south']}-{TEST_REGION['north']}")
    print(f"缩放级别: {TEST_REGION['min_zoom']}-{TEST_REGION['max_zoom']}")
    print(f"输出路径: {TEST_CONFIG['output_dir']}")
    print("="*60)
    
    # 1. 计算理论瓦片数
    print("\n1. 计算理论瓦片数...")
    theoretical_tiles = calculate_theoretical_tiles()
    print(f"总计理论瓦片数: {theoretical_tiles}")
    
    # 2. 统计实际瓦片数
    print("\n2. 统计实际瓦片数...")
    actual_tiles = count_actual_tiles()
    print(f"总计实际瓦片数: {actual_tiles}")
    
    # 3. 计算差异
    print("\n3. 计算差异...")
    difference = theoretical_tiles - actual_tiles
    print(f"瓦片差异: {difference}")
    
    # 4. 计算完成率
    completion_rate = (actual_tiles / theoretical_tiles) * 100 if theoretical_tiles > 0 else 0
    print(f"完成率: {completion_rate:.2f}%")
    
    # 5. 找出缺失的瓦片
    print("\n4. 找出缺失的瓦片...")
    missing_tiles = find_missing_tiles()
    
    print("\n" + "="*60)
    print("测试结果")
    print("="*60)
    print(f"理论瓦片数: {theoretical_tiles}")
    print(f"实际瓦片数: {actual_tiles}")
    print(f"缺失瓦片数: {difference}")
    print(f"完成率: {completion_rate:.2f}%")
    print("="*60)
    
    if difference == 0:
        print("✅ 所有瓦片都已下载完成！")
    else:
        print(f"❌ 还有 {difference} 个瓦片未下载完成！")
        for zoom, count in missing_tiles:
            print(f"   缩放级别 {zoom}: 缺失 {count} 个瓦片")
    
    return difference == 0


if __name__ == "__main__":
    exit(0 if main() else 1)
