#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
进度文件生成器

用于生成 .custom_progress.json 文件，以便在将 MBTiles 转换为目录结构后，或者在复制 MBTiles 文件到其他地方时，能够继续使用断点续传功能。
"""

import os
import json
import sqlite3
import time
from pathlib import Path
from typing import List, Tuple, Set


def convert_path(output_dir: str) -> Path:
    """
    转换路径，支持Windows路径和Linux路径，在WSL2环境中自动转换
    
    Args:
        output_dir: 输入的路径，可以是Windows路径（如D:/codes）或Linux路径（如/mnt/d/codes）
        
    Returns:
        Path: 转换后的Path对象
    """
    # 处理命令行中可能的空格和换行问题
    output_dir = output_dir.strip()
    
    # 1. 检查是否为WSL2路径（以/mnt/开头）
    if output_dir.startswith('/mnt/'):
        return Path(output_dir)
    
    # 2. 检查是否为标准Windows路径（包含盘符和斜杠）
    elif len(output_dir) > 1 and output_dir[1] == ':' and ('/' in output_dir or '\\' in output_dir):
        # 转换Windows路径到WSL2路径
        # 将盘符转换为/mnt/[小写盘符]
        drive_letter = output_dir[0].lower()
        # 替换反斜杠为正斜杠
        wsl_path = output_dir[2:].replace('\\', '/')
        # 构建完整的WSL路径
        full_path = f"/mnt/{drive_letter}/{wsl_path.lstrip('/')}"
        print(f"  转换Windows路径到WSL2路径: {output_dir} -> {full_path}")
        return Path(full_path)
    
    # 3. 特殊情况处理：路径被分割的情况
    # 例如："E:\qqg\mbtiles \\taiwan.mbtiles"（中间有空格）
    elif len(output_dir) > 1 and output_dir[1] == ':':
        # 处理可能的路径分割问题
        # 尝试修复路径，去除中间的空格
        fixed_path = output_dir.replace(' ', '')
        # 替换反斜杠为正斜杠
        fixed_path = fixed_path.replace('\\', '/')
        drive_letter = fixed_path[0].lower()
        full_path = f"/mnt/{drive_letter}/{fixed_path[2:].lstrip('/')}"
        print(f"  修复并转换Windows路径到WSL2路径: {output_dir} -> {full_path}")
        return Path(full_path)
    
    # 4. 其他情况，直接返回
    return Path(output_dir)


def generate_progress_file(input_path: str, provider_name: str = "custom") -> bool:
    """
    生成 .custom_progress.json 文件
    
    Args:
        input_path: 输入路径，可以是目录或 MBTiles 文件
        provider_name: 提供商名称，默认为 "custom"
        
    Returns:
        bool: 是否生成成功
    """
    try:
        # 转换输入路径
        input_path = convert_path(input_path)
        
        # 确定输出目录
        if input_path.exists() and input_path.is_file() and input_path.suffix == '.mbtiles':
            # 输入是 MBTiles 文件
            output_dir = input_path.parent
            progress_file = output_dir / f".{provider_name}_progress.json"
            print(f"✓ 输入是 MBTiles 文件: {input_path.name}")
            
            # 从 MBTiles 文件中提取瓦片信息
            processed_tiles = _extract_tiles_from_mbtiles(input_path)
        elif input_path.exists() and input_path.is_dir():
            # 输入是目录
            output_dir = input_path
            progress_file = output_dir / f".{provider_name}_progress.json"
            print(f"✓ 输入是目录: {input_path.name}")
            
            # 从目录中提取瓦片信息
            processed_tiles = _extract_tiles_from_directory(input_path)
        else:
            # 输入路径不存在
            print(f"✗ 输入路径不存在或无效: {input_path}")
            return False
        
        # 生成进度文件内容
        progress_data = {
            'downloaded_count': len(processed_tiles),
            'failed_count': 0,
            'skipped_count': 0,
            'total_tasks': len(processed_tiles),
            'total_bytes': 0,
            'processed_tiles': list(processed_tiles),
            'timestamp': time.time()
        }
        
        # 保存进度文件
        with open(progress_file, 'w', encoding='utf-8') as f:
            json.dump(progress_data, f, ensure_ascii=False, indent=2)
        
        print(f"✓ 成功生成进度文件: .{provider_name}_progress.json")
        print(f"   包含 {len(processed_tiles)} 个已处理的瓦片")
        
        return True
        
    except Exception as e:
        print(f"✗ 生成进度文件失败: {e}")
        return False


def _extract_tiles_from_mbtiles(mbtiles_path: Path) -> Set[Tuple[int, int, int]]:
    """
    从 MBTiles 文件中提取瓦片信息
    
    Args:
        mbtiles_path: MBTiles 文件路径
        
    Returns:
        Set[Tuple[int, int, int]]: 已处理的瓦片集合，格式为 (x, y, z)
    """
    processed_tiles = set()
    
    try:
        # 连接 MBTiles 数据库
        conn = sqlite3.connect(mbtiles_path)
        cursor = conn.cursor()
        
        # 获取所有瓦片
        cursor.execute("SELECT zoom_level, tile_column, tile_row FROM tiles")
        tiles = cursor.fetchall()
        
        # 转换瓦片坐标
        for z, x, y in tiles:
            # MBTiles 中的 tile_row 是 TMS 格式，需要转换为 XYZ 格式
            y_xyz = (2 ** z - 1) - y
            processed_tiles.add((x, y_xyz, z))
        
        conn.close()
        print(f"  从 MBTiles 文件中提取了 {len(processed_tiles)} 个瓦片")
        
    except Exception as e:
        print(f"  从 MBTiles 文件中提取瓦片失败: {e}")
    
    return processed_tiles


def _extract_tiles_from_directory(directory: Path) -> Set[Tuple[int, int, int]]:
    """
    从目录中提取瓦片信息
    
    Args:
        directory: 目录路径
        
    Returns:
        Set[Tuple[int, int, int]]: 已处理的瓦片集合，格式为 (x, y, z)
    """
    processed_tiles = set()
    
    try:
        # 遍历目录结构
        for zoom_dir in directory.iterdir():
            if not zoom_dir.is_dir():
                continue
            
            # 尝试将目录名转换为缩放级别
            try:
                z = int(zoom_dir.name)
            except ValueError:
                continue
            
            # 遍历 x 目录
            for x_dir in zoom_dir.iterdir():
                if not x_dir.is_dir():
                    continue
                
                # 尝试将目录名转换为 x 坐标
                try:
                    x = int(x_dir.name)
                except ValueError:
                    continue
                
                # 遍历瓦片文件
                for tile_file in x_dir.iterdir():
                    if not tile_file.is_file():
                        continue
                    
                    # 尝试从文件名中提取 y 坐标
                    try:
                        y_str = tile_file.stem
                        y = int(y_str)
                        processed_tiles.add((x, y, z))
                    except ValueError:
                        continue
        
        print(f"  从目录中提取了 {len(processed_tiles)} 个瓦片")
        
    except Exception as e:
        print(f"  从目录中提取瓦片失败: {e}")
    
    return processed_tiles


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="生成 .custom_progress.json 文件")
    parser.add_argument('-p', '--path', required=True, help="输入路径，可以是目录或 MBTiles 文件")
    parser.add_argument('-n', '--name', default="custom", help="提供商名称，默认为 'custom'")
    
    args = parser.parse_args()
    
    generate_progress_file(args.path, args.name)
