#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MBTiles转换工具
支持以下功能：
1. MBTiles转换为PNG目录结构
2. PNG目录结构转换为MBTiles
3. MBTiles合并功能
4. MBTiles按zoom拆分提取功能
支持多线程处理
"""

import os
import argparse
import sqlite3
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from PIL import Image
import io


def mbtiles_to_png(mbtiles_path, output_dir, max_workers=None, zoom_levels=None):
    """
    将MBTiles转换为PNG目录结构
    
    Args:
        mbtiles_path: MBTiles文件路径
        output_dir: 输出目录
        max_workers: 最大线程数
        zoom_levels: 要提取的缩放级别列表，None表示提取所有级别
    """
    # 验证MBTiles文件是否存在
    if not os.path.exists(mbtiles_path):
        print(f"✗ MBTiles文件不存在: {mbtiles_path}")
        return False
    
    # 创建输出目录
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    try:
        # 连接MBTiles数据库
        conn = sqlite3.connect(mbtiles_path)
        cursor = conn.cursor()
        
        # 获取元数据
        cursor.execute("SELECT name, value FROM metadata")
        metadata = dict(cursor.fetchall())
        print(f"✓ 读取MBTiles元数据: {metadata.get('name', 'Unknown')}")
        print(f"   格式: {metadata.get('format', 'Unknown')}")
        print(f"   类型: {metadata.get('type', 'Unknown')}")
        
        # 获取缩放级别列表
        cursor.execute("SELECT DISTINCT zoom_level FROM tiles ORDER BY zoom_level")
        all_zoom_levels = [row[0] for row in cursor.fetchall()]
        
        # 过滤要提取的缩放级别
        if zoom_levels:
            extract_zoom_levels = [z for z in all_zoom_levels if z in zoom_levels]
        else:
            extract_zoom_levels = all_zoom_levels
        
        print(f"   可用缩放级别: {all_zoom_levels}")
        print(f"   要提取的缩放级别: {extract_zoom_levels}")
        
        if not extract_zoom_levels:
            print("✗ 没有要提取的缩放级别")
            conn.close()
            return False
        
        # 获取瓦片总数
        if zoom_levels:
            placeholders = ','.join(['?'] * len(zoom_levels))
            cursor.execute(f"SELECT COUNT(*) FROM tiles WHERE zoom_level IN ({placeholders})", zoom_levels)
        else:
            cursor.execute("SELECT COUNT(*) FROM tiles")
        total_tiles = cursor.fetchone()[0]
        
        print(f"✓ 总瓦片数: {total_tiles}")
        
        # 收集所有瓦片信息
        tiles_info = []
        for zoom in extract_zoom_levels:
            cursor.execute(
                "SELECT tile_column, tile_row, tile_data FROM tiles WHERE zoom_level = ?",
                (zoom,)
            )
            
            for tile_column, tile_row, tile_data in cursor.fetchall():
                # MBTiles中的tile_row是从顶部开始计数的，需要转换
                y = (2 ** zoom) - 1 - tile_row
                
                # 构建输出路径
                zoom_dir = os.path.join(output_dir, str(zoom))
                x_dir = os.path.join(zoom_dir, str(tile_column))
                output_path = os.path.join(x_dir, f"{y}.png")
                
                tiles_info.append({
                    'zoom': zoom,
                    'tile_column': tile_column,
                    'tile_row': tile_row,
                    'tile_data': tile_data,
                    'x_dir': x_dir,
                    'output_path': output_path
                })
        
        conn.close()
        
        # 使用线程池进行并行转换
        print(f"✓ 开始转换，共 {len(tiles_info)} 个瓦片...")
        start_time = time.time()
        success_count = 0
        failed_count = 0
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_tile = {executor.submit(_save_tile, tile_info): tile_info for tile_info in tiles_info}
            
            # 处理完成的任务
            for future in as_completed(future_to_tile):
                tile_info = future_to_tile[future]
                try:
                    result = future.result()
                    if result:
                        success_count += 1
                    else:
                        failed_count += 1
                except Exception as e:
                    print(f"✗ 保存瓦片失败: {tile_info['output_path']}")
                    print(f"   错误信息: {e}")
                    failed_count += 1
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        print(f"\n✓ 转换完成！")
        print(f"   成功: {success_count}")
        print(f"   失败: {failed_count}")
        print(f"   耗时: {elapsed_time:.2f} 秒")
        print(f"   平均速度: {len(tiles_info)/elapsed_time:.2f} 个/秒")
        print(f"✓ 输出目录: {output_dir}")
        
        return True
        
    except sqlite3.Error as e:
        print(f"✗ MBTiles数据库错误: {e}")
        return False
    except Exception as e:
        print(f"✗ 转换失败: {e}")
        return False


def _save_tile(tile_info):
    """
    保存单个瓦片到文件
    
    Args:
        tile_info: 瓦片信息字典
        
    Returns:
        bool: 是否保存成功
    """
    try:
        # 创建目录
        Path(tile_info['x_dir']).mkdir(parents=True, exist_ok=True)
        
        # 保存瓦片数据
        with open(tile_info['output_path'], 'wb') as f:
            f.write(tile_info['tile_data'])
        
        return True
    except Exception as e:
        print(f"✗ 保存瓦片失败: {tile_info['output_path']}")
        print(f"   错误信息: {e}")
        return False


def png_to_mbtiles(input_dir, mbtiles_path, max_workers=None, zoom_levels=None):
    """
    将PNG目录结构转换为MBTiles
    
    Args:
        input_dir: 输入目录
        mbtiles_path: 输出MBTiles文件路径
        max_workers: 最大线程数
        zoom_levels: 要转换的缩放级别列表，None表示转换所有级别
    """
    # 验证输入目录是否存在
    if not os.path.exists(input_dir):
        print(f"✗ 输入目录不存在: {input_dir}")
        return False
    
    # 创建输出目录
    Path(os.path.dirname(mbtiles_path)).mkdir(parents=True, exist_ok=True)
    
    try:
        # 获取所有缩放级别目录
        zoom_dirs = [d for d in os.listdir(input_dir) 
                    if os.path.isdir(os.path.join(input_dir, d)) and d.isdigit()]
        
        # 转换为整数并排序
        all_zoom_levels = sorted([int(d) for d in zoom_dirs])
        
        # 过滤要转换的缩放级别
        if zoom_levels:
            convert_zoom_levels = [z for z in all_zoom_levels if z in zoom_levels]
        else:
            convert_zoom_levels = all_zoom_levels
        
        if not convert_zoom_levels:
            print("✗ 没有要转换的缩放级别")
            return False
        
        print(f"✓ 发现缩放级别: {all_zoom_levels}")
        print(f"   要转换的缩放级别: {convert_zoom_levels}")
        
        # 收集所有瓦片文件
        tiles_info = []
        for zoom in convert_zoom_levels:
            zoom_dir = os.path.join(input_dir, str(zoom))
            x_dirs = [d for d in os.listdir(zoom_dir) if os.path.isdir(os.path.join(zoom_dir, d)) and d.isdigit()]
            
            for x_str in x_dirs:
                x = int(x_str)
                x_dir = os.path.join(zoom_dir, x_str)
                
                # 获取所有png文件
                png_files = [f for f in os.listdir(x_dir) if f.lower().endswith('.png')]
                
                for png_file in png_files:
                    # 提取y坐标
                    y = int(os.path.splitext(png_file)[0])
                    input_path = os.path.join(x_dir, png_file)
                    
                    tiles_info.append({
                        'zoom': zoom,
                        'x': x,
                        'y': y,
                        'input_path': input_path
                    })
        
        print(f"✓ 找到 {len(tiles_info)} 个PNG瓦片")
        
        # 创建MBTiles数据库
        conn = sqlite3.connect(mbtiles_path)
        cursor = conn.cursor()
        
        # 创建表结构
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tiles (
                zoom_level INTEGER,
                tile_column INTEGER,
                tile_row INTEGER,
                tile_data BLOB,
                PRIMARY KEY (zoom_level, tile_column, tile_row)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS metadata (
                name TEXT,
                value TEXT,
                PRIMARY KEY (name)
            )
        ''')
        
        # 插入元数据
        metadata = [
            ('name', 'PNG to MBTiles'),
            ('type', 'baselayer'),
            ('version', '1.0'),
            ('description', 'Generated by MBTiles Converter'),
            ('format', 'png'),
        ]
        cursor.executemany('INSERT OR REPLACE INTO metadata (name, value) VALUES (?, ?)', metadata)
        conn.commit()
        
        print(f"✓ 初始化MBTiles数据库: {mbtiles_path}")
        
        # 使用线程池进行并行转换
        print(f"✓ 开始转换，共 {len(tiles_info)} 个瓦片...")
        start_time = time.time()
        success_count = 0
        failed_count = 0
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_tile = {executor.submit(_process_png_tile, tile_info): tile_info for tile_info in tiles_info}
            
            # 处理完成的任务
            for future in as_completed(future_to_tile):
                tile_info = future_to_tile[future]
                try:
                    result = future.result()
                    if result:
                        # 插入到数据库
                        cursor.execute(
                            '''INSERT OR REPLACE INTO tiles 
                            (zoom_level, tile_column, tile_row, tile_data) 
                            VALUES (?, ?, ?, ?)''',
                            result
                        )
                        success_count += 1
                        
                        # 每100个瓦片提交一次事务
                        if success_count % 100 == 0:
                            conn.commit()
                    else:
                        failed_count += 1
                except Exception as e:
                    print(f"✗ 处理瓦片失败: {tile_info['input_path']}")
                    print(f"   错误信息: {e}")
                    failed_count += 1
        
        # 提交最终事务
        conn.commit()
        conn.close()
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        print(f"\n✓ 转换完成！")
        print(f"   成功: {success_count}")
        print(f"   失败: {failed_count}")
        print(f"   耗时: {elapsed_time:.2f} 秒")
        print(f"   平均速度: {len(tiles_info)/elapsed_time:.2f} 个/秒")
        print(f"✓ 输出MBTiles: {mbtiles_path}")
        
        return True
        
    except sqlite3.Error as e:
        print(f"✗ MBTiles数据库错误: {e}")
        return False
    except Exception as e:
        print(f"✗ 转换失败: {e}")
        return False


def _process_png_tile(tile_info):
    """
    处理单个PNG瓦片
    
    Args:
        tile_info: 瓦片信息字典
        
    Returns:
        tuple: (zoom_level, tile_column, tile_row, tile_data) 或 None
    """
    try:
        # 读取PNG文件
        with open(tile_info['input_path'], 'rb') as f:
            tile_data = f.read()
        
        # MBTiles中的tile_row是从顶部开始计数的，需要转换
        zoom = tile_info['zoom']
        mbtiles_row = (2 ** zoom) - 1 - tile_info['y']
        
        return (zoom, tile_info['x'], mbtiles_row, tile_data)
    except Exception as e:
        print(f"✗ 处理瓦片失败: {tile_info['input_path']}")
        print(f"   错误信息: {e}")
        return None


def merge_mbtiles(input_files, output_file, max_workers=None):
    """
    合并多个MBTiles文件
    
    Args:
        input_files: 输入MBTiles文件列表
        output_file: 输出MBTiles文件路径
        max_workers: 最大线程数
    """
    # 验证输入文件
    for input_file in input_files:
        if not os.path.exists(input_file):
            print(f"✗ 输入文件不存在: {input_file}")
            return False
    
    # 创建输出目录
    Path(os.path.dirname(output_file)).mkdir(parents=True, exist_ok=True)
    
    try:
        # 创建输出MBTiles数据库
        output_conn = sqlite3.connect(output_file)
        output_cursor = output_conn.cursor()
        
        # 创建表结构
        output_cursor.execute('''
            CREATE TABLE IF NOT EXISTS tiles (
                zoom_level INTEGER,
                tile_column INTEGER,
                tile_row INTEGER,
                tile_data BLOB,
                PRIMARY KEY (zoom_level, tile_column, tile_row)
            )
        ''')
        
        output_cursor.execute('''
            CREATE TABLE IF NOT EXISTS metadata (
                name TEXT,
                value TEXT,
                PRIMARY KEY (name)
            )
        ''')
        
        # 插入基础元数据
        metadata = [
            ('name', 'Merged MBTiles'),
            ('type', 'baselayer'),
            ('version', '1.0'),
            ('description', 'Merged by MBTiles Converter'),
            ('format', 'png'),
        ]
        output_cursor.executemany('INSERT OR REPLACE INTO metadata (name, value) VALUES (?, ?)', metadata)
        output_conn.commit()
        
        total_tiles = 0
        
        for input_file in input_files:
            print(f"✓ 处理输入文件: {input_file}")
            
            # 连接输入MBTiles
            input_conn = sqlite3.connect(input_file)
            input_cursor = input_conn.cursor()
            
            # 获取输入文件的瓦片数
            input_cursor.execute("SELECT COUNT(*) FROM tiles")
            input_tiles_count = input_cursor.fetchone()[0]
            print(f"   瓦片数: {input_tiles_count}")
            
            # 获取输入文件的瓦片数据
            input_cursor.execute("SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles")
            tiles = input_cursor.fetchall()
            
            # 插入到输出文件
            if tiles:
                output_cursor.executemany(
                    "INSERT OR REPLACE INTO tiles VALUES (?, ?, ?, ?)",
                    tiles
                )
                output_conn.commit()
                total_tiles += len(tiles)
            
            input_conn.close()
        
        output_conn.close()
        
        print(f"\n✓ 合并完成！")
        print(f"   合并文件数: {len(input_files)}")
        print(f"   总瓦片数: {total_tiles}")
        print(f"✓ 输出MBTiles: {output_file}")
        
        return True
        
    except sqlite3.Error as e:
        print(f"✗ MBTiles数据库错误: {e}")
        return False
    except Exception as e:
        print(f"✗ 合并失败: {e}")
        return False


def split_mbtiles(mbtiles_path, output_dir, zoom_levels, max_workers=None):
    """
    按zoom级别拆分MBTiles文件
    
    Args:
        mbtiles_path: 输入MBTiles文件路径
        output_dir: 输出目录
        zoom_levels: 要拆分的缩放级别列表
        max_workers: 最大线程数
    """
    # 验证MBTiles文件是否存在
    if not os.path.exists(mbtiles_path):
        print(f"✗ MBTiles文件不存在: {mbtiles_path}")
        return False
    
    # 创建输出目录
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    try:
        # 连接MBTiles数据库
        conn = sqlite3.connect(mbtiles_path)
        cursor = conn.cursor()
        
        # 获取所有缩放级别
        cursor.execute("SELECT DISTINCT zoom_level FROM tiles ORDER BY zoom_level")
        all_zoom_levels = [row[0] for row in cursor.fetchall()]
        
        # 验证要拆分的缩放级别
        valid_zoom_levels = [z for z in zoom_levels if z in all_zoom_levels]
        if not valid_zoom_levels:
            print(f"✗ 没有有效的缩放级别，可用级别: {all_zoom_levels}")
            conn.close()
            return False
        
        print(f"✓ 可用缩放级别: {all_zoom_levels}")
        print(f"   要拆分的缩放级别: {valid_zoom_levels}")
        
        # 获取元数据
        cursor.execute("SELECT name, value FROM metadata")
        metadata = dict(cursor.fetchall())
        
        for zoom in valid_zoom_levels:
            print(f"\n✓ 处理缩放级别: {zoom}")
            
            # 创建输出MBTiles文件
            output_mbtiles = os.path.join(output_dir, f"tiles_zoom{zoom}.mbtiles")
            
            # 创建新的MBTiles数据库
            output_conn = sqlite3.connect(output_mbtiles)
            output_cursor = output_conn.cursor()
            
            # 创建表结构
            output_cursor.execute('''
                CREATE TABLE IF NOT EXISTS tiles (
                    zoom_level INTEGER,
                    tile_column INTEGER,
                    tile_row INTEGER,
                    tile_data BLOB,
                    PRIMARY KEY (zoom_level, tile_column, tile_row)
                )
            ''')
            
            output_cursor.execute('''
                CREATE TABLE IF NOT EXISTS metadata (
                    name TEXT,
                    value TEXT,
                    PRIMARY KEY (name)
                )
            ''')
            
            # 插入元数据
            zoom_metadata = metadata.copy()
            zoom_metadata['name'] = f"Tiles Zoom {zoom}"
            zoom_metadata['description'] = f"Zoom level {zoom} extracted by MBTiles Converter"
            
            output_cursor.executemany(
                "INSERT OR REPLACE INTO metadata (name, value) VALUES (?, ?)",
                zoom_metadata.items()
            )
            
            # 获取当前缩放级别的瓦片
            cursor.execute(
                "SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles WHERE zoom_level = ?",
                (zoom,)
            )
            tiles = cursor.fetchall()
            
            # 插入瓦片数据
            if tiles:
                output_cursor.executemany(
                    "INSERT OR REPLACE INTO tiles VALUES (?, ?, ?, ?)",
                    tiles
                )
                output_conn.commit()
                print(f"   瓦片数: {len(tiles)}")
            
            output_conn.close()
            print(f"   输出文件: {output_mbtiles}")
        
        conn.close()
        
        print(f"\n✓ 拆分完成！")
        print(f"   拆分缩放级别: {valid_zoom_levels}")
        print(f"   输出目录: {output_dir}")
        
        return True
        
    except sqlite3.Error as e:
        print(f"✗ MBTiles数据库错误: {e}")
        return False
    except Exception as e:
        print(f"✗ 拆分失败: {e}")
        return False


def main():
    """
    主函数，处理命令行参数
    """
    parser = argparse.ArgumentParser(description='MBTiles转换工具')
    
    # 子命令
    subparsers = parser.add_subparsers(dest='command', required=True, help='命令类型')
    
    # 1. MBTiles转PNG命令
    mbtiles_to_png_parser = subparsers.add_parser('mbtiles_to_png', help='将MBTiles转换为PNG目录结构')
    mbtiles_to_png_parser.add_argument('-i', '--input', required=True, help='MBTiles文件路径')
    mbtiles_to_png_parser.add_argument('-o', '--output', required=True, help='输出目录')
    mbtiles_to_png_parser.add_argument('-w', '--workers', type=int, default=None, help='最大线程数')
    mbtiles_to_png_parser.add_argument('-z', '--zoom', type=str, nargs='*', help='要提取的缩放级别，支持单个值或范围，如 14 或 8-15')
    
    # 2. PNG转MBTiles命令
    png_to_mbtiles_parser = subparsers.add_parser('png_to_mbtiles', help='将PNG目录结构转换为MBTiles')
    png_to_mbtiles_parser.add_argument('-i', '--input', required=True, help='输入目录')
    png_to_mbtiles_parser.add_argument('-o', '--output', required=True, help='输出MBTiles文件路径')
    png_to_mbtiles_parser.add_argument('-w', '--workers', type=int, default=None, help='最大线程数')
    png_to_mbtiles_parser.add_argument('-z', '--zoom', type=str, nargs='*', help='要转换的缩放级别，支持单个值或范围，如 14 或 8-15')
    
    # 3. 合并MBTiles命令
    merge_parser = subparsers.add_parser('merge', help='合并多个MBTiles文件')
    merge_parser.add_argument('-i', '--input', required=True, nargs='+', help='输入MBTiles文件列表')
    merge_parser.add_argument('-o', '--output', required=True, help='输出MBTiles文件路径')
    merge_parser.add_argument('-w', '--workers', type=int, default=None, help='最大线程数')
    
    # 4. 拆分MBTiles命令
    split_parser = subparsers.add_parser('split', help='按zoom级别拆分MBTiles文件')
    split_parser.add_argument('-i', '--input', required=True, help='输入MBTiles文件路径')
    split_parser.add_argument('-o', '--output', required=True, help='输出目录')
    split_parser.add_argument('-z', '--zoom', type=str, nargs='+', required=True, help='要拆分的缩放级别列表，支持单个值或范围，如 14 或 8-15')
    split_parser.add_argument('-w', '--workers', type=int, default=None, help='最大线程数')
    
    args = parser.parse_args()
    
    # 解析缩放级别参数
    zoom_levels = parse_zoom_levels(args.zoom)
    
    # 执行相应命令
    if args.command == 'mbtiles_to_png':
        mbtiles_to_png(args.input, args.output, args.workers, zoom_levels)
    elif args.command == 'png_to_mbtiles':
        png_to_mbtiles(args.input, args.output, args.workers, zoom_levels)
    elif args.command == 'merge':
        merge_mbtiles(args.input, args.output, args.workers)
    elif args.command == 'split':
        split_mbtiles(args.input, args.output, zoom_levels, args.workers)


def parse_zoom_levels(zoom_args):
    """
    解析缩放级别参数，支持单个值和范围格式
    
    Args:
        zoom_args: 命令行传递的缩放级别参数列表
        
    Returns:
        list: 解析后的缩放级别列表
    """
    if not zoom_args:
        return None
    
    zoom_levels = []
    for arg in zoom_args:
        # 检查是否是范围格式，如 8-15
        if isinstance(arg, str) and '-' in arg:
            try:
                start, end = map(int, arg.split('-'))
                # 确保start <= end
                if start > end:
                    start, end = end, start
                # 添加范围内的所有缩放级别
                zoom_levels.extend(range(start, end + 1))
            except ValueError:
                # 如果解析失败，作为单个值处理
                zoom_levels.append(int(arg))
        else:
            # 单个值
            zoom_levels.append(int(arg))
    
    # 去重并排序
    zoom_levels = sorted(list(set(zoom_levels)))
    return zoom_levels


if __name__ == "__main__":
    main()
