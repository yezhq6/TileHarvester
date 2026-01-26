#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
命令行接口模块

处理命令行参数解析和执行相应的功能
"""

import argparse
from mbtiles_converter import MBTilesConverter
from utils import parse_zoom_levels


def main():
    """
    主函数，处理命令行参数
    """
    parser = argparse.ArgumentParser(description='MBTiles转换工具')
    
    # 子命令
    subparsers = parser.add_subparsers(dest='command', required=True, help='命令类型')
    
    # 1. MBTiles转目录命令
    mbtiles_to_dir_parser = subparsers.add_parser('mbtiles_to_dir', help='将MBTiles转换为目录结构')
    mbtiles_to_dir_parser.add_argument('-i', '--input', required=True, help='MBTiles文件路径')
    mbtiles_to_dir_parser.add_argument('-o', '--output', required=True, help='输出目录')
    mbtiles_to_dir_parser.add_argument('-s', '--scheme', type=str, default=None, help='输出目录的坐标系统 (xyz/tms)')
    mbtiles_to_dir_parser.add_argument('-w', '--workers', type=int, default=None, help='最大线程数')
    mbtiles_to_dir_parser.add_argument('-z', '--zoom', type=str, nargs='*', help='要提取的缩放级别，支持单个值或范围，如 14 或 8-15')
    
    # 2. 目录转MBTiles命令
    dir_to_mbtiles_parser = subparsers.add_parser('dir_to_mbtiles', help='将目录结构转换为MBTiles')
    dir_to_mbtiles_parser.add_argument('-i', '--input', required=True, help='输入目录')
    dir_to_mbtiles_parser.add_argument('-o', '--output', required=True, help='输出MBTiles文件路径')
    dir_to_mbtiles_parser.add_argument('-s', '--scheme', type=str, default='tms', help='输入目录的坐标系统 (xyz/tms)，默认 tms (进行坐标转换)')
    dir_to_mbtiles_parser.add_argument('-w', '--workers', type=int, default=None, help='最大线程数')
    dir_to_mbtiles_parser.add_argument('-z', '--zoom', type=str, nargs='*', help='要转换的缩放级别，支持单个值或范围，如 14 或 8-15')
    
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
    
    # 5. 比较MBTiles命令
    compare_parser = subparsers.add_parser('compare', help='比较两个MBTiles文件是否相同')
    compare_parser.add_argument('-f1', '--file1', required=True, help='第一个MBTiles文件路径')
    compare_parser.add_argument('-f2', '--file2', required=True, help='第二个MBTiles文件路径')
    
    # 6. 分析MBTiles命令
    analyze_parser = subparsers.add_parser('analyze', help='分析MBTiles文件的元数据、瓦片数据、层级分布和经纬度范围')
    analyze_parser.add_argument('-i', '--input', required=True, help='MBTiles文件路径')
    
    args = parser.parse_args()
    
    # 解析缩放级别参数
    zoom_levels = parse_zoom_levels(args.zoom) if hasattr(args, 'zoom') else None
    
    # 创建转换器实例
    converter = MBTilesConverter()
    
    # 执行相应命令
    if args.command == 'mbtiles_to_dir':
        converter.convert_mbtiles_to_directory(args.input, args.output, args.scheme, args.workers, zoom_levels)
    elif args.command == 'dir_to_mbtiles':
        converter.convert_directory_to_mbtiles(args.input, args.output, args.scheme, args.workers, zoom_levels)
    elif args.command == 'merge':
        converter.merge_mbtiles(args.input, args.output, args.workers)
    elif args.command == 'split':
        converter.split_mbtiles(args.input, args.output, zoom_levels, args.workers)
    elif args.command == 'compare':
        converter.compare_mbtiles(args.file1, args.file2)
    elif args.command == 'analyze':
        converter.analyze_mbtiles(args.input)


if __name__ == "__main__":
    main()
