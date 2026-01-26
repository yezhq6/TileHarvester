#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
工具函数模块

包含MBTiles转换工具所需的各种辅助函数
"""

import os
from pathlib import Path


def convert_path(path_str):
    """
    转换路径，支持Windows路径和Linux路径，在WSL2环境中自动转换
    
    Args:
        path_str: 输入的路径，可以是Windows路径（如D:/codes或D:\\codes）或Linux路径（如/mnt/d/codes）
        
    Returns:
        str: 转换后的路径字符串
    """
    # 处理不同格式的Windows路径
    # 1. 检查是否为Windows路径格式（带有盘符）
    if len(path_str) > 1 and path_str[1] == ':' and any(c in path_str[2:] for c in '/\\'):
        # 转换Windows路径到WSL2路径
        # 将盘符转换为/mnt/[小写盘符]
        drive_letter = path_str[0].lower()
        
        # 提取路径部分（去掉盘符和冒号）
        path_part = path_str[2:]
        
        # 替换所有反斜杠为正斜杠
        # 处理所有可能的反斜杠格式：\, \\, \\\\等
        wsl_path = path_part.replace('\\', '/')
        
        # 构建完整的WSL路径
        full_path = f"/mnt/{drive_letter}/{wsl_path.lstrip('/')}"
        print(f"✓ 转换Windows路径到WSL2路径: {path_str} -> {full_path}")
        return full_path
    elif len(path_str) > 1 and path_str[1] == ':' and len(path_str) == 2:
        # 仅盘符的情况，如 E:
        drive_letter = path_str[0].lower()
        full_path = f"/mnt/{drive_letter}"
        print(f"✓ 转换Windows路径到WSL2路径: {path_str} -> {full_path}")
        return full_path
    else:
        # 直接返回Linux路径
        return path_str


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
