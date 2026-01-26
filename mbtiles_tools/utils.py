#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
工具函数模块

包含MBTiles转换工具所需的各种辅助函数
"""


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
