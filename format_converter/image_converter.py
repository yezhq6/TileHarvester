#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
图片格式转换工具
支持jpg、jpeg、png格式之间的相互转换
支持单个文件和批量转换
支持多线程转换
"""

import os
import argparse
from PIL import Image
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# 路径转换函数：处理Windows路径和Linux路径

def convert_path(output_dir: str) -> Path:
    """
    转换路径，支持Windows路径和Linux路径，在WSL2环境中自动转换
    
    Args:
        output_dir: 输入的路径，可以是Windows路径（如D:/codes）或Linux路径（如/mnt/d/codes）
        
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
        print(f"  转换Windows路径到WSL2路径: {output_dir} -> {full_path}")
        return Path(full_path)
    else:
        # 直接返回Linux路径
        return Path(output_dir)


def convert_image(input_path, output_path, output_format):
    """
    转换单个图片格式
    
    Args:
        input_path: 输入图片路径
        output_path: 输出图片路径
        output_format: 输出格式 (jpg, jpeg, png)
    """
    try:
        # 打开图片
        with Image.open(input_path) as img:
            # 如果转换为jpg或jpeg，需要确保是RGB模式
            if output_format.lower() in ['jpg', 'jpeg']:
                if img.mode in ['RGBA', 'LA', 'P']:
                    img = img.convert('RGB')
            
            # 保存图片，处理格式名称映射
            if output_format.lower() in ['jpg', 'jpeg']:
                img.save(output_path, format='JPEG')
            else:
                img.save(output_path, format=output_format.upper())
            print(f"✓ 转换成功: {input_path} -> {output_path}")
            return True
    except Exception as e:
        print(f"✗ 转换失败: {input_path} -> {output_path}")
        print(f"   错误信息: {e}")
        return False


def batch_convert(input_dir, output_dir, output_format, recursive=False, max_workers=None):
    """
    批量转换图片格式
    
    Args:
        input_dir: 输入目录
        output_dir: 输出目录
        output_format: 输出格式 (jpg, jpeg, png)
        recursive: 是否递归处理子目录
        max_workers: 最大线程数，默认使用CPU核心数
    """
    # 支持的输入格式
    supported_formats = ['.jpg', '.jpeg', '.png']
    
    # 创建输出目录
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # 收集所有需要转换的任务
    tasks = []
    
    # 遍历输入目录，收集任务
    if recursive:
        for root, dirs, files in os.walk(input_dir):
            # 计算相对路径，保持目录结构
            rel_path = os.path.relpath(root, input_dir)
            current_output_dir = os.path.join(output_dir, rel_path)
            Path(current_output_dir).mkdir(parents=True, exist_ok=True)
            
            # 处理当前目录的文件
            for file in files:
                if any(file.lower().endswith(ext) for ext in supported_formats):
                    input_path = os.path.join(root, file)
                    # 创建输出文件名
                    base_name = os.path.splitext(file)[0]
                    output_path = os.path.join(current_output_dir, f"{base_name}.{output_format.lower()}")
                    tasks.append((input_path, output_path, output_format))
    else:
        # 只处理当前目录
        for file in os.listdir(input_dir):
            if os.path.isfile(os.path.join(input_dir, file)):
                if any(file.lower().endswith(ext) for ext in supported_formats):
                    input_path = os.path.join(input_dir, file)
                    # 创建输出文件名
                    base_name = os.path.splitext(file)[0]
                    output_path = os.path.join(output_dir, f"{base_name}.{output_format.lower()}")
                    tasks.append((input_path, output_path, output_format))
    
    # 如果没有任务，直接返回
    if not tasks:
        print("✗ 没有找到需要转换的图片文件")
        return
    
    print(f"✓ 找到 {len(tasks)} 个图片文件，开始转换...")
    start_time = time.time()
    
    # 使用线程池进行并行转换
    success_count = 0
    failed_count = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务
        future_to_task = {executor.submit(convert_image, *task): task for task in tasks}
        
        # 处理完成的任务
        for future in as_completed(future_to_task):
            task = future_to_task[future]
            try:
                result = future.result()
                if result:
                    success_count += 1
                else:
                    failed_count += 1
            except Exception as e:
                print(f"✗ 转换失败: {task[0]} -> {task[1]}")
                print(f"   错误信息: {e}")
                failed_count += 1
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    print(f"\n✓ 转换完成！")
    print(f"   总文件数: {len(tasks)}")
    print(f"   成功: {success_count}")
    print(f"   失败: {failed_count}")
    print(f"   耗时: {elapsed_time:.2f} 秒")
    print(f"   平均速度: {len(tasks)/elapsed_time:.2f} 个/秒")


def main():
    """
    主函数，处理命令行参数
    """
    parser = argparse.ArgumentParser(description='图片格式转换工具，支持jpg、jpeg、png之间相互转换')
    
    # 模式选择：单个文件或批量转换
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-f', '--file', help='单个图片文件路径')
    group.add_argument('-d', '--directory', help='图片目录路径')
    
    # 输出相关参数
    parser.add_argument('-o', '--output', help='输出文件或目录路径')
    parser.add_argument('-t', '--type', required=True, choices=['jpg', 'jpeg', 'png'], help='输出格式')
    parser.add_argument('-r', '--recursive', action='store_true', help='递归处理子目录')
    parser.add_argument('-w', '--workers', type=int, default=None, help='最大线程数，默认使用CPU核心数')
    
    args = parser.parse_args()
    
    # 处理单个文件转换
    if args.file:
        input_path = str(convert_path(args.file))
        
        # 如果没有指定输出路径，在原目录生成转换后的文件
        if not args.output:
            base_name = os.path.splitext(input_path)[0]
            output_path = f"{base_name}.{args.type.lower()}"
        else:
            output_path = str(convert_path(args.output))
        
        convert_image(input_path, output_path, args.type)
    
    # 处理批量转换
    elif args.directory:
        input_dir = str(convert_path(args.directory))
        
        # 如果没有指定输出目录，在原目录同级创建output目录
        if not args.output:
            output_dir = os.path.join(os.path.dirname(input_dir), f"{os.path.basename(input_dir)}_converted")
        else:
            output_dir = str(convert_path(args.output))
        
        batch_convert(input_dir, output_dir, args.type, args.recursive, args.workers)


if __name__ == "__main__":
    main()
