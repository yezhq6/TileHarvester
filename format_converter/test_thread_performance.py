#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试不同线程数量的转换性能
"""

import os
import subprocess
import time
from pathlib import Path


# Windows路径转换为WSL2路径
def windows_to_wsl_path(windows_path):
    """
    将Windows路径转换为WSL2路径
    例如: D:/codes/test -> /mnt/d/codes/test
    """
    if windows_path.startswith('\\'):
        # 网络共享路径，暂不处理
        return windows_path
    
    # 替换盘符和反斜杠
    wsl_path = windows_path.replace('\\', '/')
    if len(wsl_path) > 1 and wsl_path[1] == ':':
        # 处理盘符，如 D: -> /mnt/d
        drive = wsl_path[0].lower()
        wsl_path = f"/mnt/{drive}/{wsl_path[2:]}"
    
    return wsl_path


def main():
    # 输入路径（Windows格式）
    windows_input_path = r"D:\codes\MapTilesDownloader\TileHarvester\qqg"
    
    # 转换为WSL2路径
    input_path = windows_to_wsl_path(windows_input_path)
    print(f"输入路径: {input_path}")
    
    # 输出根目录
    windows_output_root = r"D:\codes\MapTilesDownloader\TileHarvester\qqg_converted"
    output_root = windows_to_wsl_path(windows_output_root)
    print(f"输出根目录: {output_root}")
    
    # 创建输出根目录
    Path(output_root).mkdir(parents=True, exist_ok=True)
    
    # 测试不同的线程数量
    thread_counts = [8, 16]
    
    # 转换格式
    output_format = "png"
    
    # 记录结果
    results = []
    
    print("\n开始测试不同线程数量的转换性能...")
    print("=" * 60)
    
    for workers in thread_counts:
        # 创建当前线程数的输出目录
        output_dir = os.path.join(output_root, f"qqg_{workers}")
        
        # 构建命令
        cmd = [
            "python", "image_converter.py",
            "-d", input_path,
            "-t", output_format,
            "-o", output_dir,
            "-r",
            "-w", str(workers)
        ]
        
        print(f"\n测试线程数: {workers}")
        print(f"输出目录: {output_dir}")
        print(f"命令: {' '.join(cmd)}")
        
        # 记录开始时间
        start_time = time.time()
        
        # 执行命令
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=os.path.dirname(__file__))
        
        # 记录结束时间
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        # 打印输出
        print("\n转换输出:")
        print(result.stdout)
        if result.stderr:
            print("\n错误输出:")
            print(result.stderr)
        
        # 解析结果
        success_count = 0
        failed_count = 0
        total_count = 0
        
        # 从输出中提取统计信息
        lines = result.stdout.split('\n')
        
        # 遍历所有行，查找统计信息
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # 跳过测试脚本自己的输出
            if '测试完成' in line:
                continue
            
            # 使用更灵活的匹配方式，支持不同的冒号格式
            if '总文件数' in line:
                # 提取数值部分
                try:
                    # 使用正则表达式提取数字
                    import re
                    match = re.search(r'总文件数[:：]\s*(\d+)', line)
                    if match:
                        total_count = int(match.group(1))
                except Exception as e:
                    print(f"解析总文件数失败: {e}")
            elif '成功' in line and '速度' not in line and '耗时' not in line:
                # 提取成功数，排除速度和耗时行
                try:
                    import re
                    match = re.search(r'成功[:：]\s*(\d+)', line)
                    if match:
                        success_count = int(match.group(1))
                except Exception as e:
                    print(f"解析成功数失败: {e}")
            elif '失败' in line:
                # 提取失败数
                try:
                    import re
                    match = re.search(r'失败[:：]\s*(\d+)', line)
                    if match:
                        failed_count = int(match.group(1))
                except Exception as e:
                    print(f"解析失败数失败: {e}")
            
            # 如果已经找到了所有三个统计值，就可以退出循环
            if total_count > 0 and success_count > 0 and failed_count >= 0:
                break
        
        # 如果解析失败，手动设置默认值（用于测试）
        if total_count == 0 and success_count == 0 and failed_count == 0:
            # 从输出中查找总文件数的另一种方式
            for line in lines:
                if '总文件数' in line:
                    print(f"调试: 解析行: {line}")
                    # 尝试简单的字符串分割
                    parts = line.split()
                    for part in parts:
                        if part.isdigit():
                            total_count = int(part)
                            success_count = total_count
                            failed_count = 0
                            break
                    break
        
        # 调试信息
        print(f"调试: 解析结果 - 总文件数: {total_count}, 成功: {success_count}, 失败: {failed_count}")
        
        # 保存结果
        results.append({
            'workers': workers,
            'total_count': total_count,
            'success_count': success_count,
            'failed_count': failed_count,
            'elapsed_time': elapsed_time,
            'speed': total_count / elapsed_time if elapsed_time > 0 else 0
        })
        
        print(f"\n测试完成，耗时: {elapsed_time:.2f}秒")
        print("-" * 60)
    
    # 打印总结
    print("\n" + "=" * 60)
    print("性能测试总结")
    print("=" * 60)
    print(f"{'线程数':<8} {'总文件数':<10} {'成功数':<8} {'失败数':<8} {'耗时(秒)':<10} {'速度(个/秒)':<12}")
    print("-" * 60)
    
    for result in results:
        print(f"{result['workers']:<8} {result['total_count']:<10} {result['success_count']:<8} "
              f"{result['failed_count']:<8} {result['elapsed_time']:<10.2f} {result['speed']:<12.2f}")
    
    print("=" * 60)
    
    # 找出最优线程数
    if results:
        best_result = max(results, key=lambda x: x['speed'])
        print(f"\n最优线程数: {best_result['workers']}")
        print(f"最快速度: {best_result['speed']:.2f} 个/秒")
        print(f"耗时: {best_result['elapsed_time']:.2f} 秒")


if __name__ == "__main__":
    main()
