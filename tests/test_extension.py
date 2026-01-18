#!/usr/bin/env python3
"""
测试瓦片提供者的扩展名提取功能
"""

from src.providers import ProviderManager

# 测试默认提供者
print("=== 测试默认提供者扩展名 ===")

# OSM提供者 - 应该使用png
osm_provider = ProviderManager.get_provider("osm")
print(f"OSM提供者: {osm_provider.name}")
print(f"URL模板: {osm_provider.url_template}")
print(f"提取的扩展名: {osm_provider.extension}")
print(f"示例文件路径: {osm_provider.get_tile_path(1, 1, 1, '/tmp')}")
print()

# Bing提供者 - 应该使用jpeg
bing_provider = ProviderManager.get_provider("bing")
print(f"Bing提供者: {bing_provider.name}")
print(f"URL模板: {bing_provider.url_template}")
print(f"提取的扩展名: {bing_provider.extension}")
print(f"示例文件路径: {bing_provider.get_tile_path(1, 1, 1, '/tmp')}")
print()

# 测试自定义提供者，使用不同的扩展名
print("=== 测试自定义提供者扩展名 ===")

# 测试jpg格式
custom_jpg = ProviderManager.create_custom_provider(
    name="test_jpg",
    url_template="https://example.com/{z}/{x}/{y}.jpg"
)
print(f"自定义JPG提供者: {custom_jpg.name}")
print(f"URL模板: {custom_jpg.url_template}")
print(f"提取的扩展名: {custom_jpg.extension}")  # 应该转换为jpeg
print(f"示例文件路径: {custom_jpg.get_tile_path(1, 1, 1, '/tmp')}")
print()

# 测试png格式
custom_png = ProviderManager.create_custom_provider(
    name="test_png",
    url_template="https://example.com/{z}/{x}/{y}.png"
)
print(f"自定义PNG提供者: {custom_png.name}")
print(f"URL模板: {custom_png.url_template}")
print(f"提取的扩展名: {custom_png.extension}")  # 应该是png
print(f"示例文件路径: {custom_png.get_tile_path(1, 1, 1, '/tmp')}")
print()

# 测试webp格式
custom_webp = ProviderManager.create_custom_provider(
    name="test_webp",
    url_template="https://example.com/{z}/{x}/{y}.webp"
)
print(f"自定义WebP提供者: {custom_webp.name}")
print(f"URL模板: {custom_webp.url_template}")
print(f"提取的扩展名: {custom_webp.extension}")  # 应该是webp
print(f"示例文件路径: {custom_webp.get_tile_path(1, 1, 1, '/tmp')}")
print()

print("所有测试完成！")
