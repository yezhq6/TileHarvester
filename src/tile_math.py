# src/tile_math.py
import math
from typing import Tuple, List, Dict


class TileMath:
    """
    瓦片坐标计算工具类（Web Mercator / XYZ）
    """

    @staticmethod
    def latlon_to_tile(lat: float, lon: float, zoom: int, is_tms: bool = False, use_ceil: bool = False):
        """
        经纬度 -> 瓦片坐标 (x, y)
        
        Args:
            lat: 纬度
            lon: 经度
            zoom: 缩放级别
            is_tms: 是否使用 TMS 坐标
            use_ceil: 是否对结果向上取整（用于边界计算）
        """
        # 限制纬度避免溢出
        lat = max(min(lat, 85.0511), -85.0511)

        n = 2 ** zoom
        x_tile = (lon + 180.0) / 360.0 * n
        
        lat_rad = math.radians(lat)
        y_tile = (
            1.0 - math.log(math.tan(lat_rad) + 1.0 / math.cos(lat_rad)) / math.pi
        ) / 2.0 * n
        
        # 根据需要选择取整方式
        if use_ceil:
            # 向上取整，使用1e-10避免浮点精度问题
            x_tile = math.ceil(x_tile - 1e-10)
            y_tile = math.ceil(y_tile - 1e-10)
        else:
            # 向下取整
            x_tile = int(x_tile)
            y_tile = int(y_tile)
        
        # 如果要 TMS，就在最后翻转 y
        if is_tms:
            y_tile = (n - 1) - y_tile
        
        return int(x_tile), int(y_tile)

    @staticmethod
    def tile_to_latlon(x: int, y: int, zoom: int, is_tms: bool = False):
        n = 2 ** zoom
        """
        瓦片坐标 -> 瓦片左上角经纬度 (lat, lon)
        """
        n = 2 ** zoom
        lon = x / n * 360.0 - 180.0
        lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * y / n)))
        lat = math.degrees(lat_rad)
        
        # 如果输入是 TMS 坐标，先翻回 Slippy Map
        if is_tms:
            y = (n - 1) - y

        lon = x / n * 360.0 - 180.0
        lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * y / n)))
        lat = math.degrees(lat_rad)
        
        return lat, lon

    @staticmethod
    def get_tile_bbox(x: int, y: int, zoom: int, is_tms: bool = False):
        """
        获取单个瓦片的地理范围 (west, south, east, north)
        """
        # 左上角
        north, west = TileMath.tile_to_latlon(x, y, zoom, is_tms)
        # 右下角（x+1, y+1）
        south, east = TileMath.tile_to_latlon(x + 1, y + 1, zoom, is_tms)
        return west, south, east, north
        

    @staticmethod
    def calculate_tiles_in_bbox(
        west: float, south: float, east: float, north: float, zoom: int, is_tms: bool = False
    ):
        # 计算当前缩放级别的瓦片总数，瓦片坐标范围是0到n-1
        n = 2 ** zoom
        max_valid_tile = n - 1

        # 1. 计算边界瓦片坐标
        # 左上角使用向下取整：将经纬度转换为最接近的瓦片坐标
        min_x, min_y = TileMath.latlon_to_tile(north, west, zoom, is_tms, use_ceil=False)
        # 右下角使用向上取整：确保包含边界
        max_x, max_y = TileMath.latlon_to_tile(south, east, zoom, is_tms, use_ceil=True)

        # 2. 确保瓦片坐标在有效范围内 [0, max_valid_tile]
        min_x = max(0, min_x)
        min_y = max(0, min_y)
        max_x = min(max_valid_tile, max_x)
        max_y = min(max_valid_tile, max_y)

        # 纠正一下顺序，保证 min <= max
        if min_x > max_x:
            min_x, max_x = max_x, min_x
        if min_y > max_y:
            min_y, max_y = max_y, min_y

        tiles = []

        # 3. 生成所有边界范围内的瓦片
        for x in range(min_x, max_x + 1):
            for y in range(min_y, max_y + 1):
                tiles.append((x, y))

        return tiles

    @staticmethod
    def calculate_zoom_range_tiles(
        west: float,
        south: float,
        east: float,
        north: float,
        min_zoom: int,
        max_zoom: int,
    ) -> Dict[int, List[Tuple[int, int]]]:
        """
        多个 zoom 级别的瓦片集合
        """
        zoom_tiles = {}
        for z in range(min_zoom, max_zoom + 1):
            zoom_tiles[z] = TileMath.calculate_tiles_in_bbox(
                west, south, east, north, z
            )
        return zoom_tiles
        
    @staticmethod
    def is_bbox_intersect(tile_bbox, search_bbox):
        w1, s1, e1, n1 = tile_bbox
        w2, s2, e2, n2 = search_bbox

        # 不相交的四种情形
        if (w1 >= e2) or (e1 <= w2) or (s1 >= n2) or (n1 <= s2):
            return False
        return True


if __name__ == "__main__":
    # 简单自测：北京天安门
    lat, lon, zoom = 39.9042, 116.4074, 15
    x, y = TileMath.latlon_to_tile(lat, lon, zoom)
    print(f"({lat}, {lon}) @ z={zoom} -> tile=({x}, {y})")

    west, south, east, north = TileMath.get_tile_bbox(x, y, zoom)
    print("该瓦片 bbox:", west, south, east, north)

    tiles = TileMath.calculate_tiles_in_bbox(116.3, 39.8, 116.5, 40.0, 14)
    print("示例 bbox 内瓦片数量:", len(tiles))
