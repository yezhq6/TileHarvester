# src/providers.py
from enum import Enum
from pathlib import Path
from typing import List, Dict, Union
import math

from .tile_math import TileMath


class TileProviderType(Enum):
    OSM = "osm"
    BING = "bing"
    CUSTOM = "custom"


class TileProvider:
    """
    抽象基类，具体的 OSM / Bing 等继承它
    """

    def __init__(
        self,
        name: str,
        provider_type: TileProviderType,
        url_template: str,
        min_zoom: int,
        max_zoom: int,
        subdomains: List[str],
        attribution: str = "",
        tile_format: str = None,
    ):
        self.name = name
        self.provider_type = provider_type
        self.url_template = url_template
        self.min_zoom = min_zoom
        self.max_zoom = max_zoom
        self.subdomains = subdomains or []
        self.attribution = attribution
        # 使用指定的格式或从URL模板提取扩展名
        self.extension = self._extract_extension(tile_format)
        # TMS坐标系标志
        self.is_tms = False
    
    def set_tile_format(self, tile_format: str):
        """
        设置瓦片格式
        
        Args:
            tile_format: 瓦片格式，如jpeg, jpg, png
        """
        self.extension = tile_format.lower()

    def get_tile_url(self, x: int, y: int, zoom: int) -> str:
        raise NotImplementedError

    def get_tile_path(
        self, x: int, y: int, zoom: int, base_dir: Union[str, Path]
    ) -> Path:
        raise NotImplementedError

    def _extract_extension(self, tile_format: str = None) -> str:
        """
        从URL模板中提取瓦片文件扩展名，或使用指定的格式
        
        Args:
            tile_format: 瓦片格式，如jpeg, jpg, png
            
        Returns:
            str: 提取的扩展名（不带点），默认为jpeg
        """
        # 如果指定了格式，直接使用
        if tile_format:
            ext = tile_format.lower()
            # 标准化扩展名：jpg和jpeg视为相同，统一为jpeg
            if ext == 'jpg':
                return 'jpeg'
            return ext
            
        # 否则从URL模板提取
        import re
        # 查找URL中的文件扩展名模式
        match = re.search(r'\.([a-zA-Z0-9]+)(?:\?|$)', self.url_template)
        if match:
            # 转换为小写
            ext = match.group(1).lower()
            # 标准化扩展名：jpg和jpeg视为相同，统一为jpeg
            if ext == 'jpg':
                return 'jpeg'
            return ext
        # 默认使用jpeg
        return 'jpeg'


class OSMTileProvider(TileProvider):
    """
    OpenStreetMap 标准 XYZ 瓦片
    """

    def __init__(self):
        super().__init__(
            name="osm",
            provider_type=TileProviderType.OSM,
            url_template="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
            min_zoom=0,
            max_zoom=19,
            subdomains=["a", "b", "c"],
            attribution="© OpenStreetMap contributors",
        )

    def get_tile_url(self, x: int, y: int, zoom: int) -> str:
        s = self.subdomains[(x + y) % len(self.subdomains)]
        return self.url_template.format(s=s, z=zoom, x=x, y=y)

    def get_tile_path(
        self, x: int, y: int, zoom: int, base_dir: Union[str, Path]
    ) -> Path:
        base_dir = Path(base_dir)
        return base_dir / "osm" / str(zoom) / str(x) / f"{y}.{self.extension}"


class BingTileProvider(TileProvider):
    """
    Bing 地图，采用 QuadKey
    你给的模板： http://ecn.t3.tiles.virtualearth.net/tiles/a{q}.jpeg?g=1
    我做成可轮询子域： http://ecn.{s}.tiles.virtualearth.net/tiles/a{q}.jpeg?g=1
    """

    def __init__(self):
        super().__init__(
            name="bing",
            provider_type=TileProviderType.BING,
            url_template="http://ecn.{s}.tiles.virtualearth.net/tiles/a{q}.jpeg?g=1",
            min_zoom=1,
            max_zoom=23,
            subdomains=["t0", "t1", "t2", "t3"],
            attribution="© Microsoft Corporation",
        )

    @staticmethod
    def tile_to_quadkey(x: int, y: int, zoom: int) -> str:
        quadkey = ""
        for i in range(zoom, 0, -1):
            digit = 0
            mask = 1 << (i - 1)
            if x & mask:
                digit += 1
            if y & mask:
                digit += 2
            quadkey += str(digit)
        return quadkey

    def get_tile_url(self, x: int, y: int, zoom: int) -> str:
        q = self.tile_to_quadkey(x, y, zoom)
        s = self.subdomains[(x + y) % len(self.subdomains)]
        return self.url_template.format(s=s, q=q)

    def get_tile_path(
        self, x: int, y: int, zoom: int, base_dir: Union[str, Path]
    ) -> Path:
        base_dir = Path(base_dir)
        return base_dir / "bing" / str(zoom) / str(x) / f"{y}.{self.extension}"


class CustomTileProvider(TileProvider):
    """
    自定义瓦片提供商
    """

    def __init__(
        self,
        name: str,
        url_template: str,
        subdomains: List[str] = None,
        min_zoom: int = 0,
        max_zoom: int = 23,
    ):
        super().__init__(
            name=name,
            provider_type=TileProviderType.CUSTOM,
            url_template=url_template,
            min_zoom=min_zoom,
            max_zoom=max_zoom,
            subdomains=subdomains or [],
            attribution="Custom Provider",
        )

    def get_tile_url(self, x: int, y: int, zoom: int) -> str:
        # 替换 URL 中的占位符
        url = self.url_template
        
        # 处理不同类型的占位符
        if "{q}" in url:
            # 需要 QuadKey
            quadkey = BingTileProvider.tile_to_quadkey(x, y, zoom)
            url = url.replace("{q}", quadkey)
        
        # 替换基本占位符
        url = url.replace("{z}", str(zoom))
        url = url.replace("{x}", str(x))
        
        url = url.replace("{y}", str(y))
        
        # 处理子域名
        if "{s}" in url and self.subdomains:
            s = self.subdomains[(x + y) % len(self.subdomains)]
            url = url.replace("{s}", s)
        
        return url

    def get_tile_path(
        self, x: int, y: int, zoom: int, base_dir: Union[str, Path]
    ) -> Path:
        base_dir = Path(base_dir)
        # 如果是TMS坐标系，调整y坐标的顺序，使其与QGIS的TMS下载结果一致
        if self.is_tms:
            n = 2 ** zoom
            y = (n - 1) - y
        return base_dir / str(zoom) / str(x) / f"{y}.{self.extension}"  # 使用动态提取的扩展名


class ProviderManager:
    """
    简单的 provider 注册 / 获取
    """

    _providers: Dict[str, TileProvider] = {}

    @classmethod
    def register_provider(cls, provider: TileProvider):
        cls._providers[provider.name.lower()] = provider

    @classmethod
    def get_provider(cls, name: str) -> TileProvider:
        p = cls._providers.get(name.lower())
        if not p:
            raise ValueError(f"未知瓦片源: {name}")
        return p

    @classmethod
    def list_providers(cls) -> List[str]:
        return list(cls._providers.keys())
        
    @classmethod
    def create_custom_provider(
        cls,
        name: str,
        url_template: str,
        subdomains: List[str] = None,
        min_zoom: int = 0,
        max_zoom: int = 23,
    ) -> TileProvider:
        """
        创建并返回一个自定义瓦片提供商
        """
        provider = CustomTileProvider(
            name=name,
            url_template=url_template,
            subdomains=subdomains or [],
            min_zoom=min_zoom,
            max_zoom=max_zoom,
        )
        # 注册为临时提供商
        cls.register_provider(provider)
        return provider


# 注册默认 provider
ProviderManager.register_provider(OSMTileProvider())
ProviderManager.register_provider(BingTileProvider())


if __name__ == "__main__":
    osm = ProviderManager.get_provider("osm")
    print("OSM 示例 URL:", osm.get_tile_url(13484, 6202, 14))

    bing = ProviderManager.get_provider("bing")
    print("Bing 示例 URL:", bing.get_tile_url(13484, 6202, 14))
