# MBTiles转换工具

一个功能强大的MBTiles转换工具，支持MBTiles和PNG目录结构的相互转换，以及MBTiles的合并和拆分功能。

## 功能特点

1. **MBTiles转PNG**：将MBTiles文件转换为标准的PNG目录结构
2. **PNG转MBTiles**：将PNG目录结构转换为MBTiles文件
3. **MBTiles合并**：合并多个MBTiles文件为一个
4. **MBTiles拆分**：按缩放级别拆分MBTiles文件
5. **多线程支持**：支持并行处理，提高转换效率
6. **选择性转换**：可以选择特定的缩放级别进行转换
7. **详细的转换统计**：显示转换进度、成功/失败数量、耗时等信息

## 安装依赖

所有依赖已合并到项目根目录的`requirements.txt`中，使用以下命令安装：

```bash
pip install -r ../requirements.txt
```

## 使用方法

### 命令格式

```bash
python mbtiles_converter.py <command> [options]
```

### 命令列表

| 命令 | 描述 |
|------|------|
| `mbtiles_to_png` | 将MBTiles转换为PNG目录结构 |
| `png_to_mbtiles` | 将PNG目录结构转换为MBTiles |
| `merge` | 合并多个MBTiles文件 |
| `split` | 按缩放级别拆分MBTiles文件 |

## 详细命令说明

### 1. MBTiles转PNG

将MBTiles文件转换为标准的PNG目录结构，目录结构为：`zoom/x/y.png`

```bash
python mbtiles_converter.py mbtiles_to_png -i <mbtiles_path> -o <output_dir> [-w <workers>] [-z <zoom_levels>]
```

**参数说明**：
- `-i, --input`: MBTiles文件路径（必填）
- `-o, --output`: 输出目录（必填）
- `-w, --workers`: 最大线程数，默认使用CPU核心数
- `-z, --zoom`: 要提取的缩放级别列表，如 `-z 14 15`，默认提取所有级别

**示例**：

```bash
# 转换所有缩放级别，使用默认线程数
python mbtiles_converter.py mbtiles_to_png -i tiles.mbtiles -o tiles_png

# 只转换缩放级别14和15，使用8个线程
python mbtiles_converter.py mbtiles_to_png -i tiles.mbtiles -o tiles_png -z 14 15 -w 8
```

### 2. PNG转MBTiles

将PNG目录结构转换为MBTiles文件

```bash
python mbtiles_converter.py png_to_mbtiles -i <input_dir> -o <mbtiles_path> [-w <workers>] [-z <zoom_levels>]
```

**参数说明**：
- `-i, --input`: 输入目录，包含PNG瓦片（必填）
- `-o, --output`: 输出MBTiles文件路径（必填）
- `-w, --workers`: 最大线程数，默认使用CPU核心数
- `-z, --zoom`: 要转换的缩放级别列表，如 `-z 14 15`，默认转换所有级别

**示例**：

```bash
# 转换所有缩放级别，使用默认线程数
python mbtiles_converter.py png_to_mbtiles -i tiles_png -o output.mbtiles

# 只转换缩放级别14和15，使用8个线程
python mbtiles_converter.py png_to_mbtiles -i tiles_png -o output.mbtiles -z 14 15 -w 8
```

### 3. MBTiles合并

合并多个MBTiles文件为一个MBTiles文件

```bash
python mbtiles_converter.py merge -i <input_files> -o <output_file> [-w <workers>]
```

**参数说明**：
- `-i, --input`: 输入MBTiles文件列表，如 `-i tiles1.mbtiles tiles2.mbtiles`（必填）
- `-o, --output`: 输出MBTiles文件路径（必填）
- `-w, --workers`: 最大线程数，默认使用CPU核心数

**示例**：

```bash
# 合并两个MBTiles文件
python mbtiles_converter.py merge -i tiles1.mbtiles tiles2.mbtiles -o merged.mbtiles

# 合并多个MBTiles文件，使用4个线程
python mbtiles_converter.py merge -i tiles1.mbtiles tiles2.mbtiles tiles3.mbtiles -o merged.mbtiles -w 4
```

### 4. MBTiles拆分

按缩放级别拆分MBTiles文件，每个缩放级别生成一个独立的MBTiles文件

```bash
python mbtiles_converter.py split -i <mbtiles_path> -o <output_dir> -z <zoom_levels> [-w <workers>]
```

**参数说明**：
- `-i, --input`: 输入MBTiles文件路径（必填）
- `-o, --output`: 输出目录（必填）
- `-z, --zoom`: 要拆分的缩放级别列表，如 `-z 14 15`（必填）
- `-w, --workers`: 最大线程数，默认使用CPU核心数

**示例**：

```bash
# 拆分缩放级别14和15
python mbtiles_converter.py split -i tiles.mbtiles -o split_output -z 14 15

# 拆分缩放级别10到15，使用8个线程
python mbtiles_converter.py split -i tiles.mbtiles -o split_output -z 10 11 12 13 14 15 -w 8
```

## 转换说明

### MBTiles格式

MBTiles是一种基于SQLite的瓦片存储格式，包含以下表：
- `tiles`: 存储瓦片数据，包含`zoom_level`、`tile_column`、`tile_row`和`tile_data`字段
- `metadata`: 存储元数据，如名称、类型、格式等

### 坐标系转换

MBTiles中的`tile_row`是从顶部开始计数的，而标准的XYZ瓦片格式是从底部开始计数的。本工具会自动处理这种转换，确保输出的PNG目录结构符合标准XYZ格式。

### 性能优化

1. **多线程处理**：使用线程池并行处理瓦片转换，提高转换速度
2. **批量事务**：PNG转MBTiles时，每100个瓦片提交一次事务，减少数据库操作开销
3. **进度显示**：显示转换进度和详细统计信息，方便监控转换状态

## 示例应用场景

1. **将下载的MBTiles转换为PNG**：用于需要标准PNG目录结构的应用
2. **将PNG瓦片打包为MBTiles**：方便存储和传输大量瓦片
3. **合并多个MBTiles文件**：将不同区域或缩放级别的MBTiles合并为一个
4. **按缩放级别拆分MBTiles**：生成特定缩放级别的MBTiles，减小文件大小

## 注意事项

1. 转换过程中会创建必要的目录结构
2. 支持的图片格式：PNG（主要）
3. 转换时会覆盖已存在的文件
4. 建议根据系统配置选择合适的线程数
5. 对于大规模转换，建议使用SSD存储以提高性能

## 系统要求

- Python 3.7+
- SQLite 3+
- Pillow 10.0.0+
- mercantile 1.2.1+

## 许可证

MIT License
