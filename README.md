# TileHarvester

**TileHarvester** 是一个功能强大的地图瓦片下载工具，支持多种地图提供商，可批量下载地图瓦片并保存到本地。

## 功能特性

- 🗺️ **支持多种地图提供商**：可自定义瓦片服务器URL
- 🚀 **高性能下载**：支持多线程下载，充分利用带宽
- ⏸️ **暂停/继续下载**：灵活控制下载过程
- 📊 **实时进度显示**：直观展示下载进度和统计信息
- 🎯 **精确边界控制**：支持手动输入边界坐标
- 🔧 **灵活配置**：支持自定义输出目录、缩放级别等
- 💡 **友好的Web界面**：基于Flask和Leaflet的交互式地图界面
- 📷 **支持多种图片格式**：支持jpg、jpeg、png格式下载
- 📦 **支持MBTiles格式**：可将瓦片下载为MBTiles文件，便于存储和传输
- 🔄 **格式转换工具**：提供瓦片格式转换功能，支持不同格式间的相互转换
- 🔀 **MBTiles工具集**：支持MBTiles和PNG目录结构的相互转换、合并和拆分

## 技术栈

- **后端**：Python 3, Flask
- **前端**：HTML, CSS, JavaScript, Leaflet.js, Bootstrap
- **核心库**：requests, threading, queue, sqlite3, Pillow, mercantile

## 安装步骤

1. **克隆仓库**
   ```bash
   git clone https://github.com/yourusername/TileHarvester.git
   cd TileHarvester
   ```

2. **安装依赖**
   ```bash
   pip install -r requirements.txt
   ```

3. **启动应用**
   ```bash
   python app.py
   ```

4. **访问应用**
   打开浏览器访问 `http://127.0.0.1:5000`

## 使用说明

### 基本使用

1. **设置瓦片URL**：在右侧输入瓦片服务器URL，例如Bing地图URL
2. **设置输出目录**：输入瓦片保存的目录名称
3. **绘制下载区域**：点击"绘制区域"按钮，在地图上点击两个点绘制矩形区域
4. **设置缩放级别**：输入最小和最大缩放级别
5. **开始下载**：点击"开始下载"按钮
6. **控制下载**：可随时暂停、继续或取消下载

### 高级功能

#### 手动输入边界坐标
1. 在"设置边界坐标"区域输入精确的经纬度坐标
2. 点击"应用边界"按钮
3. 系统会自动在地图上绘制边界框

#### 使用TMS瓦片规范
- 勾选"Use TMS tiles convention"选项
- 系统会使用TMS坐标系统下载瓦片

## API文档

### 主要API端点

- `GET /`：返回主页面
- `POST /api/download`：启动下载任务
- `POST /api/pause-download`：暂停当前下载
- `POST /api/resume-download`：继续当前下载
- `POST /api/cancel-download`：取消当前下载
- `GET /api/progress`：获取实时下载进度（SSE）
- `GET /api/download-status`：获取当前下载状态

## 项目结构

```
TileHarvester/
├── app.py                # Flask应用主文件
├── requirements.txt      # 项目依赖
├── src/
│   ├── __init__.py       # 包初始化文件
│   ├── downloader.py     # 核心下载逻辑
│   ├── providers.py      # 瓦片提供商管理
│   ├── tile_math.py      # 瓦片坐标计算
│   └── cli.py            # 命令行接口（预留）
├── templates/
│   └── index.html        # Web界面模板
├── format_converter/     # 瓦片格式转换工具
│   ├── image_converter.py    # 图片格式转换脚本
│   ├── tile_merger.py        # 瓦片合并工具
│   └── README.md             # 格式转换工具说明
├── mbtiles_tools/        # MBTiles工具集
│   ├── mbtiles_converter.py  # MBTiles转换脚本
│   └── README.md             # MBTiles工具说明
├── log/                  # 日志文件目录
└── README.md            # 项目说明文档
```

## 配置说明

### 环境变量

| 变量名 | 描述 | 默认值 |
|--------|------|--------|
| `FLASK_ENV` | Flask运行环境 | development |
| `FLASK_DEBUG` | 是否开启调试模式 | True |
| `HOST` | 服务器绑定地址 | 0.0.0.0 |
| `PORT` | 服务器端口 | 5000 |

### 应用配置

在 `app.py` 中可以修改以下配置：

- `MAX_THREADS`：最大下载线程数（默认32）
- `UPLOAD_FOLDER`：默认瓦片存储目录（默认"tiles"）
- `MAX_CONTENT_LENGTH`：最大请求内容长度（默认16MB）

## 最佳实践

1. **选择合适的线程数**：根据网络带宽和服务器限制调整线程数
2. **合理设置缩放级别**：避免一次性下载过多瓦片
3. **使用代理**：如果下载量大，建议使用代理避免被封禁
4. **定期备份**：定期备份下载的瓦片数据
5. **遵守使用条款**：确保遵守各地图提供商的使用条款

## 常见问题

### Q: 下载失败怎么办？
A: 检查瓦片URL是否正确，网络连接是否正常，或尝试减少线程数

### Q: 地图显示不出来？
A: 检查瓦片URL是否支持HTTPS，或尝试更换地图提供商

### Q: 下载速度慢？
A: 尝试增加线程数，或检查网络连接

### Q: 瓦片数量计算不准确？
A: 检查边界坐标是否正确，或尝试调整缩放级别

## 许可证

MIT License

## 贡献指南

欢迎提交Issue和Pull Request！

## 更新日志

### v1.0.0 (2026-01-17)
- 初始版本发布
- 支持多线程下载
- 支持暂停/继续下载
- 支持自定义瓦片提供商
- 友好的Web界面

## 联系方式

如有问题或建议，请提交Issue或联系开发者。

---

**Enjoy TileHarvesting! 🎉**
