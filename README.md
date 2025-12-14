# 安全告警路径提取系统

这是一个基于AI模型的安全告警路径提取系统，可以从安全告警数据中自动提取可疑程序路径。

## 功能特性

- 配置管理：管理系统配置，包括Ollama服务器地址、模型名称、处理参数等
- 文件上传：上传Excel格式的安全告警数据文件进行处理
- 任务处理：异步处理安全告警数据
- 结果展示：展示并导出处理结果
- 任务管理：查看、管理和删除处理任务

## 技术架构

- 后端：Python Flask Web框架
- AI模型：Ollama + Qwen模型
- 前端：Bootstrap 5 + Jinja2模板引擎
- 数据处理：Pandas库
- 异步处理：多线程

## 安装部署

### 环境要求

- Python 3.8+
- Ollama服务
- 相关Python依赖包

### 安装步骤

1. 克隆项目代码：
   ```
   git clone <repository-url>
   cd process_white_alarm
   ```

2. 安装依赖：
   ```
   pip install -r requirements.txt
   ```

3. 启动Web服务：
   ```
   python web_app.py
   ```

4. 访问Web界面：
   打开浏览器访问 http://127.0.0.1:5000

## 使用说明

1. 配置系统参数：
   - 在"配置管理"页面设置Ollama服务器地址和模型名称
   - 设置处理参数，如最大并发数、最大处理行数等

2. 上传数据文件：
   - 准备Excel格式的安全告警数据文件
   - 在"文件上传"页面上传文件

3. 处理任务：
   - 在任务详情页面点击"开始处理"按钮
   - 系统将在后台异步处理任务

4. 查看结果：
   - 任务完成后可下载有效结果和无效记录
   - 查看处理日志以了解详细信息

## 目录结构

```
process_white_alarm/
├── config.yaml           # 系统配置文件
├── web_app.py            # Web应用主程序
├── process_white_alarm.py # 核心处理逻辑
├── ollama_client.py      # Ollama客户端
├── requirements.txt      # Python依赖列表
├── tasks.json            # 任务数据存储文件
├── templates/            # HTML模板文件
├── static/               # 静态资源文件
├── uploads/              # 上传文件存储目录
├── results/              # 处理结果存储目录
├── logs/                 # 日志文件存储目录
└── README.md             # 项目说明文档
```

## 注意事项

- 确保Ollama服务正常运行并可访问
- 大文件处理可能需要较长时间，请耐心等待
- 系统会在处理过程中生成日志文件，便于问题排查
- 可以通过任务管理页面查看和管理所有处理任务