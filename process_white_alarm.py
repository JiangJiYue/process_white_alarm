from pathlib import Path

import pandas as pd
import re
import logging
import os
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import yaml
from logging import LoggerAdapter
from datetime import datetime

# 导入抽离的 Ollama 模块
from ollama_client import OllamaClient, create_ollama_client_from_config, test_ollama_connection

# ================== 从配置文件加载 ==================
with open("config.yaml", "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

# 获取输出目录配置，如果没有设置则使用默认值
OUTPUT_DIR = config.get("output_dir", "output/run_{}".format(datetime.now().strftime("%Y%m%d_%H%M%S")))

# 在输出目录中定义输出文件
INVALID_OUTPUT_FILE = os.path.join(OUTPUT_DIR, "invalid_records.xlsx")
RESULT_OUTPUT_FILE = os.path.join(OUTPUT_DIR, "valid_results.xlsx")

# 日志文件配置
LOG_DIR = config["logging"].get("log_dir", "logs")
LOG_FILE_TEMPLATE = config["logging"]["log_file"]

# 创建日志目录
os.makedirs(LOG_DIR, exist_ok=True)

# 替换占位符生成日志文件路径
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = LOG_FILE_TEMPLATE.replace("{log_dir}", LOG_DIR).replace("{timestamp}", timestamp).replace("{task_id}", "standalone")

OLLAMA_CONFIG = config["ollama"]

MAX_WORKERS = config["processing"]["max_workers"]
MAX_ROWS_TO_PROCESS = config["processing"]["max_rows_to_process"]  # 可为 null → None

LOG_LEVEL = getattr(logging, config["logging"]["level"].upper())
LOG_FORMAT = config["logging"].get("format", "text")  # 默认为文本格式

SYSTEM_PROMPT = config["system_prompt"].rstrip()

# ===================================================

# ================== 日志配置 ==================
# 创建输出目录
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 配置日志格式
if LOG_FORMAT == "json":
    # JSON格式日志
    class JsonFormatter(logging.Formatter):
        def format(self, record):
            log_entry = {
                "timestamp": self.formatTime(record),
                "level": record.levelname,
                "message": record.getMessage()
            }
            if hasattr(record, 'task_id'):
                log_entry["task_id"] = record.task_id
            return json.dumps(log_entry, ensure_ascii=False)
    
    formatter = JsonFormatter()
else:
    # 文本格式日志
    class TextFormatter(logging.Formatter):
        def format(self, record):
            log_message = super().format(record)
            if hasattr(record, 'task_id'):
                log_message = f"[task_{record.task_id}] {log_message}"
            return log_message
    
    formatter = TextFormatter("%(asctime)s [%(levelname)s] %(message)s")

# 配置根日志记录器
root_logger = logging.getLogger()
root_logger.setLevel(LOG_LEVEL)

# 移除现有的处理器
for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)
    handler.close()

# 添加新的处理器
# 使用 RotatingFileHandler 实现日志轮转
from logging.handlers import RotatingFileHandler
file_handler = RotatingFileHandler(
    LOG_FILE, 
    maxBytes=10*1024*1024,  # 10MB
    backupCount=20,
    encoding='utf-8'
)
file_handler.setFormatter(formatter)
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

root_logger.addHandler(file_handler)
root_logger.addHandler(console_handler)

logger = logging.getLogger(__name__)

# 任务日志工厂
def task_logger_factory(task_id):
    return LoggerAdapter(logger, {'task_id': task_id})

# 允许外部设置任务日志工厂的函数
_task_logger_factory = None

def set_task_logger_factory(factory):
    """设置外部任务日志工厂"""
    global _task_logger_factory
    _task_logger_factory = factory

def get_task_logger(task_id):
    """获取任务日志记录器"""
    if _task_logger_factory:
        return _task_logger_factory(task_id)
    else:
        return task_logger_factory(task_id)

# 初始化 Ollama 客户端，传递logger
ollama_client = create_ollama_client_from_config(config)


def clean_excel_string(s):
    """
    彻底清洗字符串，移除所有 Excel 不支持的控制字符和常见隐藏字符。
    """
    if not isinstance(s, str):
        s = str(s)
    # 移除 ASCII 控制字符（保留 \t \n \r）
    s = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', s)
    # 移除 Unicode 隐藏/格式字符（零宽空格、BOM、双向控制符等）
    s = re.sub(r'[\u200B-\u200D\uFEFF\u202A-\u202E\u00AD\u180E]', '', s)
    return s.strip()


def is_valid_path(value, allow_filename_only=True):
    """
    使用Python内置函数判断是否为合法路径
    """
    if not isinstance(value, str):
        return False
    # 拒绝 URL
    if value.lower().startswith(('http://', 'https://', 'ftp://', 'file://', 'mailto:', 'javascript:')):
        return False
    # 拒绝特殊标记
    if value.startswith('<') and value.endswith('>'):
        return False
    try:
        # 使用Pathlib来验证路径
        path = Path(value)

        # 检查路径是否包含非法字符（Windows特定）
        if os.name == 'nt':  # Windows系统
            illegal_chars = '<>:"|?*'
            if any(char in value for char in illegal_chars):
                return False

        # 如果允许文件名且不是绝对路径，则认为是有效的
        if allow_filename_only and not path.is_absolute():
            is_valid = len(value) <= 255
            # 记录验证结果
            if not is_valid:
                logger.debug(f"路径验证失败（文件名太长）: {repr(value)}")
            return is_valid

        # 对于绝对路径，检查基本格式
        if path.is_absolute():
            return True

        # 尝试规范化路径，看是否有效
        normalized = path.resolve()
        is_valid = str(normalized) != '/'
        # 记录验证结果
        if not is_valid:
            logger.debug(f"路径验证失败（规范化后无效）: {repr(value)}")
        return is_valid

    except Exception as e:
        logger.debug(f"路径验证异常: {repr(value)}, 错误: {e}")
        return False


def clean_filter_string(filter_str):
    """
    清理过滤条件字符串：
      - 移除 '组织机构 = "..."'
      - 移除 '数据源 = "..."'
    返回清理后的字符串（保留其他所有内容，包括 rlike）
    """
    if not isinstance(filter_str, str):
        return filter_str

    cleaned = re.sub(r'\s*组织机构\s*=\s*"[^"]*"', '', filter_str)
    cleaned = re.sub(r'\s*数据源\s*=\s*"[^"]*"', '', cleaned)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned


def call_ollama_model(input_text, task_id):
    task_logger = get_task_logger(task_id)
    task_logger.debug({"event": "ollama_input", "input": input_text})

    success, result_text, metadata = ollama_client.call_model(
        prompt=input_text,
        system_prompt=SYSTEM_PROMPT,
        temperature=0.0,
        num_predict=500,
        task_id=task_id
    )

    if not success:
        error_msg = metadata.get('error', 'Unknown error')
        task_logger.warning({"event": "ollama_call_failed", "error": error_msg})
        return [{
            "序号": int(task_id.split('_')[1]),
            "输入内容": input_text,
            "原始路径": clean_excel_string(f"<调用失败: {error_msg}>"),
            "文件名": clean_excel_string("<无文件名>"),
            "类型": "未知",
            "应用名称": clean_excel_string("<无>")
        }]

    # --- 🔧 新增：增强 JSON 清洗逻辑 ---
    cleaned_text = result_text.strip()
    
    # 记录原始响应和清洗前的文本
    task_logger.debug({"event": "raw_model_response", "response": result_text})
    
    # 1. 移除开头的 "json" 或 "```json" 等标记
    cleaned_text = re.sub(r'^```json\s*', '', cleaned_text)
    cleaned_text = re.sub(r'^```\s*json\s*', '', cleaned_text)
    cleaned_text = re.sub(r'^json\s*', '', cleaned_text, flags=re.IGNORECASE)
    
    # 2. 移除结尾的 "```"
    cleaned_text = re.sub(r'\s*```$', '', cleaned_text)
    
    # 3. 找到 JSON 开始位置（第一个 '{' 或 '['）
    start_brace = cleaned_text.find('{')
    start_bracket = cleaned_text.find('[')
    start_pos = min(start_brace if start_brace != -1 else float('inf'), start_bracket if start_bracket != -1 else float('inf'))
    if start_pos != float('inf'):
        cleaned_text = cleaned_text[int(start_pos):]
    else:
        # 如果找不到开始符号，尝试从第一个字母开始找对象或数组
        cleaned_text = cleaned_text.lstrip()
    
    # 4. 尝试从后往前找到结束符号，确保 JSON 完整
    # （防止 Ollama 截断响应）
    last_brace = cleaned_text.rfind('}')
    last_bracket = cleaned_text.rfind(']')
    end_pos = max(last_brace, last_bracket)
    if end_pos != -1:
        cleaned_text = cleaned_text[:end_pos + 1]
    
    cleaned_text = cleaned_text.strip()
    
    # 记录清洗后的文本
    task_logger.debug({"event": "cleaned_model_response", "response": cleaned_text})
    
    # --- END JSON 清洗 ---

    try:
        data = json.loads(cleaned_text)
    except json.JSONDecodeError as e:
        # 如果还是失败，记录更详细的调试信息
        task_logger.warning({
            "event": "json_parse_failed",
            "error": str(e),
            "cleaned_response": repr(cleaned_text),
            "original_response_snippet": result_text[:500]  # 记录更多上下文
        })
        return [{
            "序号": int(task_id.split('_')[1]),
            "输入内容": input_text,
            "原始路径": clean_excel_string(f"<JSON解析失败: {str(e)[:100]}>"),
            "文件名": clean_excel_string("<无文件名>"),
            "类型": "未知",
            "应用名称": clean_excel_string("<无>")
        }]

    final_outputs = []
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                path = clean_excel_string(item.get("path", "<无路径>"))
                filename = clean_excel_string(item.get("filename", "<无文件名>"))
                typ = clean_excel_string(item.get("type", "未知"))
                app = clean_excel_string(item.get("app", "<无>"))
                
                # 记录每个提取的路径信息
                task_logger.debug({
                    "event": "extracted_path", 
                    "path": path, 
                    "filename": filename, 
                    "type": typ, 
                    "app": app
                })
                
                final_outputs.append({
                    "序号": int(task_id.split('_')[1]),
                    "输入内容": input_text,
                    "原始路径": path,
                    "文件名": filename,
                    "类型": typ,
                    "应用名称": app
                })
    elif isinstance(data, dict):
        path = clean_excel_string(data.get("path", "<无路径>"))
        filename = clean_excel_string(data.get("filename", "<无文件名>"))
        typ = clean_excel_string(data.get("type", "未知"))
        app = clean_excel_string(data.get("app", "<无>"))
        
        # 记录提取的路径信息
        task_logger.debug({
            "event": "extracted_path", 
            "path": path, 
            "filename": filename, 
            "type": typ, 
            "app": app
        })
        
        final_outputs.append({
            "序号": int(task_id.split('_')[1]),
            "输入内容": input_text,
            "原始路径": path,
            "文件名": filename,
            "类型": typ,
            "应用名称": app
        })

    task_logger.debug({"event": "ollama_processed", "count": len(final_outputs)})
    return final_outputs


def process_row(row, idx):
    original_index = idx + 1
    row_dict = row.to_dict()

    input_text = ""

    # 优先使用 "过滤条件" 列（如果存在且非空）
    if "过滤条件" in row_dict and pd.notna(row_dict["过滤条件"]):
        raw_filter = str(row_dict["过滤条件"]).strip()
        if raw_filter:
            input_text = clean_filter_string(raw_filter)
            logger.debug(f"清理后的过滤条件 (序号{original_index}): {repr(input_text)}")

    # 如果没有过滤条件，或清理后为空，则拼接整行（跳过组织机构和数据源）
    if not input_text.strip():
        parts = []
        # 从配置中读取需要忽略的列，如果配置为空则不忽略任何列
        ignored_columns = config.get("processing", {}).get("ignored_columns", [])
        # 如果ignored_columns为None，将其设置为空列表
        if ignored_columns is None:
            ignored_columns = []
        for col, val in row_dict.items():
            if col in ignored_columns:
                continue
            if pd.notna(val) and str(val).strip():
                parts.append(f"{col} = {str(val).strip()}")
        input_text = " ; ".join(parts)
        logger.debug(f"使用整行拼接 (序号{original_index}): {repr(input_text)}")

    # 如果最终 input_text 仍为空，则标记为无路径
    if not input_text.strip():
        return {
            "type": "no_path_found",
            "row": row_dict
        }

    # 直接将清理后的内容交给 Ollama
    desc = "请从以下安全告警内容中提取所有可疑程序路径、文件名，并分类输出：\n" + input_text
    parsed_results = call_ollama_model(desc, f"task_{original_index}")

    return {"type": "processed", "outputs": parsed_results}

