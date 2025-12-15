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
import contextvars

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
# 注意：standalone模式已被弃用，不会再生成standalone_*.log文件
# 所有日志现在都在Web应用中通过任务ID进行管理

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

# 移除旧的根日志记录器配置，避免生成standalone日志文件
# 所有日志现在都在Web应用中通过任务ID进行管理
logger = logging.getLogger(__name__)

# 任务日志工厂
def task_logger_factory(row_number):
    # 设置上下文变量
    row_context_var.set(row_number)
    return LoggerAdapter(logger, {'row_number': row_number})

# 全局变量，用于存储任务日志记录器工厂函数
_task_logger_factory = None
_logger = None

def set_task_logger_factory(factory):
    """设置任务日志记录器工厂函数"""
    global _task_logger_factory
    _task_logger_factory = factory

def set_logger(logger):
    """设置全局日志记录器"""
    global _logger
    _logger = logger

def get_task_logger(row_number):
    """获取任务日志记录器"""
    if _task_logger_factory:
        return _task_logger_factory(row_number)
    else:
        # 如果没有设置工厂函数，返回一个空的日志记录器
        return logging.getLogger("dummy")

# 初始化 Ollama 客户端，传递logger
# ollama_client = create_ollama_client_from_config(config)

# 延迟初始化 Ollama 客户端，使用传递的日志记录器
def get_ollama_client():
    global ollama_client
    if 'ollama_client' not in globals() or ollama_client is None:
        if _logger:
            ollama_client = create_ollama_client_from_config(config, logger=_logger)
        else:
            ollama_client = create_ollama_client_from_config(config)
    return ollama_client

# 修改 call_ollama_model 函数以使用延迟初始化的 Ollama 客户端
def call_ollama_model(input_text, row_number):
    task_logger = get_task_logger(row_number)
    task_logger.debug({"event": "ollama_input", "input": input_text})

    # 使用延迟初始化的 Ollama 客户端
    client = get_ollama_client()
    success, result_text, metadata = client.call_model(
        prompt=input_text,
        system_prompt=SYSTEM_PROMPT,
        temperature=0.0,
        num_predict=500,
        task_id=f"task_{row_number}"  # 为了兼容旧接口
    )

    if not success:
        error_msg = metadata.get('error', 'Unknown error')
        task_logger.warning({"event": "ollama_call_failed", "error": error_msg})
        return [{
            "序号": row_number,
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
            "序号": row_number,
            "输入内容": input_text,
            "原始路径": clean_excel_string(f"<JSON解析失败: {str(e)[:100]}>"),
            "文件名": clean_excel_string("<无文件名>"),
            "类型": "未知",
            "应用名称": clean_excel_string("<无>")
        }]

    final_outputs = []
    if isinstance(data, list):
        for i, item in enumerate(data, 1):  # 从1开始编号
            if isinstance(item, dict):
                path = clean_excel_string(item.get("path", "<无路径>"))
                filename = clean_excel_string(item.get("filename", "<无文件名>"))
                typ = clean_excel_string(item.get("type", "未知"))
                app = clean_excel_string(item.get("app", "<无>"))
                
                # 记录每个提取的路径信息，使用ollamaN格式
                # task_logger.debug({
                #     "event": "extracted_path", 
                #     "path": path, 
                #     "filename": filename, 
                #     "type": typ, 
                #     "app": app,
                #     "ollama_id": f"ollama{i}"
                # })
                
                final_outputs.append({
                    "序号": row_number,
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
        
        # 记录提取的路径信息，使用ollama1格式（单个结果）
        # task_logger.debug({
        #     "event": "extracted_path", 
        #     "path": path, 
        #     "filename": filename, 
        #     "type": typ, 
        #     "app": app,
        #     "ollama_id": "ollama1"
        # })
        
        final_outputs.append({
            "序号": row_number,
            "输入内容": input_text,
            "原始路径": path,
            "文件名": filename,
            "类型": typ,
            "应用名称": app
        })

    # task_logger.debug({"event": "ollama_processed", "count": len(final_outputs)})
    return final_outputs


def parse_filter_conditions(filter_str):
    """
    解析过滤条件字符串中的键值对
    例如: '数据源 = "EDR" and 命令行 = "powershell -enc ..."' 
    返回: {'数据源': 'EDR', '命令行': 'powershell -enc ...'}
    """
    if not isinstance(filter_str, str):
        return {}
    
    # 先按 "and" 分割各个条件
    conditions = re.split(r'\s+and\s+', filter_str)
    
    result = {}
    # 为每个条件匹配键值对
    for condition in conditions:
        pattern = r'([^=]+?)\s*=\s*("[^"]*"|\'[^\']*\'|\S+)'
        match = re.match(pattern, condition)
        if match:
            key, value = match.groups()
            # 清理键和值
            clean_key = key.strip()
            clean_value = value.strip().strip('"\'')
            result[clean_key] = clean_value
    
    return result

def process_row(row, idx, selected_columns=None, ignored_columns=None):
    original_index = idx + 1
    row_dict = row.to_dict()

    # 获取任务特定的日志记录器
    task_logger = get_task_logger(original_index)

    input_text = ""

    # 如果用户指定了选择的列，则使用用户指定的列
    if selected_columns is not None and len(selected_columns) > 0:
        parts = []
        # 只使用用户选定的列
        for col in selected_columns:
            val = row_dict.get(col)
            if pd.notna(val) and str(val).strip():
                # 检查是否整个列被忽略
                if col in ignored_columns:
                    task_logger.debug(f"[task_{original_index}] 列 '{col}' 被用户忽略，跳过处理")
                    continue
                # 处理所有列
                # 如果有需要忽略的键值对，尝试解析并过滤
                filtered_val = str(val).strip()
                if ignored_columns:
                    # 尝试解析当前列是否为键值对格式，如果是则过滤
                    filtered_val = filter_ignored_keys_from_filter_condition(str(val).strip(), ignored_columns)
                
                # 如果过滤后还有内容则添加
                if filtered_val:
                    parts.append(filtered_val)
                    # task_logger.debug(f"添加列 '{col}' 的内容: {repr(filtered_val)}")
                else:
                    task_logger.debug(f"列 '{col}' 过滤后无内容，跳过添加")
        
        input_text = " ; ".join(parts)
        task_logger.debug(f"最终拼接的输入文本: {repr(input_text)}")
    else:
        # 如果用户没有在页面上选择特定的列，则返回错误提示，要求用户至少选择一列
        task_logger.debug(f"用户未选择任何列，无法处理")
        return {
            "type": "no_path_found",
            "row": row_dict,
            "error": "用户未选择任何列，请至少选择一列进行处理"
        }
        
    # 直接将清理后的内容交给 Ollama
    desc = "请从以下安全告警内容中提取所有程序路径、文件名，并分类输出：\n" + input_text
    # task_logger.debug(f"发送请求到 Ollama: {repr(desc)}")
    parsed_results = call_ollama_model(desc, original_index)
    
    task_logger.debug(f"Ollama 处理完成，返回结果数: {len(parsed_results)}")

    return {"type": "processed", "outputs": parsed_results}


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
    清理过滤条件字符串，移除多余的空格和换行符
    """
    return filter_str.strip()


def clean_excel_string(value):
    """
    清理字符串，使其适合写入Excel
    """
    if isinstance(value, str):
        return value.replace("\n", " ").replace("\r", " ").replace("\t", " ")
    return value


def filter_ignored_keys_from_filter_condition(filter_str, ignored_columns):
    """
    从过滤条件字符串中移除被忽略的键值对
    
    Args:
        filter_str (str): 原始过滤条件字符串
        ignored_columns (list): 要忽略的列名列表
        
    Returns:
        str: 过滤后的字符串，如果全部被过滤则返回空字符串
    """
    if not isinstance(filter_str, str) or not ignored_columns:
        return filter_str
    
    # 先按 "and" 分割各个条件
    conditions = re.split(r'\s+and\s+', filter_str)
    
    # 存储未被忽略的条件
    remaining_conditions = []
    
    # 检查每个条件
    for condition in conditions:
        # 匹配 "key = value" 格式
        pattern = r'^([^=]+?)\s*=\s*("[^"]*"|\'[^\']*\'|\S+)'
        match = re.match(pattern, condition.strip())
        if match:
            key = match.group(1).strip()
            # 如果键不在忽略列表中，则保留该条件
            if key not in ignored_columns:
                remaining_conditions.append(condition)
        else:
            # 如果不匹配key=value格式，保留原样
            remaining_conditions.append(condition)
    
    # 重新组合条件
    return " and ".join(remaining_conditions) if remaining_conditions else ""
