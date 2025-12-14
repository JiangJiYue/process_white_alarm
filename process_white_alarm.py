import sys
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
LOG_FILE = LOG_FILE_TEMPLATE.replace("{log_dir}", LOG_DIR).replace("{timestamp}", timestamp)

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
file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
file_handler.setFormatter(formatter)
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

root_logger.addHandler(file_handler)
root_logger.addHandler(console_handler)

logger = logging.getLogger(__name__)

# 任务日志工厂
def task_logger_factory(task_id):
    return LoggerAdapter(logger, {'task_id': task_id})

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
            return len(value) <= 255

        # 对于绝对路径，检查基本格式
        if path.is_absolute():
            return True

        # 尝试规范化路径，看是否有效
        normalized = path.resolve()
        return str(normalized) != '/'

    except Exception:
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
    task_logger = task_logger_factory(task_id)
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

    if not isinstance(data, list):
        data = [data]

    results = []
    for item in data[:10]:
        if not isinstance(item, dict):
            path, filename, typ, app = "<非对象元素>", "<无文件名>", "未知", "<无>"
        else:
            raw_path = item.get("path", "<缺失path>")
            raw_filename = item.get("filename", "<无文件名>")
            raw_type = item.get("type", "未知")
            raw_app = item.get("app", "<无>")
            path = clean_excel_string(str(raw_path)).strip()
            filename = clean_excel_string(str(raw_filename)).strip()
            typ = clean_excel_string(str(raw_type)).strip()
            app = clean_excel_string(str(raw_app)).strip()

        results.append({
            "序号": int(task_id.split('_')[1]),
            "输入内容": input_text,
            "原始路径": path,
            "文件名": filename,
            "类型": typ,
            "应用名称": app
        })

    if not results:
        results.append({
            "序号": int(task_id.split('_')[1]),
            "输入内容": input_text,
            "原始路径": clean_excel_string("<无法确定路径>"),
            "文件名": clean_excel_string("<无文件名>"),
            "类型": "未知",
            "应用名称": clean_excel_string("<无>")
        })

    return results


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
        for col, val in row_dict.items():
            if col in ["组织机构", "数据源"]:
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


# ================== 主函数 ==================
def main():
    logger.info("🧪 测试 Ollama 连接...")
    if not test_ollama_connection(ollama_client):
        logger.error("❌ Ollama 连接测试失败，程序退出")
        return

    logger.info("🚀 开始处理 Excel 文件: %s", INPUT_FILE)

    # 注意：我们不再需要删除旧文件，因为每次运行都会创建一个新的带有时间戳的输出目录
    logger.info("📂 输出目录: %s", OUTPUT_DIR)

    if not os.path.exists(INPUT_FILE):
        logger.error("❌ 输入文件不存在: %s", INPUT_FILE)
        raise FileNotFoundError(f"文件 {INPUT_FILE} 不存在")

    df = pd.read_excel(INPUT_FILE)
    total_rows = len(df)
    logger.info("📊 成功加载 Excel，共 %d 行", total_rows)

    if MAX_ROWS_TO_PROCESS is not None and isinstance(MAX_ROWS_TO_PROCESS, int) and MAX_ROWS_TO_PROCESS > 0:
        df = df.head(MAX_ROWS_TO_PROCESS)
        logger.info("✂️ 仅处理前 %d 行（由 MAX_ROWS_TO_PROCESS 配置）", len(df))

    invalid_records = []
    valid_results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_row, row, idx): idx
            for idx, row in df.iterrows()
        }

        for future in as_completed(futures):
            try:
                result = future.result()
                idx_in_df = futures[future]
                original_index = idx_in_df + 1

                if result["type"] == "no_path_found":
                    invalid_records.append({
                        "序号": original_index,
                        "原始路径": "<原始行未提取到任何路径>",
                        "文件名": "<无文件名>",
                        "类型": "未知",
                        "应用名称": "<无>",
                        "输入内容": str(result["row"])
                    })
                elif result["type"] == "processed":
                    for output in result["outputs"]:
                        raw_path = output["原始路径"]
                        is_valid = is_valid_path(raw_path, allow_filename_only=True)
                        logger.debug(f"路径验证结果: {repr(raw_path)} -> {is_valid}")
                        if is_valid:
                            valid_results.append(output)
                        else:
                            invalid_records.append(output)
            except Exception as e:
                logger.error(f"🔥 处理某行时发生未预期异常: {e}", exc_info=True)

    if invalid_records:
        invalid_df = pd.DataFrame(invalid_records)
        cols = ["序号", "原始路径", "文件名", "类型", "应用名称", "输入内容"]
        for col in cols:
            if col not in invalid_df.columns:
                invalid_df[col] = ""
        invalid_df = invalid_df[cols]
        invalid_df.to_excel(INVALID_OUTPUT_FILE, index=False)
        logger.info("💾 已保存 %d 条无效记录到 %s", len(invalid_df), INVALID_OUTPUT_FILE)

    if valid_results:
        result_df = pd.DataFrame(valid_results)
        result_df.sort_values("序号", inplace=True, ignore_index=True)
        result_df.to_excel(RESULT_OUTPUT_FILE, index=False)
        logger.info("✅ 处理完成！共生成 %d 条有效路径结果，已保存到 %s", len(result_df), RESULT_OUTPUT_FILE)
    else:
        logger.warning("⚠️ 未生成任何有效路径结果")

    logger.info("📄 详细日志请查看: %s", LOG_FILE)
    logger.info("📁 有效结果保存在: %s", RESULT_OUTPUT_FILE)
    logger.info("📁 无效记录保存在: %s", INVALID_OUTPUT_FILE)


if __name__ == "__main__":
    main()