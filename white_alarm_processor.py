import pandas as pd
import re
import logging
import os
import json
from pathlib import Path

from ollama_client import OllamaClient, create_ollama_client_from_config


class WhiteAlarmProcessor:
    """
    安全告警路径提取处理器
    负责处理安全告警数据，从中提取程序路径信息
    """
    
    def __init__(self, config_manager, logger=None):
        """
        初始化处理器
        
        Args:
            config_manager: 配置管理器实例
            logger: 日志记录器
        """
        self.config_manager = config_manager
        self.logger = logger or logging.getLogger(__name__)
        self.ollama_client = None
        
        # 从配置中获取必要参数
        self.system_prompt = config_manager.get("system_prompt", "").rstrip()
        self.ollama_config = config_manager.get_ollama_config()
        
    def _get_ollama_client(self):
        """
        获取Ollama客户端实例
        
        Returns:
            OllamaClient: Ollama客户端实例
        """
        if self.ollama_client is None:
            self.ollama_client = create_ollama_client_from_config(
                {"ollama": self.ollama_config}, 
                logger=self.logger
            )
        return self.ollama_client
        
    def _call_ollama_model(self, input_text, row_number, task_logger):
        """
        调用Ollama模型处理输入文本
        
        Args:
            input_text (str): 输入文本
            row_number (int): 行号
            task_logger: 任务日志记录器
            
        Returns:
            list: 处理结果列表
        """
        task_logger.debug({"event": "ollama_input", "input": input_text})

        # 使用Ollama客户端
        client = self._get_ollama_client()
        success, result_text, metadata = client.call_model(
            prompt=input_text,
            system_prompt=self.system_prompt,
            temperature=0.0,
            num_predict=self.ollama_config.get('num_predict', 500),
            task_id=f"task_{row_number}"
        )

        if not success:
            error_msg = metadata.get('error', 'Unknown error')
            task_logger.warning({"event": "ollama_call_failed", "error": error_msg})
            return [{
                "序号": row_number,
                "输入内容": input_text,
                "原始路径": self._clean_excel_string(f"<调用失败: {error_msg}>"),
                "文件名": self._clean_excel_string("<无文件名>"),
                "类型": "未知",
                "应用名称": self._clean_excel_string("<无>")
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
        start_pos = min(start_brace if start_brace != -1 else float('inf'), 
                       start_bracket if start_bracket != -1 else float('inf'))
        if start_pos != float('inf'):
            cleaned_text = cleaned_text[int(start_pos):]
        else:
            # 如果找不到开始符号，尝试从第一个字母开始找对象或数组
            cleaned_text = cleaned_text.lstrip()
        
        # 4. 确保 JSON 完整性
        # 检查开头是否为合法的JSON开始符
        if cleaned_text.startswith(('{', '[')):
            # 尝试从后往前找到结束符号
            last_brace = cleaned_text.rfind('}')
            last_bracket = cleaned_text.rfind(']')
            end_pos = max(last_brace, last_bracket)
            
            # 确保结束符存在且位置合理
            if end_pos != -1 and end_pos > 0:
                # 根据开头符号确定应该查找的结束符号
                if cleaned_text.startswith('{') and cleaned_text[end_pos] == '}':
                    cleaned_text = cleaned_text[:end_pos + 1]
                elif cleaned_text.startswith('[') and cleaned_text[end_pos] == ']':
                    cleaned_text = cleaned_text[:end_pos + 1]
        
        cleaned_text = cleaned_text.strip()
        
        # 记录清洗后的文本
        task_logger.debug({"event": "cleaned_model_response", "response": cleaned_text})
        
        # --- END JSON 清洗 ---

        # 增强的JSON解析逻辑
        try:
            # 在解析前检查基本完整性
            if not cleaned_text:
                raise ValueError("Cleaned response is empty")
            
            # 检查是否以合法的JSON开始和结束字符开头和结尾
            if not (cleaned_text.startswith(('{', '[')) and cleaned_text.endswith(('}', ']'))):
                raise ValueError("Response doesn't start/end with valid JSON delimiters")
            
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
                "原始路径": self._clean_excel_string(f"<JSON解析失败: {str(e)[:100]}>"),
                "文件名": self._clean_excel_string("<无文件名>"),
                "类型": "未知",
                "应用名称": self._clean_excel_string("<无>")
            }]
        except ValueError as e:
            # 处理自定义验证错误
            task_logger.warning({
                "event": "json_validation_failed",
                "error": str(e),
                "cleaned_response": repr(cleaned_text),
                "original_response_snippet": result_text[:500]
            })
            return [{
                "序号": row_number,
                "输入内容": input_text,
                "原始路径": self._clean_excel_string(f"<JSON验证失败: {str(e)[:100]}>"),
                "文件名": self._clean_excel_string("<无文件名>"),
                "类型": "未知",
                "应用名称": self._clean_excel_string("<无>")
            }]

        final_outputs = []
        if isinstance(data, list):
            for i, item in enumerate(data, 1):  # 从1开始编号
                if isinstance(item, dict):
                    path = self._clean_excel_string(item.get("path", "<无路径>"))
                    filename = self._clean_excel_string(item.get("filename", "<无文件名>"))
                    typ = self._clean_excel_string(item.get("type", "未知"))
                    app = self._clean_excel_string(item.get("app", "<无>"))
                    
                    final_outputs.append({
                        "序号": row_number,
                        "输入内容": input_text,
                        "原始路径": path,
                        "文件名": filename,
                        "类型": typ,
                        "应用名称": app
                    })
        elif isinstance(data, dict):
            path = self._clean_excel_string(data.get("path", "<无路径>"))
            filename = self._clean_excel_string(data.get("filename", "<无文件名>"))
            typ = self._clean_excel_string(data.get("type", "未知"))
            app = self._clean_excel_string(data.get("app", "<无>"))
            
            final_outputs.append({
                "序号": row_number,
                "输入内容": input_text,
                "原始路径": path,
                "文件名": filename,
                "类型": typ,
                "应用名称": app
            })

        return final_outputs
        
    def process_row(self, row, idx, selected_columns=None, ignored_columns=None, task_logger=None):
        """
        处理单行数据
        
        Args:
            row: 数据行
            idx (int): 行索引
            selected_columns (list): 选中的列
            ignored_columns (list): 忽略的列
            task_logger: 任务日志记录器
            
        Returns:
            dict: 处理结果
        """
        original_index = idx + 1
        row_dict = row.to_dict()

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
                        filtered_val = self._filter_ignored_keys_from_filter_condition(str(val).strip(), ignored_columns)
                    
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
        parsed_results = self._call_ollama_model(desc, original_index, task_logger)
        
        task_logger.debug(f"Ollama 处理完成，返回结果数: {len(parsed_results)}")

        return {"type": "processed", "outputs": parsed_results}
        
    def is_valid_path(self, value, allow_filename_only=True):
        """
        使用Python内置函数判断是否为合法路径
        
        Args:
            value: 待检查的值
            allow_filename_only (bool): 是否允许仅文件名
            
        Returns:
            bool: 是否为有效路径
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
                    self.logger.debug(f"路径验证失败（文件名太长）: {repr(value)}")
                return is_valid

            # 对于绝对路径，检查基本格式
            if path.is_absolute():
                return True

            # 尝试规范化路径，看是否有效
            normalized = path.resolve()
            is_valid = str(normalized) != '/'
            # 记录验证结果
            if not is_valid:
                self.logger.debug(f"路径验证失败（规范化后无效）: {repr(value)}")
            return is_valid

        except Exception as e:
            self.logger.debug(f"路径验证异常: {repr(value)}, 错误: {e}")
            return False
            
    def _clean_excel_string(self, value):
        """
        清理字符串，使其适合写入Excel
        
        Args:
            value: 待清理的值
            
        Returns:
            清理后的字符串
        """
        if isinstance(value, str):
            return value.replace("\n", " ").replace("\r", " ").replace("\t", " ")
        return value
        
    def _filter_ignored_keys_from_filter_condition(self, filter_str, ignored_columns):
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