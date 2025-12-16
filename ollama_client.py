import httpx
import time
import re
import logging
import contextvars
from typing import List, Dict, Any, Tuple


class OllamaClient:
    """
    Ollama API 客户端封装类
    提供统一的接口用于调用 Ollama 模型服务
    """

    def __init__(self, url: str, model_name: str, timeout_seconds: int = 30, max_retries: int = 3, logger=None):
        """
        初始化 Ollama 客户端

        Args:
            url: Ollama 服务地址
            model_name: 模型名称
            timeout_seconds: 请求超时时间
            max_retries: 最大重试次数
            logger: 日志记录器
        """
        self.url = url
        self.model_name = model_name
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.logger = logger or logging.getLogger(__name__)
        # 获取行号上下文变量（如果已在其他地方定义）
        try:
            self.row_context_var = contextvars.ContextVar('row_number')
        except LookupError:
            # 如果上下文变量未定义，将在使用时动态获取
            self.row_context_var = None
            
        # 添加format属性，默认为空
        self.format = ""

    def clean_model_output(self, text: str) -> str:
        """
        清理模型输出，移除不必要的标记
        """
        text = re.sub(r'</?think>', '', text, flags=re.IGNORECASE)
        text = re.sub(r'/reason\b.*', '', text, flags=re.IGNORECASE)
        text = re.sub(r'<\|im_[a-z]+\|>', '', text)
        text = text.replace('``', '').replace('`', '')
        return text.strip()

    def _extract_row_number(self, task_id: str = None) -> int:
        """
        提取行号上下文

        Args:
            task_id: 任务ID

        Returns:
            行号,如果无法获取则返回None
        """
        # 尝试从上下文变量获取
        if hasattr(self, 'row_context_var') and self.row_context_var:
            try:
                return self.row_context_var.get()
            except LookupError:
                pass

        # 从task_id中提取行号(向后兼容)
        if task_id and task_id.startswith('task_'):
            try:
                return int(task_id.split('_')[1])
            except (IndexError, ValueError):
                pass

        return None

    def call_model(self, prompt: str, system_prompt: str = "", temperature: float = 0.0, num_predict: int = 250, task_id: str = None) -> \
    Tuple[bool, str, Dict[str, Any]]:
        """
        调用 Ollama 模型

        Args:
            prompt: 用户输入提示
            system_prompt: 系统提示词
            temperature: 温度参数
            num_predict: 最大预测token数
            task_id: 任务ID

        Returns:
            (success: bool, response: str, metadata: dict)
        """
        full_prompt = f"{system_prompt}\n{prompt}" if system_prompt else prompt

        payload = {
            "model": self.model_name,
            "prompt": full_prompt,
            "stream": False,
            "options": {
                "temperature": temperature,
                "num_predict": num_predict
            }
        }
        
        # 如果配置了format参数，则添加到payload中
        if hasattr(self, 'format') and self.format:
            payload["format"] = self.format

        # 记录开始时间
        start_time = time.time()
        
        for attempt in range(self.max_retries + 1):
            try:
                # 获取行号上下文
                row_number = self._extract_row_number(task_id)
                extra_data = {'row_number': row_number} if row_number else {}

                self.logger.info(f"调用 Ollama 模型（第 {attempt + 1} 次尝试）", extra=extra_data)

                with httpx.Client(timeout=self.timeout_seconds) as client:
                    response = client.post(self.url, json=payload)
                    response.raise_for_status()

                    raw_response = response.json()
                    result_text = raw_response.get("response", "").strip()

                    self.logger.debug(f"[原始模型响应]: {repr(result_text)}", extra=extra_data)
                    cleaned_text = self.clean_model_output(result_text)
                    self.logger.debug(f"[清理后响应]: {repr(cleaned_text)}", extra=extra_data)
                    
                    # 计算耗时
                    elapsed_time = (time.time() - start_time)  # 转换为秒

                    metadata = {
                        "attempt_count": attempt + 1,
                        "success": True,
                        "error": None,
                        "elapsed_time_s": elapsed_time
                    }

                    self.logger.info(f"模型调用成功，耗时: {elapsed_time:.2f}s", extra=extra_data)
                    return True, cleaned_text, metadata

            except httpx.TimeoutException as e:
                # 获取行号上下文
                row_number = self._extract_row_number(task_id)
                extra_data = {'row_number': row_number} if row_number else {}

                self.logger.warning(f"⏱️ Ollama 超时 (尝试 {attempt + 1}/{self.max_retries + 1}): {e}", extra=extra_data, exc_info=True)
                if attempt < self.max_retries:
                    time.sleep(5 * (attempt + 1))
                else:
                    metadata = {
                        "attempt_count": self.max_retries + 1,
                        "success": False,
                        "error": f"Timeout after {self.max_retries + 1} attempts: {str(e)}"
                    }
                    self.logger.error(f"Ollama 调用最终超时: {metadata['error']}", extra=extra_data)
                    return False, "", metadata

            except Exception as e:
                # 获取行号上下文
                row_number = self._extract_row_number(task_id)
                extra_data = {'row_number': row_number} if row_number else {}

                self.logger.error(f"💥 调用异常: {e}", extra=extra_data, exc_info=True)
                metadata = {
                    "attempt_count": attempt + 1,
                    "success": False,
                    "error": str(e)
                }
                return False, "", metadata


def create_ollama_client_from_config(config: Dict[str, Any], logger=None) -> OllamaClient:
    """
    从配置字典创建 Ollama 客户端实例

    Args:
        config: 包含 ollama 配置的字典
        logger: 日志记录器

    Returns:
        OllamaClient 实例
    """
    ollama_config = config["ollama"]
    client = OllamaClient(
        url=ollama_config["url"],
        model_name=ollama_config["model_name"],
        timeout_seconds=ollama_config.get("timeout_seconds", 30),
        max_retries=ollama_config.get("max_retries", 3),
        logger=logger
    )
    
    # 设置format参数
    if "format" in ollama_config:
        client.format = ollama_config["format"]

    return client