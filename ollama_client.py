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

    def clean_model_output(self, text: str) -> str:
        """
        清理模型输出，移除不必要的标记
        """
        text = re.sub(r'</?think>', '', text, flags=re.IGNORECASE)
        text = re.sub(r'/reason\b.*', '', text, flags=re.IGNORECASE)
        text = re.sub(r'<\|im_[a-z]+\|>', '', text)
        text = text.replace('``', '').replace('`', '')
        return text.strip()

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

        # 记录开始时间
        start_time = time.time()
        
        for attempt in range(self.max_retries + 1):
            try:
                # 获取行号上下文（如果可用）
                row_number = None
                if hasattr(self, 'row_context_var') and self.row_context_var:
                    try:
                        row_number = self.row_context_var.get()
                    except LookupError:
                        pass
                elif task_id and task_id.startswith('task_'):
                    # 从task_id中提取行号（向后兼容）
                    try:
                        row_number = int(task_id.split('_')[1])
                    except (IndexError, ValueError):
                        pass
                
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
                # 获取行号上下文（如果可用）
                row_number = None
                if hasattr(self, 'row_context_var') and self.row_context_var:
                    try:
                        row_number = self.row_context_var.get()
                    except LookupError:
                        pass
                elif task_id and task_id.startswith('task_'):
                    # 从task_id中提取行号（向后兼容）
                    try:
                        row_number = int(task_id.split('_')[1])
                    except (IndexError, ValueError):
                        pass
                
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
                # 获取行号上下文（如果可用）
                row_number = None
                if hasattr(self, 'row_context_var') and self.row_context_var:
                    try:
                        row_number = self.row_context_var.get()
                    except LookupError:
                        pass
                elif task_id and task_id.startswith('task_'):
                    # 从task_id中提取行号（向后兼容）
                    try:
                        row_number = int(task_id.split('_')[1])
                    except (IndexError, ValueError):
                        pass
                
                extra_data = {'row_number': row_number} if row_number else {}
                
                self.logger.error(f"💥 调用异常: {e}", extra=extra_data, exc_info=True)
                metadata = {
                    "attempt_count": attempt + 1,
                    "success": False,
                    "error": str(e)
                }
                return False, "", metadata

    def batch_call(self, prompts: List[str], system_prompt: str = "", **kwargs) -> List[
        Tuple[bool, str, Dict[str, Any]]]:
        """
        批量调用模型

        Args:
            prompts: 提示列表
            system_prompt: 系统提示词
            **kwargs: 传递给call_model的其他参数

        Returns:
            结果列表，每个元素为(success, response, metadata)元组
        """
        results = []
        for i, prompt in enumerate(prompts):
            print(f"处理批量请求 {i + 1}/{len(prompts)}")
            result = self.call_model(prompt, system_prompt, **kwargs)
            results.append(result)
        return results


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
    return OllamaClient(
        url=ollama_config["url"],
        model_name=ollama_config["model_name"],
        timeout_seconds=ollama_config.get("timeout_seconds", 30),
        max_retries=ollama_config.get("max_retries", 3),
        logger=logger
    )


def test_ollama_connection(client: OllamaClient) -> bool:
    """
    测试 Ollama 连接是否正常

    Args:
        client: OllamaClient 实例

    Returns:
        连接是否成功
    """
    try:
        success, response, metadata = client.call_model("你好，请简单介绍一下自己。", temperature=0.1, num_predict=50)
        if success and response:
            print(f"✅ Ollama 连接测试成功，模型响应: {response[:50]}...")
            return True
        else:
            print(f"❌ Ollama 连接测试失败: {metadata.get('error', 'Unknown error')}")
            return False
    except Exception as e:
        print(f"❌ Ollama 连接测试异常: {e}")
        return False


# 示例使用方法
if __name__ == "__main__":
    # 示例配置
    sample_config = {
        "ollama": {
            "url": "http://localhost:11434/api/generate",
            "model_name": "qwen2.5:7b",
            "timeout_seconds": 30,
            "max_retries": 3
        }
    }

    # 创建客户端
    client = create_ollama_client_from_config(sample_config)

    # 测试连接
    if test_ollama_connection(client):
        # 单次调用示例
        success, response, metadata = client.call_model(
            prompt="请列举3个常见的编程语言",
            system_prompt="你是一个编程专家",
            temperature=0.1,
            num_predict=100
        )

        if success:
            print(f"模型响应: {response}")
        else:
            print(f"调用失败: {metadata['error']}")

        # 批量调用示例
        prompts = [
            "什么是Python?",
            "JavaScript的主要用途是什么?",
            "Java的特点有哪些?"
        ]

        results = client.batch_call(
            prompts=prompts,
            system_prompt="你是一个编程语言专家",
            temperature=0.2
        )

        for i, (success, response, metadata) in enumerate(results):
            if success:
                print(f"问题{i + 1}响应: {response[:100]}...")
            else:
                print(f"问题{i + 1}失败: {metadata['error']}")
    else:
        print("Ollama 连接测试失败，无法继续执行测试")