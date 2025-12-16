import yaml
import os
from datetime import datetime


class ConfigManager:
    """
    配置管理器，负责加载、解析和提供应用配置
    """
    def __init__(self, config_file="config.yaml"):
        """
        初始化配置管理器
        
        Args:
            config_file (str): 配置文件路径
        """
        self.config_file = config_file
        self.config = self._load_config()
        
    def _load_config(self):
        """
        加载配置文件
        
        Returns:
            dict: 配置字典
        """
        with open(self.config_file, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
            
    def get(self, key, default=None):
        """
        获取配置项的值
        
        Args:
            key (str): 配置项键名，支持点号分隔的嵌套键名，如 "logging.level"
            default: 默认值
            
        Returns:
            配置项的值或默认值
        """
        keys = key.split('.')
        value = self.config
        try:
            for k in keys:
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default
            
    def get_ollama_config(self):
        """
        获取Ollama配置
        
        Returns:
            dict: Ollama配置
        """
        return self.config.get("ollama", {})
        
    def get_processing_config(self):
        """
        获取处理配置
        
        Returns:
            dict: 处理配置
        """
        return self.config.get("processing", {})
        
    def get_logging_config(self):
        """
        获取日志配置
        
        Returns:
            dict: 日志配置
        """
        return self.config.get("logging", {})
        
    def get_output_dir(self):
        """
        获取输出目录配置
        
        Returns:
            str: 输出目录路径
        """
        return self.config.get("output_dir", "results")


# 全局配置管理器实例
config_manager = ConfigManager()


def get_config():
    """
    获取全局配置管理器实例
    
    Returns:
        ConfigManager: 全局配置管理器实例
    """
    return config_manager