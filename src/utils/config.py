"""
Configuration Module
"""

import os
import yaml
from pathlib import Path
from typing import Any, Dict


class ConfigManager:
    """Manage application configuration"""
    
    _instance = None
    _config = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
        return cls._instance
    
    @staticmethod
    def load_config(config_path: str) -> Dict[str, Any]:
        """
        Load configuration from YAML file
        
        Args:
            config_path: Path to config.yaml
            
        Returns:
            Configuration dictionary
        """
        if ConfigManager._config is not None:
            return ConfigManager._config
        
        base_path = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        config_str = yaml.dump(config)
        config_str = config_str.replace('${BASE_PATH}', base_path)
        config = yaml.safe_load(config_str)
        
        ConfigManager._config = config
        return config
    
    @staticmethod
    def get_config() -> Dict[str, Any]:
        """Get loaded configuration"""
        if ConfigManager._config is None:
            raise RuntimeError("Configuration not loaded. Call load_config() first.")
        return ConfigManager._config
    
    @staticmethod
    def get(key_path: str, default: Any = None) -> Any:
        """
        Get configuration value by path
        
        Args:
            key_path: Path to config key (e.g., 'database.mongodb.uri')
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        config = ConfigManager.get_config()
        keys = key_path.split('.')
        value = config
        
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
                if value is None:
                    return default
            else:
                return default
        
        return value if value is not None else default
    
    @staticmethod
    def reset():
        """Reset configuration (useful for testing)"""
        ConfigManager._config = None
