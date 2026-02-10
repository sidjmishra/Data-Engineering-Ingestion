"""
Logging Configuration Module
"""

import logging
import logging.handlers
import os
from pathlib import Path
from datetime import datetime
import yaml


class LoggerConfig:
    """Configure application-wide logging"""
    
    _logger = None
    
    @staticmethod
    def setup_logging(config_path: str, log_dir: str) -> logging.Logger:
        """
        Setup logging based on configuration file
        
        Args:
            config_path: Path to config.yaml
            log_dir: Directory to store log files
            
        Returns:
            Configured logger instance
        """
        # Create log directory if it doesn't exist
        log_path = Path(log_dir)
        log_path.mkdir(parents=True, exist_ok=True)
        
        # Load configuration
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        log_config = config.get('logging', {})
        level = log_config.get('level', 'INFO')
        log_format = log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        # Create logger
        logger = logging.getLogger("FileIngestionPipeline")
        logger.setLevel(getattr(logging, level))
        
        # Clear existing handlers
        logger.handlers.clear()
        
        # Create formatter
        formatter = logging.Formatter(log_format)
        
        # Console Handler
        if log_config.get('console_output', True):
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)
        
        # File Handler with rotation
        if log_config.get('file_output', True):
            log_filename = os.path.join(
                log_dir, 
                f"ingestion_{datetime.now().strftime('%Y%m%d')}.log"
            )
            file_handler = logging.handlers.RotatingFileHandler(
                log_filename,
                maxBytes=10485760,  # 10MB
                backupCount=5
            )
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        
        LoggerConfig._logger = logger
        return logger
    
    @staticmethod
    def get_logger() -> logging.Logger:
        """Get the configured logger instance"""
        if LoggerConfig._logger is None:
            # Fallback to basic logging if not initialized
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            LoggerConfig._logger = logging.getLogger("FileIngestionPipeline")
        return LoggerConfig._logger
