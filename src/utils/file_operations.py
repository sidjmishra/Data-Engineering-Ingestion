"""
File Operations Utility Module
"""

import os
import shutil
import hashlib
from pathlib import Path
from datetime import datetime
from typing import Tuple
from src.utils.logger import LoggerConfig


logger = LoggerConfig.get_logger()


class FileOperations:
    """Utility functions for file operations"""
    
    @staticmethod
    def compute_file_hash(file_path: str, algorithm: str = 'md5') -> str:
        """
        Compute hash of a file for deduplication
        
        Args:
            file_path: Path to the file
            algorithm: Hash algorithm ('md5', 'sha256')
            
        Returns:
            Hash string
        """
        try:
            hasher = hashlib.new(algorithm)
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b''):
                    hasher.update(chunk)
            return hasher.hexdigest()
        except Exception as e:
            logger.error(f"Error computing file hash for {file_path}: {str(e)}")
            raise
    
    @staticmethod
    def detect_file_type(file_path: str) -> str:
        """
        Detect file type by extension
        
        Args:
            file_path: Path to the file
            
        Returns:
            File type: 'csv', 'image', 'video', or 'unknown'
        """
        ext = Path(file_path).suffix.lower()
        
        # CSV extensions
        if ext in ['.csv']:
            return 'csv'
        
        # Image extensions
        elif ext in ['.jpg', '.jpeg', '.png', '.bmp', '.gif', '.tiff']:
            return 'image'
        
        # Video extensions
        elif ext in ['.mp4', '.avi', '.mov', '.mkv', '.flv', '.wmv', '.webm']:
            return 'video'
        
        else:
            return 'unknown'
    
    @staticmethod
    def get_file_size(file_path: str) -> int:
        """
        Get file size in bytes
        
        Args:
            file_path: Path to the file
            
        Returns:
            File size in bytes
        """
        try:
            return os.path.getsize(file_path)
        except Exception as e:
            logger.error(f"Error getting file size for {file_path}: {str(e)}")
            raise
    
    @staticmethod
    def move_file(source_path: str, destination_path: str, create_dirs: bool = True) -> bool:
        """
        Move file from source to destination
        
        Args:
            source_path: Source file path
            destination_path: Destination file path
            create_dirs: Create destination directories if they don't exist
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if create_dirs:
                os.makedirs(os.path.dirname(destination_path), exist_ok=True)
            
            shutil.move(source_path, destination_path)
            logger.info(f"File moved from {source_path} to {destination_path}")
            return True
        except Exception as e:
            logger.error(f"Error moving file from {source_path} to {destination_path}: {str(e)}")
            return False
    
    @staticmethod
    def copy_file(source_path: str, destination_path: str, create_dirs: bool = True) -> bool:
        """
        Copy file from source to destination
        
        Args:
            source_path: Source file path
            destination_path: Destination file path
            create_dirs: Create destination directories if they don't exist
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if create_dirs:
                os.makedirs(os.path.dirname(destination_path), exist_ok=True)
            
            shutil.copy2(source_path, destination_path)
            logger.info(f"File copied from {source_path} to {destination_path}")
            return True
        except Exception as e:
            logger.error(f"Error copying file from {source_path} to {destination_path}: {str(e)}")
            return False
    
    @staticmethod
    def file_exists(file_path: str) -> bool:
        """Check if file exists"""
        return os.path.exists(file_path) and os.path.isfile(file_path)
    
    @staticmethod
    def get_batch_hour_dir(base_dir: str) -> str:
        """
        Get batch directory with current datetime
        
        Args:
            base_dir: Base directory path
            
        Returns:
            Full path with datetime folder
        """
        now = datetime.now()
        batch_hour = now.strftime("%Y%m%d_%H00")  # e.g., 20240115_1200
        return os.path.join(base_dir, batch_hour)
    
    @staticmethod
    def list_files(directory: str, extensions: list = None) -> list:
        """
        List files in directory, optionally filtered by extension
        
        Args:
            directory: Directory path
            extensions: List of extensions to filter (e.g., ['.csv', '.jpg'])
            
        Returns:
            List of file paths
        """
        files = []
        try:
            if not os.path.exists(directory):
                return files
            
            for file in os.listdir(directory):
                file_path = os.path.join(directory, file)
                if os.path.isfile(file_path):
                    if extensions is None:
                        files.append(file_path)
                    elif Path(file_path).suffix.lower() in extensions:
                        files.append(file_path)
            
            return files
        except Exception as e:
            logger.error(f"Error listing files in {directory}: {str(e)}")
            return files
