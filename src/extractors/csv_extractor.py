"""
CSV Metadata Extractor
"""

import pandas as pd
import os
from typing import Dict, Any
from src.utils.logger import LoggerConfig
from src.utils.file_operations import FileOperations


logger = LoggerConfig.get_logger()


class CSVExtractor:
    """Extract metadata from CSV files"""
    
    @staticmethod
    def extract_metadata(file_path: str) -> Dict[str, Any]:
        """
        Extract metadata from CSV file
        
        Args:
            file_path: Path to CSV file
            
        Returns:
            Dictionary containing CSV metadata
        """
        try:
            # Check file exists
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"File not found: {file_path}")
            
            # Read CSV
            df = pd.read_csv(file_path, encoding='utf-8')
            
            # Extract basic metadata
            metadata = {
                'file_type': 'csv',
                'file_name': os.path.basename(file_path),
                'source_path': file_path,
                'file_size': FileOperations.get_file_size(file_path),
                'file_hash': FileOperations.compute_file_hash(file_path),
                'row_count': len(df),
                'column_count': len(df.columns),
                'columns': df.columns.tolist(),
                'schema': {},
                'status': 'validated',
            }
            
            # Extract column data types
            for col in df.columns:
                dtype = str(df[col].dtype)
                metadata['schema'][col] = {
                    'type': dtype,
                    'nullable': df[col].isnull().any(),
                    'unique_count': df[col].nunique(),
                }
            
            logger.info(f"CSV metadata extracted: {metadata['file_name']}")
            return metadata
        
        except Exception as e:
            logger.error(f"Error extracting CSV metadata from {file_path}: {str(e)}")
            raise
    
    @staticmethod
    def validate_file(file_path: str) -> tuple:
        """
        Validate CSV file
        
        Args:
            file_path: Path to CSV file
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            if not os.path.exists(file_path):
                return False, "File does not exist"
            
            # Try to read the file
            df = pd.read_csv(file_path, encoding='utf-8', nrows=100)
            
            # Check if it has data
            if df.empty:
                return False, "CSV file is empty"
            
            # Check for all null columns
            if df.isnull().all().any():
                logger.warning(f"CSV has all-null columns: {file_path}")
            
            logger.info(f"CSV validation passed: {file_path}")
            return True, "Valid CSV file"
        
        except pd.errors.ParserError as e:
            error_msg = f"CSV parsing error: {str(e)}"
            logger.error(f"{error_msg} in {file_path}")
            return False, error_msg
        
        except Exception as e:
            error_msg = f"CSV validation error: {str(e)}"
            logger.error(f"{error_msg} in {file_path}")
            return False, error_msg
