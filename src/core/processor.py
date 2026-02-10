"""
Core File Ingestion Processor
"""

import os
import time
from datetime import datetime
from typing import Dict, Any, Optional
from src.extractors.factory import MetadataExtractorFactory
from src.database.factory import DatabaseFactory
from src.core.deduplication import DeduplicationService
from src.utils.file_operations import FileOperations
from src.utils.logger import LoggerConfig
from src.utils.config import ConfigManager


logger = LoggerConfig.get_logger()


class IngestionProcessor:
    """Main file ingestion processor"""
    
    def __init__(self):
        """Initialize processor"""
        self.config = ConfigManager.get_config()
        self.db = DatabaseFactory.get_handler()
        self.processed_count = 0
        self.failed_count = 0
        self.duplicate_count = 0
    
    def process_file(self, file_path: str) -> Dict[str, Any]:
        """
        Process a single file through the ingestion pipeline
        
        Args:
            file_path: Path to the file to process
            
        Returns:
            Processing result dictionary
        """
        result = {
            'file_path': file_path,
            'status': 'unknown',
            'message': '',
            'metadata': None,
            'database_id': None,
        }
        
        try:
            # Log start
            logger.info(f"Starting ingestion: {file_path}")
            
            # Step 1: Validate file
            is_valid, validation_error = MetadataExtractorFactory.validate_file(file_path)
            if not is_valid:
                result['status'] = 'validation_failed'
                result['message'] = validation_error
                self._move_to_failed(file_path, validation_error)
                self.failed_count += 1
                return result
            
            # Step 2: Extract metadata
            metadata, extraction_success, extraction_error = MetadataExtractorFactory.extract_metadata(file_path)
            if not extraction_success:
                result['status'] = 'metadata_extraction_failed'
                result['message'] = extraction_error
                self._move_to_failed(file_path, extraction_error)
                self.failed_count += 1
                return result
            
            # Step 3: Check for duplicates
            file_hash = metadata.get('file_hash')
            is_duplicate, duplicate_metadata = DeduplicationService.is_duplicate(file_hash)
            if is_duplicate:
                result['status'] = 'duplicate'
                result['message'] = f"Duplicate of {duplicate_metadata.get('file_name')}"
                DeduplicationService.log_duplicate(
                    metadata['file_name'],
                    duplicate_metadata.get('file_name')
                )
                self.duplicate_count += 1
                return result
            
            # Step 4: Store metadata in database
            try:
                metadata['status'] = 'validated'
                db_id = self.db.insert_metadata(metadata)
                result['database_id'] = db_id
            except Exception as e:
                result['status'] = 'database_insert_failed'
                result['message'] = f"Failed to insert metadata: {str(e)}"
                self._move_to_failed(file_path, result['message'])
                self.failed_count += 1
                return result
            
            # Step 5: Move file to validated storage
            success = self._move_to_validated(file_path, metadata)
            if not success:
                result['status'] = 'file_movement_failed'
                result['message'] = "Failed to move file to validated storage"
                self._move_to_failed(file_path, result['message'])
                self.failed_count += 1
                return result
            
            # Success
            result['status'] = 'success'
            result['message'] = 'File successfully ingested'
            result['metadata'] = metadata
            self.processed_count += 1
            
            # Log success
            self._log_process(metadata, 'success')
            logger.info(f"Successfully ingested: {file_path}")
            
            return result
        
        except Exception as e:
            error_msg = f"Unexpected error during ingestion: {str(e)}"
            logger.error(f"{error_msg} for {file_path}")
            result['status'] = 'unexpected_error'
            result['message'] = error_msg
            self._move_to_failed(file_path, error_msg)
            self.failed_count += 1
            return result
    
    def _move_to_validated(self, file_path: str, metadata: Dict[str, Any]) -> bool:
        """
        Move file to validated storage directory
        
        Args:
            file_path: Source file path
            metadata: File metadata
            
        Returns:
            True if successful, False otherwise
        """
        try:
            file_type = metadata['file_type']
            validated_dir = FileOperations.get_batch_hour_dir(
                self.config['folders']['validated']
            )
            
            # Create subdirectory based on file type
            type_map = {
                'csv': 'structured',
                'image': 'images',
                'video': 'videos',
            }
            
            target_dir = os.path.join(validated_dir, type_map.get(file_type, 'unknown'))
            file_name = os.path.basename(file_path)
            destination = os.path.join(target_dir, file_name)
            
            # Create a copy in validated storage
            success = FileOperations.copy_file(file_path, destination)
            
            if success:
                # Also move to raw storage
                raw_dir = os.path.join(
                    self.config['folders']['raw'],
                    type_map.get(file_type, 'unknown')
                )
                raw_destination = os.path.join(raw_dir, file_name)
                FileOperations.move_file(file_path, raw_destination)
                
                logger.info(f"File moved to validated storage: {destination}")
                return True
            
            return False
        
        except Exception as e:
            logger.error(f"Error moving file to validated storage: {str(e)}")
            return False
    
    def _move_to_failed(self, file_path: str, reason: str) -> bool:
        """
        Move file to failed directory
        
        Args:
            file_path: Source file path
            reason: Failure reason
            
        Returns:
            True if successful, False otherwise
        """
        try:
            file_type = FileOperations.detect_file_type(file_path)
            failed_dir = FileOperations.get_batch_hour_dir(
                self.config['folders']['failed']
            )
            
            # Create subdirectory based on file type
            type_map = {
                'csv': 'structured',
                'image': 'images',
                'video': 'videos',
            }
            
            target_dir = os.path.join(failed_dir, type_map.get(file_type, 'unknown'))
            file_name = os.path.basename(file_path)
            destination = os.path.join(target_dir, file_name)
            
            success = FileOperations.move_file(file_path, destination)
            
            if success:
                logger.warning(f"File moved to failed storage: {destination}. Reason: {reason}")
                self._log_process(
                    {
                        'file_name': file_name,
                        'source_path': file_path,
                        'file_type': file_type,
                    },
                    'failed',
                    reason
                )
            
            return success
        
        except Exception as e:
            logger.error(f"Error moving file to failed storage: {str(e)}")
            return False
    
    def _log_process(self, metadata: Dict[str, Any], status: str, message: str = ''):
        """
        Log process result to database
        
        Args:
            metadata: File metadata
            status: Process status
            message: Additional message
        """
        try:
            log_entry = {
                'file_name': metadata.get('file_name'),
                'file_type': metadata.get('file_type'),
                'status': status,
                'message': message or f"File {status}",
            }
            
            self.db.insert_process_log(log_entry)
        
        except Exception as e:
            logger.error(f"Error logging process: {str(e)}")
    
    def get_statistics(self) -> Dict[str, int]:
        """Get ingestion statistics"""
        return {
            'processed': self.processed_count,
            'failed': self.failed_count,
            'duplicates': self.duplicate_count,
        }
