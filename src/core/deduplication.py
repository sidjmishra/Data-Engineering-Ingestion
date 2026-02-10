"""
Deduplication Module
"""

from src.database.factory import DatabaseFactory
from src.utils.logger import LoggerConfig


logger = LoggerConfig.get_logger()


class DeduplicationService:
    """Handle file deduplication using file hashes"""
    
    @staticmethod
    def is_duplicate(file_hash: str) -> tuple:
        """
        Check if file hash already exists in database
        
        Args:
            file_hash: MD5 or SHA256 hash of file
            
        Returns:
            Tuple of (is_duplicate, existing_metadata)
        """
        try:
            db = DatabaseFactory.get_handler()
            if not db:
                logger.error("Database handler not available")
                return False, None
            
            existing_metadata = db.find_by_hash(file_hash)
            
            if existing_metadata:
                logger.warning(f"Duplicate file detected: {file_hash}")
                return True, existing_metadata
            
            return False, None
        
        except Exception as e:
            logger.error(f"Error checking for duplicates: {str(e)}")
            return False, None
    
    @staticmethod
    def log_duplicate(file_name: str, duplicate_of_file_name: str):
        """
        Log duplicate file discovery
        
        Args:
            file_name: Name of the duplicate file
            duplicate_of_file_name: Name of the original file
        """
        try:
            db = DatabaseFactory.get_handler()
            if not db:
                logger.error("Database handler not available")
                return
            
            log_entry = {
                'file_name': file_name,
                'status': 'duplicate_rejected',
                'message': f"File is a duplicate of {duplicate_of_file_name}",
            }
            
            db.insert_process_log(log_entry)
            logger.info(f"Duplicate logged: {file_name} is duplicate of {duplicate_of_file_name}")
        
        except Exception as e:
            logger.error(f"Error logging duplicate: {str(e)}")
