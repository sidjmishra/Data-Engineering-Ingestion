"""
Metadata Extractor Factory
"""

from typing import Dict, Any, Tuple
from src.extractors.csv_extractor import CSVExtractor
from src.extractors.image_extractor import ImageExtractor
from src.extractors.video_extractor import VideoExtractor
from src.utils.file_operations import FileOperations
from src.utils.logger import LoggerConfig


logger = LoggerConfig.get_logger()


class MetadataExtractorFactory:
    """Factory for extracting metadata based on file type"""
    
    EXTRACTORS = {
        'csv': CSVExtractor,
        'image': ImageExtractor,
        'video': VideoExtractor,
    }
    
    @staticmethod
    def extract_metadata(file_path: str) -> Tuple[Dict[str, Any], bool, str]:
        """
        Extract metadata from file based on its type
        
        Args:
            file_path: Path to the file
            
        Returns:
            Tuple of (metadata_dict, is_valid, error_message)
        """
        try:
            # Detect file type
            file_type = FileOperations.detect_file_type(file_path)
            
            if file_type == 'unknown':
                return {}, False, "Unknown file type"
            
            # Get appropriate extractor
            extractor = MetadataExtractorFactory.EXTRACTORS.get(file_type)
            if not extractor:
                return {}, False, f"No extractor available for {file_type} files"
            
            # Validate file
            is_valid, error_msg = MetadataExtractorFactory.validate_file(file_path, file_type)
            if not is_valid:
                return {}, False, error_msg
            
            # Extract metadata
            metadata = extractor.extract_metadata(file_path)
            
            logger.info(f"Metadata extraction successful: {file_path}")
            return metadata, True, "Success"
        
        except Exception as e:
            error_msg = f"Metadata extraction failed: {str(e)}"
            logger.error(f"{error_msg} for {file_path}")
            return {}, False, error_msg
    
    @staticmethod
    def validate_file(file_path: str, file_type: str = None) -> Tuple[bool, str]:
        """
        Validate file
        
        Args:
            file_path: Path to the file
            file_type: File type (if None, will be detected)
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            if file_type is None:
                file_type = FileOperations.detect_file_type(file_path)
            
            extractor = MetadataExtractorFactory.EXTRACTORS.get(file_type)
            if not extractor:
                return False, f"No validator available for {file_type} files"
            
            # Call validator
            is_valid, error_msg = extractor.validate_file(file_path)
            return is_valid, error_msg
        
        except Exception as e:
            error_msg = f"File validation error: {str(e)}"
            logger.error(f"{error_msg} for {file_path}")
            return False, error_msg
