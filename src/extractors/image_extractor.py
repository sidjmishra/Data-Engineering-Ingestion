"""
Image Metadata Extractor
"""

import os
from pathlib import Path
from typing import Dict, Any, Tuple
from PIL import Image
from src.utils.logger import LoggerConfig
from src.utils.file_operations import FileOperations


logger = LoggerConfig.get_logger()


class ImageExtractor:
    """Extract metadata from image files"""
    
    @staticmethod
    def extract_metadata(file_path: str) -> Dict[str, Any]:
        """
        Extract metadata from image file
        
        Args:
            file_path: Path to image file
            
        Returns:
            Dictionary containing image metadata
        """
        try:
            # Check file exists
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"File not found: {file_path}")
            
            # Open image
            image = Image.open(file_path)
            
            # Extract dimensions and format
            width, height = image.size
            file_format = image.format or Path(file_path).suffix.upper().lstrip('.')
            
            # Get number of channels
            if image.mode == 'RGB':
                channels = 3
            elif image.mode == 'RGBA':
                channels = 4
            elif image.mode == 'L':
                channels = 1
            else:
                channels = len(image.getbands())
            
            # Extract metadata
            metadata = {
                'file_type': 'image',
                'file_name': os.path.basename(file_path),
                'source_path': file_path,
                'file_size': FileOperations.get_file_size(file_path),
                'file_hash': FileOperations.compute_file_hash(file_path),
                'width': width,
                'height': height,
                'channels': channels,
                'format': file_format,
                'mode': image.mode,
                'size_mp': round((width * height) / 1_000_000, 2),  # Megapixels
                'status': 'validated',
            }
            
            # Extract EXIF data if available
            exif_data = ImageExtractor._extract_exif(image)
            if exif_data:
                metadata['exif'] = exif_data
            
            logger.info(f"Image metadata extracted: {metadata['file_name']}")
            return metadata
        
        except Exception as e:
            logger.error(f"Error extracting image metadata from {file_path}: {str(e)}")
            raise
    
    @staticmethod
    def _extract_exif(image: Image.Image) -> Dict[str, Any]:
        """
        Extract EXIF data from image
        
        Args:
            image: PIL Image object
            
        Returns:
            Dictionary with EXIF data or empty dict
        """
        try:
            exif_data = {}
            if hasattr(image, '_getexif') and image._getexif() is not None:
                raw_exif = image.getexif()
                # Extract common EXIF tags
                if 271 in raw_exif:  # Make
                    exif_data['make'] = str(raw_exif[271])
                if 272 in raw_exif:  # Model
                    exif_data['model'] = str(raw_exif[272])
                if 274 in raw_exif:  # Orientation
                    exif_data['orientation'] = str(raw_exif[274])
            return exif_data
        except Exception as e:
            logger.debug(f"Could not extract EXIF data: {str(e)}")
            return {}
    
    @staticmethod
    def validate_file(file_path: str) -> Tuple[bool, str]:
        """
        Validate image file
        
        Args:
            file_path: Path to image file
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            if not os.path.exists(file_path):
                return False, "File does not exist"
            
            # Try to open the image
            image = Image.open(file_path)
            image.verify()
            
            # Reopen because verify() moves file pointer
            image = Image.open(file_path)
            
            # Check dimensions
            width, height = image.size
            if width == 0 or height == 0:
                return False, "Image has zero dimensions"
            
            logger.info(f"Image validation passed: {file_path}")
            return True, "Valid image file"
        
        except Exception as e:
            error_msg = f"Image validation error: {str(e)}"
            logger.error(f"{error_msg} in {file_path}")
            return False, error_msg
