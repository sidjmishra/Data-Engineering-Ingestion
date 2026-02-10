"""
Video Metadata Extractor
"""

import os
import cv2
from typing import Dict, Any, Tuple, Optional
from pathlib import Path
from src.utils.logger import LoggerConfig
from src.utils.file_operations import FileOperations


logger = LoggerConfig.get_logger()


class VideoExtractor:
    """Extract metadata from video files"""
    
    @staticmethod
    def extract_metadata(file_path: str) -> Dict[str, Any]:
        """
        Extract metadata from video file
        
        Args:
            file_path: Path to video file
            
        Returns:
            Dictionary containing video metadata
        """
        try:
            # Check file exists
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"File not found: {file_path}")
            
            # Open video file
            cap = cv2.VideoCapture(file_path)
            
            if not cap.isOpened():
                raise IOError(f"Cannot open video file: {file_path}")
            
            # Extract video properties
            frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
            fps = cap.get(cv2.CAP_PROP_FPS)
            width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
            height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
            
            # Calculate duration in seconds
            duration_seconds = frame_count / fps if fps > 0 else 0
            
            # Get codec
            fourcc_code = int(cap.get(cv2.CAP_PROP_FOURCC))
            codec = VideoExtractor._decode_fourcc(fourcc_code)
            
            # Release video capture
            cap.release()
            
            # Extract metadata
            metadata = {
                'file_type': 'video',
                'file_name': os.path.basename(file_path),
                'source_path': file_path,
                'file_size': FileOperations.get_file_size(file_path),
                'file_hash': FileOperations.compute_file_hash(file_path),
                'duration_seconds': round(duration_seconds, 2),
                'duration_formatted': VideoExtractor._format_duration(duration_seconds),
                'fps': round(fps, 2),
                'width': width,
                'height': height,
                'resolution': f"{width}x{height}",
                'frame_count': frame_count,
                'codec': codec,
                'status': 'validated',
            }
            
            logger.info(f"Video metadata extracted: {metadata['file_name']}")
            return metadata
        
        except Exception as e:
            logger.error(f"Error extracting video metadata from {file_path}: {str(e)}")
            raise
    
    @staticmethod
    def _decode_fourcc(fourcc: int) -> str:
        """
        Decode FourCC code to readable codec name
        
        Args:
            fourcc: FourCC code as integer
            
        Returns:
            Readable codec name
        """
        try:
            codec_bytes = bytes([
                fourcc & 0xFF,
                (fourcc >> 8) & 0xFF,
                (fourcc >> 16) & 0xFF,
                (fourcc >> 24) & 0xFF
            ])
            return codec_bytes.decode('utf-8').rstrip('\x00')
        except Exception:
            return "Unknown"
    
    @staticmethod
    def _format_duration(seconds: float) -> str:
        """
        Format duration in seconds to HH:MM:SS format
        
        Args:
            seconds: Duration in seconds
            
        Returns:
            Formatted duration string
        """
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    
    @staticmethod
    def validate_file(file_path: str) -> Tuple[bool, str]:
        """
        Validate video file
        
        Args:
            file_path: Path to video file
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            if not os.path.exists(file_path):
                return False, "File does not exist"
            
            # Try to open the video
            cap = cv2.VideoCapture(file_path)
            
            if not cap.isOpened():
                return False, "Cannot open video file"
            
            # Check if we can read frames
            frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
            fps = cap.get(cv2.CAP_PROP_FPS)
            
            cap.release()
            
            if frame_count == 0 or fps == 0:
                return False, "Invalid video: no frames or invalid FPS"
            
            logger.info(f"Video validation passed: {file_path}")
            return True, "Valid video file"
        
        except Exception as e:
            error_msg = f"Video validation error: {str(e)}"
            logger.error(f"{error_msg} in {file_path}")
            return False, error_msg
