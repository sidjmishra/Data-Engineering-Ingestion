"""
Database Abstraction Module - Base Interface
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional


class DatabaseInterface(ABC):
    """Abstract base class for database implementations"""
    
    @abstractmethod
    def connect(self) -> bool:
        """Establish database connection"""
        pass
    
    @abstractmethod
    def disconnect(self) -> bool:
        """Close database connection"""
        pass
    
    @abstractmethod
    def insert_metadata(self, metadata: Dict[str, Any]) -> str:
        """
        Insert file metadata
        
        Args:
            metadata: Dictionary containing file metadata
            
        Returns:
            ID of inserted document
        """
        pass
    
    @abstractmethod
    def update_metadata(self, document_id: str, metadata: Dict[str, Any]) -> bool:
        """
        Update existing metadata
        
        Args:
            document_id: ID of document to update
            metadata: Updated metadata dictionary
            
        Returns:
            True if successful, False otherwise
        """
        pass
    
    @abstractmethod
    def get_metadata(self, document_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve metadata by ID
        
        Args:
            document_id: ID of document to retrieve
            
        Returns:
            Metadata dictionary or None if not found
        """
        pass
    
    @abstractmethod
    def find_by_hash(self, file_hash: str) -> Optional[Dict[str, Any]]:
        """
        Find metadata by file hash (for deduplication)
        
        Args:
            file_hash: MD5 or SHA256 hash of file
            
        Returns:
            Metadata dictionary or None if not found
        """
        pass
    
    @abstractmethod
    def insert_process_log(self, log_entry: Dict[str, Any]) -> str:
        """
        Insert process log entry
        
        Args:
            log_entry: Dictionary containing log information
            
        Returns:
            ID of inserted log entry
        """
        pass
    
    @abstractmethod
    def health_check(self) -> bool:
        """Check database connectivity and availability"""
        pass
    
    @abstractmethod
    def create_indexes(self) -> bool:
        """Create necessary indexes for optimal performance"""
        pass
