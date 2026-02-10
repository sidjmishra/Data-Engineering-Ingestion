"""
SQL Server Implementation
"""

import pyodbc
from typing import Dict, Any, Optional
from datetime import datetime
import json
from src.database.base import DatabaseInterface
from src.utils.logger import LoggerConfig


logger = LoggerConfig.get_logger()


class SQLServerHandler(DatabaseInterface):
    """SQL Server backend for metadata storage"""
    
    def __init__(self, server: str, database: str, username: str, password: str, driver: str = "ODBC Driver 17 for SQL Server"):
        """
        Initialize SQL Server handler
        
        Args:
            server: SQL Server hostname
            database: Database name
            username: Username
            password: Password
            driver: ODBC driver name
        """
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.driver = driver
        self.connection = None
        self.cursor = None
    
    def connect(self) -> bool:
        """Establish SQL Server connection"""
        try:
            connection_string = (
                f"Driver={{{self.driver}}};"
                f"Server={self.server};"
                f"Database={self.database};"
                f"UID={self.username};"
                f"PWD={self.password};"
            )
            self.connection = pyodbc.connect(connection_string)
            self.cursor = self.connection.cursor()
            logger.info(f"Connected to SQL Server: {self.server}/{self.database}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to SQL Server: {str(e)}")
            return False
    
    def disconnect(self) -> bool:
        """Close SQL Server connection"""
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
            logger.info("Disconnected from SQL Server")
            return True
        except Exception as e:
            logger.error(f"Error disconnecting from SQL Server: {str(e)}")
            return False
    
    def health_check(self) -> bool:
        """Check SQL Server connectivity"""
        try:
            self.cursor.execute("SELECT 1")
            return True
        except Exception as e:
            logger.error(f"SQL Server health check failed: {str(e)}")
            return False
    
    def create_indexes(self) -> bool:
        """Create necessary indexes"""
        try:
            # Create indexes on file_metadata table
            self.cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_FileHash')
                    CREATE UNIQUE NONCLUSTERED INDEX IX_FileHash 
                    ON file_metadata(file_hash)
            """)
            
            self.cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_FileName')
                    CREATE NONCLUSTERED INDEX IX_FileName 
                    ON file_metadata(file_name)
            """)
            
            self.cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_FileType')
                    CREATE NONCLUSTERED INDEX IX_FileType 
                    ON file_metadata(file_type)
            """)
            
            # Create indexes on process_logs table
            self.cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_LogTimestamp')
                    CREATE NONCLUSTERED INDEX IX_LogTimestamp 
                    ON process_logs(timestamp)
            """)
            
            self.connection.commit()
            logger.info("SQL Server indexes created successfully")
            return True
        except Exception as e:
            logger.error(f"Error creating indexes: {str(e)}")
            self.connection.rollback()
            return False
    
    def insert_metadata(self, metadata: Dict[str, Any]) -> str:
        """Insert file metadata into SQL Server"""
        try:
            # Convert metadata dict to JSON for storage
            metadata_json = json.dumps(metadata)
            
            query = """
                INSERT INTO file_metadata 
                (file_name, file_type, file_size, file_hash, source_path, status, metadata_json, ingested_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, GETUTCDATE());
                SELECT SCOPE_IDENTITY();
            """
            
            self.cursor.execute(query, (
                metadata.get('file_name'),
                metadata.get('file_type'),
                metadata.get('file_size'),
                metadata.get('file_hash'),
                metadata.get('source_path'),
                metadata.get('status', 'validated'),
                metadata_json
            ))
            self.connection.commit()
            
            metadata_id = self.cursor.fetchone()[0]
            logger.info(f"Metadata inserted with ID: {metadata_id}")
            return str(int(metadata_id))
        except Exception as e:
            logger.error(f"Error inserting metadata: {str(e)}")
            self.connection.rollback()
            raise
    
    def update_metadata(self, document_id: str, metadata: Dict[str, Any]) -> bool:
        """Update existing metadata"""
        try:
            metadata_json = json.dumps(metadata)
            
            query = """
                UPDATE file_metadata 
                SET metadata_json = ?, status = ?, updated_at = GETUTCDATE()
                WHERE id = ?
            """
            
            self.cursor.execute(query, (
                metadata_json,
                metadata.get('status', 'validated'),
                int(document_id)
            ))
            self.connection.commit()
            
            return self.cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Error updating metadata: {str(e)}")
            self.connection.rollback()
            return False
    
    def get_metadata(self, document_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve metadata by ID"""
        try:
            query = "SELECT metadata_json FROM file_metadata WHERE id = ?"
            self.cursor.execute(query, (int(document_id),))
            result = self.cursor.fetchone()
            
            if result:
                return json.loads(result[0])
            return None
        except Exception as e:
            logger.error(f"Error retrieving metadata: {str(e)}")
            return None
    
    def find_by_hash(self, file_hash: str) -> Optional[Dict[str, Any]]:
        """Find metadata by file hash"""
        try:
            query = "SELECT metadata_json FROM file_metadata WHERE file_hash = ?"
            self.cursor.execute(query, (file_hash,))
            result = self.cursor.fetchone()
            
            if result:
                return json.loads(result[0])
            return None
        except Exception as e:
            logger.error(f"Error finding metadata by hash: {str(e)}")
            return None
    
    def insert_process_log(self, log_entry: Dict[str, Any]) -> str:
        """Insert process log entry"""
        try:
            query = """
                INSERT INTO process_logs 
                (file_id, file_name, status, message, timestamp)
                VALUES (?, ?, ?, ?, GETUTCDATE());
                SELECT SCOPE_IDENTITY();
            """
            
            self.cursor.execute(query, (
                log_entry.get('file_id'),
                log_entry.get('file_name'),
                log_entry.get('status'),
                log_entry.get('message')
            ))
            self.connection.commit()
            
            log_id = self.cursor.fetchone()[0]
            return str(int(log_id))
        except Exception as e:
            logger.error(f"Error inserting process log: {str(e)}")
            self.connection.rollback()
            raise
