"""
MongoDB Implementation
"""
import numpy as np
from pymongo import MongoClient, ASCENDING
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
from typing import Dict, Any, Optional
from datetime import datetime
from src.database.base import DatabaseInterface
from src.utils.logger import LoggerConfig


logger = LoggerConfig.get_logger()

def sanitize_for_mongo(obj):
    if isinstance(obj, dict):
        return {k: sanitize_for_mongo(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_for_mongo(v) for v in obj]
    elif isinstance(obj, (np.bool_)):
        return bool(obj)
    elif isinstance(obj, (np.integer)):
        return int(obj)
    elif isinstance(obj, (np.floating)):
        return float(obj)
    elif isinstance(obj, (np.datetime64)):
        return str(obj)
    else:
        return obj


class MongoDBHandler(DatabaseInterface):
    """MongoDB backend for metadata storage"""
    
    def __init__(self, uri: str, database: str):
        """
        Initialize MongoDB handler
        
        Args:
            uri: MongoDB connection URI
            database: Database name
        """
        self.uri = uri
        self.database_name = database
        self.client = None
        self.db = None
    
    def connect(self) -> bool:
        """Establish MongoDB connection"""
        try:
            self.client = MongoClient(
                self.uri,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=10000
            )
            # Verify connection
            self.client.admin.command('ping')
            self.db = self.client[self.database_name]
            logger.info(f"Connected to MongoDB: {self.database_name}")
            return True
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            logger.error(f"Failed to connect to MongoDB: {str(e)}")
            return False
    
    def disconnect(self) -> bool:
        """Close MongoDB connection"""
        try:
            if self.client:
                self.client.close()
                logger.info("Disconnected from MongoDB")
            return True
        except Exception as e:
            logger.error(f"Error disconnecting from MongoDB: {str(e)}")
            return False
    
    def health_check(self) -> bool:
        """Check MongoDB connectivity"""
        try:
            if self.db is None:
                raise RuntimeError("Database not connected")
            self.db.command('ping')
            return True
        except Exception as e:
            logger.error(f"MongoDB health check failed: {str(e)}")
            return False
    
    def create_indexes(self) -> bool:
        """Create necessary indexes"""
        try:
            metadata_col = self.db['file_metadata']
            
            # Create indexes
            metadata_col.create_index([('file_hash', ASCENDING)], unique=True, sparse=True)
            metadata_col.create_index([('file_name', ASCENDING)])
            metadata_col.create_index([('file_type', ASCENDING)])
            metadata_col.create_index([('ingested_at', ASCENDING)])
            metadata_col.create_index([('status', ASCENDING)])
            metadata_col.create_index([('source_path', ASCENDING)])
            
            # Create index for process logs
            log_col = self.db['process_logs']
            log_col.create_index([('timestamp', ASCENDING)])
            log_col.create_index([('file_id', ASCENDING)])
            log_col.create_index([('status', ASCENDING)])
            
            logger.info("MongoDB indexes created successfully")
            return True
        except Exception as e:
            logger.error(f"Error creating indexes: {str(e)}")
            return False

    def insert_metadata(self, metadata: Dict[str, Any]) -> str:
        """Insert file metadata into MongoDB"""
        try:
            metadata['ingested_at'] = datetime.utcnow()
            clean_metadata = sanitize_for_mongo(metadata)
            result = self.db['file_metadata'].insert_one(clean_metadata)
            logger.info(f"Metadata inserted with ID: {result.inserted_id}")
            return str(result.inserted_id)
        except Exception as e:
            logger.error(f"Error inserting metadata: {str(e)}")
            raise
    
    def update_metadata(self, document_id: str, metadata: Dict[str, Any]) -> bool:
        """Update existing metadata"""
        try:
            from bson import ObjectId
            metadata['updated_at'] = datetime.utcnow()
            result = self.db['file_metadata'].update_one(
                {'_id': ObjectId(document_id)},
                {'$set': metadata}
            )
            return result.modified_count > 0
        except Exception as e:
            logger.error(f"Error updating metadata: {str(e)}")
            return False
    
    def get_metadata(self, document_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve metadata by ID"""
        try:
            from bson import ObjectId
            result = self.db['file_metadata'].find_one({'_id': ObjectId(document_id)})
            if result:
                result['_id'] = str(result['_id'])
            return result
        except Exception as e:
            logger.error(f"Error retrieving metadata: {str(e)}")
            return None
    
    def find_by_hash(self, file_hash: str) -> Optional[Dict[str, Any]]:
        """Find metadata by file hash"""
        try:
            result = self.db['file_metadata'].find_one({'file_hash': file_hash})
            if result:
                result['_id'] = str(result['_id'])
            return result
        except Exception as e:
            logger.error(f"Error finding metadata by hash: {str(e)}")
            return None
    
    def insert_process_log(self, log_entry: Dict[str, Any]) -> str:
        """Insert process log entry"""
        try:
            log_entry['timestamp'] = datetime.utcnow()
            result = self.db['process_logs'].insert_one(log_entry)
            return str(result.inserted_id)
        except Exception as e:
            logger.error(f"Error inserting process log: {str(e)}")
            raise
