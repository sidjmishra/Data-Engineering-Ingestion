"""
Database Factory Module
"""

from typing import Optional
from src.database.base import DatabaseInterface
from src.database.mongodb import MongoDBHandler
from src.database.sqlserver import SQLServerHandler
from src.utils.logger import LoggerConfig
from src.utils.config import ConfigManager


logger = LoggerConfig.get_logger()


class DatabaseFactory:
    """Factory for creating database handlers"""
    
    _instance: Optional[DatabaseInterface] = None
    
    @staticmethod
    def create_handler(db_type: str = None) -> DatabaseInterface:
        """
        Create a database handler based on configuration
        
        Args:
            db_type: Database type ('mongodb' or 'sqlserver'). If None, uses config.
            
        Returns:
            Database handler instance
        """
        if DatabaseFactory._instance is not None:
            return DatabaseFactory._instance
        
        if db_type is None:
            db_type = ConfigManager.get('database.type', 'mongodb').lower()
            print(db_type)
        
        try:
            if db_type == 'mongodb':
                uri = ConfigManager.get('database.mongodb.uri')
                database = ConfigManager.get('database.mongodb.database')
                handler = MongoDBHandler(uri, database)
            
            elif db_type == 'sqlserver':
                server = ConfigManager.get('database.sqlserver.server')
                database = ConfigManager.get('database.sqlserver.database')
                username = ConfigManager.get('database.sqlserver.username')
                password = ConfigManager.get('database.sqlserver.password')
                driver = ConfigManager.get('database.sqlserver.driver', 'ODBC Driver 17 for SQL Server')
                
                handler = SQLServerHandler(server, database, username, password, driver)
            
            else:
                raise ValueError(f"Unsupported database type: {db_type}")
            
            # Connect and verify
            if not handler.connect():
                raise RuntimeError(f"Failed to connect to {db_type}")
            
            # Create indexes
            handler.create_indexes()
            
            DatabaseFactory._instance = handler
            logger.info(f"Database handler created: {db_type}")
            
            return handler
        
        except Exception as e:
            logger.error(f"Error creating database handler: {str(e)}")
            raise
    
    @staticmethod
    def get_handler() -> Optional[DatabaseInterface]:
        """Get current database handler instance"""
        return DatabaseFactory._instance
    
    @staticmethod
    def close():
        """Close database connection"""
        if DatabaseFactory._instance:
            DatabaseFactory._instance.disconnect()
            DatabaseFactory._instance = None
    
    @staticmethod
    def reset():
        """Reset factory (useful for testing)"""
        DatabaseFactory._instance = None
