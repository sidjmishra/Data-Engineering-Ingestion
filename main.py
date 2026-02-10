"""
Main Application Entry Point
"""

import os
import sys
import signal
import time
from pathlib import Path
from src.utils.logger import LoggerConfig
from src.utils.config import ConfigManager
from src.database.factory import DatabaseFactory
from src.core.scheduler import FileIngestionScheduler


def setup_application():
    """Setup application configuration and logging"""
    try:
        base_path = os.path.dirname(os.path.abspath(__file__))
        # base_path = os.path.dirname(os.path.dirname(base_path))
        print(base_path)
        
        config_path = os.path.join(base_path, 'config', 'config.yaml')
        log_dir = os.path.join(base_path, 'logs')
        
        logger = LoggerConfig.setup_logging(config_path, log_dir)
        
        config = ConfigManager.load_config(config_path)
        
        logger.info("=" * 70)
        logger.info("FILE INGESTION PIPELINE - APPLICATION STARTUP")
        logger.info("=" * 70)
        logger.info(f"Base Path: {base_path}")
        logger.info(f"Config Path: {config_path}")
        logger.info(f"Log Directory: {log_dir}")
        
        return logger, config, base_path
    
    except Exception as e:
        print(f"Error setting up application: {str(e)}")
        sys.exit(1)


def main():
    """Main application entry point"""
    logger, config, base_path = setup_application()
    scheduler = None
    
    def signal_handler(sig, frame):
        """Handle interrupt signal"""
        logger.info("\nShutdown signal received...")
        if scheduler:
            scheduler.stop()
        logger.info("Application shutdown complete")
        DatabaseFactory.close()
        sys.exit(0)
    
    try:
        signal.signal(signal.SIGINT, signal_handler)
        
        logger.info("Connecting to database...")
        db = DatabaseFactory.create_handler()
        
        if not db.health_check():
            logger.error("Database health check failed")
            sys.exit(1)
        
        logger.info("Database connected successfully")
        
        logger.info("Verifying folder structure...")
        required_folders = [
            'incoming',
            'raw',
            'validated',
            'failed',
        ]
        
        for folder_key in required_folders:
            folder_path = config['folders'][folder_key]
            os.makedirs(folder_path, exist_ok=True)
            logger.info(f"  {folder_key}: {folder_path}")
        
        logger.info("\nStarting file ingestion scheduler...")
        scheduler = FileIngestionScheduler()
        scheduler.start()
        
        logger.info("=" * 70)
        logger.info("APPLICATION READY")
        logger.info("=" * 70)
        logger.info("Scheduler is running. Press Ctrl+C to stop.")
        logger.info("=" * 70)
        
        while True:
            time.sleep(1)
    
    except KeyboardInterrupt:
        logger.info("\nKeyboard interrupt received...")
    except Exception as e:
        logger.error(f"Application error: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        if scheduler:
            scheduler.stop()
        DatabaseFactory.close()
        logger.info("Application shutdown complete")


if __name__ == "__main__":
    main()
