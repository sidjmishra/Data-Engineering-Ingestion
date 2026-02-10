"""
Scheduler and File Watcher
"""

import os
from pathlib import Path
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from datetime import datetime
from typing import List
from src.core.processor import IngestionProcessor
from src.utils.file_operations import FileOperations
from src.utils.logger import LoggerConfig
from src.utils.config import ConfigManager


logger = LoggerConfig.get_logger()


class FileIngestionScheduler:
    """Scheduler for periodic file ingestion"""
    
    def __init__(self):
        """Initialize scheduler"""
        self.config = ConfigManager.get_config()
        self.scheduler = BackgroundScheduler()
        self.processor = IngestionProcessor()
        self.is_running = False
    
    def start(self):
        """Start the scheduler"""
        try:
            if self.is_running:
                logger.warning("Scheduler is already running")
                return
            
            interval_minutes = self.config.get('scheduler', {}).get('interval_minutes', 60)
            
            self.scheduler.add_job(
                self._ingest_files,
                IntervalTrigger(minutes=interval_minutes),
                id='file_ingestion',
                name='File Ingestion Job',
                replace_existing=True
            )
            
            self.scheduler.start()
            self.is_running = True
            
            logger.info(f"Scheduler started. Ingestion interval: {interval_minutes} minutes")
            
            logger.info("Running initial ingestion...")
            self._ingest_files()
        
        except Exception as e:
            logger.error(f"Error starting scheduler: {str(e)}")
            raise
    
    def stop(self):
        """Stop the scheduler"""
        try:
            if self.scheduler.running:
                self.scheduler.shutdown()
                self.is_running = False
                logger.info("Scheduler stopped")
        except Exception as e:
            logger.error(f"Error stopping scheduler: {str(e)}")
    
    def _ingest_files(self):
        """Main ingestion routine called by scheduler"""
        try:
            logger.info("=" * 60)
            logger.info(f"Starting file ingestion cycle at {datetime.now().isoformat()}")
            logger.info("=" * 60)
            
            incoming_folder = self.config['folders']['incoming']
            
            if not os.path.exists(incoming_folder):
                logger.warning(f"Incoming folder does not exist: {incoming_folder}")
                return
            
            files = FileOperations.list_files(incoming_folder)
            
            if not files:
                logger.info("No files found in incoming folder")
                return
            
            logger.info(f"Found {len(files)} file(s) to process")
            
            for file_path in files:
                result = self.processor.process_file(file_path)
                self._log_result(result)
            
            stats = self.processor.get_statistics()
            logger.info("=" * 60)
            logger.info("Ingestion Cycle Statistics:")
            logger.info(f"  Successfully Processed: {stats['processed']}")
            logger.info(f"  Failed: {stats['failed']}")
            logger.info(f"  Duplicates Detected: {stats['duplicates']}")
            logger.info("=" * 60)
        
        except Exception as e:
            logger.error(f"Error during ingestion cycle: {str(e)}")
    
    def _log_result(self, result: dict):
        """Log processing result"""
        status = result.get('status')
        file_path = result.get('file_path')
        message = result.get('message')
        
        if status == 'success':
            logger.info(f"SUCCESS: {file_path}")
        elif status == 'duplicate':
            logger.info(f"⊗ DUPLICATE: {file_path} - {message}")
        else:
            logger.warning(f"FAILED ({status}): {file_path} - {message}")
