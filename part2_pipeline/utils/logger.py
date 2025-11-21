"""
Logging Configuration
Sets up logging for the ETL pipeline
"""

import logging
import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))
from part2_pipeline.config import LOGS_DIR, LOG_LEVEL, LOG_FORMAT


def setup_logger(name, log_file=None):
    """
    Setup logger with console and file handlers
    
    Args:
        name: Logger name (usually __name__)
        log_file: Optional log file name (auto-generated if None)
    
    Returns:
        logging.Logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, LOG_LEVEL))
    
    # Avoid duplicate handlers
    if logger.handlers:
        return logger
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter(LOG_FORMAT)
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # File handler
    if log_file is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = f"pipeline_{timestamp}.log"
    
    log_path = LOGS_DIR / log_file
    file_handler = logging.FileHandler(log_path)
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(LOG_FORMAT)
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    
    return logger


class PipelineLogger:
    """Context manager for pipeline execution logging"""
    
    def __init__(self, pipeline_name):
        self.pipeline_name = pipeline_name
        self.logger = setup_logger(pipeline_name)
        self.start_time = None
    
    def __enter__(self):
        self.start_time = datetime.now()
        self.logger.info("=" * 60)
        self.logger.info(f"Starting Pipeline: {self.pipeline_name}")
        self.logger.info("=" * 60)
        return self.logger
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        end_time = datetime.now()
        duration = end_time - self.start_time
        
        if exc_type is None:
            self.logger.info("=" * 60)
            self.logger.info(f"Pipeline Completed Successfully")
            self.logger.info(f"Duration: {duration}")
            self.logger.info("=" * 60)
        else:
            self.logger.error("=" * 60)
            self.logger.error(f"Pipeline Failed: {exc_val}")
            self.logger.error(f"Duration: {duration}")
            self.logger.error("=" * 60)
        
        return False  # Don't suppress exceptions