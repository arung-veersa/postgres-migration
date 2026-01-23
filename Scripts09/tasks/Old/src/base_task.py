"""
Base task class for all ETL tasks.
Provides common functionality and interface.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, Any
from utils.logger import get_logger

logger = get_logger(__name__)


class BaseTask(ABC):
    """
    Abstract base class for all ETL tasks.
    Provides common structure and error handling.
    """
    
    def __init__(self, task_name: str):
        """
        Initialize base task.
        
        Args:
            task_name: Name of the task (e.g., 'TASK_01')
        """
        self.task_name = task_name
        self.start_time = None
        self.end_time = None
        self.logger = get_logger(f"{__name__}.{task_name}")
    
    @abstractmethod
    def execute(self) -> Dict[str, Any]:
        """
        Execute the task logic.
        Must be implemented by subclasses.
        
        Returns:
            Dictionary with task results
        """
        pass
    
    def run(self) -> Dict[str, Any]:
        """
        Run the task with timing and error handling.
        
        Returns:
            Dictionary with execution results and metadata
        """
        self.logger.info(f"{'='*60}")
        self.logger.info(f"Starting {self.task_name}")
        self.logger.info(f"{'='*60}")
        
        self.start_time = datetime.now()
        
        try:
            result = self.execute()
            
            self.end_time = datetime.now()
            duration = (self.end_time - self.start_time).total_seconds()
            
            self.logger.info(f"{'='*60}")
            self.logger.info(f"{self.task_name} completed successfully")
            self.logger.info(f"Duration: {duration:.2f} seconds")
            self.logger.info(f"{'='*60}")
            
            return {
                'status': 'success',
                'task': self.task_name,
                'start_time': self.start_time.isoformat(),
                'end_time': self.end_time.isoformat(),
                'duration_seconds': duration,
                'result': result
            }
            
        except Exception as e:
            self.end_time = datetime.now()
            duration = (self.end_time - self.start_time).total_seconds()
            
            self.logger.error(f"{'='*60}")
            self.logger.error(f"{self.task_name} failed")
            self.logger.error(f"Error: {str(e)}")
            self.logger.error(f"Duration: {duration:.2f} seconds")
            self.logger.error(f"{'='*60}")
            
            return {
                'status': 'failed',
                'task': self.task_name,
                'start_time': self.start_time.isoformat(),
                'end_time': self.end_time.isoformat(),
                'duration_seconds': duration,
                'error': str(e)
            }
    
    def log_progress(self, message: str, **kwargs):
        """
        Log progress with task context.
        
        Args:
            message: Progress message
            **kwargs: Additional key-value pairs to log
        """
        parts = [message]
        for key, value in kwargs.items():
            parts.append(f"{key}={value}")
        
        self.logger.info(" | ".join(parts))

