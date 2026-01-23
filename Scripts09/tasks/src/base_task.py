"""
Base task class for all ETL tasks.
Provides common functionality and interface.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

from utils.logger import get_logger
from connectors.postgres_connector import PostgresConnector
from config.settings import CONFLICT_SCHEMA, ANALYTICS_SCHEMA, PROJECT_ROOT

logger = get_logger(__name__)


class BaseTask(ABC):
    """
    Abstract base class for all ETL tasks.
    Provides common structure, database connection, and utilities.
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
        self.logger = get_logger(f"Task.{task_name}")
        pg_config = self._get_postgres_config()
        self.pg = PostgresConnector(
            host=pg_config['host'],
            port=pg_config['port'],
            database=pg_config['database'],
            user=pg_config['user'],
            password=pg_config['password']
        )
        self.sql_dir = PROJECT_ROOT / "sql"
    
    def _get_postgres_config(self) -> Dict[str, Any]:
        """Get Postgres configuration from settings."""
        from config.settings import POSTGRES_CONFIG
        return POSTGRES_CONFIG
    
    @abstractmethod
    def execute(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute the task logic.
        Must be implemented by subclasses.
        
        Args:
            event: Lambda event dictionary containing action, step, and task-specific parameters
            
        Returns:
            Dictionary with task results
        """
        pass
    
    def load_sql(self, sql_file: str, **kwargs) -> str:
        """
        Load SQL file from sql/ folder and substitute placeholders.
        
        Args:
            sql_file: Name of SQL file (e.g., 'task01_prepare.sql')
            **kwargs: Additional placeholders to substitute (e.g., start_id=100, end_id=200)
            
        Returns:
            SQL string with placeholders substituted
        """
        sql_path = self.sql_dir / sql_file
        
        if not sql_path.exists():
            self.logger.error(f"SQL file not found: {sql_path}")
            raise FileNotFoundError(f"SQL file not found: {sql_path}")
        
        self.logger.debug(f"Loading SQL file: {sql_file}")
        
        with open(sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Substitute schema placeholders
        sql_content = sql_content.replace('{conflict_schema}', CONFLICT_SCHEMA)
        sql_content = sql_content.replace('{analytics_schema}', ANALYTICS_SCHEMA)
        
        # Substitute additional placeholders from kwargs
        for key, value in kwargs.items():
            placeholder = f'{{{key}}}'
            sql_content = sql_content.replace(placeholder, str(value))
        
        return sql_content
    
    def log_milestone(self, message: str, **kwargs):
        """
        Log milestone with structured data.
        
        Args:
            message: Milestone message
            **kwargs: Additional key-value pairs to log
        """
        parts = [f"[{self.task_name}] {message}"]
        for key, value in kwargs.items():
            parts.append(f"{key}={value}")
        
        self.logger.info(" | ".join(parts))
    
    def run(self) -> Dict[str, Any]:
        """
        Run the task with timing and error handling.
        This is a convenience method for tasks that don't need event routing.
        
        Returns:
            Dictionary with execution results and metadata
        """
        self.logger.info("=" * 70)
        self.logger.info(f"Starting {self.task_name}")
        self.logger.info("=" * 70)
        
        self.start_time = datetime.now()
        
        try:
            result = self.execute({})
            
            self.end_time = datetime.now()
            duration = (self.end_time - self.start_time).total_seconds()
            
            self.logger.info("=" * 70)
            self.logger.info(f"{self.task_name} completed successfully")
            self.logger.info(f"Duration: {duration:.2f} seconds")
            self.logger.info("=" * 70)
            
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
            duration = (self.end_time - self.start_time).total_seconds() if self.start_time else 0
            
            self.logger.error("=" * 70)
            self.logger.error(f"{self.task_name} failed")
            self.logger.error(f"Error: {str(e)}")
            self.logger.error(f"Duration: {duration:.2f} seconds")
            self.logger.error("=" * 70)
            
            return {
                'status': 'failed',
                'task': self.task_name,
                'start_time': self.start_time.isoformat() if self.start_time else None,
                'end_time': self.end_time.isoformat() if self.end_time else None,
                'duration_seconds': duration,
                'error': str(e)
            }
