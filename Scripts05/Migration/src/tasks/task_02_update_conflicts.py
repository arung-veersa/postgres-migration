"""
TASK_02: Update ConflictVisitMaps by executing a self-contained SQL script.

New Architecture (Post-Analytics Migration to Postgres):
- All data (conflict and analytics) resides within the same Postgres database.
- The complex logic of joining, calculating, and updating is handled by a single,
  powerful SQL script: sql/task_02_update_conflicts.sql.
- This Python script's role is simplified to an orchestrator: it loads the SQL,
  injects the correct schema names, and executes it.
- This approach is significantly faster and more maintainable than the previous
  Python-heavy implementation that processed data in batches.
"""

import time
from pathlib import Path
from typing import Dict, Any

from src.tasks.base_task import BaseTask
from src.connectors.postgres_connector import PostgresConnector
from config.settings import CONFLICT_SCHEMA, ANALYTICS_SCHEMA, PROJECT_ROOT


class Task02UpdateConflictVisitMaps(BaseTask):
    """
    Orchestrates the execution of the main conflict update SQL script.
    """
    
    def __init__(self, postgres_connector: PostgresConnector):
        """
        Initializes Task 02.
        
        Args:
            postgres_connector: Connection to the Postgres database.
        """
        super().__init__('TASK_02')
        self.pg = postgres_connector
    
    def execute(self) -> Dict[str, Any]:
        """
        Loads and executes the main SQL script to update conflicts.
        
        Returns:
            A dictionary with the task's results.
        """
        self.logger.info("Starting Task 02: SQL-centric conflict update.")
        start_time = time.time()
        
        # Define the path to the SQL script
        sql_file_path = PROJECT_ROOT / "sql" / "task_02_update_conflicts.sql"
        
        if not sql_file_path.exists():
            self.logger.error(f"SQL script not found at: {sql_file_path}")
            raise FileNotFoundError(f"SQL script not found: {sql_file_path}")
            
        self.logger.info(f"Loading SQL script from: {sql_file_path}")
        
        # Read the SQL script content
        with open(sql_file_path, 'r') as f:
            sql_template = f.read()
            
        # Inject the schema names into the SQL template using simple string replacement
        # This makes the script adaptable to different environments (dev, prod)
        # Note: We use replace() instead of format() to avoid conflicts with SQL's curly braces
        formatted_sql = sql_template.replace('{conflict_schema}', CONFLICT_SCHEMA)
        formatted_sql = formatted_sql.replace('{analytics_schema}', ANALYTICS_SCHEMA)
        
        self.logger.info("Executing the main conflict update SQL script...")
        
        # Execute the entire SQL script as a single transaction
        # The connector's `execute` method will handle the transaction
        updated_rows = self.pg.execute(formatted_sql)
        
        end_time = time.time()
        duration = end_time - start_time
        
        self.logger.info(
            f"Task 02 completed successfully in {duration:.2f} seconds."
        )
        self.logger.info(f"Total rows updated in conflictvisitmaps: {updated_rows}")
        
        return {
            "status": "success",
            "updated_rows": updated_rows,
            "duration_seconds": duration
        }

