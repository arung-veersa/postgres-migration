"""
TASK_01: Copy Data from ConflictVisitMaps to Temp
Migrated from: Snowflake TASK_01_COPY_DATA_FROM_CONFLICTVISITMAPS_TO_TEMP.sql

This task performs the following steps:
1. INSERT new payer-provider reminders (if not exists)
2. UPDATE existing payer-provider reminders with latest names
3. TRUNCATE conflictvisitmaps_temp table
4. INSERT data from conflictvisitmaps to conflictvisitmaps_temp (last 2 years + 45 days)
5. UPDATE settings to set InProgressFlag = 1

No chunking required - executes in single Lambda invocation.
"""

import time
from typing import Dict, Any

from src.base_task import BaseTask


class Task01(BaseTask):
    """
    Task 01: Copy Data from ConflictVisitMaps to Temp.
    Executes all steps sequentially in a single Lambda invocation.
    """
    
    def __init__(self):
        """Initialize Task 01."""
        super().__init__('TASK_01')
    
    def execute(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute Task 01 - all steps sequentially.
        
        Args:
            event: Lambda event dictionary (step parameter ignored for this task)
            
        Returns:
            Dictionary with execution results
        """
        self.logger.info("=" * 70)
        self.logger.info("Starting TASK_01: Copy Data from ConflictVisitMaps to Temp")
        self.logger.info("=" * 70)
        
        start_time = time.time()
        step_results = {}
        
        try:
            # Step 1: INSERT new payer-provider reminders
            self.log_milestone("Step 1: Inserting new payer-provider reminders")
            step_start = time.time()
            sql = self.load_sql('task01_insert_reminders.sql')
            rows_inserted = self.pg.execute(sql)
            step_duration = time.time() - step_start
            step_results['step1_insert_reminders'] = {
                'rows_inserted': rows_inserted,
                'duration_seconds': step_duration
            }
            self.log_milestone(f"Step 1 completed: {rows_inserted:,} rows inserted in {step_duration:.2f}s")
            
            # Step 2: UPDATE existing payer-provider reminders
            self.log_milestone("Step 2: Updating existing payer-provider reminders")
            step_start = time.time()
            sql = self.load_sql('task01_update_reminders.sql')
            rows_updated = self.pg.execute(sql)
            step_duration = time.time() - step_start
            step_results['step2_update_reminders'] = {
                'rows_updated': rows_updated,
                'duration_seconds': step_duration
            }
            self.log_milestone(f"Step 2 completed: {rows_updated:,} rows updated in {step_duration:.2f}s")
            
            # Step 3: TRUNCATE temp table
            self.log_milestone("Step 3: Truncating conflictvisitmaps_temp")
            step_start = time.time()
            sql = self.load_sql('task01_truncate_temp.sql')
            self.pg.execute(sql)
            step_duration = time.time() - step_start
            step_results['step3_truncate_temp'] = {
                'duration_seconds': step_duration
            }
            self.log_milestone(f"Step 3 completed in {step_duration:.2f}s")
            
            # Step 4: INSERT data from conflictvisitmaps to temp
            from datetime import datetime, timedelta
            start_date = (datetime.now() - timedelta(days=365*2)).strftime('%Y-%m-%d')
            end_date = (datetime.now() + timedelta(days=45)).strftime('%Y-%m-%d')
            self.log_milestone(f"Step 4: Copying data from conflictvisitmaps to temp (Range: {start_date} to {end_date})")
            step_start = time.time()
            sql = self.load_sql('task01_copy_to_temp.sql')
            rows_copied = self.pg.execute(sql)
            step_duration = time.time() - step_start
            
            # Calculate throughput
            throughput = rows_copied / step_duration if step_duration > 0 else 0
            
            step_results['step4_copy_to_temp'] = {
                'rows_copied': rows_copied,
                'duration_seconds': step_duration,
                'throughput_rows_per_sec': throughput
            }
            self.log_milestone(f"Step 4 completed: {rows_copied:,} rows copied in {step_duration:.2f}s ({throughput:,.0f} rows/sec)")
            
            # Step 5: UPDATE settings flag
            self.log_milestone("Step 5: Updating settings InProgressFlag")
            step_start = time.time()
            sql = self.load_sql('task01_finalize.sql')
            rows_updated = self.pg.execute(sql)
            step_duration = time.time() - step_start
            step_results['step5_finalize'] = {
                'rows_updated': rows_updated,
                'duration_seconds': step_duration
            }
            self.log_milestone(f"Step 5 completed: {rows_updated} rows updated in {step_duration:.2f}s")
            
            total_duration = time.time() - start_time
            
            self.logger.info("=" * 70)
            self.logger.info("TASK_01 Summary")
            self.logger.info("=" * 70)
            self.logger.info(f"Total duration: {total_duration:.2f} seconds ({total_duration/60:.2f} minutes)")
            self.logger.info(f"Rows inserted (reminders): {step_results['step1_insert_reminders']['rows_inserted']:,}")
            self.logger.info(f"Rows updated (reminders): {step_results['step2_update_reminders']['rows_updated']:,}")
            self.logger.info(f"Rows copied to temp: {step_results['step4_copy_to_temp']['rows_copied']:,}")
            self.logger.info("=" * 70)
            
            return {
                "status": "success",
                "task": "task01",
                "duration_seconds": total_duration,
                "duration_minutes": total_duration / 60,
                "steps": step_results,
                "summary": {
                    "reminders_inserted": step_results['step1_insert_reminders']['rows_inserted'],
                    "reminders_updated": step_results['step2_update_reminders']['rows_updated'],
                    "rows_copied_to_temp": step_results['step4_copy_to_temp']['rows_copied']
                }
            }
            
        except Exception as e:
            total_duration = time.time() - start_time
            self.logger.error("=" * 70)
            self.logger.error(f"TASK_01 failed after {total_duration:.2f} seconds")
            self.logger.error(f"Error: {str(e)}", exc_info=True)
            self.logger.error("=" * 70)
            
            # Update settings flag to indicate failure
            try:
                from config.settings import CONFLICT_SCHEMA
                error_sql = f'UPDATE {CONFLICT_SCHEMA}.settings SET "InProgressFlag" = 2'
                self.pg.execute(error_sql)
            except Exception as update_error:
                self.logger.warning(f"Failed to update error flag: {update_error}")
            
            raise
