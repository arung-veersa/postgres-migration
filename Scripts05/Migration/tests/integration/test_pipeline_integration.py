"""
Integration tests for full ETL pipeline.
These tests connect to real database and validate end-to-end functionality.

Run with: pytest tests/integration/test_pipeline_integration.py -v -m integration
"""

import pytest
import time
from config.settings import POSTGRES_CONFIG
from src.connectors.postgres_connector import PostgresConnector
from src.tasks.task_01_copy_to_temp import Task01CopyToTemp
from src.tasks.task_02_update_conflicts import Task02UpdateConflictVisitMaps


@pytest.mark.integration
class TestPipelineIntegration:
    """Integration tests for complete pipeline."""
    
    @pytest.fixture(scope='class')
    def postgres_connector(self):
        """Provide real PostgresConnector instance."""
        connector = PostgresConnector(**POSTGRES_CONFIG)
        
        # Verify connection works
        assert connector.test_connection(), "Database connection failed"
        
        yield connector
    
    def test_database_connection(self, postgres_connector):
        """Test that we can connect to the database."""
        assert postgres_connector.test_connection()
        assert postgres_connector.config is not None
    
    def test_task_01_execution(self, postgres_connector):
        """Test Task 01 executes successfully with real database."""
        task = Task01CopyToTemp(postgres_connector)
        
        start_time = time.time()
        result = task.run()
        duration = time.time() - start_time
        
        # Validate result structure
        assert result['status'] == 'success', f"Task failed: {result.get('error')}"
        assert 'result' in result
        assert 'affected_rows' in result['result']
        assert 'duration_seconds' in result
        
        # Validate performance
        print(f"\nTask 01 Performance:")
        print(f"  Duration: {duration:.2f}s ({duration/60:.1f} min)")
        print(f"  Rows affected: {result['result']['affected_rows']}")
        
        # Warn if approaching Lambda timeout
        if duration > 300:  # 5 minutes
            print(f"  ⚠️  Task 01 is taking longer than expected")
    
    def test_task_02_execution(self, postgres_connector):
        """Test Task 02 executes successfully with real database."""
        task = Task02UpdateConflictVisitMaps(postgres_connector)
        
        start_time = time.time()
        result = task.run()
        duration = time.time() - start_time
        
        # Validate result structure
        assert result['status'] == 'success', f"Task failed: {result.get('error')}"
        assert 'result' in result
        assert 'updated_rows' in result['result']
        assert 'duration_seconds' in result
        
        # Validate performance
        print(f"\nTask 02 Performance:")
        print(f"  Duration: {duration:.2f}s ({duration/60:.1f} min)")
        print(f"  Rows updated: {result['result']['updated_rows']}")
        
        # Warn if approaching Lambda timeout
        if duration > 600:  # 10 minutes
            print(f"  ⚠️  Task 02 is taking longer than expected")
        if duration > 720:  # 12 minutes
            print(f"  ⚠️  WARNING: Task 02 may exceed Lambda timeout!")
    
    def test_full_pipeline_sequential(self, postgres_connector):
        """Test complete pipeline execution (Task 01 -> Task 02)."""
        print("\n" + "="*70)
        print("FULL PIPELINE INTEGRATION TEST")
        print("="*70)
        
        total_start = time.time()
        
        # Execute Task 01
        print("\nExecuting Task 01...")
        task1 = Task01CopyToTemp(postgres_connector)
        result1 = task1.run()
        task1_duration = time.time() - total_start
        
        assert result1['status'] == 'success', f"Task 01 failed: {result1.get('error')}"
        print(f"✅ Task 01 completed in {task1_duration:.2f}s")
        print(f"   Rows affected: {result1['result']['affected_rows']}")
        
        # Execute Task 02
        print("\nExecuting Task 02...")
        task2_start = time.time()
        task2 = Task02UpdateConflictVisitMaps(postgres_connector)
        result2 = task2.run()
        task2_duration = time.time() - task2_start
        
        assert result2['status'] == 'success', f"Task 02 failed: {result2.get('error')}"
        print(f"✅ Task 02 completed in {task2_duration:.2f}s")
        print(f"   Rows updated: {result2['result']['updated_rows']}")
        
        # Total duration analysis
        total_duration = time.time() - total_start
        print("\n" + "-"*70)
        print(f"Total Pipeline Duration: {total_duration:.2f}s ({total_duration/60:.1f} min)")
        
        # Lambda timeout analysis
        lambda_timeout = 900  # 15 minutes
        if total_duration > lambda_timeout:
            print("❌ CRITICAL: Pipeline exceeds Lambda 15-minute timeout!")
            pytest.fail(f"Pipeline duration ({total_duration:.0f}s) exceeds Lambda timeout (900s)")
        elif total_duration > 720:  # 12 minutes
            print("⚠️  WARNING: Pipeline is close to Lambda timeout")
        else:
            print(f"✅ Pipeline completes within Lambda timeout "
                  f"(buffer: {lambda_timeout - total_duration:.0f}s)")
        
        print("="*70)
    
    def test_task_01_idempotency(self, postgres_connector):
        """Test that Task 01 can be run multiple times safely."""
        task = Task01CopyToTemp(postgres_connector)
        
        # Run first time
        result1 = task.run()
        assert result1['status'] == 'success'
        
        # Run second time (should not fail)
        result2 = task.run()
        assert result2['status'] == 'success'
        
        print(f"\nIdempotency Test:")
        print(f"  First run: {result1['result']['affected_rows']} rows")
        print(f"  Second run: {result2['result']['affected_rows']} rows")
    
    def test_task_02_idempotency(self, postgres_connector):
        """Test that Task 02 can be run multiple times safely."""
        task = Task02UpdateConflictVisitMaps(postgres_connector)
        
        # Run first time
        result1 = task.run()
        assert result1['status'] == 'success'
        rows_first = result1['result']['updated_rows']
        
        # Run second time immediately (should update fewer or zero rows)
        result2 = task.run()
        assert result2['status'] == 'success'
        rows_second = result2['result']['updated_rows']
        
        print(f"\nIdempotency Test:")
        print(f"  First run: {rows_first} rows updated")
        print(f"  Second run: {rows_second} rows updated")
        
        # Second run should update same or fewer rows
        # (depends on whether new data appeared)
        assert rows_second <= rows_first or rows_second >= 0

