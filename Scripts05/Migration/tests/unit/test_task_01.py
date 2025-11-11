"""
Unit tests for Task 01: Copy Data to Temp.

Tests the business logic in isolation using mocked connectors.
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, call

from src.tasks.task_01_copy_to_temp import Task01CopyToTemp


class TestTask01CopyToTemp:
    """Test suite for TASK_01."""
    
    def test_init(self, mock_snowflake_connector, mock_postgres_connector):
        """Test task initialization."""
        task = Task01CopyToTemp(mock_snowflake_connector, mock_postgres_connector)
        
        assert task.task_name == 'TASK_01'
        assert task.sf == mock_snowflake_connector
        assert task.pg == mock_postgres_connector
    
    def test_execute_success(self, mock_snowflake_connector, mock_postgres_connector,
                            sample_payer_provider_relationships,
                            sample_conflict_visit_maps):
        """Test successful task execution."""
        # Setup mocks
        mock_snowflake_connector.fetch_dataframe.return_value = sample_payer_provider_relationships
        mock_postgres_connector.fetch_dataframe.side_effect = [
            pd.DataFrame(),  # No existing reminders
            sample_conflict_visit_maps  # Conflict visit maps
        ]
        mock_postgres_connector.bulk_insert_dataframe.return_value = len(sample_payer_provider_relationships)
        
        task = Task01CopyToTemp(mock_snowflake_connector, mock_postgres_connector)
        
        # Execute
        result = task.execute()
        
        # Verify
        assert result['payer_provider_reminders']['inserted'] == 3
        assert result['temp_table_truncated'] is True
        assert result['temp_table_rows'] == 3
        assert result['settings_updated'] is True
        
        # Verify truncate was called
        mock_postgres_connector.truncate_table.assert_called_once_with(
            'CONFLICTVISITMAPS_TEMP'
        )
    
    def test_sync_payer_provider_reminders_new_records(
        self, 
        mock_snowflake_connector, 
        mock_postgres_connector,
        sample_payer_provider_relationships
    ):
        """Test syncing when all records are new."""
        # Setup: No existing reminders
        mock_snowflake_connector.fetch_dataframe.return_value = sample_payer_provider_relationships
        mock_postgres_connector.fetch_dataframe.return_value = pd.DataFrame()
        mock_postgres_connector.bulk_insert_dataframe.return_value = 3
        
        task = Task01CopyToTemp(mock_snowflake_connector, mock_postgres_connector)
        result = task._sync_payer_provider_reminders()
        
        assert result['inserted'] == 3
        assert result['updated'] == 0
        
        # Verify bulk insert was called
        call_args = mock_postgres_connector.bulk_insert_dataframe.call_args
        inserted_df = call_args[0][0]
        assert len(inserted_df) == 3
        assert 'CreatedDateTime' in inserted_df.columns
        assert 'NumberOfDays' in inserted_df.columns
    
    def test_sync_payer_provider_reminders_existing_records(
        self, 
        mock_snowflake_connector, 
        mock_postgres_connector,
        sample_payer_provider_relationships,
        sample_existing_reminders
    ):
        """Test syncing when some records exist."""
        # Setup: 2 existing, 1 new
        mock_snowflake_connector.fetch_dataframe.return_value = sample_payer_provider_relationships
        mock_postgres_connector.fetch_dataframe.return_value = sample_existing_reminders
        mock_postgres_connector.bulk_insert_dataframe.return_value = 1
        mock_postgres_connector.execute.return_value = 1
        
        task = Task01CopyToTemp(mock_snowflake_connector, mock_postgres_connector)
        result = task._sync_payer_provider_reminders()
        
        assert result['inserted'] == 1  # Only P003 is new
        assert result['updated'] > 0  # P001 and P002 updated
    
    def test_sync_payer_provider_reminders_empty_analytics(
        self,
        mock_snowflake_connector,
        mock_postgres_connector
    ):
        """Test syncing when Analytics returns no data."""
        mock_snowflake_connector.fetch_dataframe.return_value = pd.DataFrame()
        
        task = Task01CopyToTemp(mock_snowflake_connector, mock_postgres_connector)
        result = task._sync_payer_provider_reminders()
        
        assert result['inserted'] == 0
        assert result['updated'] == 0
    
    def test_truncate_temp_table(self, mock_snowflake_connector, mock_postgres_connector):
        """Test temp table truncation."""
        task = Task01CopyToTemp(mock_snowflake_connector, mock_postgres_connector)
        task._truncate_temp_table()
        
        mock_postgres_connector.truncate_table.assert_called_once_with(
            'CONFLICTVISITMAPS_TEMP'
        )
    
    def test_copy_to_temp_table_with_data(
        self,
        mock_snowflake_connector,
        mock_postgres_connector,
        sample_conflict_visit_maps
    ):
        """Test copying data to temp table."""
        mock_postgres_connector.fetch_dataframe.return_value = sample_conflict_visit_maps
        mock_postgres_connector.bulk_insert_dataframe.return_value = len(sample_conflict_visit_maps)
        
        task = Task01CopyToTemp(mock_snowflake_connector, mock_postgres_connector)
        rows = task._copy_to_temp_table()
        
        assert rows == 3
        
        # Verify bulk insert was called
        call_args = mock_postgres_connector.bulk_insert_dataframe.call_args
        assert call_args[0][1] == 'CONFLICTVISITMAPS_TEMP'
    
    def test_copy_to_temp_table_no_data(
        self,
        mock_snowflake_connector,
        mock_postgres_connector
    ):
        """Test copying when no data in date range."""
        mock_postgres_connector.fetch_dataframe.return_value = pd.DataFrame()
        
        task = Task01CopyToTemp(mock_snowflake_connector, mock_postgres_connector)
        rows = task._copy_to_temp_table()
        
        assert rows == 0
        
        # Verify bulk insert was NOT called
        mock_postgres_connector.bulk_insert_dataframe.assert_not_called()
    
    def test_copy_to_temp_table_date_filter(
        self,
        mock_snowflake_connector,
        mock_postgres_connector,
        sample_conflict_visit_maps
    ):
        """Test that date filter is applied correctly."""
        mock_postgres_connector.fetch_dataframe.return_value = sample_conflict_visit_maps
        mock_postgres_connector.bulk_insert_dataframe.return_value = 3
        
        task = Task01CopyToTemp(mock_snowflake_connector, mock_postgres_connector)
        task._copy_to_temp_table()
        
        # Verify query includes date filter
        call_args = mock_postgres_connector.fetch_dataframe.call_args
        query = call_args[0][0]
        
        assert 'VisitDate' in query
        assert 'BETWEEN' in query
    
    def test_update_settings_flag(self, mock_snowflake_connector, mock_postgres_connector):
        """Test updating settings flag."""
        task = Task01CopyToTemp(mock_snowflake_connector, mock_postgres_connector)
        task._update_settings_flag(1)
        
        # Verify execute was called with correct query
        call_args = mock_postgres_connector.execute.call_args
        query = call_args[0][0]
        params = call_args[0][1]
        
        assert 'SETTINGS' in query
        assert 'InProgressFlag' in query
        assert params['flag'] == 1
    
    def test_run_success(self, mock_snowflake_connector, mock_postgres_connector):
        """Test full task run with success."""
        # Setup minimal mocks for full run
        mock_snowflake_connector.fetch_dataframe.return_value = pd.DataFrame()
        mock_postgres_connector.fetch_dataframe.return_value = pd.DataFrame()
        mock_postgres_connector.bulk_insert_dataframe.return_value = 0
        
        task = Task01CopyToTemp(mock_snowflake_connector, mock_postgres_connector)
        result = task.run()
        
        assert result['status'] == 'success'
        assert result['task'] == 'TASK_01'
        assert 'start_time' in result
        assert 'end_time' in result
        assert 'duration_seconds' in result
        assert 'result' in result
    
    def test_run_failure(self, mock_snowflake_connector, mock_postgres_connector):
        """Test task run with failure."""
        # Force an error
        mock_snowflake_connector.fetch_dataframe.side_effect = Exception("Database error")
        
        task = Task01CopyToTemp(mock_snowflake_connector, mock_postgres_connector)
        result = task.run()
        
        assert result['status'] == 'failed'
        assert result['task'] == 'TASK_01'
        assert 'error' in result
        assert 'Database error' in result['error']


class TestTask01Integration:
    """Integration-style tests with more realistic data."""
    
    @pytest.mark.integration
    def test_full_workflow(
        self,
        mock_snowflake_connector,
        mock_postgres_connector,
        sample_payer_provider_relationships,
        sample_conflict_visit_maps
    ):
        """Test full workflow with realistic data."""
        # Setup realistic mock responses
        mock_snowflake_connector.fetch_dataframe.return_value = sample_payer_provider_relationships
        mock_postgres_connector.fetch_dataframe.side_effect = [
            pd.DataFrame(),  # No existing reminders
            sample_conflict_visit_maps  # Conflict data
        ]
        mock_postgres_connector.bulk_insert_dataframe.side_effect = [
            len(sample_payer_provider_relationships),  # Reminders inserted
            len(sample_conflict_visit_maps)  # Conflict data inserted
        ]
        
        task = Task01CopyToTemp(mock_snowflake_connector, mock_postgres_connector)
        result = task.run()
        
        # Verify success
        assert result['status'] == 'success'
        
        # Verify all steps executed
        task_result = result['result']
        assert 'payer_provider_reminders' in task_result
        assert 'temp_table_truncated' in task_result
        assert 'temp_table_rows' in task_result
        assert 'settings_updated' in task_result
        
        # Verify correct call sequence
        assert mock_postgres_connector.truncate_table.called
        assert mock_postgres_connector.bulk_insert_dataframe.call_count == 2
        assert mock_postgres_connector.execute.called

