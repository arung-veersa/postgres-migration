"""
Unit tests for src/tasks/base_task.py
Target coverage: 100%
"""

import pytest
from datetime import datetime
from unittest.mock import Mock, patch


def test_base_task_initialization():
    """Test BaseTask initialization."""
    from src.tasks.base_task import BaseTask
    
    class TestTask(BaseTask):
        def execute(self):
            return {'result': 'success'}
    
    task = TestTask('TEST_TASK')
    
    assert task.task_name == 'TEST_TASK'
    assert task.start_time is None
    assert task.end_time is None
    assert task.logger is not None


def test_base_task_execute_is_abstract():
    """Test that BaseTask.execute is abstract and must be implemented."""
    from src.tasks.base_task import BaseTask
    
    # Should not be able to instantiate without implementing execute
    with pytest.raises(TypeError):
        BaseTask('TEST_TASK')


def test_base_task_run_success():
    """Test successful task execution via run()."""
    from src.tasks.base_task import BaseTask
    
    class TestTask(BaseTask):
        def execute(self):
            return {'data': 'test_data', 'rows': 100}
    
    task = TestTask('TEST_TASK')
    result = task.run()
    
    assert result['status'] == 'success'
    assert result['task'] == 'TEST_TASK'
    assert 'start_time' in result
    assert 'end_time' in result
    assert 'duration_seconds' in result
    assert result['result'] == {'data': 'test_data', 'rows': 100}
    assert result['duration_seconds'] >= 0


def test_base_task_run_failure():
    """Test task execution failure handling."""
    from src.tasks.base_task import BaseTask
    
    class FailingTask(BaseTask):
        def execute(self):
            raise ValueError("Test error")
    
    task = FailingTask('FAILING_TASK')
    result = task.run()
    
    assert result['status'] == 'failed'
    assert result['task'] == 'FAILING_TASK'
    assert 'start_time' in result
    assert 'end_time' in result
    assert 'duration_seconds' in result
    assert 'error' in result
    assert 'Test error' in result['error']


def test_base_task_run_timing():
    """Test that run() properly tracks timing."""
    from src.tasks.base_task import BaseTask
    import time
    
    class SlowTask(BaseTask):
        def execute(self):
            time.sleep(0.1)
            return {'status': 'done'}
    
    task = SlowTask('SLOW_TASK')
    result = task.run()
    
    assert result['duration_seconds'] >= 0.1
    assert task.start_time is not None
    assert task.end_time is not None
    assert task.end_time > task.start_time


def test_base_task_run_sets_start_time():
    """Test that run() sets start_time before execution."""
    from src.tasks.base_task import BaseTask
    
    class TestTask(BaseTask):
        def execute(self):
            assert self.start_time is not None
            return {}
    
    task = TestTask('TEST_TASK')
    task.run()


def test_base_task_run_sets_end_time():
    """Test that run() sets end_time after execution."""
    from src.tasks.base_task import BaseTask
    
    class TestTask(BaseTask):
        def execute(self):
            return {}
    
    task = TestTask('TEST_TASK')
    task.run()
    
    assert task.end_time is not None


def test_base_task_run_sets_end_time_on_error():
    """Test that run() sets end_time even on error."""
    from src.tasks.base_task import BaseTask
    
    class FailingTask(BaseTask):
        def execute(self):
            raise RuntimeError("Test error")
    
    task = FailingTask('FAILING_TASK')
    task.run()
    
    assert task.end_time is not None


def test_base_task_log_progress():
    """Test log_progress method."""
    from src.tasks.base_task import BaseTask
    
    class TestTask(BaseTask):
        def execute(self):
            return {}
    
    task = TestTask('TEST_TASK')
    
    # Should not raise exception
    task.log_progress("Processing batch", batch_num=1, rows=100)


def test_base_task_log_progress_no_kwargs():
    """Test log_progress with no additional kwargs."""
    from src.tasks.base_task import BaseTask
    
    class TestTask(BaseTask):
        def execute(self):
            return {}
    
    task = TestTask('TEST_TASK')
    
    # Should not raise exception
    task.log_progress("Starting task")


def test_base_task_log_progress_multiple_kwargs():
    """Test log_progress with multiple kwargs."""
    from src.tasks.base_task import BaseTask
    
    class TestTask(BaseTask):
        def execute(self):
            return {}
    
    task = TestTask('TEST_TASK')
    
    # Should not raise exception
    task.log_progress("Processing", batch=1, total=10, progress=0.1)


def test_base_task_result_format():
    """Test that run() returns properly formatted result."""
    from src.tasks.base_task import BaseTask
    
    class TestTask(BaseTask):
        def execute(self):
            return {'affected_rows': 500}
    
    task = TestTask('TEST_TASK')
    result = task.run()
    
    # Check all required keys are present
    required_keys = ['status', 'task', 'start_time', 'end_time', 'duration_seconds', 'result']
    for key in required_keys:
        assert key in result
    
    # Check ISO format for timestamps
    assert 'T' in result['start_time']  # ISO format contains 'T'
    assert 'T' in result['end_time']


def test_base_task_error_format():
    """Test that run() returns properly formatted error result."""
    from src.tasks.base_task import BaseTask
    
    class TestTask(BaseTask):
        def execute(self):
            raise ValueError("Test validation error")
    
    task = TestTask('TEST_TASK')
    result = task.run()
    
    # Check all required keys are present
    required_keys = ['status', 'task', 'start_time', 'end_time', 'duration_seconds', 'error']
    for key in required_keys:
        assert key in result
    
    assert result['status'] == 'failed'
    assert result['error'] == 'Test validation error'


def test_base_task_logger_name():
    """Test that logger has correct name."""
    from src.tasks.base_task import BaseTask
    
    class TestTask(BaseTask):
        def execute(self):
            return {}
    
    task = TestTask('MY_TASK')
    
    assert 'MY_TASK' in task.logger.name

