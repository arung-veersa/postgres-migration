"""
Task definitions for the ETL pipeline.
"""

from .base_task import BaseTask
from .task_01_copy_to_temp import Task01CopyToTemp
from .task_02_update_conflicts import Task02UpdateConflictVisitMaps
from .task_02_get_chunks import Task02GetChunks
from .task_02_process_chunk import Task02ProcessChunk

__all__ = [
    'BaseTask',
    'Task01CopyToTemp',
    'Task02UpdateConflictVisitMaps',
    'Task02GetChunks',
    'Task02ProcessChunk',
]

