"""
Task definitions for the ETL pipeline.
"""

from .base_task import BaseTask
from .task_01_step01_prepare import Task01Step01Prepare
from .task_01_step02_get_chunks import Task01Step02GetChunks
from .task_01_step03_process_chunk import Task01Step03ProcessChunk
from .task_02_00_step01_get_chunks import Task02Step01GetChunks
from .task_02_00_step02_process_chunk import Task02Step02ProcessChunk
from .task_02_01_step01_get_chunks import Task0201Step01GetChunks
from .task_02_01_step02_process_chunk import Task0201Step02ProcessChunk
from .task_02_03_finalize_conflicts import Task0203FinalizeConflicts

__all__ = [
    'BaseTask',
    'Task01Step01Prepare',
    'Task01Step02GetChunks',
    'Task01Step03ProcessChunk',
    'Task02Step01GetChunks',
    'Task02Step02ProcessChunk',
    'Task0201Step01GetChunks',
    'Task0201Step02ProcessChunk',
    'Task0203FinalizeConflicts',
]
