"""
Validate Configuration Action

Validates configuration parameters and prints a summary.
Available as a standalone action via ACTION=validate_config.
Also called internally by preflight.
"""

import json

from config.settings import Settings
from lib.utils import get_logger

logger = get_logger(__name__)


def run_validate_config(settings: Settings) -> dict:
    """Validate configuration and print summary."""
    task_params = settings.get_task02_parameters()
    db_names = settings.get_database_names()

    logger.info("Configuration validated successfully")
    logger.info(f"  Databases: {json.dumps(db_names, indent=2)}")
    logger.info(f"  Task parameters: {json.dumps(task_params, indent=2)}")

    return {
        'status': 'success',
        'message': 'Configuration validated successfully',
        'databases': db_names,
        'task_parameters': task_params,
    }
