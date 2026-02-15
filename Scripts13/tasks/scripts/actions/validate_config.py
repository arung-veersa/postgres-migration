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
    common_params = settings.get_common_parameters()
    task02_params = settings.get_task02_parameters()
    task03_params = settings.get_task03_parameters()
    db_names = settings.get_database_names()

    logger.info("Configuration validated successfully")
    logger.info(f"  Databases: {json.dumps(db_names, indent=2)}")
    logger.info(f"  Common parameters: {json.dumps(common_params, indent=2)}")
    logger.info(f"  Task02 parameters: {json.dumps(task02_params, indent=2)}")
    logger.info(f"  Task03 parameters: {json.dumps(task03_params, indent=2)}")

    return {
        'status': 'success',
        'message': 'Configuration validated successfully',
        'databases': db_names,
        'common_parameters': common_params,
        'task02_parameters': task02_params,
        'task03_parameters': task03_params,
    }
