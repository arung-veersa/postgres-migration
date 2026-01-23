"""
AWS Lambda handler for ETL Pipeline.
Routes actions to task classes and helper utilities.

Usage:
    Event: {"action": "validate_config"}
    Event: {"action": "test_postgres"}
    Event: {"action": "task01"}
    Event: {}  # Defaults to running: validate_config -> test_postgres -> task01
"""

import sys
from pathlib import Path
from typing import Dict, Any, Optional

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from utils.logger import get_logger
from config.settings import POSTGRES_CONFIG, validate_config
from connectors.postgres_connector import PostgresConnector
from src.task01 import Task01

logger = get_logger(__name__)


def lambda_handler(event: Dict[str, Any], context: Optional[Any] = None) -> Dict[str, Any]:
    """
    Main AWS Lambda handler.
    Routes actions to appropriate Task classes.
    
    Args:
        event: Lambda event containing action to perform.
            {
                "action": "validate_config" | "test_postgres" | "task01"
            }
        context: Lambda context (unused)
    
    Returns:
        Response dictionary with status and results
    """
    action_input = event.get('action')
    actions = []
    
    # Normalize input to list of actions
    if not action_input or action_input == "all":
        actions = ["validate_config", "test_postgres", "task01"]
        logger.info("Action is 'all' or empty: defaulting to full sequence.")
    elif isinstance(action_input, list):
        actions = action_input
    elif isinstance(action_input, str):
        if ',' in action_input:
            actions = [a.strip() for a in action_input.split(',')]
        else:
            actions = [action_input]
    else:
        raise ValueError(f"Invalid action format: {type(action_input)}")

    logger.info("=" * 70)
    logger.info(f"Lambda invoked with actions: {actions}")
    logger.info("=" * 70)
    
    results: Dict[str, Any] = {}
    
    try:
        for action in actions:
            logger.info(f"Executing action: {action}...")
            
            if action == "validate_config":
                results[action] = perform_validate_config()

            elif action == "test_postgres":
                results[action] = perform_test_postgres()

            elif action == "task01":
                results[action] = perform_task01(event)

            else:
                logger.warning(f"Unknown action skipped: {action}")
                results[action] = {"status": "skipped", "reason": "Unknown action"}
        
        logger.info("=" * 70)
        logger.info("All requested actions completed.")
        logger.info("=" * 70)
        
        return {"status": "success", "results": results}
        
    except Exception as e:
        logger.error("=" * 70)
        logger.error(f"Execution failed during action '{action}': {str(e)}", exc_info=True)
        logger.error("=" * 70)
        raise

def perform_validate_config() -> Dict[str, Any]:
    """Valdiate the environment configuration."""
    validate_config()
    return {
        "status": "success",
        "message": "Configuration validated successfully",
        "config": {
            "host": POSTGRES_CONFIG.get("host"),
            "port": POSTGRES_CONFIG.get("port"),
            "database": POSTGRES_CONFIG.get("database"),
            "user": POSTGRES_CONFIG.get("user"),
        }
    }

def perform_test_postgres() -> Dict[str, Any]:
    """Test connection to the Postgres database."""
    connector = PostgresConnector(
        host=POSTGRES_CONFIG["host"],
        port=POSTGRES_CONFIG["port"],
        database=POSTGRES_CONFIG["database"],
        user=POSTGRES_CONFIG["user"],
        password=POSTGRES_CONFIG["password"],
    )
    ok = connector.test_connection()
    if not ok:
        raise RuntimeError("Postgres connection failed")
    return {
        "status": "success",
        "message": "Postgres connection successful"
    }

def perform_task01(event: Dict[str, Any]) -> Dict[str, Any]:
    """Execute Task 01."""
    task_instance = Task01()
    # Pass the event, ensuring action is set to task01 for consistency if needed by the task
    return task_instance.execute({**event, "action": "task01"})


if __name__ == '__main__':
    # For local testing
    import json
    
    if len(sys.argv) < 2:
        print("Usage: python lambda_handler.py <action> [json_event]")
        print("Example: python lambda_handler.py task01")
        sys.exit(1)
    
    action = sys.argv[1]
    event = {'action': action}
    
    if len(sys.argv) > 2:
        # Load event from JSON file
        with open(sys.argv[2], 'r') as f:
            event = json.load(f)
    
    result = lambda_handler(event, None)
    print(json.dumps(result, indent=2, default=str))
