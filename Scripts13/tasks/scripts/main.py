"""
ECS Container Entry Point for Conflict Management Pipeline
Cross-database operation between Snowflake and PostgreSQL

Container entry point for ECS/Fargate execution.
All parameters are read from environment variables or config.json defaults.

ACTION environment variable supports:
  - Single action:   ACTION=test_connections
  - Comma-separated: ACTION=validate_config,test_connections,task02_00_conflict_update
  - Default:         ACTION not set  →  runs full DEFAULT_ACTIONS pipeline

Actions are executed sequentially. If any action fails, execution stops
and the container exits with code 1.

Special actions:
  - task00_preflight:  Pre-run validation, disables pg_cron, sets InProgressFlag=1
  - task01_copy_to_staging: Sync PPR from Snowflake dims, populate staging table
  - task99_postflight: Post-run cleanup, VACUUM/ANALYZE, re-enables pg_cron, sends email
    Postflight receives all previous action results for the status email.
"""

import sys
import os
import time
import signal
from pathlib import Path
from typing import Dict, Callable, List

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import Settings
from lib.utils import get_logger, format_duration

# Action implementations -- pattern: from scripts.actions.<key> import run_<key>
from scripts.actions.task00_preflight import run_task00_preflight
from scripts.actions.task01_copy_to_staging import run_task01_copy_to_staging
from scripts.actions.task02_00_conflict_update import run_task02_00_conflict_update
from scripts.actions.task99_postflight import run_task99_postflight
from scripts.actions.validate_config import run_validate_config
from scripts.actions.test_connections import run_test_connections

logger = get_logger(__name__)

# --- Graceful shutdown handling ---
# ECS sends SIGTERM 30 seconds before SIGKILL when stopping a task.
# We use this to log final state and close database connections cleanly.

_shutdown_requested = False


def _handle_sigterm(signum, frame):
    """Handle SIGTERM from ECS for graceful shutdown."""
    global _shutdown_requested
    _shutdown_requested = True
    logger.warning("SIGTERM received -- initiating graceful shutdown")


signal.signal(signal.SIGTERM, _handle_sigterm)


# ---------------------------------------------------------------------------
# Action Registry
# ---------------------------------------------------------------------------

# Pipeline results collected during execution.
# Postflight reads this to generate the status email and row-count deltas.
_pipeline_results: List[dict] = []


def _run_task99_postflight_wrapper(settings: Settings) -> dict:
    """Wrapper that passes accumulated pipeline results to postflight."""
    return run_task99_postflight(settings, pipeline_results=list(_pipeline_results))


def _run_task02_00_conflict_update_wrapper(settings: Settings) -> dict:
    """Wrapper that passes shutdown check to conflict update."""
    return run_task02_00_conflict_update(settings, shutdown_check=lambda: _shutdown_requested)


# Default pipeline: runs all tasks sequentially when ACTION is not set.
# Add new tasks to this list as they are developed.
# Note: validate_config and test_connections are included in preflight,
# so they don't appear in the default pipeline. They remain in
# ACTION_REGISTRY for standalone use (e.g. ACTION=validate_config).
DEFAULT_ACTIONS = [
    'task00_preflight',
    'task01_copy_to_staging',
    'task02_00_conflict_update',
    'task99_postflight',
]

# Maps action names to handler functions.
# Pattern: key = file stem, handler = run_<key> (or wrapper around it).
# Add new actions here as they are implemented.
ACTION_REGISTRY: Dict[str, Callable[[Settings], dict]] = {
    'task00_preflight': run_task00_preflight,
    'task01_copy_to_staging': run_task01_copy_to_staging,
    'task02_00_conflict_update': _run_task02_00_conflict_update_wrapper,
    'task99_postflight': _run_task99_postflight_wrapper,
    'validate_config': run_validate_config,
    'test_connections': run_test_connections,
    # Future actions:
    # 'task02_01_conflict_insert': run_task02_01_conflict_insert,
}


def _parse_actions(action_str: str) -> List[str]:
    """
    Parse the ACTION env var into a list of action names.

    Supports:
      - Single action:   "test_connections"
      - Comma-separated: "validate_config, test_connections, task02_00_conflict_update"
      - Empty/unset:     falls back to DEFAULT_ACTIONS (full pipeline)
    """
    actions = [a.strip() for a in action_str.split(',') if a.strip()]
    return actions if actions else list(DEFAULT_ACTIONS)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    """
    Container entry point for ECS Fargate execution.

    Reads ACTION env var (comma-separated list supported), executes each
    action sequentially, and exits with code 0 if all succeed or 1 if
    any action fails.
    """
    start_time = time.time()
    action_str = os.environ.get('ACTION', '').strip()
    actions = _parse_actions(action_str)

    logger.info("=" * 70)
    logger.info("CONFLICT MANAGEMENT - ECS CONTAINER")
    logger.info("=" * 70)
    logger.info(f"Actions: {' -> '.join(actions)}" if len(actions) > 1
                else f"Action: {actions[0]}")
    logger.info(f"Environment: {os.environ.get('ENVIRONMENT', 'unknown')}")

    # Validate all action names upfront before running any
    available = ', '.join(sorted(ACTION_REGISTRY.keys()))
    for action in actions:
        if action not in ACTION_REGISTRY:
            logger.error(f"Unknown action: {action}")
            logger.error(f"Available actions: {available}")
            sys.exit(1)

    try:
        settings = Settings()
        sf_config = settings.get_snowflake_config()
        pg_config = settings.get_postgres_config()
        db_names = settings.get_database_names()

        logger.info("Loading configuration...")
        logger.info(f"  Snowflake: {sf_config['account']} / {db_names['sf_database']}.{db_names['sf_schema']}")
        logger.info(f"  Postgres: {pg_config['host']} / {db_names['pg_database']}.{db_names['pg_schema']}")

        # Execute actions sequentially
        results = []
        _pipeline_results.clear()  # Reset for this run

        for i, action in enumerate(actions, 1):
            action_start = time.time()
            label = f"[{i}/{len(actions)}] " if len(actions) > 1 else ""

            logger.info("")
            logger.info("-" * 70)
            logger.info(f"{label}RUNNING: {action}")
            logger.info("-" * 70)

            # Check for SIGTERM between actions
            if _shutdown_requested:
                logger.warning(f"Shutdown requested -- skipping {action} and remaining actions")
                break

            result = ACTION_REGISTRY[action](settings)
            action_duration = time.time() - action_start
            result['action'] = action
            result['duration_seconds'] = round(action_duration, 2)
            results.append(result)
            _pipeline_results.append(result)  # Accumulate for postflight

            status = result.get('status', 'unknown')
            logger.info(f"{label}FINISHED: {action} -- {status} ({format_duration(action_duration)})")

            # Stop on failure -- but still run postflight if it's in the pipeline
            if status not in ('success', 'completed'):
                logger.error(f"Action '{action}' failed with status '{status}' -- stopping pipeline")

                # Run postflight for cleanup even on failure (re-enable pg_cron,
                # clear InProgressFlag) if it's in the remaining actions
                remaining = actions[i:]  # actions after the failed one (i is 1-based, already incremented)
                if 'task99_postflight' in remaining:
                    logger.info("")
                    logger.info("-" * 70)
                    logger.info("RUNNING: task99_postflight (cleanup after failure)")
                    logger.info("-" * 70)
                    try:
                        pf_start = time.time()
                        pf_result = ACTION_REGISTRY['task99_postflight'](settings)
                        pf_duration = time.time() - pf_start
                        pf_result['action'] = 'task99_postflight'
                        pf_result['duration_seconds'] = round(pf_duration, 2)
                        results.append(pf_result)
                        _pipeline_results.append(pf_result)
                        logger.info(f"FINISHED: task99_postflight -- {pf_result.get('status')} "
                                    f"({format_duration(pf_duration)})")
                    except Exception as pf_err:
                        logger.error(f"Postflight cleanup failed: {pf_err}")

                break

        # Final summary
        total_duration = time.time() - start_time
        all_ok = all(r.get('status') in ('success', 'completed') for r in results)
        overall_status = 'completed' if (all_ok and len(results) == len(actions)) else 'failed'

        logger.info("")
        logger.info("=" * 70)
        logger.info("EXECUTION SUMMARY")
        logger.info("=" * 70)
        for r in results:
            logger.info(f"  {r['action']}: {r['status']} ({r['duration_seconds']}s)")
        logger.info(f"Overall: {overall_status}")
        logger.info(f"Total duration: {format_duration(total_duration)}")
        logger.info("=" * 70)

        sys.exit(0 if overall_status == 'completed' else 1)

    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"Container execution failed after {format_duration(duration)}: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
