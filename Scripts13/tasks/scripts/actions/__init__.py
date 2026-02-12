"""
Pipeline actions for conflict management.

Each module exports a single entry-point function that takes a Settings object
and returns a dict with at minimum a 'status' key.

Naming convention:
  - File:     <registry_key>.py       (e.g. task01_copy_to_staging.py)
  - Function: run_<registry_key>()    (e.g. run_task01_copy_to_staging())
  - Import:   from scripts.actions.<key> import run_<key>

Pipeline actions (in DEFAULT_ACTIONS order):
  task00_preflight.py           -> run_task00_preflight()
  task01_copy_to_staging.py     -> run_task01_copy_to_staging()
  task02_00_conflict_update.py  -> run_task02_00_conflict_update()
  task99_postflight.py          -> run_task99_postflight()

Standalone actions (not in default pipeline):
  validate_config.py            -> run_validate_config()
  test_connections.py           -> run_test_connections()
"""
