-- ============================================================================
-- TASK_01 - Step 5: Update settings flag
-- ============================================================================
-- Purpose: Set InProgressFlag = 1 to indicate processing has started
-- 
-- Schema Placeholders:
--   {conflict_schema}  - Conflict data schema (e.g., conflict_dev)
-- ============================================================================

UPDATE {conflict_schema}.settings 
SET "InProgressFlag" = 1;
