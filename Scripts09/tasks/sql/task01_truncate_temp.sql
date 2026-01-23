-- ============================================================================
-- TASK_01 - Step 3: TRUNCATE temp table
-- ============================================================================
-- Purpose: Clear CONFLICTVISITMAPS_TEMP before copying new data
-- 
-- Schema Placeholders:
--   {conflict_schema}  - Conflict data schema (e.g., conflict_dev)
-- ============================================================================

TRUNCATE TABLE {conflict_schema}.conflictvisitmaps_temp;
