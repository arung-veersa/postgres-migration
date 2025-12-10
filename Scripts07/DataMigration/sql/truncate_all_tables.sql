-- =====================================================================
-- TRUNCATE ALL POSTGRESQL TARGET TABLES
-- =====================================================================
-- ⚠️ WARNING: This will DELETE ALL DATA from all migration target tables
-- ⚠️ Use this ONLY when you need a complete fresh start
-- =====================================================================

-- Option 1: Run all at once (if you're sure)
-- Option 2: Uncomment individual sections as needed

-- =====================================================================
-- ANALYTICS TABLES (analytics_dev schema)
-- =====================================================================

TRUNCATE TABLE conflict_management.analytics_dev.dimpayer CASCADE;
TRUNCATE TABLE conflict_management.analytics_dev.dimprovider CASCADE;
TRUNCATE TABLE conflict_management.analytics_dev.dimoffice CASCADE;
TRUNCATE TABLE conflict_management.analytics_dev.dimcontract CASCADE;
TRUNCATE TABLE conflict_management.analytics_dev.dimpayerprovider CASCADE;
TRUNCATE TABLE conflict_management.analytics_dev.dimuser CASCADE;
TRUNCATE TABLE conflict_management.analytics_dev.dimuseroffices CASCADE;
TRUNCATE TABLE conflict_management.analytics_dev.dimpatient CASCADE;
TRUNCATE TABLE conflict_management.analytics_dev.dimservicecode CASCADE;
TRUNCATE TABLE conflict_management.analytics_dev.dimcaregiver CASCADE;
TRUNCATE TABLE conflict_management.analytics_dev.factcaregiverabsence CASCADE;
TRUNCATE TABLE conflict_management.analytics_dev.dimpatientaddress CASCADE;
TRUNCATE TABLE conflict_management.analytics_dev.factcaregiverinservice CASCADE;
TRUNCATE TABLE conflict_management.analytics_dev.factvisitcallperformance_deleted_cr CASCADE;
TRUNCATE TABLE conflict_management.analytics_dev.factvisitcallperformance_cr CASCADE;

-- =====================================================================
-- AGGREGATOR TABLE (analytics_dev schema)
-- =====================================================================

TRUNCATE TABLE conflict_management.analytics_dev.aggnyprod CASCADE;

-- =====================================================================
-- CONFLICT TABLES (conflict_dev schema)
-- =====================================================================

TRUNCATE TABLE conflict_management.conflict_dev.settings CASCADE;
TRUNCATE TABLE conflict_management.conflict_dev.mph CASCADE;
TRUNCATE TABLE conflict_management.conflict_dev.log_fields CASCADE;
TRUNCATE TABLE conflict_management.conflict_dev.provider_dashboard_top CASCADE;
TRUNCATE TABLE conflict_management.conflict_dev.provider_dashboard_con_typ CASCADE;
TRUNCATE TABLE conflict_management.conflict_dev.provider_dashboard_payer CASCADE;
TRUNCATE TABLE conflict_management.conflict_dev.provider_dashboard_agency CASCADE;
TRUNCATE TABLE conflict_management.conflict_dev.provider_dashboard_patient CASCADE;
TRUNCATE TABLE conflict_management.conflict_dev.provider_dashboard_caregiver CASCADE;
TRUNCATE TABLE conflict_management.conflict_dev.payer_dashboard_con_typ_count CASCADE;
TRUNCATE TABLE conflict_management.conflict_dev.payer_dashboard_agency_impact CASCADE;
TRUNCATE TABLE conflict_management.conflict_dev.payer_dashboard_payer_count CASCADE;
TRUNCATE TABLE conflict_management.conflict_dev.payer_dashboard_payer_chart_count CASCADE;
TRUNCATE TABLE conflict_management.conflict_dev.payer_dashboard_payer_chart_impact CASCADE;
TRUNCATE TABLE conflict_management.conflict_dev.payer_dashboard_caregiver_impact CASCADE;
TRUNCATE TABLE conflict_management.conflict_dev.payer_dashboard_patient_impact CASCADE;
TRUNCATE TABLE conflict_management.conflict_dev.payer_dashboard_caregiver_count CASCADE;
TRUNCATE TABLE conflict_management.conflict_dev.payer_dashboard_patient_count CASCADE;
TRUNCATE TABLE conflict_management.conflict_dev.payer_dashboard_agency_count CASCADE;
TRUNCATE TABLE conflict_management.conflict_dev.payer_dashboard_payer_impact CASCADE;
TRUNCATE TABLE conflict_management.conflict_dev.state_dashboard_agency_impact CASCADE;
TRUNCATE TABLE conflict_management.conflict_dev.state_dashboard_payer_impact CASCADE;
TRUNCATE TABLE conflict_management.conflict_dev.state_dashboard_con_type_impact CASCADE;
TRUNCATE TABLE conflict_management.conflict_dev.state_dashboard_patient_impact CASCADE;
TRUNCATE TABLE conflict_management.conflict_dev.payer_conflict_summary_impact CASCADE;
TRUNCATE TABLE conflict_management.conflict_dev.state_dashboard_caregiver_impact CASCADE;
TRUNCATE TABLE conflict_management.conflict_dev.payer_conflict_summary_count CASCADE;
TRUNCATE TABLE conflict_management.conflict_dev.state_dashboard_patient_count CASCADE;
TRUNCATE TABLE conflict_management.conflict_dev.state_dashboard_payer_count CASCADE;
TRUNCATE TABLE conflict_management.conflict_dev.state_dashboard_agency_count CASCADE;
TRUNCATE TABLE conflict_management.conflict_dev.state_dashboard_con_type_count CASCADE;
TRUNCATE TABLE conflict_management.conflict_dev.state_dashboard_caregiver_count CASCADE;
TRUNCATE TABLE conflict_management.conflict_dev.failed_api_logs CASCADE;
TRUNCATE TABLE conflict_management.conflict_dev.conflicts CASCADE;
TRUNCATE TABLE conflict_management.conflict_dev.conflictvisitmaps CASCADE;
TRUNCATE TABLE conflict_management.conflict_dev.log_history CASCADE;
TRUNCATE TABLE conflict_management.conflict_dev.log_history_values CASCADE;

-- =====================================================================
-- TRUNCATE MIGRATION STATUS TABLES ONLY
-- =====================================================================
-- Use this to reset migration state WITHOUT deleting data tables
-- This forces a fresh migration run (bypasses resume logic)
-- =====================================================================

TRUNCATE TABLE conflict_management.analytics_dev.migration_chunk_status CASCADE;
TRUNCATE TABLE conflict_management.analytics_dev.migration_table_status CASCADE;
TRUNCATE TABLE conflict_management.analytics_dev.migration_runs CASCADE;

-- =====================================================================
-- VERIFY STATUS TABLES ARE EMPTY
-- =====================================================================

SELECT 'migration_runs' AS table_name, COUNT(*) AS record_count
FROM conflict_management.analytics_dev.migration_runs
UNION ALL
SELECT 'migration_table_status', COUNT(*)
FROM conflict_management.analytics_dev.migration_table_status
UNION ALL
SELECT 'migration_chunk_status', COUNT(*)
FROM conflict_management.analytics_dev.migration_chunk_status;

-- All counts should be 0 after truncation

