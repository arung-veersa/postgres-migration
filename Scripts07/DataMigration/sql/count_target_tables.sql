-- =====================================================================
-- POSTGRESQL TARGET TABLE COUNTS
-- =====================================================================
-- Run this in PostgreSQL to get target record counts
-- Copy results to Excel/spreadsheet for comparison with Snowflake
-- =====================================================================

SELECT 'ANALYTICS' AS pg_source_name, 'DIMPAYER' AS pg_table_name, COUNT(*) AS pg_record_count
FROM conflict_management.analytics_dev.dimpayer

UNION ALL

SELECT 'ANALYTICS', 'DIMPROVIDER', COUNT(*)
FROM conflict_management.analytics_dev.dimprovider

UNION ALL

SELECT 'ANALYTICS', 'DIMOFFICE', COUNT(*)
FROM conflict_management.analytics_dev.dimoffice

UNION ALL

SELECT 'ANALYTICS', 'DIMCONTRACT', COUNT(*)
FROM conflict_management.analytics_dev.dimcontract

UNION ALL

SELECT 'ANALYTICS', 'DIMPAYERPROVIDER', COUNT(*)
FROM conflict_management.analytics_dev.dimpayerprovider

UNION ALL

SELECT 'ANALYTICS', 'DIMUSER', COUNT(*)
FROM conflict_management.analytics_dev.dimuser

UNION ALL

SELECT 'ANALYTICS', 'DIMUSEROFFICES', COUNT(*)
FROM conflict_management.analytics_dev.dimuseroffices

UNION ALL

SELECT 'ANALYTICS', 'DIMPATIENT', COUNT(*)
FROM conflict_management.analytics_dev.dimpatient

UNION ALL

SELECT 'ANALYTICS', 'DIMSERVICECODE', COUNT(*)
FROM conflict_management.analytics_dev.dimservicecode

UNION ALL

SELECT 'ANALYTICS', 'DIMCAREGIVER', COUNT(*)
FROM conflict_management.analytics_dev.dimcaregiver

UNION ALL

SELECT 'ANALYTICS', 'FACTCAREGIVERABSENCE', COUNT(*)
FROM conflict_management.analytics_dev.factcaregiverabsence

UNION ALL

SELECT 'ANALYTICS', 'DIMPATIENTADDRESS', COUNT(*)
FROM conflict_management.analytics_dev.dimpatientaddress

UNION ALL

SELECT 'ANALYTICS', 'FACTCAREGIVERINSERVICE', COUNT(*)
FROM conflict_management.analytics_dev.factcaregiverinservice

UNION ALL

SELECT 'ANALYTICS', 'FACTVISITCALLPERFORMANCE_DELETED_CR', COUNT(*)
FROM conflict_management.analytics_dev.factvisitcallperformance_deleted_cr

UNION ALL

SELECT 'ANALYTICS', 'FACTVISITCALLPERFORMANCE_CR', COUNT(*)
FROM conflict_management.analytics_dev.factvisitcallperformance_cr

UNION ALL

SELECT 'AGGREGATOR', 'multipayer_payer', COUNT(*)
FROM conflict_management.analytics_dev.aggnyprod

UNION ALL

SELECT 'CONFLICT', 'SETTINGS', COUNT(*)
FROM conflict_management.conflict_dev.settings

UNION ALL

SELECT 'CONFLICT', 'MPH', COUNT(*)
FROM conflict_management.conflict_dev.mph

UNION ALL

SELECT 'CONFLICT', 'LOG_FIELDS', COUNT(*)
FROM conflict_management.conflict_dev.log_fields

UNION ALL

SELECT 'CONFLICT', 'PROVIDER_DASHBOARD_TOP', COUNT(*)
FROM conflict_management.conflict_dev.provider_dashboard_top

UNION ALL

SELECT 'CONFLICT', 'PROVIDER_DASHBOARD_CON_TYP', COUNT(*)
FROM conflict_management.conflict_dev.provider_dashboard_con_typ

UNION ALL

SELECT 'CONFLICT', 'PROVIDER_DASHBOARD_PAYER', COUNT(*)
FROM conflict_management.conflict_dev.provider_dashboard_payer

UNION ALL

SELECT 'CONFLICT', 'PROVIDER_DASHBOARD_AGENCY', COUNT(*)
FROM conflict_management.conflict_dev.provider_dashboard_agency

UNION ALL

SELECT 'CONFLICT', 'PROVIDER_DASHBOARD_PATIENT', COUNT(*)
FROM conflict_management.conflict_dev.provider_dashboard_patient

UNION ALL

SELECT 'CONFLICT', 'PROVIDER_DASHBOARD_CAREGIVER', COUNT(*)
FROM conflict_management.conflict_dev.provider_dashboard_caregiver

UNION ALL

SELECT 'CONFLICT', 'PAYER_DASHBOARD_CON_TYP_COUNT', COUNT(*)
FROM conflict_management.conflict_dev.payer_dashboard_con_typ_count

UNION ALL

SELECT 'CONFLICT', 'PAYER_DASHBOARD_AGENCY_IMPACT', COUNT(*)
FROM conflict_management.conflict_dev.payer_dashboard_agency_impact

UNION ALL

SELECT 'CONFLICT', 'PAYER_DASHBOARD_PAYER_COUNT', COUNT(*)
FROM conflict_management.conflict_dev.payer_dashboard_payer_count

UNION ALL

SELECT 'CONFLICT', 'PAYER_DASHBOARD_PAYER_CHART_COUNT', COUNT(*)
FROM conflict_management.conflict_dev.payer_dashboard_payer_chart_count

UNION ALL

SELECT 'CONFLICT', 'PAYER_DASHBOARD_PAYER_CHART_IMPACT', COUNT(*)
FROM conflict_management.conflict_dev.payer_dashboard_payer_chart_impact

UNION ALL

SELECT 'CONFLICT', 'PAYER_DASHBOARD_CAREGIVER_IMPACT', COUNT(*)
FROM conflict_management.conflict_dev.payer_dashboard_caregiver_impact

UNION ALL

SELECT 'CONFLICT', 'PAYER_DASHBOARD_PATIENT_IMPACT', COUNT(*)
FROM conflict_management.conflict_dev.payer_dashboard_patient_impact

UNION ALL

SELECT 'CONFLICT', 'PAYER_DASHBOARD_CAREGIVER_COUNT', COUNT(*)
FROM conflict_management.conflict_dev.payer_dashboard_caregiver_count

UNION ALL

SELECT 'CONFLICT', 'PAYER_DASHBOARD_PATIENT_COUNT', COUNT(*)
FROM conflict_management.conflict_dev.payer_dashboard_patient_count

UNION ALL

SELECT 'CONFLICT', 'PAYER_DASHBOARD_AGENCY_COUNT', COUNT(*)
FROM conflict_management.conflict_dev.payer_dashboard_agency_count

UNION ALL

SELECT 'CONFLICT', 'PAYER_DASHBOARD_PAYER_IMPACT', COUNT(*)
FROM conflict_management.conflict_dev.payer_dashboard_payer_impact

UNION ALL

SELECT 'CONFLICT', 'STATE_DASHBOARD_AGENCY_IMPACT', COUNT(*)
FROM conflict_management.conflict_dev.state_dashboard_agency_impact

UNION ALL

SELECT 'CONFLICT', 'STATE_DASHBOARD_PAYER_IMPACT', COUNT(*)
FROM conflict_management.conflict_dev.state_dashboard_payer_impact

UNION ALL

SELECT 'CONFLICT', 'STATE_DASHBOARD_CON_TYPE_IMPACT', COUNT(*)
FROM conflict_management.conflict_dev.state_dashboard_con_type_impact

UNION ALL

SELECT 'CONFLICT', 'STATE_DASHBOARD_PATIENT_IMPACT', COUNT(*)
FROM conflict_management.conflict_dev.state_dashboard_patient_impact

UNION ALL

SELECT 'CONFLICT', 'PAYER_CONFLICT_SUMMARY_IMPACT', COUNT(*)
FROM conflict_management.conflict_dev.payer_conflict_summary_impact

UNION ALL

SELECT 'CONFLICT', 'STATE_DASHBOARD_CAREGIVER_IMPACT', COUNT(*)
FROM conflict_management.conflict_dev.state_dashboard_caregiver_impact

UNION ALL

SELECT 'CONFLICT', 'PAYER_CONFLICT_SUMMARY_COUNT', COUNT(*)
FROM conflict_management.conflict_dev.payer_conflict_summary_count

UNION ALL

SELECT 'CONFLICT', 'STATE_DASHBOARD_PATIENT_COUNT', COUNT(*)
FROM conflict_management.conflict_dev.state_dashboard_patient_count

UNION ALL

SELECT 'CONFLICT', 'STATE_DASHBOARD_PAYER_COUNT', COUNT(*)
FROM conflict_management.conflict_dev.state_dashboard_payer_count

UNION ALL

SELECT 'CONFLICT', 'STATE_DASHBOARD_AGENCY_COUNT', COUNT(*)
FROM conflict_management.conflict_dev.state_dashboard_agency_count

UNION ALL

SELECT 'CONFLICT', 'STATE_DASHBOARD_CON_TYPE_COUNT', COUNT(*)
FROM conflict_management.conflict_dev.state_dashboard_con_type_count

UNION ALL

SELECT 'CONFLICT', 'STATE_DASHBOARD_CAREGIVER_COUNT', COUNT(*)
FROM conflict_management.conflict_dev.state_dashboard_caregiver_count

UNION ALL

SELECT 'CONFLICT', 'FAILED_API_LOGS', COUNT(*)
FROM conflict_management.conflict_dev.failed_api_logs

UNION ALL

SELECT 'CONFLICT', 'CONFLICTS', COUNT(*)
FROM conflict_management.conflict_dev.conflicts

UNION ALL

SELECT 'CONFLICT', 'CONFLICTVISITMAPS', COUNT(*)
FROM conflict_management.conflict_dev.conflictvisitmaps

UNION ALL

SELECT 'CONFLICT', 'LOG_HISTORY', COUNT(*)
FROM conflict_management.conflict_dev.log_history

UNION ALL

SELECT 'CONFLICT', 'LOG_HISTORY_VALUES', COUNT(*)
FROM conflict_management.conflict_dev.log_history_values

ORDER BY pg_source_name, pg_table_name;
