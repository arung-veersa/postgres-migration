-- ======================================================================
-- INSERT NEW CONFLICTS INTO CONFLICTVISITMAPS (Postgres version - CHUNKED)
-- Converted from Snowflake stored procedure INSERT_DATA_FROM_MAIN_TO_CONFLICTVISITMAPS
--
-- Parameters (string-substituted by Python migration script):
--   {conflict_schema}  - schema containing conflict tables (e.g. conflict_dev)
--   {analytics_schema} - schema containing analytics tables
--   {chunk_filter_inner} - WHERE clause for (VisitDate, SSN) filtering in V1/V2 subqueries
--
-- Chunking Strategy:
-- - Uses composite (VisitDate, SSN) key for safe parallel processing
-- - Each chunk contains caregivers with visits at 2+ providers on same day
-- - Idempotent: Uses NOT EXISTS to avoid duplicate inserts
--
-- Key conversion notes:
-- - Uses Haversine formula for accurate great-circle distance calculation (no PostGIS required)
-- - DATEADD/DATEDIFF converted to PostgreSQL interval arithmetic and EXTRACT
-- - GETDATE() converted to NOW()
-- - SPLIT function converted to SPLIT_PART (1-indexed in PostgreSQL)
-- - ROW_NUMBER() window functions preserved
-- - VisitID, AppVisitID, ConVisitID are uuid type
-- - ConAppVisitID is bigint type
-- ======================================================================

INSERT INTO {conflict_schema}.conflictvisitmaps (
    "SSN",
    "ProviderID", "AppProviderID", "ProviderName", "FederalTaxNumber",
    "VisitID", "AppVisitID",
    "ConProviderID", "ConAppProviderID", "ConProviderName", "ConFederalTaxNumber",
    "ConVisitID", "ConAppVisitID",
    "VisitDate",
    "SchStartTime", "SchEndTime",
    "ConSchStartTime", "ConSchEndTime",
    "VisitStartTime", "VisitEndTime",
    "ConVisitStartTime", "ConVisitEndTime",
    "EVVStartTime", "EVVEndTime",
    "ConEVVStartTime", "ConEVVEndTime",
    "CaregiverID", "AppCaregiverID",
    "AideCode", "AideName", "AideSSN",
    "ConCaregiverID", "ConAppCaregiverID",
    "ConAideCode", "ConAideName", "ConAideSSN",
    "OfficeID", "AppOfficeID", "Office",
    "ConOfficeID", "ConAppOfficeID", "ConOffice",
    "PatientID", "AppPatientID", "PAdmissionID", "PName",
    "PAddressID", "PAppAddressID",
    "PAddressL1", "PAddressL2", "PCity",
    "PAddressState", "PZipCode", "PCounty",
    "PLongitude", "PLatitude",
    "ConPatientID", "ConAppPatientID", "ConPAdmissionID",
    "ConPName", "ConPAddressID", "ConPAppAddressID",
    "ConPAddressL1", "ConPAddressL2", "ConPCity",
    "ConPAddressState", "ConPZipCode", "ConPCounty",
    "ConPLongitude", "ConPLatitude",
    "PayerID", "AppPayerID", "Contract",
    "ConPayerID", "ConAppPayerID", "ConContract",
    "BilledDate", "ConBilledDate",
    "BilledHours", "ConBilledHours",
    "Billed", "ConBilled",
    "MinuteDiffBetweenSch",
    "DistanceMilesFromLatLng",
    "AverageMilesPerHour",
    "ETATravelMinutes",
    "ServiceCodeID", "AppServiceCodeID",
    "RateType", "ServiceCode",
    "ConServiceCodeID", "ConAppServiceCodeID",
    "ConRateType", "ConServiceCode",
    "SameSchTimeFlag", "SameVisitTimeFlag",
    "SchAndVisitTimeSameFlag",
    "SchOverAnotherSchTimeFlag",
    "VisitTimeOverAnotherVisitTimeFlag",
    "SchTimeOverVisitTimeFlag",
    "DistanceFlag", "InServiceFlag", "PTOFlag",
    "AideFName", "AideLName",
    "ConAideFName", "ConAideLName",
    "PFName", "PLName",
    "ConPFName", "ConPLName",
    "PMedicaidNumber", "ConPMedicaidNumber",
    "PayerState", "ConPayerState",
    "AgencyContact", "ConAgencyContact",
    "AgencyPhone", "ConAgencyPhone",
    "LastUpdatedBy", "ConLastUpdatedBy",
    "LastUpdatedDate", "ConLastUpdatedDate",
    "BilledRate", "TotalBilledAmount",
    "ConBilledRate", "ConTotalBilledAmount",
    "IsMissed", "MissedVisitReason", "EVVType",
    "ConIsMissed", "ConMissedVisitReason", "ConEVVType",
    "PStatus", "ConPStatus",
    "AideStatus", "ConAideStatus",
    "P_PatientID", "P_AppPatientID",
    "ConP_PatientID", "ConP_AppPatientID",
    "PA_PatientID", "PA_AppPatientID",
    "ConPA_PatientID", "ConPA_AppPatientID",
    "P_PAdmissionID", "P_PName",
    "P_PAddressID", "P_PAppAddressID",
    "P_PAddressL1", "P_PAddressL2", "P_PCity",
    "P_PAddressState", "P_PZipCode", "P_PCounty",
    "P_PFName", "P_PLName", "P_PMedicaidNumber",
    "ConP_PAdmissionID", "ConP_PName",
    "ConP_PAddressID", "ConP_PAppAddressID",
    "ConP_PAddressL1", "ConP_PAddressL2", "ConP_PCity",
    "ConP_PAddressState", "ConP_PZipCode", "ConP_PCounty",
    "ConP_PFName", "ConP_PLName", "ConP_PMedicaidNumber",
    "PA_PAdmissionID", "PA_PName",
    "PA_PAddressID", "PA_PAppAddressID",
    "PA_PAddressL1", "PA_PAddressL2", "PA_PCity",
    "PA_PAddressState", "PA_PZipCode", "PA_PCounty",
    "PA_PFName", "PA_PLName", "PA_PMedicaidNumber",
    "ConPA_PAdmissionID", "ConPA_PName",
    "ConPA_PAddressID", "ConPA_PAppAddressID",
    "ConPA_PAddressL1", "ConPA_PAddressL2", "ConPA_PCity",
    "ConPA_PAddressState", "ConPA_PZipCode", "ConPA_PCounty",
    "ConPA_PFName", "ConPA_PLName", "ConPA_PMedicaidNumber",
    "ContractType", "ConContractType",
    "P_PStatus", "ConP_PStatus",
    "PA_PStatus", "ConPA_PStatus",
    "BillRateNonBilled", "ConBillRateNonBilled",
    "BillRateBoth", "ConBillRateBoth",
    "CreatedDate"
)
SELECT DISTINCT
    V1."SSN",
    V1."ProviderID"                    AS "ProviderID",
    V1."AppProviderID"                 AS "AppProviderID",
    V1."ProviderName"                  AS "ProviderName",
    V1."FederalTaxNumber"              AS "FederalTaxNumber",
    V1."VisitID"                       AS "VisitID",
    V1."AppVisitID"                    AS "AppVisitID",
    V2."ProviderID"                    AS "ConProviderID",
    V2."AppProviderID"                 AS "ConAppProviderID",
    V2."ProviderName"                  AS "ConProviderName",
    V2."FederalTaxNumber"              AS "ConFederalTaxNumber",
    V2."VisitID"                       AS "ConVisitID",
    V2."AppVisitID"                    AS "ConAppVisitID",
    V1."VisitDate"                     AS "VisitDate",
    V1."SchStartTime"                  AS "SchStartTime",
    V1."SchEndTime"                    AS "SchEndTime",
    V2."SchStartTime"                  AS "ConSchStartTime",
    V2."SchEndTime"                    AS "ConSchEndTime",
    V1."VisitStartTime"                AS "VisitStartTime",
    V1."VisitEndTime"                  AS "VisitEndTime",
    V2."VisitStartTime"                AS "ConVisitStartTime",
    V2."VisitEndTime"                  AS "ConVisitEndTime",
    V1."EVVStartTime"                  AS "EVVStartTime",
    V1."EVVEndTime"                    AS "EVVEndTime",
    V2."EVVStartTime"                  AS "ConEVVStartTime",
    V2."EVVEndTime"                    AS "ConEVVEndTime",
    V1."CaregiverID"                   AS "CaregiverID",
    V1."AppCaregiverID"                AS "AppCaregiverID",
    V1."AideCode"                      AS "AideCode",
    V1."AideName"                      AS "AideName",
    V1."AideSSN"                       AS "AideSSN",
    V2."CaregiverID"                   AS "ConCaregiverID",
    V2."AppCaregiverID"                AS "ConAppCaregiverID",
    V2."AideCode"                      AS "ConAideCode",
    V2."AideName"                      AS "ConAideName",
    V2."AideSSN"                       AS "ConAideSSN",
    V1."OfficeID"                      AS "OfficeID",
    V1."AppOfficeID"                   AS "AppOfficeID",
    V1."Office"                        AS "Office",
    V2."OfficeID"                      AS "ConOfficeID",
    V2."AppOfficeID"                   AS "ConAppOfficeID",
    V2."Office"                        AS "ConOffice",
    V1."PatientID"                     AS "PatientID",
    V1."AppPatientID"                  AS "AppPatientID",
    CAST(NULL AS VARCHAR)              AS "PAdmissionID",
    CAST(NULL AS VARCHAR)              AS "PName",
    CAST(NULL AS UUID)                 AS "PAddressID",
    CAST(NULL AS BIGINT)               AS "PAppAddressID",
    CAST(NULL AS VARCHAR)              AS "PAddressL1",
    CAST(NULL AS VARCHAR)              AS "PAddressL2",
    CAST(NULL AS VARCHAR)              AS "PCity",
    CAST(NULL AS VARCHAR)              AS "PAddressState",
    CAST(NULL AS VARCHAR)              AS "PZipCode",
    CAST(NULL AS VARCHAR)              AS "PCounty",
    V1."Longitude"                     AS "PLongitude",
    V1."Latitude"                      AS "PLatitude",
    V2."PatientID"                     AS "ConPatientID",
    V2."AppPatientID"                  AS "ConAppPatientID",
    CAST(NULL AS VARCHAR)              AS "ConPAdmissionID",
    CAST(NULL AS VARCHAR)              AS "ConPName",
    CAST(NULL AS UUID)                 AS "ConPAddressID",
    CAST(NULL AS BIGINT)               AS "ConPAppAddressID",
    CAST(NULL AS VARCHAR)              AS "ConPAddressL1",
    CAST(NULL AS VARCHAR)              AS "ConPAddressL2",
    CAST(NULL AS VARCHAR)              AS "ConPCity",
    CAST(NULL AS VARCHAR)              AS "ConPAddressState",
    CAST(NULL AS VARCHAR)              AS "ConPZipCode",
    CAST(NULL AS VARCHAR)              AS "ConPCounty",
    V2."Longitude"                     AS "ConPLongitude",
    V2."Latitude"                      AS "ConPLatitude",
    V1."PayerID"                       AS "PayerID",
    V1."AppPayerID"                    AS "AppPayerID",
    V1."Contract"                      AS "Contract",
    V2."PayerID"                       AS "ConPayerID",
    V2."AppPayerID"                    AS "ConAppPayerID",
    V2."Contract"                      AS "ConContract",
    V1."BilledDate"                    AS "BilledDate",
    V2."BilledDate"                    AS "ConBilledDate",
    V1."BilledHours"                   AS "BilledHours",
    V2."BilledHours"                   AS "ConBilledHours",
    V1."Billed"                        AS "Billed",
    V2."Billed"                        AS "ConBilled",
    -- MinuteDiffBetweenSch: absolute minute difference between visit end and start times
    ABS(EXTRACT(EPOCH FROM (V1."VisitEndTime" - V2."VisitStartTime")) / 60)::INTEGER AS "MinuteDiffBetweenSch",
    -- DistanceMilesFromLatLng: Haversine formula for great-circle distance in miles
    ROUND(
        (
            (
                6371000 * 2 * ASIN(SQRT(
                    POWER(SIN(RADIANS((V2."Latitude" - V1."Latitude") / 2)), 2) +
                    COS(RADIANS(V1."Latitude")) * COS(RADIANS(V2."Latitude")) *
                    POWER(SIN(RADIANS((V2."Longitude" - V1."Longitude") / 2)), 2)
                ))
                / 1609.34
            ) * SETT."ExtraDistancePer"
        )::numeric,
        2
    )                                  AS "DistanceMilesFromLatLng",
    MPH."AverageMilesPerHour"          AS "AverageMilesPerHour",
    -- ETATravelMinutes: estimated travel time in minutes
    ROUND(
        ((ROUND(
            (
                (
                    6371000 * 2 * ASIN(SQRT(
                        POWER(SIN(RADIANS((V2."Latitude" - V1."Latitude") / 2)), 2) +
                        COS(RADIANS(V1."Latitude")) * COS(RADIANS(V2."Latitude")) *
                        POWER(SIN(RADIANS((V2."Longitude" - V1."Longitude") / 2)), 2)
                    ))
                    / 1609.34
                ) * SETT."ExtraDistancePer"
            )::numeric,
            2
        ) / NULLIF(MPH."AverageMilesPerHour"::numeric, 0)) * 60)::numeric,
        2
    )                                  AS "ETATravelMinutes",
    V1."ServiceCodeID"                 AS "ServiceCodeID",
    V1."AppServiceCodeID"              AS "AppServiceCodeID",
    V1."RateType"                      AS "RateType",
    V1."ServiceCode"                   AS "ServiceCode",
    V2."ServiceCodeID"                 AS "ConServiceCodeID",
    V2."AppServiceCodeID"              AS "ConAppServiceCodeID",
    V2."RateType"                      AS "ConRateType",
    V2."ServiceCode"                   AS "ConServiceCode",
    -- RULE 1: SameSchTimeFlag - both visits have no actual times, identical scheduled times (future only)
    CASE
        WHEN V1."ProviderID" <> V2."ProviderID"
         AND V1."VisitStartTime" IS NULL AND V1."VisitEndTime" IS NULL
         AND V2."VisitStartTime" IS NULL AND V2."VisitEndTime" IS NULL
         AND CONCAT(V1."SchStartTime", '~', V1."SchEndTime") = CONCAT(V2."SchStartTime", '~', V2."SchEndTime")
         AND V1."VisitDate" >= CURRENT_DATE
        THEN 'Y'
        ELSE 'N'
    END                                AS "SameSchTimeFlag",
    -- RULE 2: SameVisitTimeFlag - both visits have identical actual visit times
    CASE
        WHEN V1."ProviderID" <> V2."ProviderID"
         AND V1."VisitStartTime" IS NOT NULL AND V1."VisitEndTime" IS NOT NULL
         AND V2."VisitStartTime" IS NOT NULL AND V2."VisitEndTime" IS NOT NULL
         AND CONCAT(V1."VisitStartTime", '~', V1."VisitEndTime") = CONCAT(V2."VisitStartTime", '~', V2."VisitEndTime")
        THEN 'Y'
        ELSE 'N'
    END                                AS "SameVisitTimeFlag",
    -- RULE 3: SchAndVisitTimeSameFlag - one visit's scheduled = another's actual
    CASE
        WHEN V1."ProviderID" <> V2."ProviderID"
         AND V1."VisitStartTime" IS NULL AND V1."VisitEndTime" IS NULL
         AND V2."VisitStartTime" IS NOT NULL AND V2."VisitEndTime" IS NOT NULL
         AND CONCAT(V1."SchStartTime", '~', V1."SchEndTime") = CONCAT(V2."VisitStartTime", '~', V2."VisitEndTime")
        THEN 'Y'
        ELSE 'N'
    END                                AS "SchAndVisitTimeSameFlag",
    -- RULE 4: SchOverAnotherSchTimeFlag - scheduled times overlap (not identical, future only)
    CASE
        WHEN V1."ProviderID" <> V2."ProviderID"
         AND V1."VisitStartTime" IS NULL AND V1."VisitEndTime" IS NULL
         AND V2."VisitStartTime" IS NULL AND V2."VisitEndTime" IS NULL
         AND V1."SchStartTime" < V2."SchEndTime"
         AND V1."SchEndTime" > V2."SchStartTime"
         AND CONCAT(V1."SchStartTime", '~', V1."SchEndTime") <> CONCAT(V2."SchStartTime", '~', V2."SchEndTime")
         AND V1."VisitDate" >= CURRENT_DATE
        THEN 'Y'
        ELSE 'N'
    END                                AS "SchOverAnotherSchTimeFlag",
    -- RULE 5: VisitTimeOverAnotherVisitTimeFlag - actual visit times overlap (not identical)
    CASE
        WHEN V1."ProviderID" <> V2."ProviderID"
         AND V1."VisitStartTime" IS NOT NULL AND V1."VisitEndTime" IS NOT NULL
         AND V2."VisitStartTime" IS NOT NULL AND V2."VisitEndTime" IS NOT NULL
         AND V1."VisitStartTime" < V2."VisitEndTime"
         AND V1."VisitEndTime" > V2."VisitStartTime"
         AND CONCAT(V1."VisitStartTime", '~', V1."VisitEndTime") <> CONCAT(V2."VisitStartTime", '~', V2."VisitEndTime")
        THEN 'Y'
        ELSE 'N'
    END                                AS "VisitTimeOverAnotherVisitTimeFlag",
    -- RULE 6: SchTimeOverVisitTimeFlag - scheduled time overlaps another's actual visit time
    CASE
        WHEN V1."ProviderID" <> V2."ProviderID"
         AND V1."VisitStartTime" IS NULL AND V1."VisitEndTime" IS NULL
         AND V2."VisitStartTime" IS NOT NULL AND V2."VisitEndTime" IS NOT NULL
         AND V1."SchStartTime" < V2."VisitEndTime"
         AND V1."SchEndTime" > V2."VisitStartTime"
         AND CONCAT(V1."SchStartTime", '~', V1."SchEndTime") <> CONCAT(V2."VisitStartTime", '~', V2."VisitEndTime")
        THEN 'Y'
        ELSE 'N'
    END                                AS "SchTimeOverVisitTimeFlag",
    -- RULE 7: DistanceFlag - impossible travel (ETA > actual time gap)
    CASE
        WHEN V1."ProviderID" <> V2."ProviderID"
         AND V1."Longitude" IS NOT NULL AND V1."Latitude" IS NOT NULL
         AND V2."Longitude" IS NOT NULL AND V2."Latitude" IS NOT NULL
         AND V1."VisitStartTime" IS NOT NULL AND V1."VisitEndTime" IS NOT NULL
         AND V2."VisitStartTime" IS NOT NULL AND V2."VisitEndTime" IS NOT NULL
         AND (
             (V1."P_PZipCode" IS NOT NULL AND V2."P_PZipCode" IS NOT NULL AND V1."P_PZipCode" <> V2."P_PZipCode")
             OR V1."P_PZipCode" IS NULL
             OR V2."P_PZipCode" IS NULL
         )
         AND MPH."AverageMilesPerHour" IS NOT NULL
         AND ABS(EXTRACT(EPOCH FROM (V1."VisitEndTime" - V2."VisitStartTime")) / 60) >= 0
         AND (
             ROUND(
                 ((ROUND(
                     (
                         (
                             6371000 * 2 * ASIN(SQRT(
                                 POWER(SIN(RADIANS((V2."Latitude" - V1."Latitude") / 2)), 2) +
                                 COS(RADIANS(V1."Latitude")) * COS(RADIANS(V2."Latitude")) *
                                 POWER(SIN(RADIANS((V2."Longitude" - V1."Longitude") / 2)), 2)
                             ))
                             / 1609.34
                         ) * SETT."ExtraDistancePer"
                     )::numeric,
                     2
                 ) / NULLIF(MPH."AverageMilesPerHour"::numeric, 0)) * 60)::numeric,
                 2
             ) > ABS(EXTRACT(EPOCH FROM (V1."VisitEndTime" - V2."VisitStartTime")) / 60)
         )
        THEN 'Y'
        ELSE 'N'
    END                                AS "DistanceFlag",
    'N'                                AS "InServiceFlag",
    'N'                                AS "PTOFlag",
    V1."AideFName",
    V1."AideLName",
    V2."AideFName"                     AS "ConAideFName",
    V2."AideLName"                     AS "ConAideLName",
    V1."PFName",
    V1."PLName",
    V2."PFName"                        AS "ConPFName",
    V2."PLName"                        AS "ConPLName",
    V1."PMedicaidNumber",
    V2."PMedicaidNumber"               AS "ConPMedicaidNumber",
    V1."PayerState",
    V2."PayerState"                    AS "ConPayerState",
    CAST(NULL AS VARCHAR)              AS "AgencyContact",
    CAST(NULL AS VARCHAR)              AS "ConAgencyContact",
    V1."AgencyPhone",
    V2."AgencyPhone"                   AS "ConAgencyPhone",
    V1."LastUpdatedBy",
    V2."LastUpdatedBy"                 AS "ConLastUpdatedBy",
    V1."LastUpdatedDate",
    V2."LastUpdatedDate"               AS "ConLastUpdatedDate",
    V1."BilledRate",
    V1."TotalBilledAmount",
    V2."BilledRate"                    AS "ConBilledRate",
    V2."TotalBilledAmount"             AS "ConTotalBilledAmount",
    V1."IsMissed",
    V1."MissedVisitReason",
    V1."EVVType",
    V2."IsMissed"                      AS "ConIsMissed",
    V2."MissedVisitReason"             AS "ConMissedVisitReason",
    V2."EVVType"                       AS "ConEVVType",
    CAST(NULL AS VARCHAR)              AS "PStatus",
    CAST(NULL AS VARCHAR)              AS "ConPStatus",
    V1."AideStatus",
    V2."AideStatus"                    AS "ConAideStatus",
    V1."P_PatientID",
    V1."P_AppPatientID",
    V2."P_PatientID"                   AS "ConP_PatientID",
    V2."P_AppPatientID"                AS "ConP_AppPatientID",
    V1."PA_PatientID",
    V1."PA_AppPatientID",
    V2."PA_PatientID"                  AS "ConPA_PatientID",
    V2."PA_AppPatientID"               AS "ConPA_AppPatientID",
    V1."P_PAdmissionID",
    V1."P_PName",
    V1."P_PAddressID",
    V1."P_PAppAddressID",
    V1."P_PAddressL1",
    V1."P_PAddressL2",
    V1."P_PCity",
    V1."P_PAddressState",
    V1."P_PZipCode",
    V1."P_PCounty",
    V1."P_PFName",
    V1."P_PLName",
    V1."P_PMedicaidNumber",
    V2."P_PAdmissionID"                AS "ConP_PAdmissionID",
    V2."P_PName"                       AS "ConP_PName",
    V2."P_PAddressID"                  AS "ConP_PAddressID",
    V2."P_PAppAddressID"               AS "ConP_PAppAddressID",
    V2."P_PAddressL1"                  AS "ConP_PAddressL1",
    V2."P_PAddressL2"                  AS "ConP_PAddressL2",
    V2."P_PCity"                       AS "ConP_PCity",
    V2."P_PAddressState"               AS "ConP_PAddressState",
    V2."P_PZipCode"                    AS "ConP_PZipCode",
    V2."P_PCounty"                     AS "ConP_PCounty",
    V2."P_PFName"                      AS "ConP_PFName",
    V2."P_PLName"                      AS "ConP_PLName",
    V2."P_PMedicaidNumber"             AS "ConP_PMedicaidNumber",
    V1."PA_PAdmissionID",
    V1."PA_PName",
    V1."PA_PAddressID",
    V1."PA_PAppAddressID",
    V1."PA_PAddressL1",
    V1."PA_PAddressL2",
    V1."PA_PCity",
    V1."PA_PAddressState",
    V1."PA_PZipCode",
    V1."PA_PCounty",
    V1."PA_PFName",
    V1."PA_PLName",
    V1."PA_PMedicaidNumber",
    V2."PA_PAdmissionID"               AS "ConPA_PAdmissionID",
    V2."PA_PName"                      AS "ConPA_PName",
    V2."PA_PAddressID"                 AS "ConPA_PAddressID",
    V2."PA_PAppAddressID"              AS "ConPA_PAppAddressID",
    V2."PA_PAddressL1"                 AS "ConPA_PAddressL1",
    V2."PA_PAddressL2"                 AS "ConPA_PAddressL2",
    V2."PA_PCity"                      AS "ConPA_PCity",
    V2."PA_PAddressState"              AS "ConPA_PAddressState",
    V2."PA_PZipCode"                   AS "ConPA_PZipCode",
    V2."PA_PCounty"                    AS "ConPA_PCounty",
    V2."PA_PFName"                     AS "ConPA_PFName",
    V2."PA_PLName"                     AS "ConPA_PLName",
    V2."PA_PMedicaidNumber"            AS "ConPA_PMedicaidNumber",
    V1."ContractType",
    V2."ContractType"                  AS "ConContractType",
    V1."P_PStatus",
    V2."P_PStatus"                     AS "ConP_PStatus",
    V1."PA_PStatus",
    V2."PA_PStatus"                    AS "ConPA_PStatus",
    V1."BillRateNonBilled",
    V2."BillRateNonBilled"             AS "ConBillRateNonBilled",
    V1."BillRateBoth",
    V2."BillRateBoth"                  AS "ConBillRateBoth",
    NOW()                              AS "CreatedDate"
FROM
    -- V1: Base visits with existing CONFLICTID (if any)
    (
        SELECT DISTINCT
            CVM1."CONFLICTID"                       AS "CONFLICTID",
            CR1."Bill Rate Non-Billed"              AS "BillRateNonBilled",
            CASE
                WHEN CR1."Billed" = 'yes' THEN CR1."Billed Rate"
                ELSE CR1."Bill Rate Non-Billed"
            END                                     AS "BillRateBoth",
            TRIM(CAR."SSN")                         AS "SSN",
            CAR."Status"                            AS "AideStatus",
            CR1."Missed Visit Reason"               AS "MissedVisitReason",
            CR1."Is Missed"                         AS "IsMissed",
            CR1."Call Out Device Type"              AS "EVVType",
            CR1."Billed Rate"                       AS "BilledRate",
            CR1."Total Billed Amount"               AS "TotalBilledAmount",
            CR1."Provider Id"                       AS "ProviderID",
            CR1."Application Provider Id"           AS "AppProviderID",
            DPR."Provider Name"                     AS "ProviderName",
            DPR."Phone Number 1"                    AS "AgencyPhone",
            DPR."Federal Tax Number"                AS "FederalTaxNumber",
            CR1."Visit Id"                          AS "VisitID",
            CR1."Application Visit Id"              AS "AppVisitID",
            CR1."Visit Date"::date                  AS "VisitDate",
            CR1."Scheduled Start Time"::timestamp   AS "SchStartTime",
            CR1."Scheduled End Time"::timestamp     AS "SchEndTime",
            CR1."Visit Start Time"::timestamp       AS "VisitStartTime",
            CR1."Visit End Time"::timestamp         AS "VisitEndTime",
            CR1."Call In Time"::timestamp           AS "EVVStartTime",
            CR1."Call Out Time"::timestamp          AS "EVVEndTime",
            CR1."Caregiver Id"                      AS "CaregiverID",
            CR1."Application Caregiver Id"          AS "AppCaregiverID",
            CAR."Caregiver Code"                    AS "AideCode",
            CAR."Caregiver Fullname"                AS "AideName",
            CAR."Caregiver Firstname"               AS "AideFName",
            CAR."Caregiver Lastname"                AS "AideLName",
            TRIM(CAR."SSN")                         AS "AideSSN",
            CR1."Office Id"                         AS "OfficeID",
            CR1."Application Office Id"             AS "AppOfficeID",
            DOF."Office Name"                       AS "Office",
            CR1."Payer Patient Id"                  AS "PA_PatientID",
            CR1."Application Payer Patient Id"      AS "PA_AppPatientID",
            CR1."Provider Patient Id"               AS "P_PatientID",
            CR1."Application Provider Patient Id"   AS "P_AppPatientID",
            CR1."Patient Id"                        AS "PatientID",
            CR1."Application Patient Id"            AS "AppPatientID",
            -- Longitude: prefer Call Out, then Call In, then provider address
            CASE
                WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" <> ','
                THEN CAST(NULLIF(TRIM(SPLIT_PART(REPLACE(CR1."Call Out GPS Coordinates", '"', ''), ',', 2)), '') AS DOUBLE PRECISION)
                WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" <> ','
                THEN CAST(NULLIF(TRIM(SPLIT_PART(REPLACE(CR1."Call In GPS Coordinates", '"', ''), ',', 2)), '') AS DOUBLE PRECISION)
                ELSE DPAD_P."Provider_Longitude"
            END                                     AS "Longitude",
            -- Latitude: prefer Call Out, then Call In, then provider address
            CASE
                WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" <> ','
                THEN CAST(NULLIF(TRIM(SPLIT_PART(REPLACE(CR1."Call Out GPS Coordinates", '"', ''), ',', 1)), '') AS DOUBLE PRECISION)
                WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" <> ','
                THEN CAST(NULLIF(TRIM(SPLIT_PART(REPLACE(CR1."Call In GPS Coordinates", '"', ''), ',', 1)), '') AS DOUBLE PRECISION)
                ELSE DPAD_P."Provider_Latitude"
            END                                     AS "Latitude",
            CR1."Payer Id"                          AS "PayerID",
            CR1."Application Payer Id"              AS "AppPayerID",
            COALESCE(SPA."Payer Name", DCON."Contract Name") AS "Contract",
            SPA."Payer State"                       AS "PayerState",
            CR1."Invoice Date"::timestamp           AS "BilledDate",
            CR1."Billed Hours"                      AS "BilledHours",
            CR1."Billed"                            AS "Billed",
            DSC."Service Code Id"                   AS "ServiceCodeID",
            DSC."Application Service Code Id"       AS "AppServiceCodeID",
            CR1."Bill Type"                         AS "RateType",
            DSC."Service Code"                      AS "ServiceCode",
            CR1."Visit Updated Timestamp"::timestamp AS "LastUpdatedDate",
            DUSR."User Fullname"                    AS "LastUpdatedBy",
            DPA_P."Admission Id"                    AS "P_PAdmissionID",
            DPA_P."Patient Name"                    AS "P_PName",
            DPA_P."Patient Firstname"               AS "P_PFName",
            DPA_P."Patient Lastname"                AS "P_PLName",
            DPA_P."Medicaid Number"                 AS "P_PMedicaidNumber",
            DPA_P."Status"                          AS "P_PStatus",
            DPAD_P."Patient Address Id"             AS "P_PAddressID",
            DPAD_P."Application Patient Address Id" AS "P_PAppAddressID",
            DPAD_P."Address Line 1"                 AS "P_PAddressL1",
            DPAD_P."Address Line 2"                 AS "P_PAddressL2",
            DPAD_P."City"                           AS "P_PCity",
            DPAD_P."Address State"                  AS "P_PAddressState",
            DPAD_P."Zip Code"                       AS "P_PZipCode",
            DPAD_P."County"                         AS "P_PCounty",
            DPA_PA."Admission Id"                   AS "PA_PAdmissionID",
            DPA_PA."Patient Name"                   AS "PA_PName",
            DPA_PA."Patient Firstname"              AS "PA_PFName",
            DPA_PA."Patient Lastname"               AS "PA_PLName",
            DPA_PA."Medicaid Number"                AS "PA_PMedicaidNumber",
            DPA_PA."Status"                         AS "PA_PStatus",
            DPAD_PA."Patient Address Id"            AS "PA_PAddressID",
            DPAD_PA."Application Patient Address Id" AS "PA_PAppAddressID",
            DPAD_PA."Address Line 1"                AS "PA_PAddressL1",
            DPAD_PA."Address Line 2"                AS "PA_PAddressL2",
            DPAD_PA."City"                          AS "PA_PCity",
            DPAD_PA."Address State"                 AS "PA_PAddressState",
            DPAD_PA."Zip Code"                      AS "PA_PZipCode",
            DPAD_PA."County"                        AS "PA_PCounty",
            DPA_P."Patient Firstname"               AS "PFName",
            DPA_P."Patient Lastname"                AS "PLName",
            DPA_P."Medicaid Number"                 AS "PMedicaidNumber",
            CASE
                WHEN CR1."Application Payer Id" = '0' AND CR1."Application Contract Id" <> '0' THEN 'Internal'
                WHEN CR1."Application Payer Id" <> '0' AND CR1."Application Contract Id" <> '0' THEN 'UPR'
                WHEN CR1."Application Payer Id" <> '0' AND CR1."Application Contract Id" = '0' THEN 'Payer'
            END                                     AS "ContractType"
        FROM {analytics_schema}.factvisitcallperformance_cr AS CR1
        INNER JOIN {analytics_schema}.dimcaregiver AS CAR
            ON CAR."Caregiver Id" = CR1."Caregiver Id"
           AND TRIM(CAR."SSN") IS NOT NULL
           AND TRIM(CAR."SSN") <> ''
        LEFT JOIN {analytics_schema}.dimoffice AS DOF
            ON DOF."Office Id" = CR1."Office Id"
           AND DOF."Is Active" = TRUE
        LEFT JOIN {analytics_schema}.dimpatient AS DPA_P
            ON DPA_P."Patient Id" = CR1."Provider Patient Id"
        LEFT JOIN (
            SELECT
                DDD."Patient Address Id",
                DDD."Application Patient Address Id",
                DDD."Address Line 1",
                DDD."Address Line 2",
                DDD."City",
                DDD."Address State",
                DDD."Zip Code",
                DDD."County",
                DDD."Patient Id",
                DDD."Longitude" AS "Provider_Longitude",
                DDD."Latitude" AS "Provider_Latitude",
                ROW_NUMBER() OVER (
                    PARTITION BY DDD."Patient Id"
                    ORDER BY DDD."Application Created UTC Timestamp" DESC
                ) AS rn
            FROM {analytics_schema}.dimpatientaddress AS DDD
            WHERE DDD."Primary Address" = TRUE
              AND DDD."Address Type" LIKE '%GPS%'
        ) AS DPAD_P
            ON DPAD_P."Patient Id" = DPA_P."Patient Id"
           AND DPAD_P.rn = 1
        LEFT JOIN {analytics_schema}.dimpatient AS DPA_PA
            ON DPA_PA."Patient Id" = CR1."Payer Patient Id"
        LEFT JOIN (
            SELECT
                DDD."Patient Address Id",
                DDD."Application Patient Address Id",
                DDD."Address Line 1",
                DDD."Address Line 2",
                DDD."City",
                DDD."Address State",
                DDD."Zip Code",
                DDD."County",
                DDD."Patient Id",
                ROW_NUMBER() OVER (
                    PARTITION BY DDD."Patient Id"
                    ORDER BY DDD."Application Created UTC Timestamp" DESC
                ) AS rn
            FROM {analytics_schema}.dimpatientaddress AS DDD
            WHERE DDD."Primary Address" = TRUE
              AND DDD."Address Type" LIKE '%GPS%'
        ) AS DPAD_PA
            ON DPAD_PA."Patient Id" = DPA_PA."Patient Id"
           AND DPAD_PA.rn = 1
        LEFT JOIN {analytics_schema}.dimpayer AS SPA
            ON SPA."Payer Id" = CR1."Payer Id"
           AND SPA."Is Active" = TRUE
           AND SPA."Is Demo" = FALSE
        LEFT JOIN {analytics_schema}.dimcontract AS DCON
            ON DCON."Contract Id" = CR1."Contract Id"
           AND DCON."Is Active" = TRUE
        INNER JOIN {analytics_schema}.dimprovider AS DPR
            ON DPR."Provider Id" = CR1."Provider Id"
           AND DPR."Is Active" = TRUE
           AND DPR."Is Demo" = FALSE
        LEFT JOIN {conflict_schema}.conflictvisitmaps AS CVM1
            ON CVM1."VisitID"::varchar = CR1."Visit Id"::varchar
           AND CVM1."CONFLICTID" IS NOT NULL
        LEFT JOIN {analytics_schema}.dimservicecode AS DSC
            ON DSC."Service Code Id" = CR1."Service Code Id"
        LEFT JOIN {analytics_schema}.dimuser AS DUSR
            ON DUSR."User Id"::varchar = CR1."Visit Updated User Id"::varchar
        WHERE CR1."Is Missed" = FALSE
          AND CR1."Visit Date"::date
              BETWEEN (NOW() - INTERVAL '2 years')::date
                  AND (NOW() + INTERVAL '45 days')::date
          AND CR1."Provider Id" NOT IN (
                SELECT "ProviderID" FROM {conflict_schema}.excluded_agency
          )
          AND NOT EXISTS (
                SELECT 1
                FROM {conflict_schema}.excluded_ssn AS SSN
                WHERE TRIM(CAR."SSN") = SSN."SSN"
          )
          AND {chunk_filter_inner}  -- ⭐ CHUNK FILTER: Only scan rows for this chunk
    ) AS V1
LEFT JOIN
    -- V2: Conflicting visits (no CONFLICTID)
    (
        SELECT DISTINCT
            CAST(NULL AS BIGINT)                    AS "CONFLICTID",
            CR1."Bill Rate Non-Billed"              AS "BillRateNonBilled",
            CASE
                WHEN CR1."Billed" = 'yes' THEN CR1."Billed Rate"
                ELSE CR1."Bill Rate Non-Billed"
            END                                     AS "BillRateBoth",
            TRIM(CAR."SSN")                         AS "SSN",
            CAR."Status"                            AS "AideStatus",
            CR1."Missed Visit Reason"               AS "MissedVisitReason",
            CR1."Is Missed"                         AS "IsMissed",
            CR1."Call Out Device Type"              AS "EVVType",
            CR1."Billed Rate"                       AS "BilledRate",
            CR1."Total Billed Amount"               AS "TotalBilledAmount",
            CR1."Provider Id"                       AS "ProviderID",
            CR1."Application Provider Id"           AS "AppProviderID",
            DPR."Provider Name"                     AS "ProviderName",
            DPR."Phone Number 1"                    AS "AgencyPhone",
            DPR."Federal Tax Number"                AS "FederalTaxNumber",
            CR1."Visit Id"                          AS "VisitID",
            CR1."Application Visit Id"              AS "AppVisitID",
            CR1."Visit Date"::date                  AS "VisitDate",
            CR1."Scheduled Start Time"::timestamp   AS "SchStartTime",
            CR1."Scheduled End Time"::timestamp     AS "SchEndTime",
            CR1."Visit Start Time"::timestamp       AS "VisitStartTime",
            CR1."Visit End Time"::timestamp         AS "VisitEndTime",
            CR1."Call In Time"::timestamp           AS "EVVStartTime",
            CR1."Call Out Time"::timestamp          AS "EVVEndTime",
            CR1."Caregiver Id"                      AS "CaregiverID",
            CR1."Application Caregiver Id"          AS "AppCaregiverID",
            CAR."Caregiver Code"                    AS "AideCode",
            CAR."Caregiver Fullname"                AS "AideName",
            CAR."Caregiver Firstname"               AS "AideFName",
            CAR."Caregiver Lastname"                AS "AideLName",
            TRIM(CAR."SSN")                         AS "AideSSN",
            CR1."Office Id"                         AS "OfficeID",
            CR1."Application Office Id"             AS "AppOfficeID",
            DOF."Office Name"                       AS "Office",
            CR1."Payer Patient Id"                  AS "PA_PatientID",
            CR1."Application Payer Patient Id"      AS "PA_AppPatientID",
            CR1."Provider Patient Id"               AS "P_PatientID",
            CR1."Application Provider Patient Id"   AS "P_AppPatientID",
            CR1."Patient Id"                        AS "PatientID",
            CR1."Application Patient Id"            AS "AppPatientID",
            -- Longitude: prefer Call In, then Call Out, then provider address
            CASE
                WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" <> ','
                THEN CAST(NULLIF(TRIM(SPLIT_PART(REPLACE(CR1."Call In GPS Coordinates", '"', ''), ',', 2)), '') AS DOUBLE PRECISION)
                WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" <> ','
                THEN CAST(NULLIF(TRIM(SPLIT_PART(REPLACE(CR1."Call Out GPS Coordinates", '"', ''), ',', 2)), '') AS DOUBLE PRECISION)
                ELSE DPAD_P."Provider_Longitude"
            END                                     AS "Longitude",
            -- Latitude: prefer Call In, then Call Out, then provider address
            CASE
                WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" <> ','
                THEN CAST(NULLIF(TRIM(SPLIT_PART(REPLACE(CR1."Call In GPS Coordinates", '"', ''), ',', 1)), '') AS DOUBLE PRECISION)
                WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" <> ','
                THEN CAST(NULLIF(TRIM(SPLIT_PART(REPLACE(CR1."Call Out GPS Coordinates", '"', ''), ',', 1)), '') AS DOUBLE PRECISION)
                ELSE DPAD_P."Provider_Latitude"
            END                                     AS "Latitude",
            CR1."Payer Id"                          AS "PayerID",
            CR1."Application Payer Id"              AS "AppPayerID",
            COALESCE(SPA."Payer Name", DCON."Contract Name") AS "Contract",
            SPA."Payer State"                       AS "PayerState",
            CR1."Invoice Date"::timestamp           AS "BilledDate",
            CR1."Billed Hours"                      AS "BilledHours",
            CR1."Billed"                            AS "Billed",
            DSC."Service Code Id"                   AS "ServiceCodeID",
            DSC."Application Service Code Id"       AS "AppServiceCodeID",
            CR1."Bill Type"                         AS "RateType",
            DSC."Service Code"                      AS "ServiceCode",
            CR1."Visit Updated Timestamp"::timestamp AS "LastUpdatedDate",
            DUSR."User Fullname"                    AS "LastUpdatedBy",
            DPA_P."Admission Id"                    AS "P_PAdmissionID",
            DPA_P."Patient Name"                    AS "P_PName",
            DPA_P."Patient Firstname"               AS "P_PFName",
            DPA_P."Patient Lastname"                AS "P_PLName",
            DPA_P."Medicaid Number"                 AS "P_PMedicaidNumber",
            DPA_P."Status"                          AS "P_PStatus",
            DPAD_P."Patient Address Id"             AS "P_PAddressID",
            DPAD_P."Application Patient Address Id" AS "P_PAppAddressID",
            DPAD_P."Address Line 1"                 AS "P_PAddressL1",
            DPAD_P."Address Line 2"                 AS "P_PAddressL2",
            DPAD_P."City"                           AS "P_PCity",
            DPAD_P."Address State"                  AS "P_PAddressState",
            DPAD_P."Zip Code"                       AS "P_PZipCode",
            DPAD_P."County"                         AS "P_PCounty",
            DPA_PA."Admission Id"                   AS "PA_PAdmissionID",
            DPA_PA."Patient Name"                   AS "PA_PName",
            DPA_PA."Patient Firstname"              AS "PA_PFName",
            DPA_PA."Patient Lastname"               AS "PA_PLName",
            DPA_PA."Medicaid Number"                AS "PA_PMedicaidNumber",
            DPA_PA."Status"                         AS "PA_PStatus",
            DPAD_PA."Patient Address Id"            AS "PA_PAddressID",
            DPAD_PA."Application Patient Address Id" AS "PA_PAppAddressID",
            DPAD_PA."Address Line 1"                AS "PA_PAddressL1",
            DPAD_PA."Address Line 2"                AS "PA_PAddressL2",
            DPAD_PA."City"                          AS "PA_PCity",
            DPAD_PA."Address State"                 AS "PA_PAddressState",
            DPAD_PA."Zip Code"                      AS "PA_PZipCode",
            DPAD_PA."County"                        AS "PA_PCounty",
            DPA_P."Patient Firstname"               AS "PFName",
            DPA_P."Patient Lastname"                AS "PLName",
            DPA_P."Medicaid Number"                 AS "PMedicaidNumber",
            CASE
                WHEN CR1."Application Payer Id" = '0' AND CR1."Application Contract Id" <> '0' THEN 'Internal'
                WHEN CR1."Application Payer Id" <> '0' AND CR1."Application Contract Id" <> '0' THEN 'UPR'
                WHEN CR1."Application Payer Id" <> '0' AND CR1."Application Contract Id" = '0' THEN 'Payer'
            END                                     AS "ContractType"
        FROM {analytics_schema}.factvisitcallperformance_cr AS CR1
        INNER JOIN {analytics_schema}.dimcaregiver AS CAR
            ON CAR."Caregiver Id" = CR1."Caregiver Id"
           AND TRIM(CAR."SSN") IS NOT NULL
           AND TRIM(CAR."SSN") <> ''
        LEFT JOIN {analytics_schema}.dimoffice AS DOF
            ON DOF."Office Id" = CR1."Office Id"
           AND DOF."Is Active" = TRUE
        LEFT JOIN {analytics_schema}.dimpatient AS DPA_P
            ON DPA_P."Patient Id" = CR1."Provider Patient Id"
        LEFT JOIN (
            SELECT
                DDD."Patient Address Id",
                DDD."Application Patient Address Id",
                DDD."Address Line 1",
                DDD."Address Line 2",
                DDD."City",
                DDD."Address State",
                DDD."Zip Code",
                DDD."County",
                DDD."Patient Id",
                DDD."Longitude" AS "Provider_Longitude",
                DDD."Latitude" AS "Provider_Latitude",
                ROW_NUMBER() OVER (
                    PARTITION BY DDD."Patient Id"
                    ORDER BY DDD."Application Created UTC Timestamp" DESC
                ) AS rn
            FROM {analytics_schema}.dimpatientaddress AS DDD
            WHERE DDD."Primary Address" = TRUE
              AND DDD."Address Type" LIKE '%GPS%'
        ) AS DPAD_P
            ON DPAD_P."Patient Id" = DPA_P."Patient Id"
           AND DPAD_P.rn = 1
        LEFT JOIN {analytics_schema}.dimpatient AS DPA_PA
            ON DPA_PA."Patient Id" = CR1."Payer Patient Id"
        LEFT JOIN (
            SELECT
                DDD."Patient Address Id",
                DDD."Application Patient Address Id",
                DDD."Address Line 1",
                DDD."Address Line 2",
                DDD."City",
                DDD."Address State",
                DDD."Zip Code",
                DDD."County",
                DDD."Patient Id",
                ROW_NUMBER() OVER (
                    PARTITION BY DDD."Patient Id"
                    ORDER BY DDD."Application Created UTC Timestamp" DESC
                ) AS rn
            FROM {analytics_schema}.dimpatientaddress AS DDD
            WHERE DDD."Primary Address" = TRUE
              AND DDD."Address Type" LIKE '%GPS%'
        ) AS DPAD_PA
            ON DPAD_PA."Patient Id" = DPA_PA."Patient Id"
           AND DPAD_PA.rn = 1
        LEFT JOIN {analytics_schema}.dimpayer AS SPA
            ON SPA."Payer Id" = CR1."Payer Id"
           AND SPA."Is Active" = TRUE
           AND SPA."Is Demo" = FALSE
        LEFT JOIN {analytics_schema}.dimcontract AS DCON
            ON DCON."Contract Id" = CR1."Contract Id"
           AND DCON."Is Active" = TRUE
        INNER JOIN {analytics_schema}.dimprovider AS DPR
            ON DPR."Provider Id" = CR1."Provider Id"
           AND DPR."Is Active" = TRUE
           AND DPR."Is Demo" = FALSE
        LEFT JOIN {analytics_schema}.dimservicecode AS DSC
            ON DSC."Service Code Id" = CR1."Service Code Id"
        LEFT JOIN {analytics_schema}.dimuser AS DUSR
            ON DUSR."User Id"::varchar = CR1."Visit Updated User Id"::varchar
        WHERE CR1."Is Missed" = FALSE
          AND CR1."Visit Date"::date
              BETWEEN (NOW() - INTERVAL '2 years')::date
                  AND (NOW() + INTERVAL '45 days')::date
          AND CR1."Provider Id" NOT IN (
                SELECT "ProviderID" FROM {conflict_schema}.excluded_agency
          )
          AND NOT EXISTS (
                SELECT 1
                FROM {conflict_schema}.excluded_ssn AS SSN
                WHERE TRIM(CAR."SSN") = SSN."SSN"
          )
          AND {chunk_filter_inner}  -- ⭐ CHUNK FILTER: Only scan rows for this chunk
    ) AS V2
    ON V1."SSN" = V2."SSN"
   AND V1."VisitDate" = V2."VisitDate"
   AND V1."ProviderID" <> V2."ProviderID"
   AND V1."VisitID" <> V2."VisitID"
CROSS JOIN {conflict_schema}.settings AS SETT
LEFT JOIN {conflict_schema}.mph AS MPH
    ON ROUND(
        (
            (
                6371000 * 2 * ASIN(SQRT(
                    POWER(SIN(RADIANS((V2."Latitude" - V1."Latitude") / 2)), 2) +
                    COS(RADIANS(V1."Latitude")) * COS(RADIANS(V2."Latitude")) *
                    POWER(SIN(RADIANS((V2."Longitude" - V1."Longitude") / 2)), 2)
                ))
                / 1609.34
            ) * SETT."ExtraDistancePer"
        )::numeric,
        2
    ) BETWEEN MPH."From" AND MPH."To"
WHERE
    -- Apply the 7-rule conflict filter
    (
        -- RULE 1: SameSchTimeFlag - both have no visit times, identical scheduled times (future only)
        (V1."VisitStartTime" IS NULL AND V1."VisitEndTime" IS NULL
         AND V2."VisitStartTime" IS NULL AND V2."VisitEndTime" IS NULL
         AND CONCAT(V1."SchStartTime", '~', V1."SchEndTime") = CONCAT(V2."SchStartTime", '~', V2."SchEndTime")
         AND V1."VisitDate" >= CURRENT_DATE)
     OR
        -- RULE 2: SameVisitTimeFlag - both have identical actual visit times
        (V1."VisitStartTime" IS NOT NULL AND V1."VisitEndTime" IS NOT NULL
         AND V2."VisitStartTime" IS NOT NULL AND V2."VisitEndTime" IS NOT NULL
         AND CONCAT(V1."VisitStartTime", '~', V1."VisitEndTime") = CONCAT(V2."VisitStartTime", '~', V2."VisitEndTime"))
     OR
        -- RULE 3: SchVisitTimeSame - one's scheduled = another's actual
        (V1."VisitStartTime" IS NULL AND V1."VisitEndTime" IS NULL
         AND V2."VisitStartTime" IS NOT NULL AND V2."VisitEndTime" IS NOT NULL
         AND CONCAT(V1."SchStartTime", '~', V1."SchEndTime") = CONCAT(V2."VisitStartTime", '~', V2."VisitEndTime"))
     OR
        -- RULE 4: SchOverAnotherSchTimeFlag - scheduled times overlap (not identical, future only)
        (V1."VisitStartTime" IS NULL AND V1."VisitEndTime" IS NULL
         AND V2."VisitStartTime" IS NULL AND V2."VisitEndTime" IS NULL
         AND V1."SchStartTime" < V2."SchEndTime"
         AND V1."SchEndTime" > V2."SchStartTime"
         AND CONCAT(V1."SchStartTime", '~', V1."SchEndTime") <> CONCAT(V2."SchStartTime", '~', V2."SchEndTime")
         AND V1."VisitDate" >= CURRENT_DATE)
     OR
        -- RULE 5: VisitTimeOverAnotherVisitTimeFlag - actual visit times overlap (not identical)
        (V1."VisitStartTime" IS NOT NULL AND V1."VisitEndTime" IS NOT NULL
         AND V2."VisitStartTime" IS NOT NULL AND V2."VisitEndTime" IS NOT NULL
         AND V1."VisitStartTime" < V2."VisitEndTime"
         AND V1."VisitEndTime" > V2."VisitStartTime"
         AND CONCAT(V1."VisitStartTime", '~', V1."VisitEndTime") <> CONCAT(V2."VisitStartTime", '~', V2."VisitEndTime"))
     OR
        -- RULE 6: SchTimeOverVisitTimeFlag - scheduled overlaps another's actual visit time
        (V1."VisitStartTime" IS NULL AND V1."VisitEndTime" IS NULL
         AND V2."VisitStartTime" IS NOT NULL AND V2."VisitEndTime" IS NOT NULL
         AND V1."SchStartTime" < V2."VisitEndTime"
         AND V1."SchEndTime" > V2."VisitStartTime"
         AND CONCAT(V1."SchStartTime", '~', V1."SchEndTime") <> CONCAT(V2."VisitStartTime", '~', V2."VisitEndTime"))
     OR
        -- RULE 7: DistanceFlag - impossible travel (ETA > actual time gap)
        (V1."Longitude" IS NOT NULL AND V1."Latitude" IS NOT NULL
         AND V2."Longitude" IS NOT NULL AND V2."Latitude" IS NOT NULL
         AND V1."VisitStartTime" IS NOT NULL AND V1."VisitEndTime" IS NOT NULL
         AND V2."VisitStartTime" IS NOT NULL AND V2."VisitEndTime" IS NOT NULL
         AND ((V1."P_PZipCode" IS NOT NULL AND V2."P_PZipCode" IS NOT NULL AND V1."P_PZipCode" <> V2."P_PZipCode")
              OR V1."P_PZipCode" IS NULL OR V2."P_PZipCode" IS NULL)
         AND MPH."AverageMilesPerHour" IS NOT NULL
         AND ABS(EXTRACT(EPOCH FROM (V1."VisitEndTime" - V2."VisitStartTime")) / 60) >= 0
         AND (
             ROUND(
                 ((ROUND(
                     (
                         (
                             6371000 * 2 * ASIN(SQRT(
                                 POWER(SIN(RADIANS((V2."Latitude" - V1."Latitude") / 2)), 2) +
                                 COS(RADIANS(V1."Latitude")) * COS(RADIANS(V2."Latitude")) *
                                 POWER(SIN(RADIANS((V2."Longitude" - V1."Longitude") / 2)), 2)
                             ))
                             / 1609.34
                         ) * SETT."ExtraDistancePer"
                     )::numeric,
                     2
                 ) / NULLIF(MPH."AverageMilesPerHour"::numeric, 0)) * 60)::numeric,
                 2
             ) > ABS(EXTRACT(EPOCH FROM (V1."VisitEndTime" - V2."VisitStartTime")) / 60)
         ))
    )
    -- Idempotent: Only insert if this conflict pair doesn't already exist
    AND NOT EXISTS (
        SELECT 1
        FROM {conflict_schema}.conflictvisitmaps AS CVM
        WHERE COALESCE(CVM."VisitID"::varchar, '') = COALESCE(V1."VisitID"::varchar, '')
          AND COALESCE(CVM."ConVisitID"::varchar, '') = COALESCE(V2."VisitID"::varchar, '')
    );
