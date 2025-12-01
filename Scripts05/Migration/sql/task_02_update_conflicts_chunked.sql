-- ======================================================================
-- UPDATE CONFLICTVISITMAPS FROM ANALYTICS (Postgres version - CHUNKED)
-- Converted from Snowflake stored procedure TASK_02_UPDATE_DATA_CONFLICTVISITMAPS_0
--
-- Parameters (string-substituted by Python migration script):
--   {conflict_schema}  - schema containing conflict tables (e.g. conflict_dev)
--   {analytics_schema} - schema containing analytics tables
--   {chunk_filter}     - WHERE clause for (VisitDate, SSN) filtering
--
-- Chunking Strategy:
-- - Uses composite (VisitDate, SSN) key for safe parallel processing
-- - Each chunk is completely independent (no conflict pairs split across chunks)
-- - Idempotent: Can safely re-run failed chunks
--
-- Key conversion notes:
-- - Uses Haversine formula for accurate great-circle distance calculation (no PostGIS required)
-- - DATEADD/DATEDIFF converted to PostgreSQL interval arithmetic and EXTRACT
-- - Distance calculations use pure SQL math functions (SIN, COS, ASIN, RADIANS, etc.)
-- - GETDATE() converted to NOW()
-- - STRING type converted to VARCHAR
-- - NUMBER type converted to DOUBLE PRECISION or BIGINT as appropriate
-- - CURRENT_TIMESTAMP() converted to NOW()
-- - SPLIT function converted to SPLIT_PART
-- - ROW_NUMBER() window functions preserved
-- - LEAST function preserved (same in both)
-- ======================================================================

-- Note: Transaction management is handled by the Python connector
-- Do not include explicit BEGIN/COMMIT statements
-- Note: Step 1 (Mark rows) is handled separately by the Python orchestrator
-- This SQL file only contains Step 2 (Main update)

-- Step 2: Main update - compute conflict pairs and update conflictvisitmaps
UPDATE {conflict_schema}.conflictvisitmaps AS CVM
SET
    "CONFLICTID"            = ALLDATA."CONFLICTID",
    "SSN"                   = ALLDATA."SSN",
    "ProviderID"            = ALLDATA."ProviderID",
    "AppProviderID"         = ALLDATA."AppProviderID",
    "ProviderName"          = ALLDATA."ProviderName",
    "VisitID"               = ALLDATA."VisitID",
    "AppVisitID"            = ALLDATA."AppVisitID",
    "ConProviderID"         = ALLDATA."ConProviderID",
    "ConAppProviderID"      = ALLDATA."ConAppProviderID",
    "ConProviderName"       = ALLDATA."ConProviderName",
    "ConVisitID"            = ALLDATA."ConVisitID",
    "ConAppVisitID"         = ALLDATA."ConAppVisitID",
    "VisitDate"             = ALLDATA."VisitDate",
    "SchStartTime"          = ALLDATA."SchStartTime",
    "SchEndTime"            = ALLDATA."SchEndTime",
    "ConSchStartTime"       = ALLDATA."ConSchStartTime",
    "ConSchEndTime"         = ALLDATA."ConSchEndTime",
    "VisitStartTime"        = ALLDATA."VisitStartTime",
    "VisitEndTime"          = ALLDATA."VisitEndTime",
    "ConVisitStartTime"     = ALLDATA."ConVisitStartTime",
    "ConVisitEndTime"       = ALLDATA."ConVisitEndTime",
    "EVVStartTime"          = ALLDATA."EVVStartTime",
    "EVVEndTime"            = ALLDATA."EVVEndTime",
    "ConEVVStartTime"       = ALLDATA."ConEVVStartTime",
    "ConEVVEndTime"         = ALLDATA."ConEVVEndTime",
    "CaregiverID"           = ALLDATA."CaregiverID",
    "AppCaregiverID"        = ALLDATA."AppCaregiverID",
    "AideCode"              = ALLDATA."AideCode",
    "AideName"              = ALLDATA."AideName",
    "AideSSN"               = ALLDATA."AideSSN",
    "ConCaregiverID"        = ALLDATA."ConCaregiverID",
    "ConAppCaregiverID"     = ALLDATA."ConAppCaregiverID",
    "ConAideCode"           = ALLDATA."ConAideCode",
    "ConAideName"           = ALLDATA."ConAideName",
    "ConAideSSN"            = ALLDATA."ConAideSSN",
    "OfficeID"              = ALLDATA."OfficeID",
    "AppOfficeID"           = ALLDATA."AppOfficeID",
    "Office"                = ALLDATA."Office",
    "ConOfficeID"           = ALLDATA."ConOfficeID",
    "ConAppOfficeID"        = ALLDATA."ConAppOfficeID",
    "ConOffice"             = ALLDATA."ConOffice",
    "PatientID"             = ALLDATA."PatientID",
    "AppPatientID"          = ALLDATA."AppPatientID",
    "PAdmissionID"          = ALLDATA."PAdmissionID",
    "PName"                 = ALLDATA."PName",
    "PAddressID"            = ALLDATA."PAddressID",
    "PAppAddressID"         = ALLDATA."PAppAddressID",
    "PAddressL1"            = ALLDATA."PAddressL1",
    "PAddressL2"            = ALLDATA."PAddressL2",
    "PCity"                 = ALLDATA."PCity",
    "PAddressState"         = ALLDATA."PAddressState",
    "PZipCode"              = ALLDATA."PZipCode",
    "PCounty"               = ALLDATA."PCounty",
    "PLongitude"            = ALLDATA."PLongitude",
    "PLatitude"             = ALLDATA."PLatitude",
    "ConPatientID"          = ALLDATA."ConPatientID",
    "ConAppPatientID"       = ALLDATA."ConAppPatientID",
    "ConPAdmissionID"       = ALLDATA."ConPAdmissionID",
    "ConPName"              = ALLDATA."ConPName",
    "ConPAddressID"         = ALLDATA."ConPAddressID",
    "ConPAppAddressID"      = ALLDATA."ConPAppAddressID",
    "ConPAddressL1"         = ALLDATA."ConPAddressL1",
    "ConPAddressL2"         = ALLDATA."ConPAddressL2",
    "ConPCity"              = ALLDATA."ConPCity",
    "ConPAddressState"      = ALLDATA."ConPAddressState",
    "ConPZipCode"           = ALLDATA."ConPZipCode",
    "ConPCounty"            = ALLDATA."ConPCounty",
    "ConPLongitude"         = ALLDATA."ConPLongitude",
    "ConPLatitude"          = ALLDATA."ConPLatitude",
    "PayerID"               = ALLDATA."PayerID",
    "AppPayerID"            = ALLDATA."AppPayerID",
    "Contract"              = ALLDATA."Contract",
    "ConPayerID"            = ALLDATA."ConPayerID",
    "ConAppPayerID"         = ALLDATA."ConAppPayerID",
    "ConContract"           = ALLDATA."ConContract",
    "BilledDate"            = ALLDATA."BilledDate",
    "ConBilledDate"         = ALLDATA."ConBilledDate",
    "BilledHours"           = ALLDATA."BilledHours",
    "ConBilledHours"        = ALLDATA."ConBilledHours",
    "Billed"                = ALLDATA."Billed",
    "ConBilled"             = ALLDATA."ConBilled",
    "MinuteDiffBetweenSch"  = ALLDATA."MinuteDiffBetweenSch",
    "DistanceMilesFromLatLng" = ALLDATA."DistanceMilesFromLatLng",
    "AverageMilesPerHour"   = ALLDATA."AverageMilesPerHour",
    "ETATravleMinutes"      = ALLDATA."ETATravleMinutes",
    "ServiceCodeID"         = ALLDATA."ServiceCodeID",
    "AppServiceCodeID"      = ALLDATA."AppServiceCodeID",
    "RateType"              = ALLDATA."RateType",
    "ServiceCode"           = ALLDATA."ServiceCode",
    "ConServiceCodeID"      = ALLDATA."ConServiceCodeID",
    "ConAppServiceCodeID"   = ALLDATA."ConAppServiceCodeID",
    "ConRateType"           = ALLDATA."ConRateType",
    "ConServiceCode"        = ALLDATA."ConServiceCode",
    "UpdateFlag"            = NULL,
    "UpdatedDate"           = NOW(),
    "StatusFlag"            = CASE WHEN CVM."StatusFlag" NOT IN ('W', 'I') THEN 'U' ELSE CVM."StatusFlag" END,
    "ResolveDate"           = NULL,
    "AideFName"             = ALLDATA."AideFName",
    "AideLName"             = ALLDATA."AideLName",
    "ConAideFName"          = ALLDATA."ConAideFName",
    "ConAideLName"          = ALLDATA."ConAideLName",
    "PFName"                = ALLDATA."PFName",
    "PLName"                = ALLDATA."PLName",
    "ConPFName"             = ALLDATA."ConPFName",
    "ConPLName"             = ALLDATA."ConPLName",
    "PMedicaidNumber"       = ALLDATA."PMedicaidNumber",
    "ConPMedicaidNumber"    = ALLDATA."ConPMedicaidNumber",
    "PayerState"            = ALLDATA."PayerState",
    "ConPayerState"         = ALLDATA."ConPayerState",
    "LastUpdatedBy"         = ALLDATA."LastUpdatedBy",
    "ConLastUpdatedBy"      = ALLDATA."ConLastUpdatedBy",
    "LastUpdatedDate"       = ALLDATA."LastUpdatedDate",
    "ConLastUpdatedDate"    = ALLDATA."ConLastUpdatedDate",
    "BilledRate"            = ALLDATA."BilledRate",
    "TotalBilledAmount"     = ALLDATA."TotalBilledAmount",
    "ConBilledRate"         = ALLDATA."ConBilledRate",
    "ConTotalBilledAmount"  = ALLDATA."ConTotalBilledAmount",
    "IsMissed"              = ALLDATA."IsMissed",
    "MissedVisitReason"     = ALLDATA."MissedVisitReason",
    "EVVType"               = ALLDATA."EVVType",
    "ConIsMissed"           = ALLDATA."ConIsMissed",
    "ConMissedVisitReason"  = ALLDATA."ConMissedVisitReason",
    "ConEVVType"            = ALLDATA."ConEVVType",
    "PStatus"               = ALLDATA."PStatus",
    "ConPStatus"            = ALLDATA."ConPStatus",
    "AideStatus"            = ALLDATA."AideStatus",
    "ConAideStatus"         = ALLDATA."ConAideStatus",
    "P_PatientID"           = ALLDATA."P_PatientID",
    "P_AppPatientID"        = ALLDATA."P_AppPatientID",
    "ConP_PatientID"        = ALLDATA."ConP_PatientID",
    "ConP_AppPatientID"     = ALLDATA."ConP_AppPatientID",
    "PA_PatientID"          = ALLDATA."PA_PatientID",
    "PA_AppPatientID"       = ALLDATA."PA_AppPatientID",
    "ConPA_PatientID"       = ALLDATA."ConPA_PatientID",
    "ConPA_AppPatientID"    = ALLDATA."ConPA_AppPatientID",
    "P_PAdmissionID"        = ALLDATA."P_PAdmissionID",
    "P_PName"               = ALLDATA."P_PName",
    "P_PAddressID"          = ALLDATA."P_PAddressID",
    "P_PAppAddressID"       = ALLDATA."P_PAppAddressID",
    "P_PAddressL1"          = ALLDATA."P_PAddressL1",
    "P_PAddressL2"          = ALLDATA."P_PAddressL2",
    "P_PCity"               = ALLDATA."P_PCity",
    "P_PAddressState"       = ALLDATA."P_PAddressState",
    "P_PZipCode"            = ALLDATA."P_PZipCode",
    "P_PCounty"             = ALLDATA."P_PCounty",
    "P_PFName"              = ALLDATA."P_PFName",
    "P_PLName"              = ALLDATA."P_PLName",
    "P_PMedicaidNumber"     = ALLDATA."P_PMedicaidNumber",
    "ConP_PAdmissionID"     = ALLDATA."ConP_PAdmissionID",
    "ConP_PName"            = ALLDATA."ConP_PName",
    "ConP_PAddressID"       = ALLDATA."ConP_PAddressID",
    "ConP_PAppAddressID"    = ALLDATA."ConP_PAppAddressID",
    "ConP_PAddressL1"       = ALLDATA."ConP_PAddressL1",
    "ConP_PAddressL2"       = ALLDATA."ConP_PAddressL2",
    "ConP_PCity"            = ALLDATA."ConP_PCity",
    "ConP_PAddressState"    = ALLDATA."ConP_PAddressState",
    "ConP_PZipCode"         = ALLDATA."ConP_PZipCode",
    "ConP_PCounty"          = ALLDATA."ConP_PCounty",
    "ConP_PFName"           = ALLDATA."ConP_PFName",
    "ConP_PLName"           = ALLDATA."ConP_PLName",
    "ConP_PMedicaidNumber"  = ALLDATA."ConP_PMedicaidNumber",
    "PA_PAdmissionID"       = ALLDATA."PA_PAdmissionID",
    "PA_PName"              = ALLDATA."PA_PName",
    "PA_PAddressID"         = ALLDATA."PA_PAddressID",
    "PA_PAppAddressID"      = ALLDATA."PA_PAppAddressID",
    "PA_PAddressL1"         = ALLDATA."PA_PAddressL1",
    "PA_PAddressL2"         = ALLDATA."PA_PAddressL2",
    "PA_PCity"              = ALLDATA."PA_PCity",
    "PA_PAddressState"      = ALLDATA."PA_PAddressState",
    "PA_PZipCode"           = ALLDATA."PA_PZipCode",
    "PA_PCounty"            = ALLDATA."PA_PCounty",
    "PA_PFName"             = ALLDATA."PA_PFName",
    "PA_PLName"             = ALLDATA."PA_PLName",
    "PA_PMedicaidNumber"    = ALLDATA."PA_PMedicaidNumber",
    "ConPA_PAdmissionID"    = ALLDATA."ConPA_PAdmissionID",
    "ConPA_PName"           = ALLDATA."ConPA_PName",
    "ConPA_PAddressID"      = ALLDATA."ConPA_PAddressID",
    "ConPA_PAppAddressID"   = ALLDATA."ConPA_PAppAddressID",
    "ConPA_PAddressL1"      = ALLDATA."ConPA_PAddressL1",
    "ConPA_PAddressL2"      = ALLDATA."ConPA_PAddressL2",
    "ConPA_PCity"           = ALLDATA."ConPA_PCity",
    "ConPA_PAddressState"   = ALLDATA."ConPA_PAddressState",
    "ConPA_PZipCode"        = ALLDATA."ConPA_PZipCode",
    "ConPA_PCounty"         = ALLDATA."ConPA_PCounty",
    "ConPA_PFName"          = ALLDATA."ConPA_PFName",
    "ConPA_PLName"          = ALLDATA."ConPA_PLName",
    "ConPA_PMedicaidNumber" = ALLDATA."ConPA_PMedicaidNumber",
    "ContractType"          = ALLDATA."ContractType",
    "ConContractType"       = ALLDATA."ConContractType",
    "SameSchTimeFlag"       = CASE WHEN CVM."SameSchTimeFlag" = 'N' THEN ALLDATA."SameSchTimeFlag" ELSE CVM."SameSchTimeFlag" END,
    "SameVisitTimeFlag"     = CASE WHEN CVM."SameVisitTimeFlag" = 'N' THEN ALLDATA."SameVisitTimeFlag" ELSE CVM."SameVisitTimeFlag" END,
    "SchAndVisitTimeSameFlag" = CASE WHEN CVM."SchAndVisitTimeSameFlag" = 'N' THEN ALLDATA."SchVisitTimeSame" ELSE CVM."SchAndVisitTimeSameFlag" END,
    "SchOverAnotherSchTimeFlag" = CASE WHEN CVM."SchOverAnotherSchTimeFlag" = 'N' THEN ALLDATA."SchOverAnotherSchTimeFlag" ELSE CVM."SchOverAnotherSchTimeFlag" END,
    "VisitTimeOverAnotherVisitTimeFlag" = CASE WHEN CVM."VisitTimeOverAnotherVisitTimeFlag" = 'N' THEN ALLDATA."VisitTimeOverAnotherVisitTimeFlag" ELSE CVM."VisitTimeOverAnotherVisitTimeFlag" END,
    "SchTimeOverVisitTimeFlag" = CASE WHEN CVM."SchTimeOverVisitTimeFlag" = 'N' THEN ALLDATA."SchTimeOverVisitTimeFlag" ELSE CVM."SchTimeOverVisitTimeFlag" END,
    "DistanceFlag"          = CASE WHEN CVM."DistanceFlag" = 'N' THEN ALLDATA."DistanceFlag" ELSE CVM."DistanceFlag" END,
    "BillRateNonBilled"     = ALLDATA."BillRateNonBilled",
    "ConBillRateNonBilled"  = ALLDATA."ConBillRateNonBilled",
    "BillRateBoth"          = ALLDATA."BillRateBoth",
    "ConBillRateBoth"       = ALLDATA."ConBillRateBoth",
    "FederalTaxNumber"      = ALLDATA."FederalTaxNumber",
    "ConFederalTaxNumber"   = ALLDATA."ConFederalTaxNumber"
FROM (
    SELECT DISTINCT
        V1."CONFLICTID",
        V1."SSN",
        V1."ProviderID"                    AS "ProviderID",
        V1."AppProviderID"                 AS "AppProviderID",
        V1."ProviderName"                  AS "ProviderName",
        V1."VisitID"                       AS "VisitID",
        V1."AppVisitID"                    AS "AppVisitID",
        V2."ProviderID"                    AS "ConProviderID",
        V2."AppProviderID"                 AS "ConAppProviderID",
        V2."ProviderName"                  AS "ConProviderName",
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
        V1."PAdmissionID"                  AS "PAdmissionID",
        V1."PName"                         AS "PName",
        V1."PAddressID"                    AS "PAddressID",
        V1."PAppAddressID"                 AS "PAppAddressID",
        V1."PAddressL1"                    AS "PAddressL1",
        V1."PAddressL2"                    AS "PAddressL2",
        V1."PCity"                         AS "PCity",
        V1."PAddressState"                 AS "PAddressState",
        V1."PZipCode"                      AS "PZipCode",
        V1."PCounty"                       AS "PCounty",
        V1."Longitude"                     AS "PLongitude",
        V1."Latitude"                      AS "PLatitude",
        V2."PatientID"                     AS "ConPatientID",
        V2."AppPatientID"                  AS "ConAppPatientID",
        V2."PAdmissionID"                  AS "ConPAdmissionID",
        V2."PName"                         AS "ConPName",
        V2."PAddressID"                    AS "ConPAddressID",
        V2."PAppAddressID"                 AS "ConPAppAddressID",
        V2."PAddressL1"                    AS "ConPAddressL1",
        V2."PAddressL2"                    AS "ConPAddressL2",
        V2."PCity"                         AS "ConPCity",
        V2."PAddressState"                 AS "ConPAddressState",
        V2."PZipCode"                      AS "ConPZipCode",
        V2."PCounty"                       AS "ConPCounty",
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
        -- MinuteDiffBetweenSch: calculate minute difference between visit times
        CASE
            WHEN EXTRACT(EPOCH FROM (V2."VisitStartTime" - V1."VisitEndTime")) / 60 > 0
             AND EXTRACT(EPOCH FROM (V1."VisitStartTime" - V2."VisitEndTime")) / 60 > 0
            THEN LEAST(
                EXTRACT(EPOCH FROM (V2."VisitStartTime" - V1."VisitEndTime")) / 60,
                EXTRACT(EPOCH FROM (V1."VisitStartTime" - V2."VisitEndTime")) / 60
            )
            WHEN EXTRACT(EPOCH FROM (V2."VisitStartTime" - V1."VisitEndTime")) / 60 > 0
            THEN EXTRACT(EPOCH FROM (V2."VisitStartTime" - V1."VisitEndTime")) / 60
            WHEN EXTRACT(EPOCH FROM (V1."VisitStartTime" - V2."VisitEndTime")) / 60 > 0
            THEN EXTRACT(EPOCH FROM (V1."VisitStartTime" - V2."VisitEndTime")) / 60
            ELSE 0
        END                                AS "MinuteDiffBetweenSch",
        -- DistanceMilesFromLatLng: calculate distance between coordinates using Haversine formula
        ROUND(
            (
                (
                    -- Haversine formula: calculates great-circle distance in meters
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
        -- ETATravleMinutes: estimated travel time in minutes
        ROUND(
            ((ROUND(
                (
                    (
                        -- Haversine formula: calculates great-circle distance in meters
                        6371000 * 2 * ASIN(SQRT(
                            POWER(SIN(RADIANS((V2."Latitude" - V1."Latitude") / 2)), 2) +
                            COS(RADIANS(V1."Latitude")) * COS(RADIANS(V2."Latitude")) *
                            POWER(SIN(RADIANS((V2."Longitude" - V1."Longitude") / 2)), 2)
                        ))
                        / 1609.34
                    ) * SETT."ExtraDistancePer"
                )::numeric,
                2
            ) / NULLIF(MPH."AverageMilesPerHour"::numeric, 0)) * 60),
            2
        )                                  AS "ETATravleMinutes",
        V1."ServiceCodeID"                 AS "ServiceCodeID",
        V1."AppServiceCodeID"              AS "AppServiceCodeID",
        V1."RateType"                      AS "RateType",
        V1."ServiceCode"                   AS "ServiceCode",
        V2."ServiceCodeID"                 AS "ConServiceCodeID",
        V2."AppServiceCodeID"              AS "ConAppServiceCodeID",
        V2."RateType"                      AS "ConRateType",
        V2."ServiceCode"                   AS "ConServiceCode",
        -- RULE 1: SameSchTimeFlag
        CASE
            WHEN V1."ProviderID" <> V2."ProviderID"
             AND V1."VisitStartTime" IS NULL AND V1."VisitEndTime" IS NULL
             AND V2."VisitStartTime" IS NULL AND V2."VisitEndTime" IS NULL
             AND CONCAT(V1."SchStartTime", '~', V1."SchEndTime") = CONCAT(V2."SchStartTime", '~', V2."SchEndTime")
            THEN 'Y'
            ELSE 'N'
        END                                AS "SameSchTimeFlag",
        -- RULE 2: SameVisitTimeFlag
        CASE
            WHEN V1."ProviderID" <> V2."ProviderID"
             AND V1."VisitStartTime" IS NOT NULL AND V1."VisitEndTime" IS NOT NULL
             AND V2."VisitStartTime" IS NOT NULL AND V2."VisitEndTime" IS NOT NULL
             AND CONCAT(V1."VisitStartTime", '~', V1."VisitEndTime") = CONCAT(V2."VisitStartTime", '~', V2."VisitEndTime")
            THEN 'Y'
            ELSE 'N'
        END                                AS "SameVisitTimeFlag",
        -- RULE 3: SchVisitTimeSame
        CASE
            WHEN V1."ProviderID" <> V2."ProviderID"
             AND (
                 (V1."VisitStartTime" IS NULL AND V1."VisitEndTime" IS NULL
                  AND V2."VisitStartTime" IS NOT NULL AND V2."VisitEndTime" IS NOT NULL
                  AND CONCAT(V1."SchStartTime", '~', V1."SchEndTime") = CONCAT(V2."VisitStartTime", '~', V2."VisitEndTime"))
                 OR
                 (V2."VisitStartTime" IS NULL AND V2."VisitEndTime" IS NULL
                  AND V1."VisitStartTime" IS NOT NULL AND V1."VisitEndTime" IS NOT NULL
                  AND CONCAT(V2."SchStartTime", '~', V2."SchEndTime") = CONCAT(V1."VisitStartTime", '~', V1."VisitEndTime"))
             )
            THEN 'Y'
            ELSE 'N'
        END                                AS "SchVisitTimeSame",
        -- RULE 4: SchOverAnotherSchTimeFlag
        CASE
            WHEN V1."ProviderID" <> V2."ProviderID"
             AND V1."VisitStartTime" IS NULL AND V1."VisitEndTime" IS NULL
             AND V2."VisitStartTime" IS NULL AND V2."VisitEndTime" IS NULL
             AND V1."SchStartTime" < V2."SchEndTime"
             AND V1."SchEndTime" > V2."SchStartTime"
             AND CONCAT(V1."SchStartTime", '~', V1."SchEndTime") <> CONCAT(V2."SchStartTime", '~', V2."SchEndTime")
            THEN 'Y'
            ELSE 'N'
        END                                AS "SchOverAnotherSchTimeFlag",
        -- RULE 5: VisitTimeOverAnotherVisitTimeFlag
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
        -- RULE 6: SchTimeOverVisitTimeFlag
        CASE
            WHEN V1."ProviderID" <> V2."ProviderID"
             AND (
                 (V1."VisitStartTime" IS NULL AND V1."VisitEndTime" IS NULL
                  AND V2."VisitStartTime" IS NOT NULL AND V2."VisitEndTime" IS NOT NULL
                  AND V1."SchStartTime" < V2."VisitEndTime"
                  AND V1."SchEndTime" > V2."VisitStartTime"
                  AND CONCAT(V1."SchStartTime", '~', V1."SchEndTime") <> CONCAT(V2."VisitStartTime", '~', V2."VisitEndTime"))
                 OR
                 (V2."VisitStartTime" IS NULL AND V2."VisitEndTime" IS NULL
                  AND V1."VisitStartTime" IS NOT NULL AND V1."VisitEndTime" IS NOT NULL
                  AND V2."SchStartTime" < V1."VisitEndTime"
                  AND V2."SchEndTime" > V1."VisitStartTime"
                  AND CONCAT(V2."SchStartTime", '~', V2."SchEndTime") <> CONCAT(V1."VisitStartTime", '~', V1."VisitEndTime"))
             )
            THEN 'Y'
            ELSE 'N'
        END                                AS "SchTimeOverVisitTimeFlag",
        -- RULE 7: DistanceFlag
        CASE
            WHEN V1."ProviderID" <> V2."ProviderID"
             AND V1."Longitude" IS NOT NULL AND V1."Latitude" IS NOT NULL
             AND V2."Longitude" IS NOT NULL AND V2."Latitude" IS NOT NULL
             AND V1."VisitStartTime" IS NOT NULL AND V1."VisitEndTime" IS NOT NULL
             AND V2."VisitStartTime" IS NOT NULL AND V2."VisitEndTime" IS NOT NULL
             AND (
                 (V1."PZipCode" IS NOT NULL AND V2."PZipCode" IS NOT NULL AND V1."PZipCode" <> V2."PZipCode")
                 OR (V1."PZipCode" IS NULL OR V2."PZipCode" IS NULL)
             )
             AND MPH."AverageMilesPerHour" IS NOT NULL
             AND (
                 (EXTRACT(EPOCH FROM (V2."VisitStartTime" - V1."VisitEndTime")) / 60 > 0
                  AND ROUND(
                      ((ROUND(
                          (
                              (
                                  -- Haversine formula: V1 to V2
                                  6371000 * 2 * ASIN(SQRT(
                                      POWER(SIN(RADIANS((V2."Latitude" - V1."Latitude") / 2)), 2) +
                                      COS(RADIANS(V1."Latitude")) * COS(RADIANS(V2."Latitude")) *
                                      POWER(SIN(RADIANS((V2."Longitude" - V1."Longitude") / 2)), 2)
                                  ))
                                  / 1609.34
                              ) * SETT."ExtraDistancePer"
                          )::numeric,
                          2
                      ) / NULLIF(MPH."AverageMilesPerHour"::numeric, 0)) * 60),
                      2
                  ) > EXTRACT(EPOCH FROM (V2."VisitStartTime" - V1."VisitEndTime")) / 60)
                 OR
                 (EXTRACT(EPOCH FROM (V1."VisitStartTime" - V2."VisitEndTime")) / 60 > 0
                  AND ROUND(
                      ((ROUND(
                          (
                              (
                                  -- Haversine formula: V2 to V1
                                  6371000 * 2 * ASIN(SQRT(
                                      POWER(SIN(RADIANS((V1."Latitude" - V2."Latitude") / 2)), 2) +
                                      COS(RADIANS(V2."Latitude")) * COS(RADIANS(V1."Latitude")) *
                                      POWER(SIN(RADIANS((V1."Longitude" - V2."Longitude") / 2)), 2)
                                  ))
                                  / 1609.34
                              ) * SETT."ExtraDistancePer"
                          )::numeric,
                          2
                      ) / NULLIF(MPH."AverageMilesPerHour"::numeric, 0)) * 60),
                      2
                  ) > EXTRACT(EPOCH FROM (V1."VisitStartTime" - V2."VisitEndTime")) / 60)
             )
            THEN 'Y'
            ELSE 'N'
        END                                AS "DistanceFlag",
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
        V1."PStatus",
        V2."PStatus"                       AS "ConPStatus",
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
        V1."BillRateNonBilled",
        V2."BillRateNonBilled"             AS "ConBillRateNonBilled",
        V1."BillRateBoth",
        V2."BillRateBoth"                  AS "ConBillRateBoth",
        V1."FederalTaxNumber",
        V2."FederalTaxNumber"              AS "ConFederalTaxNumber"
    FROM
        -- V1: Base visits (with existing CONFLICTID)
        (
            SELECT DISTINCT
                CVM1."CONFLICTID"                       AS "CONFLICTID",
                CR1."Bill Rate Non-Billed"              AS "BillRateNonBilled",
                CASE
                    WHEN CR1."Billed" = 'yes' THEN CR1."Billed Rate"
                    ELSE CR1."Bill Rate Non-Billed"
                END                                     AS "BillRateBoth",
                TRIM(CAR."SSN")                         AS "SSN",
                CAST(NULL AS VARCHAR)                   AS "PStatus",
                CAR."Status"                            AS "AideStatus",
                CR1."Missed Visit Reason"               AS "MissedVisitReason",
                CR1."Is Missed"                         AS "IsMissed",
                CR1."Call Out Device Type"              AS "EVVType",
                CR1."Billed Rate"                       AS "BilledRate",
                CR1."Total Billed Amount"               AS "TotalBilledAmount",
                CR1."Provider Id"                       AS "ProviderID",
                CR1."Application Provider Id"           AS "AppProviderID",
                DPR."Provider Name"                     AS "ProviderName",
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
                CAST(NULL AS VARCHAR)                   AS "PAdmissionID",
                CAST(NULL AS VARCHAR)                   AS "PName",
                CAST(NULL AS VARCHAR)                   AS "PFName",
                CAST(NULL AS VARCHAR)                   AS "PLName",
                CAST(NULL AS VARCHAR)                   AS "PMedicaidNumber",
                CAST(NULL AS NUMERIC)                   AS "PAddressID",
                CAST(NULL AS NUMERIC)                   AS "PAppAddressID",
                CAST(NULL AS VARCHAR)                   AS "PAddressL1",
                CAST(NULL AS VARCHAR)                   AS "PAddressL2",
                CAST(NULL AS VARCHAR)                   AS "PCity",
                CAST(NULL AS VARCHAR)                   AS "PAddressState",
                CAST(NULL AS VARCHAR)                   AS "PZipCode",
                CAST(NULL AS VARCHAR)                   AS "PCounty",
                -- Longitude: prefer Call Out, then Call In, then provider address
                CASE
                    WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" <> ','
                    THEN CAST(SPLIT_PART(REPLACE(CR1."Call Out GPS Coordinates", '"', ''), ',', 2) AS DOUBLE PRECISION)
                    WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" <> ','
                    THEN CAST(SPLIT_PART(REPLACE(CR1."Call In GPS Coordinates", '"', ''), ',', 2) AS DOUBLE PRECISION)
                    ELSE DPAD_P."Provider_Longitude"
                END                                     AS "Longitude",
                -- Latitude: prefer Call Out, then Call In, then provider address
                CASE
                    WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" <> ','
                    THEN CAST(SPLIT_PART(REPLACE(CR1."Call Out GPS Coordinates", '"', ''), ',', 1) AS DOUBLE PRECISION)
                    WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" <> ','
                    THEN CAST(SPLIT_PART(REPLACE(CR1."Call In GPS Coordinates", '"', ''), ',', 1) AS DOUBLE PRECISION)
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
                CASE
                    WHEN CR1."Application Payer Id" = '0' AND CR1."Application Contract Id" <> '0' THEN 'Internal'
                    WHEN CR1."Application Payer Id" <> '0' AND CR1."Application Contract Id" <> '0' THEN 'UPR'
                    WHEN CR1."Application Payer Id" <> '0' AND CR1."Application Contract Id" = '0' THEN 'Payer'
                END                                     AS "ContractType",
                DPR."Phone Number 1"                    AS "AgencyPhone",
                DPR."Federal Tax Number"                AS "FederalTaxNumber"
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
                    DDD."Application Patient Id",
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
                    DDD."Application Patient Id",
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
                ON CVM1."VisitID" = CR1."Visit Id"::varchar
               AND CVM1."AppVisitID" = CR1."Application Visit Id"::varchar
               AND CVM1."CONFLICTID" IS NOT NULL
            LEFT JOIN {analytics_schema}.dimservicecode AS DSC
                ON DSC."Service Code Id" = CR1."Service Code Id"
            LEFT JOIN {analytics_schema}.dimuser AS DUSR
                ON DUSR."User Id" = CR1."Visit Updated User Id"
            WHERE CR1."Visit Date"::date
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
                CAST(NULL AS VARCHAR)                   AS "PStatus",
                CAR."Status"                            AS "AideStatus",
                CR1."Missed Visit Reason"               AS "MissedVisitReason",
                CR1."Is Missed"                         AS "IsMissed",
                CR1."Call Out Device Type"              AS "EVVType",
                CR1."Billed Rate"                       AS "BilledRate",
                CR1."Total Billed Amount"               AS "TotalBilledAmount",
                CR1."Provider Id"                       AS "ProviderID",
                CR1."Application Provider Id"           AS "AppProviderID",
                DPR."Provider Name"                     AS "ProviderName",
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
                CAST(NULL AS VARCHAR)                   AS "PAdmissionID",
                CAST(NULL AS VARCHAR)                   AS "PName",
                CAST(NULL AS VARCHAR)                   AS "PFName",
                CAST(NULL AS VARCHAR)                   AS "PLName",
                CAST(NULL AS VARCHAR)                   AS "PMedicaidNumber",
                CAST(NULL AS NUMERIC)                   AS "PAddressID",
                CAST(NULL AS NUMERIC)                   AS "PAppAddressID",
                CAST(NULL AS VARCHAR)                   AS "PAddressL1",
                CAST(NULL AS VARCHAR)                   AS "PAddressL2",
                CAST(NULL AS VARCHAR)                   AS "PCity",
                CAST(NULL AS VARCHAR)                   AS "PAddressState",
                CAST(NULL AS VARCHAR)                   AS "PZipCode",
                CAST(NULL AS VARCHAR)                   AS "PCounty",
                -- Longitude: prefer Call In, then Call Out, then provider address
                CASE
                    WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" <> ','
                    THEN CAST(SPLIT_PART(REPLACE(CR1."Call In GPS Coordinates", '"', ''), ',', 2) AS DOUBLE PRECISION)
                    WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" <> ','
                    THEN CAST(SPLIT_PART(REPLACE(CR1."Call Out GPS Coordinates", '"', ''), ',', 2) AS DOUBLE PRECISION)
                    ELSE DPAD_P."Provider_Longitude"
                END                                     AS "Longitude",
                -- Latitude: prefer Call In, then Call Out, then provider address
                CASE
                    WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" <> ','
                    THEN CAST(SPLIT_PART(REPLACE(CR1."Call In GPS Coordinates", '"', ''), ',', 1) AS DOUBLE PRECISION)
                    WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" <> ','
                    THEN CAST(SPLIT_PART(REPLACE(CR1."Call Out GPS Coordinates", '"', ''), ',', 1) AS DOUBLE PRECISION)
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
                CASE
                    WHEN CR1."Application Payer Id" = '0' AND CR1."Application Contract Id" <> '0' THEN 'Internal'
                    WHEN CR1."Application Payer Id" <> '0' AND CR1."Application Contract Id" <> '0' THEN 'UPR'
                    WHEN CR1."Application Payer Id" <> '0' AND CR1."Application Contract Id" = '0' THEN 'Payer'
                END                                     AS "ContractType",
                DPR."Phone Number 1"                    AS "AgencyPhone",
                DPR."Federal Tax Number"                AS "FederalTaxNumber"
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
                    DDD."Application Patient Id",
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
                    DDD."Application Patient Id",
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
                ON DUSR."User Id" = CR1."Visit Updated User Id"
            WHERE CR1."Visit Date"::date
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
        ) AS V2
        ON V1."VisitDate" = V2."VisitDate"
       AND V1."VisitID" <> V2."VisitID"
       AND V1."SSN" = V2."SSN"
       AND V1."ProviderID" <> V2."ProviderID"
    CROSS JOIN {conflict_schema}.settings AS SETT
    LEFT JOIN {conflict_schema}.mph AS MPH
        ON ROUND(
            (
                (
                    -- Haversine formula: calculates great-circle distance in meters
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
        -- 7-rule conflict filter
        (
            -- RULE 1: SameSchTimeFlag
            (V1."VisitStartTime" IS NULL AND V1."VisitEndTime" IS NULL
             AND V2."VisitStartTime" IS NULL AND V2."VisitEndTime" IS NULL
             AND CONCAT(V1."SchStartTime", '~', V1."SchEndTime") = CONCAT(V2."SchStartTime", '~', V2."SchEndTime"))
         OR
            -- RULE 2: SameVisitTimeFlag
            (V1."VisitStartTime" IS NOT NULL AND V1."VisitEndTime" IS NOT NULL
             AND V2."VisitStartTime" IS NOT NULL AND V2."VisitEndTime" IS NOT NULL
             AND CONCAT(V1."VisitStartTime", '~', V1."VisitEndTime") = CONCAT(V2."VisitStartTime", '~', V2."VisitEndTime"))
         OR
            -- RULE 3: SchVisitTimeSame
            ((V2."VisitStartTime" IS NULL AND V2."VisitEndTime" IS NULL
              AND V1."VisitStartTime" IS NOT NULL AND V1."VisitEndTime" IS NOT NULL
              AND CONCAT(V2."SchStartTime", '~', V2."SchEndTime") = CONCAT(V1."VisitStartTime", '~', V1."VisitEndTime"))
             OR
             (V1."VisitStartTime" IS NULL AND V1."VisitEndTime" IS NULL
              AND V2."VisitStartTime" IS NOT NULL AND V2."VisitEndTime" IS NOT NULL
              AND CONCAT(V1."SchStartTime", '~', V1."SchEndTime") = CONCAT(V2."VisitStartTime", '~', V2."VisitEndTime")))
         OR
            -- RULE 4: SchOverAnotherSchTimeFlag
            (V1."VisitStartTime" IS NULL AND V1."VisitEndTime" IS NULL
             AND V2."VisitStartTime" IS NULL AND V2."VisitEndTime" IS NULL
             AND V1."SchStartTime" < V2."SchEndTime"
             AND V1."SchEndTime" > V2."SchStartTime"
             AND CONCAT(V1."SchStartTime", '~', V1."SchEndTime") <> CONCAT(V2."SchStartTime", '~', V2."SchEndTime"))
         OR
            -- RULE 5: VisitTimeOverAnotherVisitTimeFlag
            (V1."VisitStartTime" IS NOT NULL AND V1."VisitEndTime" IS NOT NULL
             AND V2."VisitStartTime" IS NOT NULL AND V2."VisitEndTime" IS NOT NULL
             AND V1."VisitStartTime" < V2."VisitEndTime"
             AND V1."VisitEndTime" > V2."VisitStartTime"
             AND CONCAT(V1."VisitStartTime", '~', V1."VisitEndTime") <> CONCAT(V2."VisitStartTime", '~', V2."VisitEndTime"))
         OR
            -- RULE 6: SchTimeOverVisitTimeFlag
            ((V1."VisitStartTime" IS NULL AND V1."VisitEndTime" IS NULL
              AND V2."VisitStartTime" IS NOT NULL AND V2."VisitEndTime" IS NOT NULL
              AND V1."SchStartTime" < V2."VisitEndTime"
              AND V1."SchEndTime" > V2."VisitStartTime"
              AND CONCAT(V1."SchStartTime", '~', V1."SchEndTime") <> CONCAT(V2."VisitStartTime", '~', V2."VisitEndTime"))
             OR
             (V2."VisitStartTime" IS NULL AND V2."VisitEndTime" IS NULL
              AND V1."VisitStartTime" IS NOT NULL AND V1."VisitEndTime" IS NOT NULL
              AND V2."SchStartTime" < V1."VisitEndTime"
              AND V2."SchEndTime" > V1."VisitStartTime"
              AND CONCAT(V2."SchStartTime", '~', V2."SchEndTime") <> CONCAT(V1."VisitStartTime", '~', V1."VisitEndTime")))
         OR
            -- RULE 7: DistanceFlag
            (V1."Longitude" IS NOT NULL AND V1."Latitude" IS NOT NULL
             AND V2."Longitude" IS NOT NULL AND V2."Latitude" IS NOT NULL
             AND V1."VisitStartTime" IS NOT NULL AND V1."VisitEndTime" IS NOT NULL
             AND V2."VisitStartTime" IS NOT NULL AND V2."VisitEndTime" IS NOT NULL
             AND ((V1."PZipCode" IS NOT NULL AND V2."PZipCode" IS NOT NULL AND V1."PZipCode" <> V2."PZipCode")
                  OR (V1."PZipCode" IS NULL OR V2."PZipCode" IS NULL))
             AND MPH."AverageMilesPerHour" IS NOT NULL
             AND (
                 (EXTRACT(EPOCH FROM (V2."VisitStartTime" - V1."VisitEndTime")) / 60 > 0
                  AND ROUND(
                      ((ROUND(
                          (
                              (
                                  -- Haversine formula: V1 to V2
                                  6371000 * 2 * ASIN(SQRT(
                                      POWER(SIN(RADIANS((V2."Latitude" - V1."Latitude") / 2)), 2) +
                                      COS(RADIANS(V1."Latitude")) * COS(RADIANS(V2."Latitude")) *
                                      POWER(SIN(RADIANS((V2."Longitude" - V1."Longitude") / 2)), 2)
                                  ))
                                  / 1609.34
                              ) * SETT."ExtraDistancePer"
                          )::numeric,
                          2
                      ) / NULLIF(MPH."AverageMilesPerHour"::numeric, 0)) * 60),
                      2
                  ) > EXTRACT(EPOCH FROM (V2."VisitStartTime" - V1."VisitEndTime")) / 60)
                 OR
                 (EXTRACT(EPOCH FROM (V1."VisitStartTime" - V2."VisitEndTime")) / 60 > 0
                  AND ROUND(
                      ((ROUND(
                          (
                              (
                                  -- Haversine formula: V2 to V1
                                  6371000 * 2 * ASIN(SQRT(
                                      POWER(SIN(RADIANS((V1."Latitude" - V2."Latitude") / 2)), 2) +
                                      COS(RADIANS(V2."Latitude")) * COS(RADIANS(V1."Latitude")) *
                                      POWER(SIN(RADIANS((V1."Longitude" - V2."Longitude") / 2)), 2)
                                  ))
                                  / 1609.34
                              ) * SETT."ExtraDistancePer"
                          )::numeric,
                          2
                      ) / NULLIF(MPH."AverageMilesPerHour"::numeric, 0)) * 60),
                      2
                  ) > EXTRACT(EPOCH FROM (V1."VisitStartTime" - V2."VisitEndTime")) / 60)
             ))
        )
) AS ALLDATA
WHERE
    (
        (CVM."VisitID" = ALLDATA."VisitID"::varchar AND CVM."ConVisitID" = ALLDATA."ConVisitID"::varchar)
        OR
        (CVM."VisitID" = ALLDATA."VisitID"::varchar AND CVM."ConVisitID" IS NULL AND ALLDATA."ConVisitID" IS NULL)
    )
    AND CVM."InserviceStartDate" IS NULL
    AND CVM."InserviceEndDate" IS NULL
    AND CVM."PTOStartDate" IS NULL
    AND CVM."PTOEndDate" IS NULL
    AND CVM."ConInserviceStartDate" IS NULL
    AND CVM."ConInserviceEndDate" IS NULL
    AND CVM."ConPTOStartDate" IS NULL
    AND CVM."ConPTOEndDate" IS NULL
    AND CVM."UpdateFlag" = 1
    AND {chunk_filter};  -- ⭐ CHUNK FILTER: Only update rows in this chunk

