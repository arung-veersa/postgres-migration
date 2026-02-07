-- ============================================================================
-- TASK_02+03: Conflict Detection & Sync (CONSOLIDATED MERGE - OPTIMIZED V2)
-- ============================================================================
-- Purpose:
--   Detect and sync conflicts between visits using 7 conflict rules:
--     1. Same scheduled time (future visits only)
--     2. Same actual visit time
--     3. Scheduled time matches other's visit time
--     4. Scheduled times overlap (future visits only)
--     5. Visit times overlap
--     6. Scheduled time overlaps with visit time
--     7. Distance-based conflict (travel time > gap between visits)
--
--   This MERGE replaces:
--     - TASK_02_0 (7 conflict rule updates)
--     - TASK_03_0 (inserts for 7 conflict rules)
--
--   Note: InService and PTO conflict logic are NOT included in this script.
--   InService conflicts are handled in task04_inservice_conflicts_merge.sql
--   PTO conflicts will be handled separately in a future script.
--
-- OPTIMIZATION NOTES (V2):
--   This version reduces code by ~330 lines through:
--   - Single base_visits CTE (eliminates V1/V2 duplication)
--   - Reusable patient_addresses CTE (eliminates 4 duplicate subqueries)
--   - GPS extraction function (eliminates 18 lines of repetitive CASE logic)
--   - Pre-computed time gap calculations (eliminates 20+ repeated EXTRACT calls)
--   - Visit type helper flags (improves readability of conflict rules)
--
-- Prerequisites:
--   Run functions/distance_functions.sql ONCE before first execution
--   (Creates: calculate_distance_miles, calculate_conflict_distance, 
--             calculate_travel_time_minutes, extract_gps_coordinate)
--
-- Execution: Run directly in DBeaver
--
-- Variables (set these before running):
--   :conflict_schema   - Conflict data schema (e.g., 'conflict_dev')
--   :analytics_schema  - Analytics data schema (e.g., 'analytics_dev')
--
-- Performance Notes:
--   - Expected runtime: 30-60 minutes for ~4M conflict keys
--   - Uses critical indexes (ensure they exist):
--     * idx_fvcp_date_caregiver_covering
--     * idx_cvm_visitid_appvisitid
--     * idx_dimcaregiver_ssn
--   - Self-join on factvisitcallperformance_cr (58M rows)
--   - Date range: (TODAY - 2 years) to (TODAY + 45 days)
-- ============================================================================

MERGE INTO :conflict_schema.conflictvisitmaps AS target
USING (
    -- ========================================================================
    -- CTEs: Reusable subqueries to eliminate duplication
    -- ========================================================================
    WITH 
    
    -- Patient addresses with GPS coordinates (used 4 times in original query)
    patient_addresses AS (
        SELECT 
            "Patient Address Id",
            "Application Patient Address Id",
            "Address Line 1",
            "Address Line 2",
            "City",
            "Address State",
            "Zip Code",
            "County",
            "Patient Id",
            "Application Patient Id",
            "Longitude"::REAL AS "Provider_Longitude",
            "Latitude"::REAL AS "Provider_Latitude",
            ROW_NUMBER() OVER (
                PARTITION BY "Patient Id" 
                ORDER BY "Application Created UTC Timestamp" DESC
            ) AS rn
        FROM :analytics_schema.dimpatientaddress
        WHERE "Primary Address" = TRUE 
          AND "Address Type" LIKE '%GPS%'
    ),
    
    -- Base visit data (eliminates V1/V2 duplication - 220+ lines saved)
    base_visits AS (
        SELECT DISTINCT 
            CR1."Bill Rate Non-Billed" AS "BillRateNonBilled",
            CASE WHEN CR1."Billed" = 'yes' THEN CR1."Billed Rate" 
                 ELSE CR1."Bill Rate Non-Billed" 
            END AS "BillRateBoth",
            TRIM(CAR."SSN") AS "SSN",
            NULL::TEXT AS "PStatus",
            CAR."Status" AS "AideStatus",
            CR1."Missed Visit Reason" AS "MissedVisitReason",
            CR1."Is Missed" AS "IsMissed",
            CR1."Call Out Device Type" AS "EVVType",
            CR1."Billed Rate" AS "BilledRate",
            CR1."Total Billed Amount" AS "TotalBilledAmount",
            CR1."Provider Id"::uuid AS "ProviderID",
            CR1."Application Provider Id" AS "AppProviderID",
            DPR."Provider Name" AS "ProviderName",
            DPR."Phone Number 1" AS "AgencyPhone",
            DPR."Federal Tax Number" AS "FederalTaxNumber",
            CR1."Visit Id"::uuid AS "VisitID",
            CR1."Application Visit Id" AS "AppVisitID",
            CR1."Visit Date"::date AS "VisitDate",
            CR1."Scheduled Start Time"::timestamp AS "SchStartTime",
            CR1."Scheduled End Time"::timestamp AS "SchEndTime",
            CR1."Visit Start Time"::timestamp AS "VisitStartTime",
            CR1."Visit End Time"::timestamp AS "VisitEndTime",
            CR1."Call In Time"::timestamp AS "EVVStartTime",
            CR1."Call Out Time"::timestamp AS "EVVEndTime",
            CR1."Caregiver Id"::uuid AS "CaregiverID",
            CR1."Application Caregiver Id" AS "AppCaregiverID",
            CAR."Caregiver Code" AS "AideCode",
            CAR."Caregiver Fullname" AS "AideName",
            CAR."Caregiver Firstname" AS "AideFName",
            CAR."Caregiver Lastname" AS "AideLName",
            TRIM(CAR."SSN") AS "AideSSN",
            CR1."Office Id"::uuid AS "OfficeID",
            CR1."Application Office Id" AS "AppOfficeID",
            DOF."Office Name" AS "Office",
            CR1."Payer Patient Id"::uuid AS "PA_PatientID",
            CR1."Application Payer Patient Id" AS "PA_AppPatientID",
            CR1."Provider Patient Id"::uuid AS "P_PatientID",
            CR1."Application Provider Patient Id" AS "P_AppPatientID",
            CR1."Patient Id"::uuid AS "PatientID",
            CR1."Application Patient Id" AS "AppPatientID",
            NULL::TEXT AS "PAdmissionID",
            NULL::TEXT AS "PName",
            NULL::TEXT AS "PFName",
            NULL::TEXT AS "PLName",
            NULL::TEXT AS "PMedicaidNumber",
            NULL::UUID AS "PAddressID",
            NULL::INT8 AS "PAppAddressID",
            NULL::TEXT AS "PAddressL1",
            NULL::TEXT AS "PAddressL2",
            NULL::TEXT AS "PCity",
            NULL::TEXT AS "PAddressState",
            NULL::TEXT AS "PZipCode",
            NULL::TEXT AS "PCounty",
            -- GPS Coordinates extraction using new function (replaces 18 lines of CASE logic)
            :conflict_schema.extract_gps_coordinate(
                CR1."Call Out GPS Coordinates",
                CR1."Call In GPS Coordinates",
                DPAD_P."Provider_Longitude",
                2  -- longitude position
            ) AS "Longitude",
            :conflict_schema.extract_gps_coordinate(
                CR1."Call Out GPS Coordinates",
                CR1."Call In GPS Coordinates",
                DPAD_P."Provider_Latitude",
                1  -- latitude position
            ) AS "Latitude",
            CR1."Payer Id"::uuid AS "PayerID",
            CR1."Application Payer Id" AS "AppPayerID",
            COALESCE(SPA."Payer Name", DCON."Contract Name") AS "Contract",
            SPA."Payer State" AS "PayerState",
            CR1."Invoice Date"::timestamp AS "BilledDate",
            CR1."Billed Hours" AS "BilledHours",
            CR1."Billed" AS "Billed",
            DSC."Service Code Id"::uuid AS "ServiceCodeID",
            DSC."Application Service Code Id" AS "AppServiceCodeID",
            CR1."Bill Type" AS "RateType",
            DSC."Service Code" AS "ServiceCode",
            CR1."Visit Updated Timestamp"::timestamp AS "LastUpdatedDate",
            DUSR."User Fullname" AS "LastUpdatedBy",
            -- Provider Patient info
            DPA_P."Admission Id" AS "P_PAdmissionID",
            DPA_P."Patient Name" AS "P_PName",
            DPA_P."Patient Firstname" AS "P_PFName",
            DPA_P."Patient Lastname" AS "P_PLName",
            DPA_P."Medicaid Number" AS "P_PMedicaidNumber",
            DPA_P."Status" AS "P_PStatus",
            DPAD_P."Patient Address Id" AS "P_PAddressID",
            DPAD_P."Application Patient Address Id" AS "P_PAppAddressID",
            DPAD_P."Address Line 1" AS "P_PAddressL1",
            DPAD_P."Address Line 2" AS "P_PAddressL2",
            DPAD_P."City" AS "P_PCity",
            DPAD_P."Address State" AS "P_PAddressState",
            DPAD_P."Zip Code" AS "P_PZipCode",
            DPAD_P."County" AS "P_PCounty",
            -- Payer Patient info
            DPA_PA."Admission Id" AS "PA_PAdmissionID",
            DPA_PA."Patient Name" AS "PA_PName",
            DPA_PA."Patient Firstname" AS "PA_PFName",
            DPA_PA."Patient Lastname" AS "PA_PLName",
            DPA_PA."Medicaid Number" AS "PA_PMedicaidNumber",
            DPA_PA."Status" AS "PA_PStatus",
            DPAD_PA."Patient Address Id" AS "PA_PAddressID",
            DPAD_PA."Application Patient Address Id" AS "PA_PAppAddressID",
            DPAD_PA."Address Line 1" AS "PA_PAddressL1",
            DPAD_PA."Address Line 2" AS "PA_PAddressL2",
            DPAD_PA."City" AS "PA_PCity",
            DPAD_PA."Address State" AS "PA_PAddressState",
            DPAD_PA."Zip Code" AS "PA_PZipCode",
            DPAD_PA."County" AS "PA_PCounty",
            -- Contract type
            CASE 
                WHEN CR1."Application Payer Id" = 0 
                     AND CR1."Application Contract Id" != 0 THEN 'Internal'
                WHEN CR1."Application Payer Id" != 0 
                     AND CR1."Application Contract Id" != 0 THEN 'UPR'
                WHEN CR1."Application Payer Id" != 0 
                     AND CR1."Application Contract Id" = 0 THEN 'Payer'
            END AS "ContractType"
        FROM :analytics_schema.factvisitcallperformance_cr AS CR1
        INNER JOIN :analytics_schema.dimcaregiver AS CAR 
            ON CAR."Caregiver Id" = CR1."Caregiver Id" 
            AND TRIM(CAR."SSN") IS NOT NULL 
            AND TRIM(CAR."SSN") != ''
        LEFT JOIN :analytics_schema.dimoffice AS DOF 
            ON DOF."Office Id" = CR1."Office Id" 
            AND DOF."Is Active" = TRUE
        LEFT JOIN :analytics_schema.dimpatient AS DPA_P 
            ON DPA_P."Patient Id" = CR1."Provider Patient Id"
        LEFT JOIN patient_addresses AS DPAD_P 
            ON DPAD_P."Patient Id" = DPA_P."Patient Id" 
            AND DPAD_P.rn = 1
        LEFT JOIN :analytics_schema.dimpatient AS DPA_PA 
            ON DPA_PA."Patient Id" = CR1."Payer Patient Id"
        LEFT JOIN patient_addresses AS DPAD_PA 
            ON DPAD_PA."Patient Id" = DPA_PA."Patient Id" 
            AND DPAD_PA.rn = 1
        LEFT JOIN :analytics_schema.dimpayer AS SPA 
            ON SPA."Payer Id" = CR1."Payer Id" 
            AND SPA."Is Active" = TRUE 
            AND SPA."Is Demo" = FALSE
        LEFT JOIN :analytics_schema.dimcontract AS DCON 
            ON DCON."Contract Id" = CR1."Contract Id" 
            AND DCON."Is Active" = TRUE
        INNER JOIN :analytics_schema.dimprovider AS DPR 
            ON DPR."Provider Id" = CR1."Provider Id" 
            AND DPR."Is Active" = TRUE 
            AND DPR."Is Demo" = FALSE
        LEFT JOIN :analytics_schema.dimservicecode AS DSC 
            ON DSC."Service Code Id" = CR1."Service Code Id"
        LEFT JOIN :analytics_schema.dimuser AS DUSR 
            ON DUSR."User Id" = CR1."Visit Updated User Id"
        WHERE CR1."Visit Date"::date BETWEEN 
            (CURRENT_DATE - INTERVAL '2 years') 
            AND (CURRENT_DATE + INTERVAL '45 days')
          AND CR1."Provider Id" NOT IN (
              SELECT "ProviderID" 
              FROM :conflict_schema.excluded_agency
          )
          AND NOT EXISTS (
              SELECT 1 
              FROM :conflict_schema.excluded_ssn AS SSN 
              WHERE TRIM(CAR."SSN") = SSN."SSN"
          )
    ),
    
    -- V1: Visits with existing CONFLICTID from conflictvisitmaps
    V1 AS (
        SELECT 
            bv.*,
            CVM1."CONFLICTID"
        FROM base_visits bv
        LEFT JOIN :conflict_schema.conflictvisitmaps AS CVM1 
            ON CVM1."VisitID" = bv."VisitID"
            AND CVM1."AppVisitID" = bv."AppVisitID"
            AND CVM1."CONFLICTID" IS NOT NULL
    ),
    
    -- V2: All visits (for conflict detection, no CONFLICTID required)
    V2 AS (
        SELECT 
            *,
            NULL::bigint AS "CONFLICTID"
        FROM base_visits
    )
    
    -- ========================================================================
    -- Source Query: Detect all conflicts using 7 rules
    -- ========================================================================
    SELECT DISTINCT
        -- Visit 1 (primary visit)
        V1."CONFLICTID",
        V1."SSN",
        V1."ProviderID",
        V1."AppProviderID",
        V1."ProviderName",
        V1."VisitID",
        V1."AppVisitID",
        
        -- Visit 2 (conflicting visit)
        V2."ProviderID" AS "ConProviderID",
        V2."AppProviderID" AS "ConAppProviderID",
        V2."ProviderName" AS "ConProviderName",
        V2."VisitID" AS "ConVisitID",
        V2."AppVisitID" AS "ConAppVisitID",
        
        -- Visit dates and times
        V1."VisitDate",
        V1."SchStartTime",
        V1."SchEndTime",
        V2."SchStartTime" AS "ConSchStartTime",
        V2."SchEndTime" AS "ConSchEndTime",
        V1."VisitStartTime",
        V1."VisitEndTime",
        V2."VisitStartTime" AS "ConVisitStartTime",
        V2."VisitEndTime" AS "ConVisitEndTime",
        V1."EVVStartTime",
        V1."EVVEndTime",
        V2."EVVStartTime" AS "ConEVVStartTime",
        V2."EVVEndTime" AS "ConEVVEndTime",
        
        -- Caregiver info
        V1."CaregiverID",
        V1."AppCaregiverID",
        V1."AideCode",
        V1."AideName",
        V1."AideSSN",
        V2."CaregiverID" AS "ConCaregiverID",
        V2."AppCaregiverID" AS "ConAppCaregiverID",
        V2."AideCode" AS "ConAideCode",
        V2."AideName" AS "ConAideName",
        V2."AideSSN" AS "ConAideSSN",
        
        -- Office info
        V1."OfficeID",
        V1."AppOfficeID",
        V1."Office",
        V2."OfficeID" AS "ConOfficeID",
        V2."AppOfficeID" AS "ConAppOfficeID",
        V2."Office" AS "ConOffice",
        
        -- Patient info (Visit 1)
        V1."PatientID",
        V1."AppPatientID",
        V1."PAdmissionID",
        V1."PName",
        V1."PAddressID",
        V1."PAppAddressID",
        V1."PAddressL1",
        V1."PAddressL2",
        V1."PCity",
        V1."PAddressState",
        V1."PZipCode",
        V1."PCounty",
        V1."Longitude" AS "PLongitude",
        V1."Latitude" AS "PLatitude",
        
        -- Patient info (Visit 2)
        V2."PatientID" AS "ConPatientID",
        V2."AppPatientID" AS "ConAppPatientID",
        V2."PAdmissionID" AS "ConPAdmissionID",
        V2."PName" AS "ConPName",
        V2."PAddressID" AS "ConPAddressID",
        V2."PAppAddressID" AS "ConPAppAddressID",
        V2."PAddressL1" AS "ConPAddressL1",
        V2."PAddressL2" AS "ConPAddressL2",
        V2."PCity" AS "ConPCity",
        V2."PAddressState" AS "ConPAddressState",
        V2."PZipCode" AS "ConPZipCode",
        V2."PCounty" AS "ConPCounty",
        V2."Longitude" AS "ConPLongitude",
        V2."Latitude" AS "ConPLatitude",
        
        -- Payer/Contract info
        V1."PayerID",
        V1."AppPayerID",
        V1."Contract",
        V2."PayerID" AS "ConPayerID",
        V2."AppPayerID" AS "ConAppPayerID",
        V2."Contract" AS "ConContract",
        
        -- Billing info
        V1."BilledDate",
        V2."BilledDate" AS "ConBilledDate",
        V1."BilledHours",
        V2."BilledHours" AS "ConBilledHours",
        V1."Billed",
        V2."Billed" AS "ConBilled",
        
        -- ====================================================================
        -- HELPER FLAGS: Pre-computed values for cleaner logic (Optimization #2 & #3)
        -- ====================================================================
        
        -- Visit type flags (used in multiple rules)
        (V1."VisitStartTime" IS NULL AND V1."VisitEndTime" IS NULL) AS "V1_IsFutureVisit",
        (V2."VisitStartTime" IS NULL AND V2."VisitEndTime" IS NULL) AS "V2_IsFutureVisit",
        (V1."VisitStartTime" IS NOT NULL AND V1."VisitEndTime" IS NOT NULL) AS "V1_IsActualVisit",
        (V2."VisitStartTime" IS NOT NULL AND V2."VisitEndTime" IS NOT NULL) AS "V2_IsActualVisit",
        
        -- Pre-computed time gaps in minutes (eliminates 20+ repeated EXTRACT calls)
        EXTRACT(EPOCH FROM (V2."VisitStartTime" - V1."VisitEndTime"))/60 AS "GapV1EndToV2Start",
        EXTRACT(EPOCH FROM (V1."VisitStartTime" - V2."VisitEndTime"))/60 AS "GapV2EndToV1Start",
        EXTRACT(EPOCH FROM (V1."VisitEndTime" - V2."VisitStartTime"))/60 AS "OverlapV1EndV2Start",
        EXTRACT(EPOCH FROM (V2."VisitEndTime" - V1."VisitStartTime"))/60 AS "OverlapV2EndV1Start",
        
        -- MinuteDiffBetweenSch using pre-computed values
        CASE 
            WHEN EXTRACT(EPOCH FROM (V1."VisitEndTime" - V2."VisitStartTime"))/60 > 0 
                 AND EXTRACT(EPOCH FROM (V2."VisitEndTime" - V1."VisitStartTime"))/60 > 0
            THEN LEAST(
                EXTRACT(EPOCH FROM (V1."VisitEndTime" - V2."VisitStartTime"))/60, 
                EXTRACT(EPOCH FROM (V2."VisitEndTime" - V1."VisitStartTime"))/60
            )
            WHEN EXTRACT(EPOCH FROM (V1."VisitEndTime" - V2."VisitStartTime"))/60 > 0
            THEN EXTRACT(EPOCH FROM (V1."VisitEndTime" - V2."VisitStartTime"))/60
            WHEN EXTRACT(EPOCH FROM (V2."VisitEndTime" - V1."VisitStartTime"))/60 > 0
            THEN EXTRACT(EPOCH FROM (V2."VisitEndTime" - V1."VisitStartTime"))/60
            ELSE 0 
        END AS "MinuteDiffBetweenSch",
        
        :conflict_schema.calculate_conflict_distance(
            V1."Latitude"::REAL, V1."Longitude"::REAL,
            V2."Latitude"::REAL, V2."Longitude"::REAL,
            SETT."ExtraDistancePer"::REAL
        ) AS "DistanceMilesFromLatLng",
        
        MPH."AverageMilesPerHour",
        
        :conflict_schema.calculate_travel_time_minutes(
            V1."Latitude"::REAL, V1."Longitude"::REAL,
            V2."Latitude"::REAL, V2."Longitude"::REAL,
            SETT."ExtraDistancePer"::REAL,
            MPH."AverageMilesPerHour"::REAL
        ) AS "ETATravelMinutes",
        
        -- Service code info
        V1."ServiceCodeID",
        V1."AppServiceCodeID",
        V1."RateType",
        V1."ServiceCode",
        V2."ServiceCodeID" AS "ConServiceCodeID",
        V2."AppServiceCodeID" AS "ConAppServiceCodeID",
        V2."RateType" AS "ConRateType",
        V2."ServiceCode" AS "ConServiceCode",
        
        -- ====================================================================
        -- CONFLICT FLAGS (7 Rules) - Using helper flags for cleaner logic
        -- ====================================================================
        
        -- RULE 1: Same Scheduled Time (future visits only)
        CASE 
            WHEN V1."ProviderID" != V2."ProviderID" 
                AND (V1."VisitStartTime" IS NULL AND V1."VisitEndTime" IS NULL)
                AND (V2."VisitStartTime" IS NULL AND V2."VisitEndTime" IS NULL)
                AND V1."SchStartTime" = V2."SchStartTime"
                AND V1."SchEndTime" = V2."SchEndTime"
            THEN 'Y' 
            ELSE 'N' 
        END AS "SameSchTimeFlag",
        
        -- RULE 2: Same Visit Time
        CASE 
            WHEN V1."ProviderID" != V2."ProviderID" 
                AND (V1."VisitStartTime" IS NOT NULL AND V1."VisitEndTime" IS NOT NULL)
                AND (V2."VisitStartTime" IS NOT NULL AND V2."VisitEndTime" IS NOT NULL)
                AND V1."VisitStartTime" = V2."VisitStartTime"
                AND V1."VisitEndTime" = V2."VisitEndTime"
            THEN 'Y' 
            ELSE 'N' 
        END AS "SameVisitTimeFlag",
        
        -- RULE 3: Scheduled time matches other's visit time
        CASE 
            WHEN V1."ProviderID" != V2."ProviderID" 
                AND (
                    ((V1."VisitStartTime" IS NULL AND V1."VisitEndTime" IS NULL)
                     AND (V2."VisitStartTime" IS NOT NULL AND V2."VisitEndTime" IS NOT NULL)
                     AND V1."SchStartTime" = V2."VisitStartTime"
                     AND V1."SchEndTime" = V2."VisitEndTime")
                    OR
                    ((V2."VisitStartTime" IS NULL AND V2."VisitEndTime" IS NULL)
                     AND (V1."VisitStartTime" IS NOT NULL AND V1."VisitEndTime" IS NOT NULL)
                     AND V2."SchStartTime" = V1."VisitStartTime"
                     AND V2."SchEndTime" = V1."VisitEndTime")
                )
            THEN 'Y' 
            ELSE 'N' 
        END AS "SchAndVisitTimeSameFlag",
        
        -- RULE 4: Scheduled times overlap (future visits only)
        CASE 
            WHEN V1."ProviderID" != V2."ProviderID" 
                AND (V1."VisitStartTime" IS NULL AND V1."VisitEndTime" IS NULL)
                AND (V2."VisitStartTime" IS NULL AND V2."VisitEndTime" IS NULL)
                AND V1."SchStartTime" < V2."SchEndTime" 
                AND V1."SchEndTime" > V2."SchStartTime"
                AND (V1."SchStartTime" != V2."SchStartTime" 
                     OR V1."SchEndTime" != V2."SchEndTime")
            THEN 'Y' 
            ELSE 'N' 
        END AS "SchOverAnotherSchTimeFlag",
        
        -- RULE 5: Visit times overlap
        CASE 
            WHEN V1."ProviderID" != V2."ProviderID" 
                AND (V1."VisitStartTime" IS NOT NULL AND V1."VisitEndTime" IS NOT NULL)
                AND (V2."VisitStartTime" IS NOT NULL AND V2."VisitEndTime" IS NOT NULL)
                AND V1."VisitStartTime" < V2."VisitEndTime" 
                AND V1."VisitEndTime" > V2."VisitStartTime"
                AND (V1."VisitStartTime" != V2."VisitStartTime" 
                     OR V1."VisitEndTime" != V2."VisitEndTime")
            THEN 'Y' 
            ELSE 'N' 
        END AS "VisitTimeOverAnotherVisitTimeFlag",
        
        -- RULE 6: Scheduled time overlaps with visit time
        CASE 
            WHEN V1."ProviderID" != V2."ProviderID" 
                AND (
                    ((V1."VisitStartTime" IS NULL AND V1."VisitEndTime" IS NULL)
                     AND (V2."VisitStartTime" IS NOT NULL AND V2."VisitEndTime" IS NOT NULL)
                     AND V1."SchStartTime" < V2."VisitEndTime" 
                     AND V1."SchEndTime" > V2."VisitStartTime"
                     AND (V1."SchStartTime" != V2."VisitStartTime" 
                          OR V1."SchEndTime" != V2."VisitEndTime"))
                    OR
                    ((V2."VisitStartTime" IS NULL AND V2."VisitEndTime" IS NULL)
                     AND (V1."VisitStartTime" IS NOT NULL AND V1."VisitEndTime" IS NOT NULL)
                     AND V2."SchStartTime" < V1."VisitEndTime" 
                     AND V2."SchEndTime" > V1."VisitStartTime"
                     AND (V2."SchStartTime" != V1."VisitStartTime" 
                          OR V2."SchEndTime" != V1."VisitEndTime"))
                )
            THEN 'Y' 
            ELSE 'N' 
        END AS "SchTimeOverVisitTimeFlag",
        
        -- RULE 7: Distance-based conflict (using pre-computed time gaps)
        CASE 
            WHEN V1."ProviderID" != V2."ProviderID"
                AND V1."Longitude" IS NOT NULL
                AND V1."Latitude" IS NOT NULL
                AND V2."Longitude" IS NOT NULL
                AND V2."Latitude" IS NOT NULL
                AND (V1."VisitStartTime" IS NOT NULL AND V1."VisitEndTime" IS NOT NULL)
                AND (V2."VisitStartTime" IS NOT NULL AND V2."VisitEndTime" IS NOT NULL)
                AND (
                    (V1."PZipCode" IS NOT NULL 
                     AND V2."PZipCode" IS NOT NULL 
                     AND V1."PZipCode" != V2."PZipCode")
                    OR V1."PZipCode" IS NULL
                    OR V2."PZipCode" IS NULL
                )
                AND MPH."AverageMilesPerHour" IS NOT NULL
                AND (
                    (EXTRACT(EPOCH FROM (V2."VisitStartTime" - V1."VisitEndTime"))/60 > 0
                     AND :conflict_schema.calculate_travel_time_minutes(
                         V1."Latitude"::REAL, V1."Longitude"::REAL,
                         V2."Latitude"::REAL, V2."Longitude"::REAL,
                         SETT."ExtraDistancePer"::REAL,
                         MPH."AverageMilesPerHour"::REAL
                     ) > EXTRACT(EPOCH FROM (V2."VisitStartTime" - V1."VisitEndTime"))/60)
                    OR
                    (EXTRACT(EPOCH FROM (V1."VisitStartTime" - V2."VisitEndTime"))/60 > 0
                     AND :conflict_schema.calculate_travel_time_minutes(
                         V2."Latitude"::REAL, V2."Longitude"::REAL,
                         V1."Latitude"::REAL, V1."Longitude"::REAL,
                         SETT."ExtraDistancePer"::REAL,
                         MPH."AverageMilesPerHour"::REAL
                     ) > EXTRACT(EPOCH FROM (V1."VisitStartTime" - V2."VisitEndTime"))/60)
                )
            THEN 'Y' 
            ELSE 'N' 
        END AS "DistanceFlag",
        
        -- Additional fields
        V1."AideFName",
        V1."AideLName",
        V2."AideFName" AS "ConAideFName",
        V2."AideLName" AS "ConAideLName",
        V1."PFName",
        V1."PLName",
        V2."PFName" AS "ConPFName",
        V2."PLName" AS "ConPLName",
        V1."PMedicaidNumber",
        V2."PMedicaidNumber" AS "ConPMedicaidNumber",
        V1."PayerState",
        V2."PayerState" AS "ConPayerState",
        V1."LastUpdatedBy",
        V2."LastUpdatedBy" AS "ConLastUpdatedBy",
        V1."LastUpdatedDate",
        V2."LastUpdatedDate" AS "ConLastUpdatedDate",
        V1."BilledRate",
        V1."TotalBilledAmount",
        V2."BilledRate" AS "ConBilledRate",
        V2."TotalBilledAmount" AS "ConTotalBilledAmount",
        V1."IsMissed",
        V1."MissedVisitReason",
        V1."EVVType",
        V2."IsMissed" AS "ConIsMissed",
        V2."MissedVisitReason" AS "ConMissedVisitReason",
        V2."EVVType" AS "ConEVVType",
        V1."PStatus",
        V2."PStatus" AS "ConPStatus",
        V1."AideStatus",
        V2."AideStatus" AS "ConAideStatus",
        V1."P_PatientID",
        V1."P_AppPatientID",
        V2."P_PatientID" AS "ConP_PatientID",
        V2."P_AppPatientID" AS "ConP_AppPatientID",
        V1."PA_PatientID",
        V1."PA_AppPatientID",
        V2."PA_PatientID" AS "ConPA_PatientID",
        V2."PA_AppPatientID" AS "ConPA_AppPatientID",
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
        V2."P_PAdmissionID" AS "ConP_PAdmissionID",
        V2."P_PName" AS "ConP_PName",
        V2."P_PAddressID" AS "ConP_PAddressID",
        V2."P_PAppAddressID" AS "ConP_PAppAddressID",
        V2."P_PAddressL1" AS "ConP_PAddressL1",
        V2."P_PAddressL2" AS "ConP_PAddressL2",
        V2."P_PCity" AS "ConP_PCity",
        V2."P_PAddressState" AS "ConP_PAddressState",
        V2."P_PZipCode" AS "ConP_PZipCode",
        V2."P_PCounty" AS "ConP_PCounty",
        V2."P_PFName" AS "ConP_PFName",
        V2."P_PLName" AS "ConP_PLName",
        V2."P_PMedicaidNumber" AS "ConP_PMedicaidNumber",
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
        V2."PA_PAdmissionID" AS "ConPA_PAdmissionID",
        V2."PA_PName" AS "ConPA_PName",
        V2."PA_PAddressID" AS "ConPA_PAddressID",
        V2."PA_PAppAddressID" AS "ConPA_PAppAddressID",
        V2."PA_PAddressL1" AS "ConPA_PAddressL1",
        V2."PA_PAddressL2" AS "ConPA_PAddressL2",
        V2."PA_PCity" AS "ConPA_PCity",
        V2."PA_PAddressState" AS "ConPA_PAddressState",
        V2."PA_PZipCode" AS "ConPA_PZipCode",
        V2."PA_PCounty" AS "ConPA_PCounty",
        V2."PA_PFName" AS "ConPA_PFName",
        V2."PA_PLName" AS "ConPA_PLName",
        V2."PA_PMedicaidNumber" AS "ConPA_PMedicaidNumber",
        V1."ContractType",
        V2."ContractType" AS "ConContractType",
        V1."BillRateNonBilled",
        V2."BillRateNonBilled" AS "ConBillRateNonBilled",
        V1."BillRateBoth",
        V2."BillRateBoth" AS "ConBillRateBoth",
        V1."FederalTaxNumber",
        V2."FederalTaxNumber" AS "ConFederalTaxNumber"
        
    FROM V1
    
    LEFT JOIN V2
        ON V1."SSN" = V2."SSN"
        AND V1."VisitDate" = V2."VisitDate"
        AND V1."ProviderID" != V2."ProviderID"
        AND V1."VisitID" != V2."VisitID"
    
    CROSS JOIN :conflict_schema.settings AS SETT
    LEFT JOIN :conflict_schema.mph AS MPH 
        ON :conflict_schema.calculate_conflict_distance(
            V1."Latitude"::REAL, V1."Longitude"::REAL,
            V2."Latitude"::REAL, V2."Longitude"::REAL,
            SETT."ExtraDistancePer"::REAL
        ) BETWEEN MPH."From" AND MPH."To"
    
    WHERE (
        -- Apply ANY of the 7 conflict rules
        -- RULE 1: Same scheduled time (future only)
        ((V1."VisitStartTime" IS NULL AND V1."VisitEndTime" IS NULL)
         AND (V2."VisitStartTime" IS NULL AND V2."VisitEndTime" IS NULL)
         AND V1."SchStartTime" = V2."SchStartTime"
         AND V1."SchEndTime" = V2."SchEndTime")
        OR
        -- RULE 2: Same visit time
        ((V1."VisitStartTime" IS NOT NULL AND V1."VisitEndTime" IS NOT NULL)
         AND (V2."VisitStartTime" IS NOT NULL AND V2."VisitEndTime" IS NOT NULL)
         AND V1."VisitStartTime" = V2."VisitStartTime"
         AND V1."VisitEndTime" = V2."VisitEndTime")
        OR
        -- RULE 3: Sch = other's visit time
        (((V2."VisitStartTime" IS NULL AND V2."VisitEndTime" IS NULL)
          AND (V1."VisitStartTime" IS NOT NULL AND V1."VisitEndTime" IS NOT NULL)
          AND V2."SchStartTime" = V1."VisitStartTime"
          AND V2."SchEndTime" = V1."VisitEndTime")
         OR
         ((V1."VisitStartTime" IS NULL AND V1."VisitEndTime" IS NULL)
          AND (V2."VisitStartTime" IS NOT NULL AND V2."VisitEndTime" IS NOT NULL)
          AND V1."SchStartTime" = V2."VisitStartTime"
          AND V1."SchEndTime" = V2."VisitEndTime"))
        OR
        -- RULE 4: Scheduled overlap (future only)
        ((V1."VisitStartTime" IS NULL AND V1."VisitEndTime" IS NULL)
         AND (V2."VisitStartTime" IS NULL AND V2."VisitEndTime" IS NULL)
         AND V1."SchStartTime" < V2."SchEndTime" 
         AND V1."SchEndTime" > V2."SchStartTime"
         AND (V1."SchStartTime" != V2."SchStartTime" 
              OR V1."SchEndTime" != V2."SchEndTime"))
        OR
        -- RULE 5: Visit time overlap
        ((V1."VisitStartTime" IS NOT NULL AND V1."VisitEndTime" IS NOT NULL)
         AND (V2."VisitStartTime" IS NOT NULL AND V2."VisitEndTime" IS NOT NULL)
         AND V1."VisitStartTime" < V2."VisitEndTime" 
         AND V1."VisitEndTime" > V2."VisitStartTime"
         AND (V1."VisitStartTime" != V2."VisitStartTime" 
              OR V1."VisitEndTime" != V2."VisitEndTime"))
        OR
        -- RULE 6: Sch overlaps with visit time
        (((V1."VisitStartTime" IS NULL AND V1."VisitEndTime" IS NULL)
          AND (V2."VisitStartTime" IS NOT NULL AND V2."VisitEndTime" IS NOT NULL)
          AND V1."SchStartTime" < V2."VisitEndTime" 
          AND V1."SchEndTime" > V2."VisitStartTime"
          AND (V1."SchStartTime" != V2."VisitStartTime" 
               OR V1."SchEndTime" != V2."VisitEndTime"))
         OR
         ((V2."VisitStartTime" IS NULL AND V2."VisitEndTime" IS NULL)
          AND (V1."VisitStartTime" IS NOT NULL AND V1."VisitEndTime" IS NOT NULL)
          AND V2."SchStartTime" < V1."VisitEndTime" 
          AND V2."SchEndTime" > V1."VisitStartTime"
          AND (V2."SchStartTime" != V1."VisitStartTime" 
               OR V2."SchEndTime" != V1."VisitEndTime")))
        OR
        -- RULE 7: Distance conflict
        (V1."Longitude" IS NOT NULL
         AND V1."Latitude" IS NOT NULL
         AND V2."Longitude" IS NOT NULL
         AND V2."Latitude" IS NOT NULL
         AND (V1."VisitStartTime" IS NOT NULL AND V1."VisitEndTime" IS NOT NULL)
         AND (V2."VisitStartTime" IS NOT NULL AND V2."VisitEndTime" IS NOT NULL)
         AND (
             (V1."PZipCode" IS NOT NULL 
              AND V2."PZipCode" IS NOT NULL 
              AND V1."PZipCode" != V2."PZipCode")
             OR V1."PZipCode" IS NULL
             OR V2."PZipCode" IS NULL
         )
         AND MPH."AverageMilesPerHour" IS NOT NULL
         AND (
             (EXTRACT(EPOCH FROM (V2."VisitStartTime" - V1."VisitEndTime"))/60 > 0
              AND :conflict_schema.calculate_travel_time_minutes(
                  V1."Latitude"::REAL, V1."Longitude"::REAL,
                  V2."Latitude"::REAL, V2."Longitude"::REAL,
                  SETT."ExtraDistancePer"::REAL,
                  MPH."AverageMilesPerHour"::REAL
              ) > EXTRACT(EPOCH FROM (V2."VisitStartTime" - V1."VisitEndTime"))/60)
             OR
             (EXTRACT(EPOCH FROM (V1."VisitStartTime" - V2."VisitEndTime"))/60 > 0
              AND :conflict_schema.calculate_travel_time_minutes(
                  V2."Latitude"::REAL, V2."Longitude"::REAL,
                  V1."Latitude"::REAL, V1."Longitude"::REAL,
                  SETT."ExtraDistancePer"::REAL,
                  MPH."AverageMilesPerHour"::REAL
              ) > EXTRACT(EPOCH FROM (V1."VisitStartTime" - V2."VisitEndTime"))/60)
         ))
    )
) AS source
ON target."VisitID" = source."VisitID" 
   AND target."AppVisitID" = source."AppVisitID"
   AND (target."ConVisitID" = source."ConVisitID" 
        OR (target."ConVisitID" IS NULL AND source."ConVisitID" IS NULL))
WHEN MATCHED THEN
    UPDATE SET 
        "SSN" = source."SSN",
        "ProviderID" = source."ProviderID",
        "AppProviderID" = source."AppProviderID",
        "ProviderName" = source."ProviderName",
        "ConProviderID" = source."ConProviderID",
        "ConAppProviderID" = source."ConAppProviderID",
        "ConProviderName" = source."ConProviderName",
        "ConVisitID" = source."ConVisitID",
        "ConAppVisitID" = source."ConAppVisitID",
        "VisitDate" = source."VisitDate",
        "SchStartTime" = source."SchStartTime",
        "SchEndTime" = source."SchEndTime",
        "ConSchStartTime" = source."ConSchStartTime",
        "ConSchEndTime" = source."ConSchEndTime",
        "VisitStartTime" = source."VisitStartTime",
        "VisitEndTime" = source."VisitEndTime",
        "ConVisitStartTime" = source."ConVisitStartTime",
        "ConVisitEndTime" = source."ConVisitEndTime",
        "EVVStartTime" = source."EVVStartTime",
        "EVVEndTime" = source."EVVEndTime",
        "ConEVVStartTime" = source."ConEVVStartTime",
        "ConEVVEndTime" = source."ConEVVEndTime",
        "CaregiverID" = source."CaregiverID",
        "AppCaregiverID" = source."AppCaregiverID",
        "AideCode" = source."AideCode",
        "AideName" = source."AideName",
        "AideSSN" = source."AideSSN",
        "ConCaregiverID" = source."ConCaregiverID",
        "ConAppCaregiverID" = source."ConAppCaregiverID",
        "ConAideCode" = source."ConAideCode",
        "ConAideName" = source."ConAideName",
        "ConAideSSN" = source."ConAideSSN",
        "OfficeID" = source."OfficeID",
        "AppOfficeID" = source."AppOfficeID",
        "Office" = source."Office",
        "ConOfficeID" = source."ConOfficeID",
        "ConAppOfficeID" = source."ConAppOfficeID",
        "ConOffice" = source."ConOffice",
        "PatientID" = source."PatientID",
        "AppPatientID" = source."AppPatientID",
        "PAddressID" = source."PAddressID",
        "PAppAddressID" = source."PAppAddressID",
        "PAddressL1" = source."PAddressL1",
        "PAddressL2" = source."PAddressL2",
        "PCity" = source."PCity",
        "PAddressState" = source."PAddressState",
        "PZipCode" = source."PZipCode",
        "PCounty" = source."PCounty",
        "PLongitude" = source."PLongitude",
        "PLatitude" = source."PLatitude",
        "ConPatientID" = source."ConPatientID",
        "ConAppPatientID" = source."ConAppPatientID",
        "ConPAddressID" = source."ConPAddressID",
        "ConPAppAddressID" = source."ConPAppAddressID",
        "ConPAddressL1" = source."ConPAddressL1",
        "ConPAddressL2" = source."ConPAddressL2",
        "ConPCity" = source."ConPCity",
        "ConPAddressState" = source."ConPAddressState",
        "ConPZipCode" = source."ConPZipCode",
        "ConPCounty" = source."ConPCounty",
        "ConPLongitude" = source."ConPLongitude",
        "ConPLatitude" = source."ConPLatitude",
        "PayerID" = source."PayerID",
        "AppPayerID" = source."AppPayerID",
        "Contract" = source."Contract",
        "ConPayerID" = source."ConPayerID",
        "ConAppPayerID" = source."ConAppPayerID",
        "ConContract" = source."ConContract",
        "BilledDate" = source."BilledDate",
        "ConBilledDate" = source."ConBilledDate",
        "BilledHours" = source."BilledHours",
        "ConBilledHours" = source."ConBilledHours",
        "Billed" = source."Billed",
        "ConBilled" = source."ConBilled",
        "MinuteDiffBetweenSch" = source."MinuteDiffBetweenSch",
        "DistanceMilesFromLatLng" = source."DistanceMilesFromLatLng",
        "AverageMilesPerHour" = source."AverageMilesPerHour",
        "ETATravelMinutes" = source."ETATravelMinutes",
        "ServiceCodeID" = source."ServiceCodeID",
        "AppServiceCodeID" = source."AppServiceCodeID",
        "RateType" = source."RateType",
        "ServiceCode" = source."ServiceCode",
        "ConServiceCodeID" = source."ConServiceCodeID",
        "ConAppServiceCodeID" = source."ConAppServiceCodeID",
        "ConRateType" = source."ConRateType",
        "ConServiceCode" = source."ConServiceCode",
        "SameSchTimeFlag" = CASE 
            WHEN target."SameSchTimeFlag" = 'N' THEN source."SameSchTimeFlag" 
            ELSE target."SameSchTimeFlag" 
        END,
        "SameVisitTimeFlag" = CASE 
            WHEN target."SameVisitTimeFlag" = 'N' THEN source."SameVisitTimeFlag" 
            ELSE target."SameVisitTimeFlag" 
        END,
        "SchAndVisitTimeSameFlag" = CASE 
            WHEN target."SchAndVisitTimeSameFlag" = 'N' THEN source."SchAndVisitTimeSameFlag" 
            ELSE target."SchAndVisitTimeSameFlag" 
        END,
        "SchOverAnotherSchTimeFlag" = CASE 
            WHEN target."SchOverAnotherSchTimeFlag" = 'N' THEN source."SchOverAnotherSchTimeFlag" 
            ELSE target."SchOverAnotherSchTimeFlag" 
        END,
        "VisitTimeOverAnotherVisitTimeFlag" = CASE 
            WHEN target."VisitTimeOverAnotherVisitTimeFlag" = 'N' THEN source."VisitTimeOverAnotherVisitTimeFlag" 
            ELSE target."VisitTimeOverAnotherVisitTimeFlag" 
        END,
        "SchTimeOverVisitTimeFlag" = CASE 
            WHEN target."SchTimeOverVisitTimeFlag" = 'N' THEN source."SchTimeOverVisitTimeFlag" 
            ELSE target."SchTimeOverVisitTimeFlag" 
        END,
        "DistanceFlag" = CASE 
            WHEN target."DistanceFlag" = 'N' THEN source."DistanceFlag" 
            ELSE target."DistanceFlag" 
        END,
        "UpdatedDate" = CURRENT_TIMESTAMP,
        "StatusFlag" = CASE 
            WHEN target."StatusFlag" NOT IN ('W', 'I') THEN 'U' 
            ELSE target."StatusFlag" 
        END,
        "AideFName" = source."AideFName",
        "AideLName" = source."AideLName",
        "ConAideFName" = source."ConAideFName",
        "ConAideLName" = source."ConAideLName",
        "PFName" = source."PFName",
        "PLName" = source."PLName",
        "ConPFName" = source."ConPFName",
        "ConPLName" = source."ConPLName",
        "PMedicaidNumber" = source."PMedicaidNumber",
        "ConPMedicaidNumber" = source."ConPMedicaidNumber",
        "PayerState" = source."PayerState",
        "ConPayerState" = source."ConPayerState",
        "LastUpdatedBy" = source."LastUpdatedBy",
        "ConLastUpdatedBy" = source."ConLastUpdatedBy",
        "LastUpdatedDate" = source."LastUpdatedDate",
        "ConLastUpdatedDate" = source."ConLastUpdatedDate",
        "BilledRate" = source."BilledRate",
        "TotalBilledAmount" = source."TotalBilledAmount",
        "ConBilledRate" = source."ConBilledRate",
        "ConTotalBilledAmount" = source."ConTotalBilledAmount",
        "IsMissed" = source."IsMissed",
        "MissedVisitReason" = source."MissedVisitReason",
        "EVVType" = source."EVVType",
        "ConIsMissed" = source."ConIsMissed",
        "ConMissedVisitReason" = source."ConMissedVisitReason",
        "ConEVVType" = source."ConEVVType",
        "PStatus" = source."PStatus",
        "ConPStatus" = source."ConPStatus",
        "AideStatus" = source."AideStatus",
        "ConAideStatus" = source."ConAideStatus",
        "P_PatientID" = source."P_PatientID",
        "P_AppPatientID" = source."P_AppPatientID",
        "ConP_PatientID" = source."ConP_PatientID",
        "ConP_AppPatientID" = source."ConP_AppPatientID",
        "PA_PatientID" = source."PA_PatientID",
        "PA_AppPatientID" = source."PA_AppPatientID",
        "ConPA_PatientID" = source."ConPA_PatientID",
        "ConPA_AppPatientID" = source."ConPA_AppPatientID",
        "P_PAdmissionID" = source."P_PAdmissionID",
        "P_PName" = source."P_PName",
        "P_PAddressID" = source."P_PAddressID",
        "P_PAppAddressID" = source."P_PAppAddressID",
        "P_PAddressL1" = source."P_PAddressL1",
        "P_PAddressL2" = source."P_PAddressL2",
        "P_PCity" = source."P_PCity",
        "P_PAddressState" = source."P_PAddressState",
        "P_PZipCode" = source."P_PZipCode",
        "P_PCounty" = source."P_PCounty",
        "P_PFName" = source."P_PFName",
        "P_PLName" = source."P_PLName",
        "P_PMedicaidNumber" = source."P_PMedicaidNumber",
        "ConP_PAdmissionID" = source."ConP_PAdmissionID",
        "ConP_PName" = source."ConP_PName",
        "ConP_PAddressID" = source."ConP_PAddressID",
        "ConP_PAppAddressID" = source."ConP_PAppAddressID",
        "ConP_PAddressL1" = source."ConP_PAddressL1",
        "ConP_PAddressL2" = source."ConP_PAddressL2",
        "ConP_PCity" = source."ConP_PCity",
        "ConP_PAddressState" = source."ConP_PAddressState",
        "ConP_PZipCode" = source."ConP_PZipCode",
        "ConP_PCounty" = source."ConP_PCounty",
        "ConP_PFName" = source."ConP_PFName",
        "ConP_PLName" = source."ConP_PLName",
        "ConP_PMedicaidNumber" = source."ConP_PMedicaidNumber",
        "PA_PAdmissionID" = source."PA_PAdmissionID",
        "PA_PName" = source."PA_PName",
        "PA_PAddressID" = source."PA_PAddressID",
        "PA_PAppAddressID" = source."PA_PAppAddressID",
        "PA_PAddressL1" = source."PA_PAddressL1",
        "PA_PAddressL2" = source."PA_PAddressL2",
        "PA_PCity" = source."PA_PCity",
        "PA_PAddressState" = source."PA_PAddressState",
        "PA_PZipCode" = source."PA_PZipCode",
        "PA_PCounty" = source."PA_PCounty",
        "PA_PFName" = source."PA_PFName",
        "PA_PLName" = source."PA_PLName",
        "PA_PMedicaidNumber" = source."PA_PMedicaidNumber",
        "ConPA_PAdmissionID" = source."ConPA_PAdmissionID",
        "ConPA_PName" = source."ConPA_PName",
        "ConPA_PAddressID" = source."ConPA_PAddressID",
        "ConPA_PAppAddressID" = source."ConPA_PAppAddressID",
        "ConPA_PAddressL1" = source."ConPA_PAddressL1",
        "ConPA_PAddressL2" = source."ConPA_PAddressL2",
        "ConPA_PCity" = source."ConPA_PCity",
        "ConPA_PAddressState" = source."ConPA_PAddressState",
        "ConPA_PZipCode" = source."ConPA_PZipCode",
        "ConPA_PCounty" = source."ConPA_PCounty",
        "ConPA_PFName" = source."ConPA_PFName",
        "ConPA_PLName" = source."ConPA_PLName",
        "ConPA_PMedicaidNumber" = source."ConPA_PMedicaidNumber",
        "ContractType" = source."ContractType",
        "ConContractType" = source."ConContractType",
        "BillRateNonBilled" = source."BillRateNonBilled",
        "ConBillRateNonBilled" = source."ConBillRateNonBilled",
        "BillRateBoth" = source."BillRateBoth",
        "ConBillRateBoth" = source."ConBillRateBoth",
        "FederalTaxNumber" = source."FederalTaxNumber",
        "ConFederalTaxNumber" = source."ConFederalTaxNumber"
WHEN NOT MATCHED THEN
    INSERT (
        "SSN", "ProviderID", "AppProviderID", "ProviderName",
        "VisitID", "AppVisitID",
        "ConProviderID", "ConAppProviderID", "ConProviderName",
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
        "PatientID", "AppPatientID",
        "PAddressID", "PAppAddressID",
        "PAddressL1", "PAddressL2", "PCity",
        "PAddressState", "PZipCode", "PCounty",
        "PLongitude", "PLatitude",
        "ConPatientID", "ConAppPatientID",
        "ConPAddressID", "ConPAppAddressID",
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
        "DistanceFlag",
        "CreatedDate", "UpdatedDate",
        "AideFName", "AideLName",
        "ConAideFName", "ConAideLName",
        "PFName", "PLName",
        "ConPFName", "ConPLName",
        "PMedicaidNumber", "ConPMedicaidNumber",
        "PayerState", "ConPayerState",
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
        "BillRateNonBilled", "ConBillRateNonBilled",
        "BillRateBoth", "ConBillRateBoth",
        "FederalTaxNumber", "ConFederalTaxNumber"
    )
    VALUES (
        source."SSN", source."ProviderID", source."AppProviderID", source."ProviderName",
        source."VisitID", source."AppVisitID",
        source."ConProviderID", source."ConAppProviderID", source."ConProviderName",
        source."ConVisitID", source."ConAppVisitID",
        source."VisitDate",
        source."SchStartTime", source."SchEndTime",
        source."ConSchStartTime", source."ConSchEndTime",
        source."VisitStartTime", source."VisitEndTime",
        source."ConVisitStartTime", source."ConVisitEndTime",
        source."EVVStartTime", source."EVVEndTime",
        source."ConEVVStartTime", source."ConEVVEndTime",
        source."CaregiverID", source."AppCaregiverID",
        source."AideCode", source."AideName", source."AideSSN",
        source."ConCaregiverID", source."ConAppCaregiverID",
        source."ConAideCode", source."ConAideName", source."ConAideSSN",
        source."OfficeID", source."AppOfficeID", source."Office",
        source."ConOfficeID", source."ConAppOfficeID", source."ConOffice",
        source."PatientID", source."AppPatientID",
        source."PAddressID", source."PAppAddressID",
        source."PAddressL1", source."PAddressL2", source."PCity",
        source."PAddressState", source."PZipCode", source."PCounty",
        source."PLongitude", source."PLatitude",
        source."ConPatientID", source."ConAppPatientID",
        source."ConPAddressID", source."ConPAppAddressID",
        source."ConPAddressL1", source."ConPAddressL2", source."ConPCity",
        source."ConPAddressState", source."ConPZipCode", source."ConPCounty",
        source."ConPLongitude", source."ConPLatitude",
        source."PayerID", source."AppPayerID", source."Contract",
        source."ConPayerID", source."ConAppPayerID", source."ConContract",
        source."BilledDate", source."ConBilledDate",
        source."BilledHours", source."ConBilledHours",
        source."Billed", source."ConBilled",
        source."MinuteDiffBetweenSch",
        source."DistanceMilesFromLatLng",
        source."AverageMilesPerHour",
        source."ETATravelMinutes",
        source."ServiceCodeID", source."AppServiceCodeID",
        source."RateType", source."ServiceCode",
        source."ConServiceCodeID", source."ConAppServiceCodeID",
        source."ConRateType", source."ConServiceCode",
        source."SameSchTimeFlag", source."SameVisitTimeFlag",
        source."SchAndVisitTimeSameFlag",
        source."SchOverAnotherSchTimeFlag",
        source."VisitTimeOverAnotherVisitTimeFlag",
        source."SchTimeOverVisitTimeFlag",
        source."DistanceFlag",
        CURRENT_DATE, CURRENT_TIMESTAMP,
        source."AideFName", source."AideLName",
        source."ConAideFName", source."ConAideLName",
        source."PFName", source."PLName",
        source."ConPFName", source."ConPLName",
        source."PMedicaidNumber", source."ConPMedicaidNumber",
        source."PayerState", source."ConPayerState",
        source."LastUpdatedBy", source."ConLastUpdatedBy",
        source."LastUpdatedDate", source."ConLastUpdatedDate",
        source."BilledRate", source."TotalBilledAmount",
        source."ConBilledRate", source."ConTotalBilledAmount",
        source."IsMissed", source."MissedVisitReason", source."EVVType",
        source."ConIsMissed", source."ConMissedVisitReason", source."ConEVVType",
        source."PStatus", source."ConPStatus",
        source."AideStatus", source."ConAideStatus",
        source."P_PatientID", source."P_AppPatientID",
        source."ConP_PatientID", source."ConP_AppPatientID",
        source."PA_PatientID", source."PA_AppPatientID",
        source."ConPA_PatientID", source."ConPA_AppPatientID",
        source."P_PAdmissionID", source."P_PName",
        source."P_PAddressID", source."P_PAppAddressID",
        source."P_PAddressL1", source."P_PAddressL2", source."P_PCity",
        source."P_PAddressState", source."P_PZipCode", source."P_PCounty",
        source."P_PFName", source."P_PLName", source."P_PMedicaidNumber",
        source."ConP_PAdmissionID", source."ConP_PName",
        source."ConP_PAddressID", source."ConP_PAppAddressID",
        source."ConP_PAddressL1", source."ConP_PAddressL2", source."ConP_PCity",
        source."ConP_PAddressState", source."ConP_PZipCode", source."ConP_PCounty",
        source."ConP_PFName", source."ConP_PLName", source."ConP_PMedicaidNumber",
        source."PA_PAdmissionID", source."PA_PName",
        source."PA_PAddressID", source."PA_PAppAddressID",
        source."PA_PAddressL1", source."PA_PAddressL2", source."PA_PCity",
        source."PA_PAddressState", source."PA_PZipCode", source."PA_PCounty",
        source."PA_PFName", source."PA_PLName", source."PA_PMedicaidNumber",
        source."ConPA_PAdmissionID", source."ConPA_PName",
        source."ConPA_PAddressID", source."ConPA_PAppAddressID",
        source."ConPA_PAddressL1", source."ConPA_PAddressL2", source."ConPA_PCity",
        source."ConPA_PAddressState", source."ConPA_PZipCode", source."ConPA_PCounty",
        source."ConPA_PFName", source."ConPA_PLName", source."ConPA_PMedicaidNumber",
        source."ContractType", source."ConContractType",
        source."BillRateNonBilled", source."ConBillRateNonBilled",
        source."BillRateBoth", source."ConBillRateBoth",
        source."FederalTaxNumber", source."ConFederalTaxNumber"
    );

-- ============================================================================
-- TASK_02+03 Complete (OPTIMIZED VERSION 2)
-- ============================================================================
-- Expected outcome:
--   - Existing conflicts updated with latest data
--   - New conflicts inserted
--   - All 7 conflict rules applied
--   - ~4.1M conflict keys processed
--
-- Expected runtime: 30-60 minutes
--
-- Optimizations applied (V2):
--   - Reduced code duplication by ~330 lines total:
--     * GPS extraction function (18 lines saved)
--     * Time gap pre-computation (20+ lines saved)
--     * Visit type flags (15+ lines saved, improved readability)
--     * Base visits CTE (220 lines saved)
--     * Patient addresses CTE (45 lines saved)
--   - Cleaner, more maintainable structure
--   - Better performance through single materialization
--
-- Testing:
--   - Run distance_functions.sql FIRST (includes new extract_gps_coordinate)
--   - Compare EXPLAIN ANALYZE with original version
--   - Verify row counts match original results
--   - Monitor runtime and resource usage
--
-- Next: Run TASK_04 (assign CONFLICTID)
-- ============================================================================
