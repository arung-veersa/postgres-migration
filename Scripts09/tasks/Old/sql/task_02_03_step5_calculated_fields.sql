-- ======================================================================
-- TASK 04 - STEP 5: Calculated Fields
-- Converted from Snowflake UPDATE_DATA_CONFLICTVISITMAPS_3 procedure
-- 
-- This step calculates derived fields:
-- 1. Helper time fields (ShVTSTTime, ShVTENTime, etc.)
-- 2. Billed rate per minute calculations
--
-- Queries:
-- 1. updatequery - Calculate time helper fields
-- 2. updatequerya - Calculate BilledRateMinute and ConBilledRateMinute
-- ======================================================================

-- Query 1: Set helper time fields using COALESCE priority
-- ShVTSTTime = Visit Start Time, or Scheduled Start Time, or InService Start Date
-- ShVTENTime = Visit End Time, or Scheduled End Time, or InService End Date
UPDATE {conflict_schema}.conflictvisitmaps
SET 
    "ShVTSTTime" = COALESCE("VisitStartTime", "SchStartTime", "InserviceStartDate"),
    "ShVTENTime" = COALESCE("VisitEndTime", "SchEndTime", "InserviceEndDate"),
    "CShVTSTTime" = COALESCE("ConVisitStartTime", "ConSchStartTime", "ConInserviceStartDate"),
    "CShVTENTime" = COALESCE("ConVisitEndTime", "ConSchEndTime", "ConInserviceEndDate")
WHERE "VisitDate" >= '{start_date}'::timestamp
  AND "VisitDate" < ('{end_date}'::date + INTERVAL '1 day');


-- Query 2: Calculate BilledRateMinute and ConBilledRateMinute
-- Rate per minute based on RateType (Hourly, Daily, Visit)
-- Uses different formulas for billed vs non-billed visits
UPDATE {conflict_schema}.conflictvisitmaps
SET 
    "BilledRateMinute" = (
        CASE 
            -- Billed visits with Hourly rate
            WHEN "Billed" = 'yes' AND "RateType" = 'Hourly' AND "BillRateBoth" > 0 
                THEN "BillRateBoth" / 60
            -- Billed visits with Daily rate
            WHEN "Billed" = 'yes' AND "RateType" = 'Daily' AND "BillRateBoth" > 0 AND "BilledHours" > 0 
                THEN ("BillRateBoth" / "BilledHours") / 60
            -- Billed visits with Visit rate
            WHEN "Billed" = 'yes' AND "RateType" = 'Visit' AND "BillRateBoth" > 0 AND "BilledHours" > 0 
                THEN ("BillRateBoth" / "BilledHours") / 60
            -- Non-billed visits with Hourly rate
            WHEN "Billed" != 'yes' AND "RateType" = 'Hourly' AND "BillRateBoth" > 0 
                THEN "BillRateBoth" / 60
            -- Non-billed visits with Daily rate (use scheduled times)
            WHEN "Billed" != 'yes' AND "RateType" = 'Daily' AND "BillRateBoth" > 0 
                 AND "SchStartTime" IS NOT NULL AND "SchEndTime" IS NOT NULL 
                 AND "SchStartTime" != "SchEndTime" 
                THEN ("BillRateBoth" / (EXTRACT(EPOCH FROM ("SchEndTime" - "SchStartTime")) / 3600)) / 60
            -- Non-billed visits with Visit rate (use scheduled times)
            WHEN "Billed" != 'yes' AND "RateType" = 'Visit' AND "BillRateBoth" > 0 
                 AND "SchStartTime" IS NOT NULL AND "SchEndTime" IS NOT NULL 
                 AND "SchStartTime" != "SchEndTime" 
                THEN ("BillRateBoth" / (EXTRACT(EPOCH FROM ("SchEndTime" - "SchStartTime")) / 3600)) / 60
            ELSE 0
        END
    ),
    "ConBilledRateMinute" = (
        CASE 
            -- Billed visits with Hourly rate
            WHEN "ConBilled" = 'yes' AND "ConRateType" = 'Hourly' AND "ConBillRateBoth" > 0 
                THEN "ConBillRateBoth" / 60
            -- Billed visits with Daily rate
            WHEN "ConBilled" = 'yes' AND "ConRateType" = 'Daily' AND "ConBillRateBoth" > 0 AND "ConBilledHours" > 0 
                THEN ("ConBillRateBoth" / "ConBilledHours") / 60
            -- Billed visits with Visit rate
            WHEN "ConBilled" = 'yes' AND "ConRateType" = 'Visit' AND "ConBillRateBoth" > 0 AND "ConBilledHours" > 0 
                THEN ("ConBillRateBoth" / "ConBilledHours") / 60
            -- Non-billed visits with Hourly rate
            WHEN "ConBilled" != 'yes' AND "ConRateType" = 'Hourly' AND "ConBillRateBoth" > 0 
                THEN "ConBillRateBoth" / 60
            -- Non-billed visits with Daily rate (use scheduled times)
            WHEN "ConBilled" != 'yes' AND "ConRateType" = 'Daily' AND "ConBillRateBoth" > 0 
                 AND "ConSchStartTime" IS NOT NULL AND "ConSchEndTime" IS NOT NULL 
                 AND "ConSchStartTime" != "ConSchEndTime" 
                THEN ("ConBillRateBoth" / (EXTRACT(EPOCH FROM ("ConSchEndTime" - "ConSchStartTime")) / 3600)) / 60
            -- Non-billed visits with Visit rate (use scheduled times)
            WHEN "ConBilled" != 'yes' AND "ConRateType" = 'Visit' AND "ConBillRateBoth" > 0 
                 AND "ConSchStartTime" IS NOT NULL AND "ConSchEndTime" IS NOT NULL 
                 AND "ConSchStartTime" != "ConSchEndTime" 
                THEN ("ConBillRateBoth" / (EXTRACT(EPOCH FROM ("ConSchEndTime" - "ConSchStartTime")) / 3600)) / 60
            ELSE 0
        END
    )
WHERE "VisitDate" >= '{start_date}'::timestamp
  AND "VisitDate" < ('{end_date}'::date + INTERVAL '1 day');
