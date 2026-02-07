TRUNCATE TABLE CONFLICTREPORT.PUBLIC.PROVIDER_DASHBOARD_TOP;
INSERT INTO CONFLICTREPORT.PUBLIC.PROVIDER_DASHBOARD_TOP (
  PROVIDERID, OFFICEID, TODAYTOTAL, 
  TODAYSHIFTPRICE, TODAYOVERLAPPRICE, 
  SEVENTOTAL, SEVENFINALPRICE, THIRTYTOTAL, 
  THIRTYFINALPRICE
) 
SELECT 
  CVM."ProviderID" AS PROVIDERID, 
  CVM."OfficeID" AS OFFICEID, 
  COUNT(
    DISTINCT CASE WHEN TO_CHAR(
      CVM."CRDATEUNIQUE", 'YYYY-MM-DD'
    ) = TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') THEN CVM.CONFLICTID END
  ) AS TodayTotal, 
  SUM(
    CASE WHEN TO_CHAR(
      CVM."CRDATEUNIQUE", 'YYYY-MM-DD'
    ) = TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') THEN CVMCH."ShiftPrice" ELSE 0 END
  ) AS TodayShiftPrice, 
  SUM(
    CASE WHEN TO_CHAR(
      CVM."CRDATEUNIQUE", 'YYYY-MM-DD'
    ) = TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') THEN CVMCH."OverlapPrice" ELSE 0 END
  ) AS TodayOverlapPrice, 
  COUNT(
    DISTINCT CASE WHEN TO_CHAR(
      CVM."CRDATEUNIQUE", 'YYYY-MM-DD'
    ) BETWEEN TO_CHAR(CURRENT_DATE - 7, 'YYYY-MM-DD') 
    AND TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') 
    AND V2."StatusFlag" IN ('R', 'D') THEN CVM.CONFLICTID END
  ) AS SevenTotal, 
  SUM(
    CASE WHEN TO_CHAR(
      CVM."CRDATEUNIQUE", 'YYYY-MM-DD'
    ) BETWEEN TO_CHAR(CURRENT_DATE - 7, 'YYYY-MM-DD') 
    AND TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') 
    AND V2."StatusFlag" IN ('R', 'D') THEN CVMCH."OverlapPrice" ELSE 0 END
  ) AS SevenFinalPrice, 
  COUNT(
    DISTINCT CASE WHEN TO_CHAR(
      CVM."CRDATEUNIQUE", 'YYYY-MM-DD'
    ) BETWEEN TO_CHAR(CURRENT_DATE - 30, 'YYYY-MM-DD') 
    AND TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') 
    AND V2."StatusFlag" IN ('R', 'D') THEN CVM.CONFLICTID END
  ) AS ThirtyTotal, 
  SUM(
    CASE WHEN TO_CHAR(
      CVM."CRDATEUNIQUE", 'YYYY-MM-DD'
    ) BETWEEN TO_CHAR(CURRENT_DATE - 30, 'YYYY-MM-DD') 
    AND TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') 
    AND V2."StatusFlag" IN ('R', 'D') THEN CVMCH."OverlapPrice" ELSE 0 END
  ) AS ThirtyFinalPrice, 
FROM 
  CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS CVM 
  INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 ON V2."CONFLICTID" = CVM."CONFLICTID" 
  LEFT JOIN (
    SELECT 
      CVM1.ID, 
      CASE WHEN CVM1."BilledRateMinute" > 0 THEN TIMESTAMPDIFF(
        MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime"
      ) * CVM1."BilledRateMinute" ELSE 0 END AS "ShiftPrice", 
      ROW_NUMBER() OVER (
        PARTITION BY CVM1."CONFLICTID" 
        ORDER BY 
          CASE WHEN CVM1."CShVTSTTime" >= CVM1."ShVTSTTime" 
          AND CVM1."CShVTSTTime" <= CVM1."ShVTENTime" 
          AND CVM1."CShVTENTime" > CVM1."ShVTENTime" THEN TIMESTAMPDIFF(
            MINUTE, CVM1."CShVTSTTime", CVM1."ShVTENTime"
          ) WHEN CVM1."ShVTSTTime" >= CVM1."CShVTSTTime" 
          AND CVM1."ShVTSTTime" <= CVM1."CShVTENTime" 
          AND CVM1."ShVTENTime" > CVM1."CShVTENTime" THEN TIMESTAMPDIFF(
            MINUTE, CVM1."ShVTSTTime", CVM1."CShVTENTime"
          ) WHEN CVM1."CShVTSTTime" >= CVM1."ShVTSTTime" 
          AND CVM1."CShVTENTime" <= CVM1."ShVTENTime" THEN TIMESTAMPDIFF(
            MINUTE, CVM1."CShVTSTTime", CVM1."CShVTENTime"
          ) WHEN CVM1."ShVTSTTime" >= CVM1."CShVTSTTime" 
          AND CVM1."ShVTENTime" <= CVM1."CShVTENTime" THEN TIMESTAMPDIFF(
            MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime"
          ) WHEN CVM1."CShVTSTTime" < CVM1."ShVTSTTime" 
          AND CVM1."CShVTENTime" > CVM1."ShVTENTime" THEN TIMESTAMPDIFF(
            MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime"
          ) WHEN CVM1."ShVTSTTime" < CVM1."CShVTSTTime" 
          AND CVM1."ShVTENTime" > CVM1."CShVTENTime" THEN TIMESTAMPDIFF(
            MINUTE, CVM1."CShVTSTTime", CVM1."CShVTENTime"
          ) ELSE 0 END DESC
      ) AS RN, 
      CASE WHEN CVM1."BilledRateMinute" > 0 
      AND CVM1."CShVTSTTime" >= CVM1."ShVTSTTime" 
      AND CVM1."CShVTSTTime" <= CVM1."ShVTENTime" 
      AND CVM1."CShVTENTime" > CVM1."ShVTENTime" THEN TIMESTAMPDIFF(
        MINUTE, CVM1."CShVTSTTime", CVM1."ShVTENTime"
      ) * CVM1."BilledRateMinute" WHEN CVM1."BilledRateMinute" > 0 
      AND CVM1."ShVTSTTime" >= CVM1."CShVTSTTime" 
      AND CVM1."ShVTSTTime" <= CVM1."CShVTENTime" 
      AND CVM1."ShVTENTime" > CVM1."CShVTENTime" THEN TIMESTAMPDIFF(
        MINUTE, CVM1."ShVTSTTime", CVM1."CShVTENTime"
      ) * CVM1."BilledRateMinute" WHEN CVM1."BilledRateMinute" > 0 
      AND CVM1."CShVTSTTime" >= CVM1."ShVTSTTime" 
      AND CVM1."CShVTENTime" <= CVM1."ShVTENTime" THEN TIMESTAMPDIFF(
        MINUTE, CVM1."CShVTSTTime", CVM1."CShVTENTime"
      ) * CVM1."BilledRateMinute" WHEN CVM1."BilledRateMinute" > 0 
      AND CVM1."ShVTSTTime" >= CVM1."CShVTSTTime" 
      AND CVM1."ShVTENTime" <= CVM1."CShVTENTime" THEN TIMESTAMPDIFF(
        MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime"
      ) * CVM1."BilledRateMinute" WHEN CVM1."BilledRateMinute" > 0 
      AND CVM1."CShVTSTTime" < CVM1."ShVTSTTime" 
      AND CVM1."CShVTENTime" > CVM1."ShVTENTime" THEN TIMESTAMPDIFF(
        MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime"
      ) * CVM1."BilledRateMinute" WHEN CVM1."BilledRateMinute" > 0 
      AND CVM1."ShVTSTTime" < CVM1."CShVTSTTime" 
      AND CVM1."ShVTENTime" > CVM1."CShVTENTime" THEN TIMESTAMPDIFF(
        MINUTE, CVM1."CShVTSTTime", CVM1."CShVTENTime"
      ) * CVM1."BilledRateMinute" ELSE 0 END AS "OverlapPrice" 
    FROM 
      CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS CVM1
  ) AS CVMCH ON CVMCH.ID = CVM.ID 
  AND CVMCH.RN = 1 
WHERE 
  TO_CHAR(
    CVM."CRDATEUNIQUE", 'YYYY-MM-DD'
  ) BETWEEN TO_CHAR(CURRENT_DATE - 30, 'YYYY-MM-DD') 
  AND TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') --AND (CVM."SchOverAnotherSchTimeFlag" = 'Y'
  --  OR CVM."VisitTimeOverAnotherVisitTimeFlag" = 'Y')    
GROUP BY 
  CVM."ProviderID", 
  CVM."OfficeID";
TRUNCATE TABLE CONFLICTREPORT.PUBLIC.PROVIDER_DASHBOARD_CON_TYP;
INSERT INTO CONFLICTREPORT.PUBLIC.PROVIDER_DASHBOARD_CON_TYP (
  PROVIDERID, OFFICEID, CRDATEUNIQUE, 
  EX_ST_MATCH_TO, EX_ST_MATCH_SP, 
  EX_ST_MATCH_OP, EX_ST_MATCH_FP, 
  EX_VT_MATCH_TO, EX_VT_MATCH_SP, 
  EX_VT_MATCH_OP, EX_VT_MATCH_FP, 
  EX_ST_VT_MATCH_TO, EX_ST_VT_MATCH_SP, 
  EX_ST_VT_MATCH_OP, EX_ST_VT_MATCH_FP, 
  ST_OVR_TO, ST_OVR_SP, ST_OVR_OP, 
  ST_OVR_FP, VT_OVR_TO, VT_OVR_SP, 
  VT_OVR_OP, VT_OVR_FP, ST_VT_OVR_TO, 
  ST_VT_OVR_SP, ST_VT_OVR_OP, ST_VT_OVR_FP, 
  TD_TO, TD_SP, TD_OP, TD_FP, IN_TO, 
  IN_SP, IN_OP, IN_FP, PT_TO, PT_SP, 
  PT_OP, PT_FP
) 
SELECT 
  CVM."ProviderID" AS PROVIDERID, 
  CVM."OfficeID" AS OFFICEID, 
  TO_CHAR(
    CVM."CRDATEUNIQUE", 'YYYY-MM-DD'
  ) AS CRDATEUNIQUE, 
  COUNT(
    DISTINCT CASE WHEN CVM."SameSchTimeFlag" = 'Y' THEN CVM.CONFLICTID END
  ) AS EX_ST_MATCH_TO, 
  SUM(
    CASE WHEN CVM."SameSchTimeFlag" = 'Y' THEN CVMCH."ShiftPrice" ELSE 0 END
  ) AS EX_ST_MATCH_SP, 
  SUM(
    CASE WHEN CVM."SameSchTimeFlag" = 'Y' THEN CVMCH."OverlapPrice" ELSE 0 END
  ) AS EX_ST_MATCH_OP, 
  SUM(
    CASE WHEN CVM."SameSchTimeFlag" = 'Y' 
    AND V2."StatusFlag" IN ('R', 'D') THEN CVMCH."OverlapPrice" ELSE 0 END
  ) AS EX_ST_MATCH_FP, 
  COUNT(
    DISTINCT CASE WHEN CVM."SameVisitTimeFlag" = 'Y' THEN CVM.CONFLICTID END
  ) AS EX_VT_MATCH_TO, 
  SUM(
    CASE WHEN CVM."SameVisitTimeFlag" = 'Y' THEN CVMCH."ShiftPrice" ELSE 0 END
  ) AS EX_VT_MATCH_SP, 
  SUM(
    CASE WHEN CVM."SameVisitTimeFlag" = 'Y' THEN CVMCH."OverlapPrice" ELSE 0 END
  ) AS EX_VT_MATCH_OP, 
  SUM(
    CASE WHEN CVM."SameVisitTimeFlag" = 'Y' 
    AND V2."StatusFlag" IN ('R', 'D') THEN CVMCH."OverlapPrice" ELSE 0 END
  ) AS EX_VT_MATCH_FP, 
  COUNT(
    DISTINCT CASE WHEN CVM."SchAndVisitTimeSameFlag" = 'Y' THEN CVM.CONFLICTID END
  ) AS EX_ST_VT_MATCH_TO, 
  SUM(
    CASE WHEN CVM."SchAndVisitTimeSameFlag" = 'Y' THEN CVMCH."ShiftPrice" ELSE 0 END
  ) AS EX_ST_VT_MATCH_SP, 
  SUM(
    CASE WHEN CVM."SchAndVisitTimeSameFlag" = 'Y' THEN CVMCH."OverlapPrice" ELSE 0 END
  ) AS EX_ST_VT_MATCH_OP, 
  SUM(
    CASE WHEN CVM."SchAndVisitTimeSameFlag" = 'Y' 
    AND V2."StatusFlag" IN ('R', 'D') THEN CVMCH."OverlapPrice" ELSE 0 END
  ) AS EX_ST_VT_MATCH_FP, 
  COUNT(
    DISTINCT CASE WHEN CVM."SchOverAnotherSchTimeFlag" = 'Y' THEN CVM.CONFLICTID END
  ) AS ST_OVR_TO, 
  SUM(
    CASE WHEN CVM."SchOverAnotherSchTimeFlag" = 'Y' THEN CVMCH."ShiftPrice" ELSE 0 END
  ) AS ST_OVR_SP, 
  SUM(
    CASE WHEN CVM."SchOverAnotherSchTimeFlag" = 'Y' THEN CVMCH."OverlapPrice" ELSE 0 END
  ) AS ST_OVR_OP, 
  SUM(
    CASE WHEN CVM."SchOverAnotherSchTimeFlag" = 'Y' 
    AND V2."StatusFlag" IN ('R', 'D') THEN CVMCH."OverlapPrice" ELSE 0 END
  ) AS ST_OVR_FP, 
  COUNT(
    DISTINCT CASE WHEN CVM."VisitTimeOverAnotherVisitTimeFlag" = 'Y' THEN CVM.CONFLICTID END
  ) AS VT_OVR_TO, 
  SUM(
    CASE WHEN CVM."VisitTimeOverAnotherVisitTimeFlag" = 'Y' THEN CVMCH."ShiftPrice" ELSE 0 END
  ) AS VT_OVR_SP, 
  SUM(
    CASE WHEN CVM."VisitTimeOverAnotherVisitTimeFlag" = 'Y' THEN CVMCH."OverlapPrice" ELSE 0 END
  ) AS VT_OVR_OP, 
  SUM(
    CASE WHEN CVM."VisitTimeOverAnotherVisitTimeFlag" = 'Y' 
    AND V2."StatusFlag" IN ('R', 'D') THEN CVMCH."OverlapPrice" ELSE 0 END
  ) AS VT_OVR_FP, 
  COUNT(
    DISTINCT CASE WHEN CVM."SchTimeOverVisitTimeFlag" = 'Y' THEN CVM.CONFLICTID END
  ) AS ST_VT_OVR_TO, 
  SUM(
    CASE WHEN CVM."SchTimeOverVisitTimeFlag" = 'Y' THEN CVMCH."ShiftPrice" ELSE 0 END
  ) AS ST_VT_OVR_SP, 
  SUM(
    CASE WHEN CVM."SchTimeOverVisitTimeFlag" = 'Y' THEN CVMCH."OverlapPrice" ELSE 0 END
  ) AS ST_VT_OVR_OP, 
  SUM(
    CASE WHEN CVM."SchTimeOverVisitTimeFlag" = 'Y' 
    AND V2."StatusFlag" IN ('R', 'D') THEN CVMCH."OverlapPrice" ELSE 0 END
  ) AS ST_VT_OVR_FP, 
  COUNT(
    DISTINCT CASE WHEN CVM."DistanceFlag" = 'Y' THEN CVM.CONFLICTID END
  ) AS TD_TO, 
  SUM(
    CASE WHEN CVM."DistanceFlag" = 'Y' THEN CVMCH."ShiftPrice" ELSE 0 END
  ) AS TD_SP, 
  SUM(
    CASE WHEN CVM."DistanceFlag" = 'Y' THEN CVMCH."OverlapPrice" ELSE 0 END
  ) AS TD_OP, 
  SUM(
    CASE WHEN CVM."DistanceFlag" = 'Y' 
    AND V2."StatusFlag" IN ('R', 'D') THEN CVMCH."OverlapPrice" ELSE 0 END
  ) AS TD_FP, 
  COUNT(
    DISTINCT CASE WHEN CVM."InServiceFlag" = 'Y' THEN CVM.CONFLICTID END
  ) AS IN_TO, 
  SUM(
    CASE WHEN CVM."InServiceFlag" = 'Y' THEN CVMCH."ShiftPrice" ELSE 0 END
  ) AS IN_SP, 
  SUM(
    CASE WHEN CVM."InServiceFlag" = 'Y' THEN CVMCH."OverlapPrice" ELSE 0 END
  ) AS IN_OP, 
  SUM(
    CASE WHEN CVM."InServiceFlag" = 'Y' 
    AND V2."StatusFlag" IN ('R', 'D') THEN CVMCH."OverlapPrice" ELSE 0 END
  ) AS IN_FP, 
  COUNT(
    DISTINCT CASE WHEN CVM."PTOFlag" = 'Y' THEN CVM.CONFLICTID END
  ) AS PT_TO, 
  SUM(
    CASE WHEN CVM."PTOFlag" = 'Y' THEN CVMCH."ShiftPrice" ELSE 0 END
  ) AS PT_SP, 
  SUM(
    CASE WHEN CVM."PTOFlag" = 'Y' THEN CVMCH."OverlapPrice" ELSE 0 END
  ) AS PT_OP, 
  SUM(
    CASE WHEN CVM."PTOFlag" = 'Y' 
    AND V2."StatusFlag" IN ('R', 'D') THEN CVMCH."OverlapPrice" ELSE 0 END
  ) AS PT_FP 
FROM 
  CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS CVM 
  INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 ON V2."CONFLICTID" = CVM."CONFLICTID" 
  LEFT JOIN (
    SELECT 
      CVM1.ID, 
      CASE WHEN CVM1."BilledRateMinute" > 0 THEN TIMESTAMPDIFF(
        MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime"
      ) * CVM1."BilledRateMinute" ELSE 0 END AS "ShiftPrice", 
      ROW_NUMBER() OVER (
        PARTITION BY CVM1."CONFLICTID" 
        ORDER BY 
          CASE WHEN CVM1."CShVTSTTime" >= CVM1."ShVTSTTime" 
          AND CVM1."CShVTSTTime" <= CVM1."ShVTENTime" 
          AND CVM1."CShVTENTime" > CVM1."ShVTENTime" THEN TIMESTAMPDIFF(
            MINUTE, CVM1."CShVTSTTime", CVM1."ShVTENTime"
          ) WHEN CVM1."ShVTSTTime" >= CVM1."CShVTSTTime" 
          AND CVM1."ShVTSTTime" <= CVM1."CShVTENTime" 
          AND CVM1."ShVTENTime" > CVM1."CShVTENTime" THEN TIMESTAMPDIFF(
            MINUTE, CVM1."ShVTSTTime", CVM1."CShVTENTime"
          ) WHEN CVM1."CShVTSTTime" >= CVM1."ShVTSTTime" 
          AND CVM1."CShVTENTime" <= CVM1."ShVTENTime" THEN TIMESTAMPDIFF(
            MINUTE, CVM1."CShVTSTTime", CVM1."CShVTENTime"
          ) WHEN CVM1."ShVTSTTime" >= CVM1."CShVTSTTime" 
          AND CVM1."ShVTENTime" <= CVM1."CShVTENTime" THEN TIMESTAMPDIFF(
            MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime"
          ) WHEN CVM1."CShVTSTTime" < CVM1."ShVTSTTime" 
          AND CVM1."CShVTENTime" > CVM1."ShVTENTime" THEN TIMESTAMPDIFF(
            MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime"
          ) WHEN CVM1."ShVTSTTime" < CVM1."CShVTSTTime" 
          AND CVM1."ShVTENTime" > CVM1."CShVTENTime" THEN TIMESTAMPDIFF(
            MINUTE, CVM1."CShVTSTTime", CVM1."CShVTENTime"
          ) ELSE 0 END DESC
      ) AS RN, 
      CASE WHEN CVM1."BilledRateMinute" > 0 
      AND CVM1."CShVTSTTime" >= CVM1."ShVTSTTime" 
      AND CVM1."CShVTSTTime" <= CVM1."ShVTENTime" 
      AND CVM1."CShVTENTime" > CVM1."ShVTENTime" THEN TIMESTAMPDIFF(
        MINUTE, CVM1."CShVTSTTime", CVM1."ShVTENTime"
      ) * CVM1."BilledRateMinute" WHEN CVM1."BilledRateMinute" > 0 
      AND CVM1."ShVTSTTime" >= CVM1."CShVTSTTime" 
      AND CVM1."ShVTSTTime" <= CVM1."CShVTENTime" 
      AND CVM1."ShVTENTime" > CVM1."CShVTENTime" THEN TIMESTAMPDIFF(
        MINUTE, CVM1."ShVTSTTime", CVM1."CShVTENTime"
      ) * CVM1."BilledRateMinute" WHEN CVM1."BilledRateMinute" > 0 
      AND CVM1."CShVTSTTime" >= CVM1."ShVTSTTime" 
      AND CVM1."CShVTENTime" <= CVM1."ShVTENTime" THEN TIMESTAMPDIFF(
        MINUTE, CVM1."CShVTSTTime", CVM1."CShVTENTime"
      ) * CVM1."BilledRateMinute" WHEN CVM1."BilledRateMinute" > 0 
      AND CVM1."ShVTSTTime" >= CVM1."CShVTSTTime" 
      AND CVM1."ShVTENTime" <= CVM1."CShVTENTime" THEN TIMESTAMPDIFF(
        MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime"
      ) * CVM1."BilledRateMinute" WHEN CVM1."BilledRateMinute" > 0 
      AND CVM1."CShVTSTTime" < CVM1."ShVTSTTime" 
      AND CVM1."CShVTENTime" > CVM1."ShVTENTime" THEN TIMESTAMPDIFF(
        MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime"
      ) * CVM1."BilledRateMinute" WHEN CVM1."BilledRateMinute" > 0 
      AND CVM1."ShVTSTTime" < CVM1."CShVTSTTime" 
      AND CVM1."ShVTENTime" > CVM1."CShVTENTime" THEN TIMESTAMPDIFF(
        MINUTE, CVM1."CShVTSTTime", CVM1."CShVTENTime"
      ) * CVM1."BilledRateMinute" ELSE 0 END AS "OverlapPrice" 
    FROM 
      CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS CVM1
  ) AS CVMCH ON CVMCH.ID = CVM.ID 
  AND CVMCH.RN = 1 
GROUP BY 
  TO_CHAR(
    CVM."CRDATEUNIQUE", 'YYYY-MM-DD'
  ), 
  CVM."ProviderID", 
  CVM."OfficeID";
TRUNCATE TABLE CONFLICTREPORT.PUBLIC.PROVIDER_DASHBOARD_AGENCY;
INSERT INTO CONFLICTREPORT.PUBLIC.PROVIDER_DASHBOARD_AGENCY (
  PROVIDERID, OFFICEID, CRDATEUNIQUE, 
  CONPROVIDERID, CON_P_NAME, CON_TIN, 
  CON_TO, CON_SP, CON_OP, CON_FP
) 
SELECT 
  CVM."ProviderID" AS PROVIDERID, 
  CVM."OfficeID" AS OFFICEID, 
  TO_CHAR(
    CVM."CRDATEUNIQUE", 'YYYY-MM-DD'
  ) AS CRDATEUNIQUE, 
  CVM."ConProviderID" AS CONPROVIDERID, 
  CVM."ConProviderName" AS CON_P_NAME, 
  CVM."ConFederalTaxNumber" AS CON_TIN, 
  COUNT(DISTINCT CVM.CONFLICTID) AS CON_TO, 
  SUM(
    CASE WHEN CVMCH."ShiftAmount" IS NOT NULL THEN CVMCH."ShiftAmount" ELSE 0 END
  ) AS CON_SP, 
  SUM(
    CASE WHEN CVMCH."OverLapAmount" IS NOT NULL THEN CVMCH."OverLapAmount" ELSE 0 END
  ) AS CON_OP, 
  SUM(
    CASE WHEN V2."StatusFlag" IN ('R', 'D') THEN CVMCH."OverLapAmount" ELSE 0 END
  ) AS CON_FP 
FROM 
  CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS CVM 
  INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 ON V2."CONFLICTID" = CVM."CONFLICTID" 
  LEFT JOIN (
    SELECT 
      CVM1.ID, 
      CASE WHEN CVM1."BilledRateMinute" > 0 THEN TIMESTAMPDIFF(
        MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime"
      ) * CVM1."BilledRateMinute" ELSE 0 END AS "ShiftAmount", 
      ROW_NUMBER() OVER (
        PARTITION BY CVM1."CONFLICTID" 
        ORDER BY 
          CASE WHEN CVM1."CShVTSTTime" >= CVM1."ShVTSTTime" 
          AND CVM1."CShVTSTTime" <= CVM1."ShVTENTime" 
          AND CVM1."CShVTENTime" > CVM1."ShVTENTime" THEN TIMESTAMPDIFF(
            MINUTE, CVM1."CShVTSTTime", CVM1."ShVTENTime"
          ) WHEN CVM1."ShVTSTTime" >= CVM1."CShVTSTTime" 
          AND CVM1."ShVTSTTime" <= CVM1."CShVTENTime" 
          AND CVM1."ShVTENTime" > CVM1."CShVTENTime" THEN TIMESTAMPDIFF(
            MINUTE, CVM1."ShVTSTTime", CVM1."CShVTENTime"
          ) WHEN CVM1."CShVTSTTime" >= CVM1."ShVTSTTime" 
          AND CVM1."CShVTENTime" <= CVM1."ShVTENTime" THEN TIMESTAMPDIFF(
            MINUTE, CVM1."CShVTSTTime", CVM1."CShVTENTime"
          ) WHEN CVM1."ShVTSTTime" >= CVM1."CShVTSTTime" 
          AND CVM1."ShVTENTime" <= CVM1."CShVTENTime" THEN TIMESTAMPDIFF(
            MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime"
          ) WHEN CVM1."CShVTSTTime" < CVM1."ShVTSTTime" 
          AND CVM1."CShVTENTime" > CVM1."ShVTENTime" THEN TIMESTAMPDIFF(
            MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime"
          ) WHEN CVM1."ShVTSTTime" < CVM1."CShVTSTTime" 
          AND CVM1."ShVTENTime" > CVM1."CShVTENTime" THEN TIMESTAMPDIFF(
            MINUTE, CVM1."CShVTSTTime", CVM1."CShVTENTime"
          ) ELSE 0 END DESC
      ) AS RN, 
      CASE WHEN CVM1."BilledRateMinute" > 0 
      AND CVM1."CShVTSTTime" >= CVM1."ShVTSTTime" 
      AND CVM1."CShVTSTTime" <= CVM1."ShVTENTime" 
      AND CVM1."CShVTENTime" > CVM1."ShVTENTime" THEN TIMESTAMPDIFF(
        MINUTE, CVM1."CShVTSTTime", CVM1."ShVTENTime"
      ) * CVM1."BilledRateMinute" WHEN CVM1."BilledRateMinute" > 0 
      AND CVM1."ShVTSTTime" >= CVM1."CShVTSTTime" 
      AND CVM1."ShVTSTTime" <= CVM1."CShVTENTime" 
      AND CVM1."ShVTENTime" > CVM1."CShVTENTime" THEN TIMESTAMPDIFF(
        MINUTE, CVM1."ShVTSTTime", CVM1."CShVTENTime"
      ) * CVM1."BilledRateMinute" WHEN CVM1."BilledRateMinute" > 0 
      AND CVM1."CShVTSTTime" >= CVM1."ShVTSTTime" 
      AND CVM1."CShVTENTime" <= CVM1."ShVTENTime" THEN TIMESTAMPDIFF(
        MINUTE, CVM1."CShVTSTTime", CVM1."CShVTENTime"
      ) * CVM1."BilledRateMinute" WHEN CVM1."BilledRateMinute" > 0 
      AND CVM1."ShVTSTTime" >= CVM1."CShVTSTTime" 
      AND CVM1."ShVTENTime" <= CVM1."CShVTENTime" THEN TIMESTAMPDIFF(
        MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime"
      ) * CVM1."BilledRateMinute" WHEN CVM1."BilledRateMinute" > 0 
      AND CVM1."CShVTSTTime" < CVM1."ShVTSTTime" 
      AND CVM1."CShVTENTime" > CVM1."ShVTENTime" THEN TIMESTAMPDIFF(
        MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime"
      ) * CVM1."BilledRateMinute" WHEN CVM1."BilledRateMinute" > 0 
      AND CVM1."ShVTSTTime" < CVM1."CShVTSTTime" 
      AND CVM1."ShVTENTime" > CVM1."CShVTENTime" THEN TIMESTAMPDIFF(
        MINUTE, CVM1."CShVTSTTime", CVM1."CShVTENTime"
      ) * CVM1."BilledRateMinute" ELSE 0 END AS "OverLapAmount" 
    FROM 
      CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS CVM1
  ) AS CVMCH ON CVMCH.ID = CVM.ID 
  AND CVMCH.RN = 1 
WHERE 
  CVM."ConProviderID" IS NOT NULL 
  AND CVM."ConProviderName" IS NOT NULL --AND (CVM."SchOverAnotherSchTimeFlag" = 'Y'
  --  OR CVM."VisitTimeOverAnotherVisitTimeFlag" = 'Y')
GROUP BY 
  CVM."ConProviderID", 
  CVM."ConFederalTaxNumber", 
  CVM."ConProviderName", 
  TO_CHAR(
    CVM."CRDATEUNIQUE", 'YYYY-MM-DD'
  ), 
  CVM."ProviderID", 
  CVM."OfficeID";
TRUNCATE TABLE CONFLICTREPORT.PUBLIC.PROVIDER_DASHBOARD_CAREGIVER;
INSERT INTO CONFLICTREPORT.PUBLIC.PROVIDER_DASHBOARD_CAREGIVER (
  PROVIDERID, OFFICEID, CRDATEUNIQUE, 
  CAREGIVERID, C_CODE, C_NAME, CON_TO, 
  CON_SP, CON_OP, CON_FP
) 
SELECT 
  CVM."ProviderID" AS PROVIDERID, 
  CVM."OfficeID" AS OFFICEID, 
  TO_CHAR(
    CVM."CRDATEUNIQUE", 'YYYY-MM-DD'
  ) AS CRDATEUNIQUE, 
  CVM."CaregiverID" AS CAREGIVERID, 
  CVM."AideCode" AS C_CODE, 
  CVM."AideName" AS C_NAME, 
  COUNT(DISTINCT CVM.CONFLICTID) AS CON_TO, 
  SUM(
    CASE WHEN CVMCH."ShiftAmount" IS NOT NULL THEN CVMCH."ShiftAmount" ELSE 0 END
  ) AS CON_SP, 
  SUM(
    CASE WHEN CVMCH."OverLapAmount" IS NOT NULL THEN CVMCH."OverLapAmount" ELSE 0 END
  ) AS CON_OP, 
  SUM(
    CASE WHEN V2."StatusFlag" IN ('R', 'D') THEN CVMCH."OverLapAmount" ELSE 0 END
  ) AS CON_FP 
FROM 
  CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS CVM 
  INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 ON V2."CONFLICTID" = CVM."CONFLICTID" 
  LEFT JOIN (
    SELECT 
      CVM1.ID, 
      CASE WHEN CVM1."BilledRateMinute" > 0 THEN TIMESTAMPDIFF(
        MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime"
      ) * CVM1."BilledRateMinute" ELSE 0 END AS "ShiftAmount", 
      ROW_NUMBER() OVER (
        PARTITION BY CVM1."CONFLICTID" 
        ORDER BY 
          CASE WHEN CVM1."CShVTSTTime" >= CVM1."ShVTSTTime" 
          AND CVM1."CShVTSTTime" <= CVM1."ShVTENTime" 
          AND CVM1."CShVTENTime" > CVM1."ShVTENTime" THEN TIMESTAMPDIFF(
            MINUTE, CVM1."CShVTSTTime", CVM1."ShVTENTime"
          ) WHEN CVM1."ShVTSTTime" >= CVM1."CShVTSTTime" 
          AND CVM1."ShVTSTTime" <= CVM1."CShVTENTime" 
          AND CVM1."ShVTENTime" > CVM1."CShVTENTime" THEN TIMESTAMPDIFF(
            MINUTE, CVM1."ShVTSTTime", CVM1."CShVTENTime"
          ) WHEN CVM1."CShVTSTTime" >= CVM1."ShVTSTTime" 
          AND CVM1."CShVTENTime" <= CVM1."ShVTENTime" THEN TIMESTAMPDIFF(
            MINUTE, CVM1."CShVTSTTime", CVM1."CShVTENTime"
          ) WHEN CVM1."ShVTSTTime" >= CVM1."CShVTSTTime" 
          AND CVM1."ShVTENTime" <= CVM1."CShVTENTime" THEN TIMESTAMPDIFF(
            MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime"
          ) WHEN CVM1."CShVTSTTime" < CVM1."ShVTSTTime" 
          AND CVM1."CShVTENTime" > CVM1."ShVTENTime" THEN TIMESTAMPDIFF(
            MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime"
          ) WHEN CVM1."ShVTSTTime" < CVM1."CShVTSTTime" 
          AND CVM1."ShVTENTime" > CVM1."CShVTENTime" THEN TIMESTAMPDIFF(
            MINUTE, CVM1."CShVTSTTime", CVM1."CShVTENTime"
          ) ELSE 0 END DESC
      ) AS RN, 
      CASE WHEN CVM1."BilledRateMinute" > 0 
      AND CVM1."CShVTSTTime" >= CVM1."ShVTSTTime" 
      AND CVM1."CShVTSTTime" <= CVM1."ShVTENTime" 
      AND CVM1."CShVTENTime" > CVM1."ShVTENTime" THEN TIMESTAMPDIFF(
        MINUTE, CVM1."CShVTSTTime", CVM1."ShVTENTime"
      ) * CVM1."BilledRateMinute" WHEN CVM1."BilledRateMinute" > 0 
      AND CVM1."ShVTSTTime" >= CVM1."CShVTSTTime" 
      AND CVM1."ShVTSTTime" <= CVM1."CShVTENTime" 
      AND CVM1."ShVTENTime" > CVM1."CShVTENTime" THEN TIMESTAMPDIFF(
        MINUTE, CVM1."ShVTSTTime", CVM1."CShVTENTime"
      ) * CVM1."BilledRateMinute" WHEN CVM1."BilledRateMinute" > 0 
      AND CVM1."CShVTSTTime" >= CVM1."ShVTSTTime" 
      AND CVM1."CShVTENTime" <= CVM1."ShVTENTime" THEN TIMESTAMPDIFF(
        MINUTE, CVM1."CShVTSTTime", CVM1."CShVTENTime"
      ) * CVM1."BilledRateMinute" WHEN CVM1."BilledRateMinute" > 0 
      AND CVM1."ShVTSTTime" >= CVM1."CShVTSTTime" 
      AND CVM1."ShVTENTime" <= CVM1."CShVTENTime" THEN TIMESTAMPDIFF(
        MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime"
      ) * CVM1."BilledRateMinute" WHEN CVM1."BilledRateMinute" > 0 
      AND CVM1."CShVTSTTime" < CVM1."ShVTSTTime" 
      AND CVM1."CShVTENTime" > CVM1."ShVTENTime" THEN TIMESTAMPDIFF(
        MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime"
      ) * CVM1."BilledRateMinute" WHEN CVM1."BilledRateMinute" > 0 
      AND CVM1."ShVTSTTime" < CVM1."CShVTSTTime" 
      AND CVM1."ShVTENTime" > CVM1."CShVTENTime" THEN TIMESTAMPDIFF(
        MINUTE, CVM1."CShVTSTTime", CVM1."CShVTENTime"
      ) * CVM1."BilledRateMinute" ELSE 0 END AS "OverLapAmount" 
    FROM 
      CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS CVM1
  ) AS CVMCH ON CVMCH.ID = CVM.ID 
  AND CVMCH.RN = 1 --WHERE
  --  (CVM."SchOverAnotherSchTimeFlag" = 'Y' OR CVM."VisitTimeOverAnotherVisitTimeFlag" = 'Y')
GROUP BY 
  CVM."CaregiverID", 
  CVM."AideCode", 
  CVM."AideName", 
  TO_CHAR(
    CVM."CRDATEUNIQUE", 'YYYY-MM-DD'
  ), 
  CVM."ProviderID", 
  CVM."OfficeID";
TRUNCATE TABLE CONFLICTREPORT.PUBLIC.PROVIDER_DASHBOARD_PATIENT;
INSERT INTO CONFLICTREPORT.PUBLIC.PROVIDER_DASHBOARD_PATIENT (
  PROVIDERID, OFFICEID, CRDATEUNIQUE, 
  PATIENTID, PFNAME, PLNAME, PNAME, 
  CON_TO, CON_SP, CON_OP, CON_FP
) 
SELECT 
  CVM."ProviderID" AS PROVIDERID, 
  CVM."OfficeID" AS OFFICEID, 
  TO_CHAR(
    CVM."CRDATEUNIQUE", 'YYYY-MM-DD'
  ) AS CRDATEUNIQUE, 
  CVM."P_PatientID" AS PATIENTID, 
  CVM."P_PFName" AS PFNAME, 
  CVM."P_PLName" AS PLNAME, 
  CONCAT(
    CVM."P_PLName", ' ', CVM."P_PFName"
  ) AS PNAME, 
  COUNT(DISTINCT CVM.CONFLICTID) AS CON_TO, 
  SUM(
    CASE WHEN CVMCH."ShiftAmount" IS NOT NULL THEN CVMCH."ShiftAmount" ELSE 0 END
  ) AS CON_SP, 
  SUM(
    CASE WHEN CVMCH."OverLapAmount" IS NOT NULL THEN CVMCH."OverLapAmount" ELSE 0 END
  ) AS CON_OP, 
  SUM(
    CASE WHEN V2."StatusFlag" IN ('R', 'D') THEN CVMCH."OverLapAmount" ELSE 0 END
  ) AS CON_FP 
FROM 
  CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS CVM 
  INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 ON V2."CONFLICTID" = CVM."CONFLICTID" 
  LEFT JOIN (
    SELECT 
      CVM1.ID, 
      CASE WHEN CVM1."BilledRateMinute" > 0 THEN TIMESTAMPDIFF(
        MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime"
      ) * CVM1."BilledRateMinute" ELSE 0 END AS "ShiftAmount", 
      ROW_NUMBER() OVER (
        PARTITION BY CVM1."CONFLICTID" 
        ORDER BY 
          CASE WHEN CVM1."CShVTSTTime" >= CVM1."ShVTSTTime" 
          AND CVM1."CShVTSTTime" <= CVM1."ShVTENTime" 
          AND CVM1."CShVTENTime" > CVM1."ShVTENTime" THEN TIMESTAMPDIFF(
            MINUTE, CVM1."CShVTSTTime", CVM1."ShVTENTime"
          ) WHEN CVM1."ShVTSTTime" >= CVM1."CShVTSTTime" 
          AND CVM1."ShVTSTTime" <= CVM1."CShVTENTime" 
          AND CVM1."ShVTENTime" > CVM1."CShVTENTime" THEN TIMESTAMPDIFF(
            MINUTE, CVM1."ShVTSTTime", CVM1."CShVTENTime"
          ) WHEN CVM1."CShVTSTTime" >= CVM1."ShVTSTTime" 
          AND CVM1."CShVTENTime" <= CVM1."ShVTENTime" THEN TIMESTAMPDIFF(
            MINUTE, CVM1."CShVTSTTime", CVM1."CShVTENTime"
          ) WHEN CVM1."ShVTSTTime" >= CVM1."CShVTSTTime" 
          AND CVM1."ShVTENTime" <= CVM1."CShVTENTime" THEN TIMESTAMPDIFF(
            MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime"
          ) WHEN CVM1."CShVTSTTime" < CVM1."ShVTSTTime" 
          AND CVM1."CShVTENTime" > CVM1."ShVTENTime" THEN TIMESTAMPDIFF(
            MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime"
          ) WHEN CVM1."ShVTSTTime" < CVM1."CShVTSTTime" 
          AND CVM1."ShVTENTime" > CVM1."CShVTENTime" THEN TIMESTAMPDIFF(
            MINUTE, CVM1."CShVTSTTime", CVM1."CShVTENTime"
          ) ELSE 0 END DESC
      ) AS RN, 
      CASE WHEN CVM1."BilledRateMinute" > 0 
      AND CVM1."CShVTSTTime" >= CVM1."ShVTSTTime" 
      AND CVM1."CShVTSTTime" <= CVM1."ShVTENTime" 
      AND CVM1."CShVTENTime" > CVM1."ShVTENTime" THEN TIMESTAMPDIFF(
        MINUTE, CVM1."CShVTSTTime", CVM1."ShVTENTime"
      ) * CVM1."BilledRateMinute" WHEN CVM1."BilledRateMinute" > 0 
      AND CVM1."ShVTSTTime" >= CVM1."CShVTSTTime" 
      AND CVM1."ShVTSTTime" <= CVM1."CShVTENTime" 
      AND CVM1."ShVTENTime" > CVM1."CShVTENTime" THEN TIMESTAMPDIFF(
        MINUTE, CVM1."ShVTSTTime", CVM1."CShVTENTime"
      ) * CVM1."BilledRateMinute" WHEN CVM1."BilledRateMinute" > 0 
      AND CVM1."CShVTSTTime" >= CVM1."ShVTSTTime" 
      AND CVM1."CShVTENTime" <= CVM1."ShVTENTime" THEN TIMESTAMPDIFF(
        MINUTE, CVM1."CShVTSTTime", CVM1."CShVTENTime"
      ) * CVM1."BilledRateMinute" WHEN CVM1."BilledRateMinute" > 0 
      AND CVM1."ShVTSTTime" >= CVM1."CShVTSTTime" 
      AND CVM1."ShVTENTime" <= CVM1."CShVTENTime" THEN TIMESTAMPDIFF(
        MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime"
      ) * CVM1."BilledRateMinute" WHEN CVM1."BilledRateMinute" > 0 
      AND CVM1."CShVTSTTime" < CVM1."ShVTSTTime" 
      AND CVM1."CShVTENTime" > CVM1."ShVTENTime" THEN TIMESTAMPDIFF(
        MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime"
      ) * CVM1."BilledRateMinute" WHEN CVM1."BilledRateMinute" > 0 
      AND CVM1."ShVTSTTime" < CVM1."CShVTSTTime" 
      AND CVM1."ShVTENTime" > CVM1."CShVTENTime" THEN TIMESTAMPDIFF(
        MINUTE, CVM1."CShVTSTTime", CVM1."CShVTENTime"
      ) * CVM1."BilledRateMinute" ELSE 0 END AS "OverLapAmount" 
    FROM 
      CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS CVM1
  ) AS CVMCH ON CVMCH.ID = CVM.ID 
  AND CVMCH.RN = 1 
WHERE 
  --(CVM."SchOverAnotherSchTimeFlag" = 'Y' OR CVM."VisitTimeOverAnotherVisitTimeFlag" = 'Y') AND 
  CVM."P_PatientID" IS NOT NULL 
GROUP BY 
  CVM."P_PatientID", 
  CVM."P_PFName", 
  CVM."P_PLName", 
  TO_CHAR(
    CVM."CRDATEUNIQUE", 'YYYY-MM-DD'
  ), 
  CVM."ProviderID", 
  CVM."OfficeID";
TRUNCATE TABLE CONFLICTREPORT.PUBLIC.PROVIDER_DASHBOARD_PAYER;
INSERT INTO CONFLICTREPORT.PUBLIC.PROVIDER_DASHBOARD_PAYER (
  PROVIDERID, OFFICEID, CRDATEUNIQUE, 
  PAYERID, PNAME, CON_TO, CON_SP, CON_OP, 
  CON_FP
) 
SELECT 
  CVM."ProviderID" AS PROVIDERID, 
  CVM."OfficeID" AS OFFICEID, 
  TO_CHAR(
    CVM."CRDATEUNIQUE", 'YYYY-MM-DD'
  ) AS CRDATEUNIQUE, 
  CVM."PayerID" AS PAYERID, 
  CVM."Contract" AS PNAME, 
  COUNT(DISTINCT CVM.CONFLICTID) AS CON_TO, 
  SUM(
    CASE WHEN CVMCH."ShiftAmount" IS NOT NULL THEN CVMCH."ShiftAmount" ELSE 0 END
  ) AS CON_SP, 
  SUM(
    CASE WHEN CVMCH."OverLapAmount" IS NOT NULL THEN CVMCH."OverLapAmount" ELSE 0 END
  ) AS CON_OP, 
  SUM(
    CASE WHEN V2."StatusFlag" IN ('R', 'D') THEN CVMCH."OverLapAmount" ELSE 0 END
  ) AS CON_FP 
FROM 
  CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS CVM 
  INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 ON V2."CONFLICTID" = CVM."CONFLICTID" 
  LEFT JOIN (
    SELECT 
      CVM1.ID, 
      CASE WHEN CVM1."BilledRateMinute" > 0 THEN TIMESTAMPDIFF(
        MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime"
      ) * CVM1."BilledRateMinute" ELSE 0 END AS "ShiftAmount", 
      ROW_NUMBER() OVER (
        PARTITION BY CVM1."CONFLICTID" 
        ORDER BY 
          CASE WHEN CVM1."CShVTSTTime" >= CVM1."ShVTSTTime" 
          AND CVM1."CShVTSTTime" <= CVM1."ShVTENTime" 
          AND CVM1."CShVTENTime" > CVM1."ShVTENTime" THEN TIMESTAMPDIFF(
            MINUTE, CVM1."CShVTSTTime", CVM1."ShVTENTime"
          ) WHEN CVM1."ShVTSTTime" >= CVM1."CShVTSTTime" 
          AND CVM1."ShVTSTTime" <= CVM1."CShVTENTime" 
          AND CVM1."ShVTENTime" > CVM1."CShVTENTime" THEN TIMESTAMPDIFF(
            MINUTE, CVM1."ShVTSTTime", CVM1."CShVTENTime"
          ) WHEN CVM1."CShVTSTTime" >= CVM1."ShVTSTTime" 
          AND CVM1."CShVTENTime" <= CVM1."ShVTENTime" THEN TIMESTAMPDIFF(
            MINUTE, CVM1."CShVTSTTime", CVM1."CShVTENTime"
          ) WHEN CVM1."ShVTSTTime" >= CVM1."CShVTSTTime" 
          AND CVM1."ShVTENTime" <= CVM1."CShVTENTime" THEN TIMESTAMPDIFF(
            MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime"
          ) WHEN CVM1."CShVTSTTime" < CVM1."ShVTSTTime" 
          AND CVM1."CShVTENTime" > CVM1."ShVTENTime" THEN TIMESTAMPDIFF(
            MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime"
          ) WHEN CVM1."ShVTSTTime" < CVM1."CShVTSTTime" 
          AND CVM1."ShVTENTime" > CVM1."CShVTENTime" THEN TIMESTAMPDIFF(
            MINUTE, CVM1."CShVTSTTime", CVM1."CShVTENTime"
          ) ELSE 0 END DESC
      ) AS RN, 
      CASE WHEN CVM1."BilledRateMinute" > 0 
      AND CVM1."CShVTSTTime" >= CVM1."ShVTSTTime" 
      AND CVM1."CShVTSTTime" <= CVM1."ShVTENTime" 
      AND CVM1."CShVTENTime" > CVM1."ShVTENTime" THEN TIMESTAMPDIFF(
        MINUTE, CVM1."CShVTSTTime", CVM1."ShVTENTime"
      ) * CVM1."BilledRateMinute" WHEN CVM1."BilledRateMinute" > 0 
      AND CVM1."ShVTSTTime" >= CVM1."CShVTSTTime" 
      AND CVM1."ShVTSTTime" <= CVM1."CShVTENTime" 
      AND CVM1."ShVTENTime" > CVM1."CShVTENTime" THEN TIMESTAMPDIFF(
        MINUTE, CVM1."ShVTSTTime", CVM1."CShVTENTime"
      ) * CVM1."BilledRateMinute" WHEN CVM1."BilledRateMinute" > 0 
      AND CVM1."CShVTSTTime" >= CVM1."ShVTSTTime" 
      AND CVM1."CShVTENTime" <= CVM1."ShVTENTime" THEN TIMESTAMPDIFF(
        MINUTE, CVM1."CShVTSTTime", CVM1."CShVTENTime"
      ) * CVM1."BilledRateMinute" WHEN CVM1."BilledRateMinute" > 0 
      AND CVM1."ShVTSTTime" >= CVM1."CShVTSTTime" 
      AND CVM1."ShVTENTime" <= CVM1."CShVTENTime" THEN TIMESTAMPDIFF(
        MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime"
      ) * CVM1."BilledRateMinute" WHEN CVM1."BilledRateMinute" > 0 
      AND CVM1."CShVTSTTime" < CVM1."ShVTSTTime" 
      AND CVM1."CShVTENTime" > CVM1."ShVTENTime" THEN TIMESTAMPDIFF(
        MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime"
      ) * CVM1."BilledRateMinute" WHEN CVM1."BilledRateMinute" > 0 
      AND CVM1."ShVTSTTime" < CVM1."CShVTSTTime" 
      AND CVM1."ShVTENTime" > CVM1."CShVTENTime" THEN TIMESTAMPDIFF(
        MINUTE, CVM1."CShVTSTTime", CVM1."CShVTENTime"
      ) * CVM1."BilledRateMinute" ELSE 0 END AS "OverLapAmount" 
    FROM 
      CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS CVM1
  ) AS CVMCH ON CVMCH.ID = CVM.ID 
  AND CVMCH.RN = 1 
WHERE 
  --(CVM."SchOverAnotherSchTimeFlag" = 'Y' OR CVM."VisitTimeOverAnotherVisitTimeFlag" = 'Y')
  --AND
  CVM."AppPayerID" != '0' 
GROUP BY 
  CVM."PayerID", 
  CVM."Contract", 
  TO_CHAR(
    CVM."CRDATEUNIQUE", 'YYYY-MM-DD'
  ), 
  CVM."ProviderID", 
  CVM."OfficeID";
