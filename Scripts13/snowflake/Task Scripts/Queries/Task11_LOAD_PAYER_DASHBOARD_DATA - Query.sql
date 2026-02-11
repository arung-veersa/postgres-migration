-- Step 1: TRUNCATE the table
TRUNCATE TABLE CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_TOP;
TRUNCATE TABLE CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_CON_TYP;
TRUNCATE TABLE CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_AGENCY;
TRUNCATE TABLE CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_CAREGIVER;
TRUNCATE TABLE CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_PATIENT;
TRUNCATE TABLE CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_PAYER;

-- Step 2: Fetch payer IDs
SELECT 
  DISTINCT a.APID 
FROM 
  ANALYTICS.BI.DIMPAYER AS P 
  JOIN (
    SELECT 
      DISTINCT V1."GroupID", 
      V1."CONFLICTID", 
      V1."PayerID" AS APID 
    FROM 
      CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS V1 
      INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 ON V2."CONFLICTID" = V1."CONFLICTID" 
    WHERE 
      V1."GroupID" IN (
        SELECT 
          DISTINCT "GroupID" 
        FROM 
          CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS --WHERE
          --  ("SchOverAnotherSchTimeFlag" = 'Y'
          --     OR "VisitTimeOverAnotherVisitTimeFlag" = 'Y')
          )
  ) a 
  LEFT JOIN (
    SELECT 
      DISTINCT V1."GroupID", 
      V1."CONFLICTID" 
    FROM 
      CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS V1 
      INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 ON V2."CONFLICTID" = V1."CONFLICTID" 
    WHERE 
      V1."GroupID" IN (
        SELECT 
          DISTINCT "GroupID" 
        FROM 
          CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS --WHERE
          -- ("SchOverAnotherSchTimeFlag" = 'Y'
          --   OR "VisitTimeOverAnotherVisitTimeFlag" = 'Y')
          )
  ) b ON a.CONFLICTID <> b.CONFLICTID 
  AND a."GroupID" = b."GroupID" 
WHERE 
  P."Is Active" = TRUE 
  AND P."Is Demo" = FALSE 
  AND P."Payer Id" = a.APID;

-- Step 3: Loop through result set
---------------------------PAYER TOP---------------------
INSERT INTO CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_TOP (
  PAYERID, STATUS, STATUS_VALUE, SEVEN_TO, 
  SEVEN_SP, SEVEN_OP, SEVEN_FP, THIRTY_TO, 
  THIRTY_SP, THIRTY_OP, THIRTY_FP, 
  SIXTY_TO, SIXTY_SP, SIXTY_OP, SIXTY_FP, 
  NINETY_TO, NINETY_SP, NINETY_OP, 
  NINETY_FP
) 
SELECT 
  '${payerId}' AS PAYERID, 
  a."StatusFlag" AS "StatusF", 
  CASE WHEN a."StatusFlag" = 'N' THEN 'NO RESPONSE' WHEN a."StatusFlag" = 'R' THEN 'Resolved' ELSE 'Open/In-Progress' END AS "StatusFlag", 
  COUNT(
    DISTINCT CASE WHEN TO_CHAR(
      a."G_CRDATEUNIQUE", 'YYYY-MM-DD'
    ) BETWEEN TO_CHAR(CURRENT_DATE - 7, 'YYYY-MM-DD') 
    AND TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') THEN a."GroupID" END
  ) AS "Total7", 
  SUM(
    CASE WHEN TO_CHAR(
      a."G_CRDATEUNIQUE", 'YYYY-MM-DD'
    ) BETWEEN TO_CHAR(CURRENT_DATE - 7, 'YYYY-MM-DD') 
    AND TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') 
    AND a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" ELSE 0 END
  ) AS "ShiftPrice7", 
  SUM(
    CASE WHEN TO_CHAR(
      a."G_CRDATEUNIQUE", 'YYYY-MM-DD'
    ) BETWEEN TO_CHAR(CURRENT_DATE - 7, 'YYYY-MM-DD') 
    AND TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') 
    AND a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 
    AND b."ShVTSTTime" >= a."ShVTSTTime" 
    AND b."ShVTSTTime" <= a."ShVTENTime" 
    AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, b."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" WHEN TO_CHAR(
      a."G_CRDATEUNIQUE", 'YYYY-MM-DD'
    ) BETWEEN TO_CHAR(CURRENT_DATE - 7, 'YYYY-MM-DD') 
    AND TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') 
    AND a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 
    AND a."ShVTSTTime" >= b."ShVTSTTime" 
    AND a."ShVTSTTime" <= b."ShVTENTime" 
    AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", b."ShVTENTime"
    ) * a."BilledRateMinute" WHEN TO_CHAR(
      a."G_CRDATEUNIQUE", 'YYYY-MM-DD'
    ) BETWEEN TO_CHAR(CURRENT_DATE - 7, 'YYYY-MM-DD') 
    AND TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') 
    AND a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 
    AND b."ShVTSTTime" >= a."ShVTSTTime" 
    AND b."ShVTENTime" <= a."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, b."ShVTSTTime", b."ShVTENTime"
    ) * a."BilledRateMinute" WHEN TO_CHAR(
      a."G_CRDATEUNIQUE", 'YYYY-MM-DD'
    ) BETWEEN TO_CHAR(CURRENT_DATE - 7, 'YYYY-MM-DD') 
    AND TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') 
    AND a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 
    AND a."ShVTSTTime" >= b."ShVTSTTime" 
    AND a."ShVTENTime" <= b."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" WHEN TO_CHAR(
      a."G_CRDATEUNIQUE", 'YYYY-MM-DD'
    ) BETWEEN TO_CHAR(CURRENT_DATE - 7, 'YYYY-MM-DD') 
    AND TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') 
    AND a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 
    AND b."ShVTSTTime" < a."ShVTSTTime" 
    AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" WHEN TO_CHAR(
      a."G_CRDATEUNIQUE", 'YYYY-MM-DD'
    ) BETWEEN TO_CHAR(CURRENT_DATE - 7, 'YYYY-MM-DD') 
    AND TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') 
    AND a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 
    AND a."ShVTSTTime" < b."ShVTSTTime" 
    AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, b."ShVTSTTime", b."ShVTENTime"
    ) * a."BilledRateMinute" ELSE 0 END
  ) AS "OverlapPrice7", 
  SUM(
    CASE WHEN TO_CHAR(
      a."G_CRDATEUNIQUE", 'YYYY-MM-DD'
    ) BETWEEN TO_CHAR(CURRENT_DATE - 7, 'YYYY-MM-DD') 
    AND TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') 
    AND a.APID = '${payerId}' 
    AND a."StatusFlag" = 'R' 
    AND a."BilledRateMinute" > 0 
    AND b."ShVTSTTime" >= a."ShVTSTTime" 
    AND b."ShVTSTTime" <= a."ShVTENTime" 
    AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, b."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" WHEN TO_CHAR(
      a."G_CRDATEUNIQUE", 'YYYY-MM-DD'
    ) BETWEEN TO_CHAR(CURRENT_DATE - 7, 'YYYY-MM-DD') 
    AND TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') 
    AND a.APID = '${payerId}' 
    AND a."StatusFlag" = 'R' 
    AND a."BilledRateMinute" > 0 
    AND a."ShVTSTTime" >= b."ShVTSTTime" 
    AND a."ShVTSTTime" <= b."ShVTENTime" 
    AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", b."ShVTENTime"
    ) * a."BilledRateMinute" WHEN TO_CHAR(
      a."G_CRDATEUNIQUE", 'YYYY-MM-DD'
    ) BETWEEN TO_CHAR(CURRENT_DATE - 7, 'YYYY-MM-DD') 
    AND TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') 
    AND a.APID = '${payerId}' 
    AND a."StatusFlag" = 'R' 
    AND a."BilledRateMinute" > 0 
    AND b."ShVTSTTime" >= a."ShVTSTTime" 
    AND b."ShVTENTime" <= a."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, b."ShVTSTTime", b."ShVTENTime"
    ) * a."BilledRateMinute" WHEN TO_CHAR(
      a."G_CRDATEUNIQUE", 'YYYY-MM-DD'
    ) BETWEEN TO_CHAR(CURRENT_DATE - 7, 'YYYY-MM-DD') 
    AND TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') 
    AND a.APID = '${payerId}' 
    AND a."StatusFlag" = 'R' 
    AND a."BilledRateMinute" > 0 
    AND a."ShVTSTTime" >= b."ShVTSTTime" 
    AND a."ShVTENTime" <= b."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" WHEN TO_CHAR(
      a."G_CRDATEUNIQUE", 'YYYY-MM-DD'
    ) BETWEEN TO_CHAR(CURRENT_DATE - 7, 'YYYY-MM-DD') 
    AND TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') 
    AND a.APID = '${payerId}' 
    AND a."StatusFlag" = 'R' 
    AND a."BilledRateMinute" > 0 
    AND b."ShVTSTTime" < a."ShVTSTTime" 
    AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" WHEN TO_CHAR(
      a."G_CRDATEUNIQUE", 'YYYY-MM-DD'
    ) BETWEEN TO_CHAR(CURRENT_DATE - 7, 'YYYY-MM-DD') 
    AND TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') 
    AND a.APID = '${payerId}' 
    AND a."StatusFlag" = 'R' 
    AND a."BilledRateMinute" > 0 
    AND a."ShVTSTTime" < b."ShVTSTTime" 
    AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, b."ShVTSTTime", b."ShVTENTime"
    ) * a."BilledRateMinute" ELSE 0 END
  ) AS "FinalPrice7", 
  COUNT(
    DISTINCT CASE WHEN TO_CHAR(
      a."G_CRDATEUNIQUE", 'YYYY-MM-DD'
    ) BETWEEN TO_CHAR(CURRENT_DATE - 30, 'YYYY-MM-DD') 
    AND TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') THEN a."GroupID" END
  ) AS "Total30", 
  SUM(
    CASE WHEN TO_CHAR(
      a."G_CRDATEUNIQUE", 'YYYY-MM-DD'
    ) BETWEEN TO_CHAR(CURRENT_DATE - 30, 'YYYY-MM-DD') 
    AND TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') 
    AND a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" ELSE 0 END
  ) AS "ShiftPrice30", 
  SUM(
    CASE WHEN TO_CHAR(
      a."G_CRDATEUNIQUE", 'YYYY-MM-DD'
    ) BETWEEN TO_CHAR(CURRENT_DATE - 30, 'YYYY-MM-DD') 
    AND TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') 
    AND a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 
    AND b."ShVTSTTime" >= a."ShVTSTTime" 
    AND b."ShVTSTTime" <= a."ShVTENTime" 
    AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, b."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" WHEN TO_CHAR(
      a."G_CRDATEUNIQUE", 'YYYY-MM-DD'
    ) BETWEEN TO_CHAR(CURRENT_DATE - 30, 'YYYY-MM-DD') 
    AND TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') 
    AND a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 
    AND a."ShVTSTTime" >= b."ShVTSTTime" 
    AND a."ShVTSTTime" <= b."ShVTENTime" 
    AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", b."ShVTENTime"
    ) * a."BilledRateMinute" WHEN TO_CHAR(
      a."G_CRDATEUNIQUE", 'YYYY-MM-DD'
    ) BETWEEN TO_CHAR(CURRENT_DATE - 30, 'YYYY-MM-DD') 
    AND TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') 
    AND a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 
    AND b."ShVTSTTime" >= a."ShVTSTTime" 
    AND b."ShVTENTime" <= a."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, b."ShVTSTTime", b."ShVTENTime"
    ) * a."BilledRateMinute" WHEN TO_CHAR(
      a."G_CRDATEUNIQUE", 'YYYY-MM-DD'
    ) BETWEEN TO_CHAR(CURRENT_DATE - 30, 'YYYY-MM-DD') 
    AND TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') 
    AND a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 
    AND a."ShVTSTTime" >= b."ShVTSTTime" 
    AND a."ShVTENTime" <= b."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" WHEN TO_CHAR(
      a."G_CRDATEUNIQUE", 'YYYY-MM-DD'
    ) BETWEEN TO_CHAR(CURRENT_DATE - 30, 'YYYY-MM-DD') 
    AND TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') 
    AND a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 
    AND b."ShVTSTTime" < a."ShVTSTTime" 
    AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" WHEN TO_CHAR(
      a."G_CRDATEUNIQUE", 'YYYY-MM-DD'
    ) BETWEEN TO_CHAR(CURRENT_DATE - 30, 'YYYY-MM-DD') 
    AND TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') 
    AND a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 
    AND a."ShVTSTTime" < b."ShVTSTTime" 
    AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, b."ShVTSTTime", b."ShVTENTime"
    ) * a."BilledRateMinute" ELSE 0 END
  ) AS "OverlapPrice30", 
  SUM(
    CASE WHEN TO_CHAR(
      a."G_CRDATEUNIQUE", 'YYYY-MM-DD'
    ) BETWEEN TO_CHAR(CURRENT_DATE - 30, 'YYYY-MM-DD') 
    AND TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') 
    AND a.APID = '${payerId}' 
    AND a."StatusFlag" = 'R' 
    AND a."BilledRateMinute" > 0 
    AND b."ShVTSTTime" >= a."ShVTSTTime" 
    AND b."ShVTSTTime" <= a."ShVTENTime" 
    AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, b."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" WHEN TO_CHAR(
      a."G_CRDATEUNIQUE", 'YYYY-MM-DD'
    ) BETWEEN TO_CHAR(CURRENT_DATE - 30, 'YYYY-MM-DD') 
    AND TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') 
    AND a.APID = '${payerId}' 
    AND a."StatusFlag" = 'R' 
    AND a."BilledRateMinute" > 0 
    AND a."ShVTSTTime" >= b."ShVTSTTime" 
    AND a."ShVTSTTime" <= b."ShVTENTime" 
    AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", b."ShVTENTime"
    ) * a."BilledRateMinute" WHEN TO_CHAR(
      a."G_CRDATEUNIQUE", 'YYYY-MM-DD'
    ) BETWEEN TO_CHAR(CURRENT_DATE - 30, 'YYYY-MM-DD') 
    AND TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') 
    AND a.APID = '${payerId}' 
    AND a."StatusFlag" = 'R' 
    AND a."BilledRateMinute" > 0 
    AND b."ShVTSTTime" >= a."ShVTSTTime" 
    AND b."ShVTENTime" <= a."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, b."ShVTSTTime", b."ShVTENTime"
    ) * a."BilledRateMinute" WHEN TO_CHAR(
      a."G_CRDATEUNIQUE", 'YYYY-MM-DD'
    ) BETWEEN TO_CHAR(CURRENT_DATE - 30, 'YYYY-MM-DD') 
    AND TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') 
    AND a.APID = '${payerId}' 
    AND a."StatusFlag" = 'R' 
    AND a."BilledRateMinute" > 0 
    AND a."ShVTSTTime" >= b."ShVTSTTime" 
    AND a."ShVTENTime" <= b."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" WHEN TO_CHAR(
      a."G_CRDATEUNIQUE", 'YYYY-MM-DD'
    ) BETWEEN TO_CHAR(CURRENT_DATE - 30, 'YYYY-MM-DD') 
    AND TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') 
    AND a.APID = '${payerId}' 
    AND a."StatusFlag" = 'R' 
    AND a."BilledRateMinute" > 0 
    AND b."ShVTSTTime" < a."ShVTSTTime" 
    AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" WHEN TO_CHAR(
      a."G_CRDATEUNIQUE", 'YYYY-MM-DD'
    ) BETWEEN TO_CHAR(CURRENT_DATE - 30, 'YYYY-MM-DD') 
    AND TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') 
    AND a.APID = '${payerId}' 
    AND a."StatusFlag" = 'R' 
    AND a."BilledRateMinute" > 0 
    AND a."ShVTSTTime" < b."ShVTSTTime" 
    AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, b."ShVTSTTime", b."ShVTENTime"
    ) * a."BilledRateMinute" ELSE 0 END
  ) AS "FinalPrice30", 
  COUNT(
    DISTINCT CASE WHEN TO_CHAR(
      a."G_CRDATEUNIQUE", 'YYYY-MM-DD'
    ) BETWEEN TO_CHAR(CURRENT_DATE - 60, 'YYYY-MM-DD') 
    AND TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') THEN a."GroupID" END
  ) AS "Total60", 
  SUM(
    CASE WHEN TO_CHAR(
      a."G_CRDATEUNIQUE", 'YYYY-MM-DD'
    ) BETWEEN TO_CHAR(CURRENT_DATE - 60, 'YYYY-MM-DD') 
    AND TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') 
    AND a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" ELSE 0 END
  ) AS "ShiftPrice60", 
  SUM(
    CASE WHEN TO_CHAR(
      a."G_CRDATEUNIQUE", 'YYYY-MM-DD'
    ) BETWEEN TO_CHAR(CURRENT_DATE - 60, 'YYYY-MM-DD') 
    AND TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') 
    AND a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 
    AND b."ShVTSTTime" >= a."ShVTSTTime" 
    AND b."ShVTSTTime" <= a."ShVTENTime" 
    AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, b."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" WHEN TO_CHAR(
      a."G_CRDATEUNIQUE", 'YYYY-MM-DD'
    ) BETWEEN TO_CHAR(CURRENT_DATE - 60, 'YYYY-MM-DD') 
    AND TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') 
    AND a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 
    AND a."ShVTSTTime" >= b."ShVTSTTime" 
    AND a."ShVTSTTime" <= b."ShVTENTime" 
    AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", b."ShVTENTime"
    ) * a."BilledRateMinute" WHEN TO_CHAR(
      a."G_CRDATEUNIQUE", 'YYYY-MM-DD'
    ) BETWEEN TO_CHAR(CURRENT_DATE - 60, 'YYYY-MM-DD') 
    AND TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') 
    AND a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 
    AND b."ShVTSTTime" >= a."ShVTSTTime" 
    AND b."ShVTENTime" <= a."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, b."ShVTSTTime", b."ShVTENTime"
    ) * a."BilledRateMinute" WHEN TO_CHAR(
      a."G_CRDATEUNIQUE", 'YYYY-MM-DD'
    ) BETWEEN TO_CHAR(CURRENT_DATE - 60, 'YYYY-MM-DD') 
    AND TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') 
    AND a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 
    AND a."ShVTSTTime" >= b."ShVTSTTime" 
    AND a."ShVTENTime" <= b."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" WHEN TO_CHAR(
      a."G_CRDATEUNIQUE", 'YYYY-MM-DD'
    ) BETWEEN TO_CHAR(CURRENT_DATE - 60, 'YYYY-MM-DD') 
    AND TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') 
    AND a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 
    AND b."ShVTSTTime" < a."ShVTSTTime" 
    AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" WHEN TO_CHAR(
      a."G_CRDATEUNIQUE", 'YYYY-MM-DD'
    ) BETWEEN TO_CHAR(CURRENT_DATE - 60, 'YYYY-MM-DD') 
    AND TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') 
    AND a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 
    AND a."ShVTSTTime" < b."ShVTSTTime" 
    AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, b."ShVTSTTime", b."ShVTENTime"
    ) * a."BilledRateMinute" ELSE 0 END
  ) AS "OverlapPrice60", 
  SUM(
    CASE WHEN TO_CHAR(
      a."G_CRDATEUNIQUE", 'YYYY-MM-DD'
    ) BETWEEN TO_CHAR(CURRENT_DATE - 60, 'YYYY-MM-DD') 
    AND TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') 
    AND a.APID = '${payerId}' 
    AND a."StatusFlag" = 'R' 
    AND a."BilledRateMinute" > 0 
    AND b."ShVTSTTime" >= a."ShVTSTTime" 
    AND b."ShVTSTTime" <= a."ShVTENTime" 
    AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, b."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" WHEN TO_CHAR(
      a."G_CRDATEUNIQUE", 'YYYY-MM-DD'
    ) BETWEEN TO_CHAR(CURRENT_DATE - 60, 'YYYY-MM-DD') 
    AND TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') 
    AND a.APID = '${payerId}' 
    AND a."StatusFlag" = 'R' 
    AND a."BilledRateMinute" > 0 
    AND a."ShVTSTTime" >= b."ShVTSTTime" 
    AND a."ShVTSTTime" <= b."ShVTENTime" 
    AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", b."ShVTENTime"
    ) * a."BilledRateMinute" WHEN TO_CHAR(
      a."G_CRDATEUNIQUE", 'YYYY-MM-DD'
    ) BETWEEN TO_CHAR(CURRENT_DATE - 60, 'YYYY-MM-DD') 
    AND TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') 
    AND a.APID = '${payerId}' 
    AND a."StatusFlag" = 'R' 
    AND a."BilledRateMinute" > 0 
    AND b."ShVTSTTime" >= a."ShVTSTTime" 
    AND b."ShVTENTime" <= a."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, b."ShVTSTTime", b."ShVTENTime"
    ) * a."BilledRateMinute" WHEN TO_CHAR(
      a."G_CRDATEUNIQUE", 'YYYY-MM-DD'
    ) BETWEEN TO_CHAR(CURRENT_DATE - 60, 'YYYY-MM-DD') 
    AND TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') 
    AND a.APID = '${payerId}' 
    AND a."StatusFlag" = 'R' 
    AND a."BilledRateMinute" > 0 
    AND a."ShVTSTTime" >= b."ShVTSTTime" 
    AND a."ShVTENTime" <= b."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" WHEN TO_CHAR(
      a."G_CRDATEUNIQUE", 'YYYY-MM-DD'
    ) BETWEEN TO_CHAR(CURRENT_DATE - 60, 'YYYY-MM-DD') 
    AND TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') 
    AND a.APID = '${payerId}' 
    AND a."StatusFlag" = 'R' 
    AND a."BilledRateMinute" > 0 
    AND b."ShVTSTTime" < a."ShVTSTTime" 
    AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" WHEN TO_CHAR(
      a."G_CRDATEUNIQUE", 'YYYY-MM-DD'
    ) BETWEEN TO_CHAR(CURRENT_DATE - 60, 'YYYY-MM-DD') 
    AND TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') 
    AND a.APID = '${payerId}' 
    AND a."StatusFlag" = 'R' 
    AND a."BilledRateMinute" > 0 
    AND a."ShVTSTTime" < b."ShVTSTTime" 
    AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, b."ShVTSTTime", b."ShVTENTime"
    ) * a."BilledRateMinute" ELSE 0 END
  ) AS "FinalPrice60", 
  COUNT(DISTINCT a."GroupID") AS "TotalAll", 
  SUM(
    CASE WHEN a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" ELSE 0 END
  ) AS "ShiftPriceAll", 
  SUM(
    CASE WHEN a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 
    AND b."ShVTSTTime" >= a."ShVTSTTime" 
    AND b."ShVTSTTime" <= a."ShVTENTime" 
    AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, b."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 
    AND a."ShVTSTTime" >= b."ShVTSTTime" 
    AND a."ShVTSTTime" <= b."ShVTENTime" 
    AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", b."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 
    AND b."ShVTSTTime" >= a."ShVTSTTime" 
    AND b."ShVTENTime" <= a."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, b."ShVTSTTime", b."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 
    AND a."ShVTSTTime" >= b."ShVTSTTime" 
    AND a."ShVTENTime" <= b."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 
    AND b."ShVTSTTime" < a."ShVTSTTime" 
    AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 
    AND a."ShVTSTTime" < b."ShVTSTTime" 
    AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, b."ShVTSTTime", b."ShVTENTime"
    ) * a."BilledRateMinute" ELSE 0 END
  ) AS "OverlapPriceAll", 
  SUM(
    CASE WHEN a.APID = '${payerId}' 
    AND a."StatusFlag" = 'R' 
    AND a."BilledRateMinute" > 0 
    AND b."ShVTSTTime" >= a."ShVTSTTime" 
    AND b."ShVTSTTime" <= a."ShVTENTime" 
    AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, b."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."StatusFlag" = 'R' 
    AND a."BilledRateMinute" > 0 
    AND a."ShVTSTTime" >= b."ShVTSTTime" 
    AND a."ShVTSTTime" <= b."ShVTENTime" 
    AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", b."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."StatusFlag" = 'R' 
    AND a."BilledRateMinute" > 0 
    AND b."ShVTSTTime" >= a."ShVTSTTime" 
    AND b."ShVTENTime" <= a."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, b."ShVTSTTime", b."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."StatusFlag" = 'R' 
    AND a."BilledRateMinute" > 0 
    AND a."ShVTSTTime" >= b."ShVTSTTime" 
    AND a."ShVTENTime" <= b."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."StatusFlag" = 'R' 
    AND a."BilledRateMinute" > 0 
    AND b."ShVTSTTime" < a."ShVTSTTime" 
    AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."StatusFlag" = 'R' 
    AND a."BilledRateMinute" > 0 
    AND a."ShVTSTTime" < b."ShVTSTTime" 
    AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, b."ShVTSTTime", b."ShVTENTime"
    ) * a."BilledRateMinute" ELSE 0 END
  ) AS "FinalPriceAll" 
FROM 
  (
    SELECT 
      DISTINCT V1."GroupID", 
      V1."CONFLICTID", 
      V1."ShVTSTTime", 
      V1."ShVTENTime", 
      V1."BilledRateMinute", 
      V1."G_CRDATEUNIQUE", 
      V1."PayerID" AS APID, 
      CASE WHEN V2."StatusFlag" IN('R', 'D') THEN 'R' WHEN V2."StatusFlag" IN ('N') THEN 'N' ELSE 'U' END AS "StatusFlag", 
      V1."Contract" 
    FROM 
      CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS V1 
      INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 ON V2."CONFLICTID" = V1."CONFLICTID" 
    WHERE 
      V1."GroupID" IN (
        SELECT 
          DISTINCT "GroupID" 
        FROM 
          CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS 
        WHERE 
          --("SchOverAnotherSchTimeFlag" = 'Y'
          --  OR "VisitTimeOverAnotherVisitTimeFlag" = 'Y')
          --AND 
          "PayerID" = '${payerId}'
      )
  ) a 
  LEFT JOIN (
    SELECT 
      DISTINCT V1."GroupID", 
      V1."CONFLICTID", 
      V1."ShVTSTTime", 
      V1."ShVTENTime" 
    FROM 
      CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS V1 
      INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 ON V2."CONFLICTID" = V1."CONFLICTID" 
    WHERE 
      V1."GroupID" IN (
        SELECT 
          DISTINCT "GroupID" 
        FROM 
          CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS 
        WHERE 
          --("SchOverAnotherSchTimeFlag" = 'Y'
          --  OR "VisitTimeOverAnotherVisitTimeFlag" = 'Y')
          --AND
          "PayerID" = '${payerId}'
      )
  ) b ON a.CONFLICTID <> b.CONFLICTID 
  AND a."GroupID" = b."GroupID" 
GROUP BY 
  a."StatusFlag";
---------------------------END PAYER TOP---------------------
---------------------------PAYER CON TYPE---------------------
INSERT INTO CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_CON_TYP (
  PAYERID, CRDATEUNIQUE, CONTYPE, CONTYPES, 
  CO_TO, CO_SP, CO_OP, CO_FP
) 
SELECT 
  * 
FROM 
  (
    SELECT 
      '${payerId}' AS PAYERID, 
      a."CRDATEUNIQUE" AS "CRDATEUNIQUE", 
      'Exact Schedule Time Match' AS "ConflictType", 
      '1' AS "ConflictTypeF", 
      COUNT(DISTINCT a."GroupID") AS "Total", 
      SUM(
        CASE WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" ELSE 0 END
      ) AS "ShiftPrice", 
      SUM(
        CASE WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" >= a."ShVTSTTime" 
        AND b."ShVTSTTime" <= a."ShVTENTime" 
        AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" >= b."ShVTSTTime" 
        AND a."ShVTSTTime" <= b."ShVTENTime" 
        AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" >= a."ShVTSTTime" 
        AND b."ShVTENTime" <= a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" >= b."ShVTSTTime" 
        AND a."ShVTENTime" <= b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" < a."ShVTSTTime" 
        AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" < b."ShVTSTTime" 
        AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" ELSE 0 END
      ) AS "OverlapPrice", 
      SUM(
        CASE WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" >= a."ShVTSTTime" 
        AND b."ShVTSTTime" <= a."ShVTENTime" 
        AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" >= b."ShVTSTTime" 
        AND a."ShVTSTTime" <= b."ShVTENTime" 
        AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" >= a."ShVTSTTime" 
        AND b."ShVTENTime" <= a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" >= b."ShVTSTTime" 
        AND a."ShVTENTime" <= b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" < a."ShVTSTTime" 
        AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" < b."ShVTSTTime" 
        AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" ELSE 0 END
      ) AS "FinalPrice" 
    FROM 
      (
        SELECT 
          DISTINCT V1."GroupID", 
          V1."CONFLICTID", 
          V1."ShVTSTTime", 
          V1."ShVTENTime", 
          V1."BilledRateMinute", 
          V1."G_CRDATEUNIQUE", 
          TO_CHAR(
            V1."G_CRDATEUNIQUE", 'YYYY-MM-DD'
          ) AS CRDATEUNIQUE, 
          V1."PayerID" AS APID, 
          CASE WHEN V2."StatusFlag" IN('R', 'D') THEN 'R' WHEN V2."StatusFlag" IN ('N') THEN 'N' ELSE 'U' END AS "StatusFlag" 
        FROM 
          CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS V1 
          INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 ON V2."CONFLICTID" = V1."CONFLICTID" 
        WHERE 
          V1."GroupID" IN (
            SELECT 
              DISTINCT "GroupID" 
            FROM 
              CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS 
            WHERE 
              "PayerID" = '${payerId}' 
              AND "SameSchTimeFlag" = 'Y'
          )
      ) a 
      LEFT JOIN (
        SELECT 
          DISTINCT V1."GroupID", 
          V1."CONFLICTID", 
          V1."ShVTSTTime", 
          V1."ShVTENTime", 
          TO_CHAR(
            V1."G_CRDATEUNIQUE", 'YYYY-MM-DD'
          ) AS CRDATEUNIQUE, 
        FROM 
          CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS V1 
          INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 ON V2."CONFLICTID" = V1."CONFLICTID" 
        WHERE 
          V1."GroupID" IN (
            SELECT 
              DISTINCT "GroupID" 
            FROM 
              CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS 
            WHERE 
              "PayerID" = '${payerId}'
          )
      ) b ON a.CONFLICTID <> b.CONFLICTID 
      AND a."GroupID" = b."GroupID" 
    GROUP BY 
      a.CRDATEUNIQUE 
    UNION ALL 
    SELECT 
      '${payerId}' AS PAYERID, 
      a."CRDATEUNIQUE" AS "CRDATEUNIQUE", 
      'Exact Visit Time Match' AS "ConflictType", 
      '2' AS "ConflictTypeF", 
      COUNT(DISTINCT a."GroupID") AS "Total", 
      SUM(
        CASE WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" ELSE 0 END
      ) AS "ShiftPrice", 
      SUM(
        CASE WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" >= a."ShVTSTTime" 
        AND b."ShVTSTTime" <= a."ShVTENTime" 
        AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" >= b."ShVTSTTime" 
        AND a."ShVTSTTime" <= b."ShVTENTime" 
        AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" >= a."ShVTSTTime" 
        AND b."ShVTENTime" <= a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" >= b."ShVTSTTime" 
        AND a."ShVTENTime" <= b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" < a."ShVTSTTime" 
        AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" < b."ShVTSTTime" 
        AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" ELSE 0 END
      ) AS "OverlapPrice", 
      SUM(
        CASE WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" >= a."ShVTSTTime" 
        AND b."ShVTSTTime" <= a."ShVTENTime" 
        AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" >= b."ShVTSTTime" 
        AND a."ShVTSTTime" <= b."ShVTENTime" 
        AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" >= a."ShVTSTTime" 
        AND b."ShVTENTime" <= a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" >= b."ShVTSTTime" 
        AND a."ShVTENTime" <= b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" < a."ShVTSTTime" 
        AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" < b."ShVTSTTime" 
        AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" ELSE 0 END
      ) AS "FinalPrice" 
    FROM 
      (
        SELECT 
          DISTINCT V1."GroupID", 
          V1."CONFLICTID", 
          V1."ShVTSTTime", 
          V1."ShVTENTime", 
          V1."BilledRateMinute", 
          V1."G_CRDATEUNIQUE", 
          TO_CHAR(
            V1."G_CRDATEUNIQUE", 'YYYY-MM-DD'
          ) AS CRDATEUNIQUE, 
          V1."PayerID" AS APID, 
          CASE WHEN V2."StatusFlag" IN('R', 'D') THEN 'R' WHEN V2."StatusFlag" IN ('N') THEN 'N' ELSE 'U' END AS "StatusFlag" 
        FROM 
          CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS V1 
          INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 ON V2."CONFLICTID" = V1."CONFLICTID" 
        WHERE 
          V1."GroupID" IN (
            SELECT 
              DISTINCT "GroupID" 
            FROM 
              CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS 
            WHERE 
              "PayerID" = '${payerId}' 
              AND "SameVisitTimeFlag" = 'Y'
          )
      ) a 
      LEFT JOIN (
        SELECT 
          DISTINCT V1."GroupID", 
          V1."CONFLICTID", 
          V1."ShVTSTTime", 
          V1."ShVTENTime", 
          TO_CHAR(
            V1."G_CRDATEUNIQUE", 'YYYY-MM-DD'
          ) AS CRDATEUNIQUE, 
        FROM 
          CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS V1 
          INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 ON V2."CONFLICTID" = V1."CONFLICTID" 
        WHERE 
          V1."GroupID" IN (
            SELECT 
              DISTINCT "GroupID" 
            FROM 
              CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS 
            WHERE 
              "PayerID" = '${payerId}'
          )
      ) b ON a.CONFLICTID <> b.CONFLICTID 
      AND a."GroupID" = b."GroupID" 
    GROUP BY 
      a.CRDATEUNIQUE 
    UNION ALL 
    SELECT 
      '${payerId}' AS PAYERID, 
      a."CRDATEUNIQUE" AS "CRDATEUNIQUE", 
      'Exact Schedule and Visit Time Match' AS "ConflictType", 
      '3' AS "ConflictTypeF", 
      COUNT(DISTINCT a."GroupID") AS "Total", 
      SUM(
        CASE WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" ELSE 0 END
      ) AS "ShiftPrice", 
      SUM(
        CASE WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" >= a."ShVTSTTime" 
        AND b."ShVTSTTime" <= a."ShVTENTime" 
        AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" >= b."ShVTSTTime" 
        AND a."ShVTSTTime" <= b."ShVTENTime" 
        AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" >= a."ShVTSTTime" 
        AND b."ShVTENTime" <= a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" >= b."ShVTSTTime" 
        AND a."ShVTENTime" <= b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" < a."ShVTSTTime" 
        AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" < b."ShVTSTTime" 
        AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" ELSE 0 END
      ) AS "OverlapPrice", 
      SUM(
        CASE WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" >= a."ShVTSTTime" 
        AND b."ShVTSTTime" <= a."ShVTENTime" 
        AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" >= b."ShVTSTTime" 
        AND a."ShVTSTTime" <= b."ShVTENTime" 
        AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" >= a."ShVTSTTime" 
        AND b."ShVTENTime" <= a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" >= b."ShVTSTTime" 
        AND a."ShVTENTime" <= b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" < a."ShVTSTTime" 
        AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" < b."ShVTSTTime" 
        AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" ELSE 0 END
      ) AS "FinalPrice" 
    FROM 
      (
        SELECT 
          DISTINCT V1."GroupID", 
          V1."CONFLICTID", 
          V1."ShVTSTTime", 
          V1."ShVTENTime", 
          V1."BilledRateMinute", 
          V1."G_CRDATEUNIQUE", 
          TO_CHAR(
            V1."G_CRDATEUNIQUE", 'YYYY-MM-DD'
          ) AS CRDATEUNIQUE, 
          V1."PayerID" AS APID, 
          CASE WHEN V2."StatusFlag" IN('R', 'D') THEN 'R' WHEN V2."StatusFlag" IN ('N') THEN 'N' ELSE 'U' END AS "StatusFlag" 
        FROM 
          CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS V1 
          INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 ON V2."CONFLICTID" = V1."CONFLICTID" 
        WHERE 
          V1."GroupID" IN (
            SELECT 
              DISTINCT "GroupID" 
            FROM 
              CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS 
            WHERE 
              "PayerID" = '${payerId}' 
              AND "SchAndVisitTimeSameFlag" = 'Y'
          )
      ) a 
      LEFT JOIN (
        SELECT 
          DISTINCT V1."GroupID", 
          V1."CONFLICTID", 
          V1."ShVTSTTime", 
          V1."ShVTENTime", 
          TO_CHAR(
            V1."G_CRDATEUNIQUE", 'YYYY-MM-DD'
          ) AS CRDATEUNIQUE, 
        FROM 
          CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS V1 
          INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 ON V2."CONFLICTID" = V1."CONFLICTID" 
        WHERE 
          V1."GroupID" IN (
            SELECT 
              DISTINCT "GroupID" 
            FROM 
              CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS 
            WHERE 
              "PayerID" = '${payerId}'
          )
      ) b ON a.CONFLICTID <> b.CONFLICTID 
      AND a."GroupID" = b."GroupID" 
    GROUP BY 
      a.CRDATEUNIQUE 
    UNION ALL 
    SELECT 
      '${payerId}' AS PAYERID, 
      a."CRDATEUNIQUE" AS "CRDATEUNIQUE", 
      'Schedule time overlap' AS "ConflictType", 
      '4' AS "ConflictTypeF", 
      COUNT(DISTINCT a."GroupID") AS "Total", 
      SUM(
        CASE WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" ELSE 0 END
      ) AS "ShiftPrice", 
      SUM(
        CASE WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" >= a."ShVTSTTime" 
        AND b."ShVTSTTime" <= a."ShVTENTime" 
        AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" >= b."ShVTSTTime" 
        AND a."ShVTSTTime" <= b."ShVTENTime" 
        AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" >= a."ShVTSTTime" 
        AND b."ShVTENTime" <= a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" >= b."ShVTSTTime" 
        AND a."ShVTENTime" <= b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" < a."ShVTSTTime" 
        AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" < b."ShVTSTTime" 
        AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" ELSE 0 END
      ) AS "OverlapPrice", 
      SUM(
        CASE WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" >= a."ShVTSTTime" 
        AND b."ShVTSTTime" <= a."ShVTENTime" 
        AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" >= b."ShVTSTTime" 
        AND a."ShVTSTTime" <= b."ShVTENTime" 
        AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" >= a."ShVTSTTime" 
        AND b."ShVTENTime" <= a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" >= b."ShVTSTTime" 
        AND a."ShVTENTime" <= b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" < a."ShVTSTTime" 
        AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" < b."ShVTSTTime" 
        AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" ELSE 0 END
      ) AS "FinalPrice" 
    FROM 
      (
        SELECT 
          DISTINCT V1."GroupID", 
          V1."CONFLICTID", 
          V1."ShVTSTTime", 
          V1."ShVTENTime", 
          V1."BilledRateMinute", 
          V1."G_CRDATEUNIQUE", 
          TO_CHAR(
            V1."G_CRDATEUNIQUE", 'YYYY-MM-DD'
          ) AS CRDATEUNIQUE, 
          V1."PayerID" AS APID, 
          CASE WHEN V2."StatusFlag" IN('R', 'D') THEN 'R' WHEN V2."StatusFlag" IN ('N') THEN 'N' ELSE 'U' END AS "StatusFlag" 
        FROM 
          CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS V1 
          INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 ON V2."CONFLICTID" = V1."CONFLICTID" 
        WHERE 
          V1."GroupID" IN (
            SELECT 
              DISTINCT "GroupID" 
            FROM 
              CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS 
            WHERE 
              "PayerID" = '${payerId}' 
              AND "SchOverAnotherSchTimeFlag" = 'Y'
          )
      ) a 
      LEFT JOIN (
        SELECT 
          DISTINCT V1."GroupID", 
          V1."CONFLICTID", 
          V1."ShVTSTTime", 
          V1."ShVTENTime", 
          TO_CHAR(
            V1."G_CRDATEUNIQUE", 'YYYY-MM-DD'
          ) AS CRDATEUNIQUE, 
        FROM 
          CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS V1 
          INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 ON V2."CONFLICTID" = V1."CONFLICTID" 
        WHERE 
          V1."GroupID" IN (
            SELECT 
              DISTINCT "GroupID" 
            FROM 
              CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS 
            WHERE 
              "PayerID" = '${payerId}'
          )
      ) b ON a.CONFLICTID <> b.CONFLICTID 
      AND a."GroupID" = b."GroupID" 
    GROUP BY 
      a.CRDATEUNIQUE 
    UNION ALL 
    SELECT 
      '${payerId}' AS PAYERID, 
      a."CRDATEUNIQUE" AS "CRDATEUNIQUE", 
      'Visit Time Overlap' AS "ConflictType", 
      '5' AS "ConflictTypeF", 
      COUNT(DISTINCT a."GroupID") AS "Total", 
      SUM(
        CASE WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" ELSE 0 END
      ) AS "ShiftPrice", 
      SUM(
        CASE WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" >= a."ShVTSTTime" 
        AND b."ShVTSTTime" <= a."ShVTENTime" 
        AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" >= b."ShVTSTTime" 
        AND a."ShVTSTTime" <= b."ShVTENTime" 
        AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" >= a."ShVTSTTime" 
        AND b."ShVTENTime" <= a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" >= b."ShVTSTTime" 
        AND a."ShVTENTime" <= b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" < a."ShVTSTTime" 
        AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" < b."ShVTSTTime" 
        AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" ELSE 0 END
      ) AS "OverlapPrice", 
      SUM(
        CASE WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" >= a."ShVTSTTime" 
        AND b."ShVTSTTime" <= a."ShVTENTime" 
        AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" >= b."ShVTSTTime" 
        AND a."ShVTSTTime" <= b."ShVTENTime" 
        AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" >= a."ShVTSTTime" 
        AND b."ShVTENTime" <= a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" >= b."ShVTSTTime" 
        AND a."ShVTENTime" <= b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" < a."ShVTSTTime" 
        AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" < b."ShVTSTTime" 
        AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" ELSE 0 END
      ) AS "FinalPrice" 
    FROM 
      (
        SELECT 
          DISTINCT V1."GroupID", 
          V1."CONFLICTID", 
          V1."ShVTSTTime", 
          V1."ShVTENTime", 
          V1."BilledRateMinute", 
          V1."G_CRDATEUNIQUE", 
          TO_CHAR(
            V1."G_CRDATEUNIQUE", 'YYYY-MM-DD'
          ) AS CRDATEUNIQUE, 
          V1."PayerID" AS APID, 
          CASE WHEN V2."StatusFlag" IN('R', 'D') THEN 'R' WHEN V2."StatusFlag" IN ('N') THEN 'N' ELSE 'U' END AS "StatusFlag" 
        FROM 
          CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS V1 
          INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 ON V2."CONFLICTID" = V1."CONFLICTID" 
        WHERE 
          V1."GroupID" IN (
            SELECT 
              DISTINCT "GroupID" 
            FROM 
              CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS 
            WHERE 
              "PayerID" = '${payerId}' 
              AND "VisitTimeOverAnotherVisitTimeFlag" = 'Y'
          )
      ) a 
      LEFT JOIN (
        SELECT 
          DISTINCT V1."GroupID", 
          V1."CONFLICTID", 
          V1."ShVTSTTime", 
          V1."ShVTENTime", 
          TO_CHAR(
            V1."G_CRDATEUNIQUE", 'YYYY-MM-DD'
          ) AS CRDATEUNIQUE, 
        FROM 
          CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS V1 
          INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 ON V2."CONFLICTID" = V1."CONFLICTID" 
        WHERE 
          V1."GroupID" IN (
            SELECT 
              DISTINCT "GroupID" 
            FROM 
              CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS 
            WHERE 
              "PayerID" = '${payerId}'
          )
      ) b ON a.CONFLICTID <> b.CONFLICTID 
      AND a."GroupID" = b."GroupID" 
    GROUP BY 
      a.CRDATEUNIQUE 
    UNION ALL 
    SELECT 
      '${payerId}' AS PAYERID, 
      a."CRDATEUNIQUE" AS "CRDATEUNIQUE", 
      'Schedule and Visit time overlap' AS "ConflictType", 
      '6' AS "ConflictTypeF", 
      COUNT(DISTINCT a."GroupID") AS "Total", 
      SUM(
        CASE WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" ELSE 0 END
      ) AS "ShiftPrice", 
      SUM(
        CASE WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" >= a."ShVTSTTime" 
        AND b."ShVTSTTime" <= a."ShVTENTime" 
        AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" >= b."ShVTSTTime" 
        AND a."ShVTSTTime" <= b."ShVTENTime" 
        AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" >= a."ShVTSTTime" 
        AND b."ShVTENTime" <= a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" >= b."ShVTSTTime" 
        AND a."ShVTENTime" <= b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" < a."ShVTSTTime" 
        AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" < b."ShVTSTTime" 
        AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" ELSE 0 END
      ) AS "OverlapPrice", 
      SUM(
        CASE WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" >= a."ShVTSTTime" 
        AND b."ShVTSTTime" <= a."ShVTENTime" 
        AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" >= b."ShVTSTTime" 
        AND a."ShVTSTTime" <= b."ShVTENTime" 
        AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" >= a."ShVTSTTime" 
        AND b."ShVTENTime" <= a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" >= b."ShVTSTTime" 
        AND a."ShVTENTime" <= b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" < a."ShVTSTTime" 
        AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" < b."ShVTSTTime" 
        AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" ELSE 0 END
      ) AS "FinalPrice" 
    FROM 
      (
        SELECT 
          DISTINCT V1."GroupID", 
          V1."CONFLICTID", 
          V1."ShVTSTTime", 
          V1."ShVTENTime", 
          V1."BilledRateMinute", 
          V1."G_CRDATEUNIQUE", 
          TO_CHAR(
            V1."G_CRDATEUNIQUE", 'YYYY-MM-DD'
          ) AS CRDATEUNIQUE, 
          V1."PayerID" AS APID, 
          CASE WHEN V2."StatusFlag" IN('R', 'D') THEN 'R' WHEN V2."StatusFlag" IN ('N') THEN 'N' ELSE 'U' END AS "StatusFlag" 
        FROM 
          CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS V1 
          INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 ON V2."CONFLICTID" = V1."CONFLICTID" 
        WHERE 
          V1."GroupID" IN (
            SELECT 
              DISTINCT "GroupID" 
            FROM 
              CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS 
            WHERE 
              "PayerID" = '${payerId}' 
              AND "SchTimeOverVisitTimeFlag" = 'Y'
          )
      ) a 
      LEFT JOIN (
        SELECT 
          DISTINCT V1."GroupID", 
          V1."CONFLICTID", 
          V1."ShVTSTTime", 
          V1."ShVTENTime", 
          TO_CHAR(
            V1."G_CRDATEUNIQUE", 'YYYY-MM-DD'
          ) AS CRDATEUNIQUE, 
        FROM 
          CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS V1 
          INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 ON V2."CONFLICTID" = V1."CONFLICTID" 
        WHERE 
          V1."GroupID" IN (
            SELECT 
              DISTINCT "GroupID" 
            FROM 
              CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS 
            WHERE 
              "PayerID" = '${payerId}'
          )
      ) b ON a.CONFLICTID <> b.CONFLICTID 
      AND a."GroupID" = b."GroupID" 
    GROUP BY 
      a.CRDATEUNIQUE 
    UNION ALL 
    SELECT 
      '${payerId}' AS PAYERID, 
      a."CRDATEUNIQUE" AS "CRDATEUNIQUE", 
      'Time- Distance' AS "ConflictType", 
      '7' AS "ConflictTypeF", 
      COUNT(DISTINCT a."GroupID") AS "Total", 
      SUM(
        CASE WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" ELSE 0 END
      ) AS "ShiftPrice", 
      SUM(
        CASE WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" >= a."ShVTSTTime" 
        AND b."ShVTSTTime" <= a."ShVTENTime" 
        AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" >= b."ShVTSTTime" 
        AND a."ShVTSTTime" <= b."ShVTENTime" 
        AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" >= a."ShVTSTTime" 
        AND b."ShVTENTime" <= a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" >= b."ShVTSTTime" 
        AND a."ShVTENTime" <= b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" < a."ShVTSTTime" 
        AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" < b."ShVTSTTime" 
        AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" ELSE 0 END
      ) AS "OverlapPrice", 
      SUM(
        CASE WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" >= a."ShVTSTTime" 
        AND b."ShVTSTTime" <= a."ShVTENTime" 
        AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" >= b."ShVTSTTime" 
        AND a."ShVTSTTime" <= b."ShVTENTime" 
        AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" >= a."ShVTSTTime" 
        AND b."ShVTENTime" <= a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" >= b."ShVTSTTime" 
        AND a."ShVTENTime" <= b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" < a."ShVTSTTime" 
        AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" < b."ShVTSTTime" 
        AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" ELSE 0 END
      ) AS "FinalPrice" 
    FROM 
      (
        SELECT 
          DISTINCT V1."GroupID", 
          V1."CONFLICTID", 
          V1."ShVTSTTime", 
          V1."ShVTENTime", 
          V1."BilledRateMinute", 
          V1."G_CRDATEUNIQUE", 
          TO_CHAR(
            V1."G_CRDATEUNIQUE", 'YYYY-MM-DD'
          ) AS CRDATEUNIQUE, 
          V1."PayerID" AS APID, 
          CASE WHEN V2."StatusFlag" IN('R', 'D') THEN 'R' WHEN V2."StatusFlag" IN ('N') THEN 'N' ELSE 'U' END AS "StatusFlag" 
        FROM 
          CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS V1 
          INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 ON V2."CONFLICTID" = V1."CONFLICTID" 
        WHERE 
          V1."GroupID" IN (
            SELECT 
              DISTINCT "GroupID" 
            FROM 
              CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS 
            WHERE 
              "PayerID" = '${payerId}' 
              AND "DistanceFlag" = 'Y'
          )
      ) a 
      LEFT JOIN (
        SELECT 
          DISTINCT V1."GroupID", 
          V1."CONFLICTID", 
          V1."ShVTSTTime", 
          V1."ShVTENTime", 
          TO_CHAR(
            V1."G_CRDATEUNIQUE", 'YYYY-MM-DD'
          ) AS CRDATEUNIQUE, 
        FROM 
          CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS V1 
          INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 ON V2."CONFLICTID" = V1."CONFLICTID" 
        WHERE 
          V1."GroupID" IN (
            SELECT 
              DISTINCT "GroupID" 
            FROM 
              CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS 
            WHERE 
              "PayerID" = '${payerId}'
          )
      ) b ON a.CONFLICTID <> b.CONFLICTID 
      AND a."GroupID" = b."GroupID" 
    GROUP BY 
      a.CRDATEUNIQUE 
    UNION ALL 
    SELECT 
      '${payerId}' AS PAYERID, 
      a."CRDATEUNIQUE" AS "CRDATEUNIQUE", 
      'In-Service' AS "ConflictType", 
      '8' AS "ConflictTypeF", 
      COUNT(DISTINCT a."GroupID") AS "Total", 
      SUM(
        CASE WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" ELSE 0 END
      ) AS "ShiftPrice", 
      SUM(
        CASE WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" >= a."ShVTSTTime" 
        AND b."ShVTSTTime" <= a."ShVTENTime" 
        AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" >= b."ShVTSTTime" 
        AND a."ShVTSTTime" <= b."ShVTENTime" 
        AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" >= a."ShVTSTTime" 
        AND b."ShVTENTime" <= a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" >= b."ShVTSTTime" 
        AND a."ShVTENTime" <= b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" < a."ShVTSTTime" 
        AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" < b."ShVTSTTime" 
        AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" ELSE 0 END
      ) AS "OverlapPrice", 
      SUM(
        CASE WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" >= a."ShVTSTTime" 
        AND b."ShVTSTTime" <= a."ShVTENTime" 
        AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" >= b."ShVTSTTime" 
        AND a."ShVTSTTime" <= b."ShVTENTime" 
        AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" >= a."ShVTSTTime" 
        AND b."ShVTENTime" <= a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" >= b."ShVTSTTime" 
        AND a."ShVTENTime" <= b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" < a."ShVTSTTime" 
        AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" < b."ShVTSTTime" 
        AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" ELSE 0 END
      ) AS "FinalPrice" 
    FROM 
      (
        SELECT 
          DISTINCT V1."GroupID", 
          V1."CONFLICTID", 
          V1."ShVTSTTime", 
          V1."ShVTENTime", 
          V1."BilledRateMinute", 
          V1."G_CRDATEUNIQUE", 
          TO_CHAR(
            V1."G_CRDATEUNIQUE", 'YYYY-MM-DD'
          ) AS CRDATEUNIQUE, 
          V1."PayerID" AS APID, 
          CASE WHEN V2."StatusFlag" IN('R', 'D') THEN 'R' WHEN V2."StatusFlag" IN ('N') THEN 'N' ELSE 'U' END AS "StatusFlag" 
        FROM 
          CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS V1 
          INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 ON V2."CONFLICTID" = V1."CONFLICTID" 
        WHERE 
          V1."GroupID" IN (
            SELECT 
              DISTINCT "GroupID" 
            FROM 
              CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS 
            WHERE 
              "PayerID" = '${payerId}' 
              AND "InServiceFlag" = 'Y'
          )
      ) a 
      LEFT JOIN (
        SELECT 
          DISTINCT V1."GroupID", 
          V1."CONFLICTID", 
          V1."ShVTSTTime", 
          V1."ShVTENTime", 
          TO_CHAR(
            V1."G_CRDATEUNIQUE", 'YYYY-MM-DD'
          ) AS CRDATEUNIQUE, 
        FROM 
          CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS V1 
          INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 ON V2."CONFLICTID" = V1."CONFLICTID" 
        WHERE 
          V1."GroupID" IN (
            SELECT 
              DISTINCT "GroupID" 
            FROM 
              CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS 
            WHERE 
              "PayerID" = '${payerId}'
          )
      ) b ON a.CONFLICTID <> b.CONFLICTID 
      AND a."GroupID" = b."GroupID" 
    GROUP BY 
      a.CRDATEUNIQUE 
    UNION ALL 
    SELECT 
      '${payerId}' AS PAYERID, 
      a."CRDATEUNIQUE" AS "CRDATEUNIQUE", 
      'PTO' AS "ConflictType", 
      '9' AS "ConflictTypeF", 
      COUNT(DISTINCT a."GroupID") AS "Total", 
      SUM(
        CASE WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" ELSE 0 END
      ) AS "ShiftPrice", 
      SUM(
        CASE WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" >= a."ShVTSTTime" 
        AND b."ShVTSTTime" <= a."ShVTENTime" 
        AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" >= b."ShVTSTTime" 
        AND a."ShVTSTTime" <= b."ShVTENTime" 
        AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" >= a."ShVTSTTime" 
        AND b."ShVTENTime" <= a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" >= b."ShVTSTTime" 
        AND a."ShVTENTime" <= b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" < a."ShVTSTTime" 
        AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" < b."ShVTSTTime" 
        AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" ELSE 0 END
      ) AS "OverlapPrice", 
      SUM(
        CASE WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" >= a."ShVTSTTime" 
        AND b."ShVTSTTime" <= a."ShVTENTime" 
        AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" >= b."ShVTSTTime" 
        AND a."ShVTSTTime" <= b."ShVTENTime" 
        AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" >= a."ShVTSTTime" 
        AND b."ShVTENTime" <= a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" >= b."ShVTSTTime" 
        AND a."ShVTENTime" <= b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND b."ShVTSTTime" < a."ShVTSTTime" 
        AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, a."ShVTSTTime", a."ShVTENTime"
        ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
        AND a."StatusFlag" = 'R' 
        AND a."BilledRateMinute" > 0 
        AND a."ShVTSTTime" < b."ShVTSTTime" 
        AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
          MINUTE, b."ShVTSTTime", b."ShVTENTime"
        ) * a."BilledRateMinute" ELSE 0 END
      ) AS "FinalPrice" 
    FROM 
      (
        SELECT 
          DISTINCT V1."GroupID", 
          V1."CONFLICTID", 
          V1."ShVTSTTime", 
          V1."ShVTENTime", 
          V1."BilledRateMinute", 
          V1."G_CRDATEUNIQUE", 
          TO_CHAR(
            V1."G_CRDATEUNIQUE", 'YYYY-MM-DD'
          ) AS CRDATEUNIQUE, 
          V1."PayerID" AS APID, 
          CASE WHEN V2."StatusFlag" IN('R', 'D') THEN 'R' WHEN V2."StatusFlag" IN ('N') THEN 'N' ELSE 'U' END AS "StatusFlag" 
        FROM 
          CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS V1 
          INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 ON V2."CONFLICTID" = V1."CONFLICTID" 
        WHERE 
          V1."GroupID" IN (
            SELECT 
              DISTINCT "GroupID" 
            FROM 
              CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS 
            WHERE 
              "PayerID" = '${payerId}' 
              AND "PTOFlag" = 'Y'
          )
      ) a 
      LEFT JOIN (
        SELECT 
          DISTINCT V1."GroupID", 
          V1."CONFLICTID", 
          V1."ShVTSTTime", 
          V1."ShVTENTime", 
          TO_CHAR(
            V1."G_CRDATEUNIQUE", 'YYYY-MM-DD'
          ) AS CRDATEUNIQUE, 
        FROM 
          CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS V1 
          INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 ON V2."CONFLICTID" = V1."CONFLICTID" 
        WHERE 
          V1."GroupID" IN (
            SELECT 
              DISTINCT "GroupID" 
            FROM 
              CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS 
            WHERE 
              "PayerID" = '${payerId}'
          )
      ) b ON a.CONFLICTID <> b.CONFLICTID 
      AND a."GroupID" = b."GroupID" 
    GROUP BY 
      a.CRDATEUNIQUE
  );
---------------------------END PAYER CON TYPE---------------------
---------------------------PAYER AGENCY---------------------
INSERT INTO CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_AGENCY (
  PAYERID, CRDATEUNIQUE, PROVIDERID, 
  P_NAME, TIN, CON_TO, CON_SP, CON_OP, 
  CON_FP
) 
SELECT 
  '${payerId}' AS PAYERID, 
  a.CRDATEUNIQUE, 
  a."APRID" AS PROVIDERID, 
  a."ProviderName" AS P_NAME, 
  a."FederalTaxNumber" AS TIN, 
  COUNT(DISTINCT a."GroupID") AS CON_TO, 
  SUM(
    CASE WHEN a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" ELSE 0 END
  ) AS CON_SP, 
  SUM(
    CASE WHEN a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 
    AND b."ShVTSTTime" >= a."ShVTSTTime" 
    AND b."ShVTSTTime" <= a."ShVTENTime" 
    AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, b."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 
    AND a."ShVTSTTime" >= b."ShVTSTTime" 
    AND a."ShVTSTTime" <= b."ShVTENTime" 
    AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", b."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 
    AND b."ShVTSTTime" >= a."ShVTSTTime" 
    AND b."ShVTENTime" <= a."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, b."ShVTSTTime", b."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 
    AND a."ShVTSTTime" >= b."ShVTSTTime" 
    AND a."ShVTENTime" <= b."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 
    AND b."ShVTSTTime" < a."ShVTSTTime" 
    AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 
    AND a."ShVTSTTime" < b."ShVTSTTime" 
    AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, b."ShVTSTTime", b."ShVTENTime"
    ) * a."BilledRateMinute" ELSE 0 END
  ) AS CON_OP, 
  SUM(
    CASE WHEN a.APID = '${payerId}' 
    AND a."StatusFlag" = 'R' 
    AND a."BilledRateMinute" > 0 
    AND b."ShVTSTTime" >= a."ShVTSTTime" 
    AND b."ShVTSTTime" <= a."ShVTENTime" 
    AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, b."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."StatusFlag" = 'R' 
    AND a."BilledRateMinute" > 0 
    AND a."ShVTSTTime" >= b."ShVTSTTime" 
    AND a."ShVTSTTime" <= b."ShVTENTime" 
    AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", b."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."StatusFlag" = 'R' 
    AND a."BilledRateMinute" > 0 
    AND b."ShVTSTTime" >= a."ShVTSTTime" 
    AND b."ShVTENTime" <= a."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, b."ShVTSTTime", b."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."StatusFlag" = 'R' 
    AND a."BilledRateMinute" > 0 
    AND a."ShVTSTTime" >= b."ShVTSTTime" 
    AND a."ShVTENTime" <= b."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."StatusFlag" = 'R' 
    AND a."BilledRateMinute" > 0 
    AND b."ShVTSTTime" < a."ShVTSTTime" 
    AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."StatusFlag" = 'R' 
    AND a."BilledRateMinute" > 0 
    AND a."ShVTSTTime" < b."ShVTSTTime" 
    AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, b."ShVTSTTime", b."ShVTENTime"
    ) * a."BilledRateMinute" ELSE 0 END
  ) AS CON_FP 
FROM 
  (
    SELECT 
      DISTINCT V1."GroupID", 
      V1."CONFLICTID", 
      V1."ShVTSTTime", 
      V1."ShVTENTime", 
      V1."BilledRateMinute", 
      V1."ProviderID" AS APRID, 
      V1."ProviderName", 
      V1."FederalTaxNumber", 
      V1."G_CRDATEUNIQUE", 
      TO_CHAR(
        V1."G_CRDATEUNIQUE", 'YYYY-MM-DD'
      ) AS CRDATEUNIQUE, 
      V1."PayerID" AS APID, 
      CASE WHEN V2."StatusFlag" IN('R', 'D') THEN 'R' WHEN V2."StatusFlag" IN ('N') THEN 'N' ELSE 'U' END AS "StatusFlag" 
    FROM 
      CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS V1 
      INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 ON V2."CONFLICTID" = V1."CONFLICTID" 
    WHERE 
      V1."GroupID" IN (
        SELECT 
          DISTINCT "GroupID" 
        FROM 
          CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS 
        WHERE 
          --("SchOverAnotherSchTimeFlag" = 'Y'
          --  OR "VisitTimeOverAnotherVisitTimeFlag" = 'Y')
          --AND
          "PayerID" = '${payerId}'
      )
  ) a 
  LEFT JOIN (
    SELECT 
      DISTINCT V1."GroupID", 
      V1."CONFLICTID", 
      V1."ShVTSTTime", 
      V1."ShVTENTime", 
      TO_CHAR(
        V1."G_CRDATEUNIQUE", 'YYYY-MM-DD'
      ) AS CRDATEUNIQUE 
    FROM 
      CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS V1 
      INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 ON V2."CONFLICTID" = V1."CONFLICTID" 
    WHERE 
      V1."GroupID" IN (
        SELECT 
          DISTINCT "GroupID" 
        FROM 
          CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS 
        WHERE 
          --("SchOverAnotherSchTimeFlag" = 'Y'
          --  OR "VisitTimeOverAnotherVisitTimeFlag" = 'Y')
          --AND
          "PayerID" = '${payerId}'
      )
  ) b ON a.CONFLICTID <> b.CONFLICTID 
  AND a."GroupID" = b."GroupID" 
GROUP BY 
  a."APRID", 
  a."FederalTaxNumber", 
  a."ProviderName", 
  a.CRDATEUNIQUE;
---------------------------END PAYER AGENCY---------------------
---------------------------PAYER CAREGIVER---------------------
INSERT INTO CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_CAREGIVER (
  PAYERID, CRDATEUNIQUE, CAREGIVERID, 
  C_NAME, C_LNAME, C_FNAME, CON_TO, 
  CON_SP, CON_OP, CON_FP
) 
SELECT 
  '${payerId}' AS PAYERID, 
  a."CRDATEUNIQUE", 
  a."CaregiverID", 
  a."AideName", 
  a."AideLName", 
  a."AideFName", 
  COUNT(DISTINCT a."GroupID") AS "Total", 
  SUM(
    CASE WHEN a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" ELSE 0 END
  ) AS "ShiftPrice", 
  SUM(
    CASE WHEN a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 
    AND b."ShVTSTTime" >= a."ShVTSTTime" 
    AND b."ShVTSTTime" <= a."ShVTENTime" 
    AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, b."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 
    AND a."ShVTSTTime" >= b."ShVTSTTime" 
    AND a."ShVTSTTime" <= b."ShVTENTime" 
    AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", b."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 
    AND b."ShVTSTTime" >= a."ShVTSTTime" 
    AND b."ShVTENTime" <= a."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, b."ShVTSTTime", b."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 
    AND a."ShVTSTTime" >= b."ShVTSTTime" 
    AND a."ShVTENTime" <= b."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 
    AND b."ShVTSTTime" < a."ShVTSTTime" 
    AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 
    AND a."ShVTSTTime" < b."ShVTSTTime" 
    AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, b."ShVTSTTime", b."ShVTENTime"
    ) * a."BilledRateMinute" ELSE 0 END
  ) AS "OverlapPrice", 
  SUM(
    CASE WHEN a.APID = '${payerId}' 
    AND a."StatusFlag" = 'R' 
    AND a."BilledRateMinute" > 0 
    AND b."ShVTSTTime" >= a."ShVTSTTime" 
    AND b."ShVTSTTime" <= a."ShVTENTime" 
    AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, b."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."StatusFlag" = 'R' 
    AND a."BilledRateMinute" > 0 
    AND a."ShVTSTTime" >= b."ShVTSTTime" 
    AND a."ShVTSTTime" <= b."ShVTENTime" 
    AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", b."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."StatusFlag" = 'R' 
    AND a."BilledRateMinute" > 0 
    AND b."ShVTSTTime" >= a."ShVTSTTime" 
    AND b."ShVTENTime" <= a."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, b."ShVTSTTime", b."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."StatusFlag" = 'R' 
    AND a."BilledRateMinute" > 0 
    AND a."ShVTSTTime" >= b."ShVTSTTime" 
    AND a."ShVTENTime" <= b."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."StatusFlag" = 'R' 
    AND a."BilledRateMinute" > 0 
    AND b."ShVTSTTime" < a."ShVTSTTime" 
    AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."StatusFlag" = 'R' 
    AND a."BilledRateMinute" > 0 
    AND a."ShVTSTTime" < b."ShVTSTTime" 
    AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, b."ShVTSTTime", b."ShVTENTime"
    ) * a."BilledRateMinute" ELSE 0 END
  ) AS "FinalPrice" 
FROM 
  (
    SELECT 
      DISTINCT V1."GroupID", 
      V1."CONFLICTID", 
      V1."ShVTSTTime", 
      V1."ShVTENTime", 
      V1."BilledRateMinute", 
      V1."CaregiverID", 
      INITCAP(
        LOWER(V1."AideName")
      ) AS "AideName", 
      INITCAP(
        LOWER(V1."AideFName")
      ) AS "AideFName", 
      INITCAP(
        LOWER(V1."AideLName")
      ) AS "AideLName", 
      V1."G_CRDATEUNIQUE", 
      TO_CHAR(
        V1."G_CRDATEUNIQUE", 'YYYY-MM-DD'
      ) AS CRDATEUNIQUE, 
      V1."PayerID" AS APID, 
      CASE WHEN V2."StatusFlag" IN('R', 'D') THEN 'R' WHEN V2."StatusFlag" IN ('N') THEN 'N' ELSE 'U' END AS "StatusFlag" 
    FROM 
      CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS V1 
      INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 ON V2."CONFLICTID" = V1."CONFLICTID" 
    WHERE 
      V1."PayerID" = '${payerId}' 
      AND V1."GroupID" IN (
        SELECT 
          DISTINCT "GroupID" 
        FROM 
          CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS 
        WHERE 
          --("SchOverAnotherSchTimeFlag" = 'Y'
          --  OR "VisitTimeOverAnotherVisitTimeFlag" = 'Y')
          -- AND
          "PayerID" = '${payerId}'
      )
  ) a 
  LEFT JOIN (
    SELECT 
      DISTINCT V1."GroupID", 
      V1."CONFLICTID", 
      V1."ShVTSTTime", 
      V1."ShVTENTime", 
      TO_CHAR(
        V1."G_CRDATEUNIQUE", 'YYYY-MM-DD'
      ) AS CRDATEUNIQUE 
    FROM 
      CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS V1 
      INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 ON V2."CONFLICTID" = V1."CONFLICTID" 
    WHERE 
      V1."GroupID" IN (
        SELECT 
          DISTINCT "GroupID" 
        FROM 
          CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS 
        WHERE 
          --("SchOverAnotherSchTimeFlag" = 'Y'
          --  OR "VisitTimeOverAnotherVisitTimeFlag" = 'Y')
          --AND
          "PayerID" = '${payerId}'
      )
  ) b ON a.CONFLICTID <> b.CONFLICTID 
  AND a."GroupID" = b."GroupID" 
GROUP BY 
  a."AideName", 
  a."AideFName", 
  a."AideLName", 
  a."CRDATEUNIQUE", 
  a."CaregiverID";
---------------------------END PAYER CAREGIVER---------------------
---------------------------PAYER PATIENT---------------------
INSERT INTO CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_PATIENT(
  PAYERID, CRDATEUNIQUE, PATIENTID, 
  PFNAME, PLNAME, PNAME, ADMISSIONID, 
  CON_TO, CON_SP, CON_OP, CON_FP
) 
SELECT 
  '${payerId}' AS PAYERID, 
  a.CRDATEUNIQUE, 
  a.APAID, 
  a."PA_PFName", 
  a."PA_PLName", 
  a."PA_PName" AS "Name", 
  a."PA_PAdmissionID" AS "AdmissionID", 
  COUNT(DISTINCT a."GroupID") AS "Total", 
  SUM(
    CASE WHEN a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" ELSE 0 END
  ) AS "ShiftPrice", 
  SUM(
    CASE WHEN a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 
    AND b."ShVTSTTime" >= a."ShVTSTTime" 
    AND b."ShVTSTTime" <= a."ShVTENTime" 
    AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, b."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 
    AND a."ShVTSTTime" >= b."ShVTSTTime" 
    AND a."ShVTSTTime" <= b."ShVTENTime" 
    AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", b."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 
    AND b."ShVTSTTime" >= a."ShVTSTTime" 
    AND b."ShVTENTime" <= a."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, b."ShVTSTTime", b."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 
    AND a."ShVTSTTime" >= b."ShVTSTTime" 
    AND a."ShVTENTime" <= b."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 
    AND b."ShVTSTTime" < a."ShVTSTTime" 
    AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 
    AND a."ShVTSTTime" < b."ShVTSTTime" 
    AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, b."ShVTSTTime", b."ShVTENTime"
    ) * a."BilledRateMinute" ELSE 0 END
  ) AS "OverlapPrice", 
  SUM(
    CASE WHEN a.APID = '${payerId}' 
    AND a."StatusFlag" = 'R' 
    AND a."BilledRateMinute" > 0 
    AND b."ShVTSTTime" >= a."ShVTSTTime" 
    AND b."ShVTSTTime" <= a."ShVTENTime" 
    AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, b."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."StatusFlag" = 'R' 
    AND a."BilledRateMinute" > 0 
    AND a."ShVTSTTime" >= b."ShVTSTTime" 
    AND a."ShVTSTTime" <= b."ShVTENTime" 
    AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", b."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."StatusFlag" = 'R' 
    AND a."BilledRateMinute" > 0 
    AND b."ShVTSTTime" >= a."ShVTSTTime" 
    AND b."ShVTENTime" <= a."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, b."ShVTSTTime", b."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."StatusFlag" = 'R' 
    AND a."BilledRateMinute" > 0 
    AND a."ShVTSTTime" >= b."ShVTSTTime" 
    AND a."ShVTENTime" <= b."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."StatusFlag" = 'R' 
    AND a."BilledRateMinute" > 0 
    AND b."ShVTSTTime" < a."ShVTSTTime" 
    AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."StatusFlag" = 'R' 
    AND a."BilledRateMinute" > 0 
    AND a."ShVTSTTime" < b."ShVTSTTime" 
    AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, b."ShVTSTTime", b."ShVTENTime"
    ) * a."BilledRateMinute" ELSE 0 END
  ) AS "FinalPrice" 
FROM 
  (
    SELECT 
      DISTINCT V1."GroupID", 
      V1."CONFLICTID", 
      V1."ShVTSTTime", 
      V1."ShVTENTime", 
      V1."BilledRateMinute", 
      V1."PA_PatientID" AS APAID, 
      V1."PA_PName", 
      V1."PA_PFName", 
      V1."PA_PLName", 
      V1."G_CRDATEUNIQUE", 
      TO_CHAR(
        V1."G_CRDATEUNIQUE", 'YYYY-MM-DD'
      ) AS CRDATEUNIQUE, 
      V1."PayerID" AS APID, 
      CASE WHEN V2."StatusFlag" IN('R', 'D') THEN 'R' WHEN V2."StatusFlag" IN ('N') THEN 'N' ELSE 'U' END AS "StatusFlag", 
      (
        CASE WHEN V1."PayerID" = '${payerId}' 
        AND V1."PA_PAdmissionID" != '' THEN V1."PA_PAdmissionID" WHEN V1."PayerID" = '${payerId}' 
        AND V1."PA_PAdmissionID" = '' THEN '' ELSE 'Restricted' END
      ) AS "PA_PAdmissionID" 
    FROM 
      CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS V1 
      INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 ON V2."CONFLICTID" = V1."CONFLICTID" 
    WHERE 
      V1."GroupID" IN (
        SELECT 
          DISTINCT "GroupID" 
        FROM 
          CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS 
        WHERE 
          --("SchOverAnotherSchTimeFlag" = 'Y'
          --  OR "VisitTimeOverAnotherVisitTimeFlag" = 'Y')
          --AND
          "PayerID" = '${payerId}'
      )
  ) a 
  LEFT JOIN (
    SELECT 
      DISTINCT V1."GroupID", 
      V1."CONFLICTID", 
      V1."ShVTSTTime", 
      V1."ShVTENTime" 
    FROM 
      CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS V1 
      INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 ON V2."CONFLICTID" = V1."CONFLICTID" 
    WHERE 
      V1."GroupID" IN (
        SELECT 
          DISTINCT "GroupID" 
        FROM 
          CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS 
        WHERE 
          --("SchOverAnotherSchTimeFlag" = 'Y'
          --  OR "VisitTimeOverAnotherVisitTimeFlag" = 'Y')
          --AND
          "PayerID" = '${payerId}'
      )
  ) b ON a.CONFLICTID <> b.CONFLICTID 
  AND a."GroupID" = b."GroupID" 
WHERE 
  a."PA_PName" IS NOT NULL 
  AND a."PA_PAdmissionID" != 'Restricted' 
GROUP BY 
  a."APAID", 
  a."PA_PName", 
  a."PA_PFName", 
  a."PA_PLName", 
  a."PA_PAdmissionID", 
  a.CRDATEUNIQUE;
---------------------------END PAYER CAREGIVER---------------------
---------------------------PAYER PAYER---------------------
INSERT INTO CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_PAYER(
  PAYERID, CRDATEUNIQUE, CONPAYERID, 
  PNAME, CON_TO, CON_SP, CON_OP, CON_FP
) 
SELECT 
  '${payerId}' AS PAYERID, 
  a.CRDATEUNIQUE, 
  a.APID, 
  a."Contract" AS "Name", 
  COUNT(DISTINCT a."GroupID") AS "Total", 
  SUM(
    CASE WHEN a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" ELSE 0 END
  ) AS "ShiftPrice", 
  SUM(
    CASE WHEN a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 
    AND b."ShVTSTTime" >= a."ShVTSTTime" 
    AND b."ShVTSTTime" <= a."ShVTENTime" 
    AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, b."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 
    AND a."ShVTSTTime" >= b."ShVTSTTime" 
    AND a."ShVTSTTime" <= b."ShVTENTime" 
    AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", b."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 
    AND b."ShVTSTTime" >= a."ShVTSTTime" 
    AND b."ShVTENTime" <= a."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, b."ShVTSTTime", b."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 
    AND a."ShVTSTTime" >= b."ShVTSTTime" 
    AND a."ShVTENTime" <= b."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 
    AND b."ShVTSTTime" < a."ShVTSTTime" 
    AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."BilledRateMinute" > 0 
    AND a."ShVTSTTime" < b."ShVTSTTime" 
    AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, b."ShVTSTTime", b."ShVTENTime"
    ) * a."BilledRateMinute" ELSE 0 END
  ) AS "OverlapPrice", 
  SUM(
    CASE WHEN a.APID = '${payerId}' 
    AND a."StatusFlag" = 'R' 
    AND a."BilledRateMinute" > 0 
    AND b."ShVTSTTime" >= a."ShVTSTTime" 
    AND b."ShVTSTTime" <= a."ShVTENTime" 
    AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, b."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."StatusFlag" = 'R' 
    AND a."BilledRateMinute" > 0 
    AND a."ShVTSTTime" >= b."ShVTSTTime" 
    AND a."ShVTSTTime" <= b."ShVTENTime" 
    AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", b."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."StatusFlag" = 'R' 
    AND a."BilledRateMinute" > 0 
    AND b."ShVTSTTime" >= a."ShVTSTTime" 
    AND b."ShVTENTime" <= a."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, b."ShVTSTTime", b."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."StatusFlag" = 'R' 
    AND a."BilledRateMinute" > 0 
    AND a."ShVTSTTime" >= b."ShVTSTTime" 
    AND a."ShVTENTime" <= b."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."StatusFlag" = 'R' 
    AND a."BilledRateMinute" > 0 
    AND b."ShVTSTTime" < a."ShVTSTTime" 
    AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, a."ShVTSTTime", a."ShVTENTime"
    ) * a."BilledRateMinute" WHEN a.APID = '${payerId}' 
    AND a."StatusFlag" = 'R' 
    AND a."BilledRateMinute" > 0 
    AND a."ShVTSTTime" < b."ShVTSTTime" 
    AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(
      MINUTE, b."ShVTSTTime", b."ShVTENTime"
    ) * a."BilledRateMinute" ELSE 0 END
  ) AS "FinalPrice" 
FROM 
  (
    SELECT 
      DISTINCT V1."GroupID", 
      V1."CONFLICTID", 
      V1."ShVTSTTime", 
      V1."ShVTENTime", 
      V1."BilledRateMinute", 
      V1."G_CRDATEUNIQUE", 
      TO_CHAR("G_CRDATEUNIQUE", 'YYYY-MM-DD') AS CRDATEUNIQUE, 
      V1."PayerID" AS APID, 
      V1."Contract", 
      CASE WHEN V2."StatusFlag" IN('R', 'D') THEN 'R' WHEN V2."StatusFlag" IN ('N') THEN 'N' ELSE 'U' END AS "StatusFlag" 
    FROM 
      CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS V1 
      INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 ON V2."CONFLICTID" = V1."CONFLICTID" 
    WHERE 
      V1."AppPayerID" != '0' 
      AND V1."GroupID" IN (
        SELECT 
          DISTINCT "GroupID" 
        FROM 
          CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS 
        WHERE 
          --("SchOverAnotherSchTimeFlag" = 'Y'
          --  OR "VisitTimeOverAnotherVisitTimeFlag" = 'Y')
          --AND
          "PayerID" = '${payerId}'
      )
  ) a 
  LEFT JOIN (
    SELECT 
      DISTINCT V1."GroupID", 
      V1."CONFLICTID", 
      V1."ShVTSTTime", 
      V1."ShVTENTime" 
    FROM 
      CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS V1 
      INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 ON V2."CONFLICTID" = V1."CONFLICTID" 
    WHERE 
      V1."GroupID" IN (
        SELECT 
          DISTINCT "GroupID" 
        FROM 
          CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS 
        WHERE 
          --("SchOverAnotherSchTimeFlag" = 'Y'
          --  OR "VisitTimeOverAnotherVisitTimeFlag" = 'Y')
          --AND
          "PayerID" = '${payerId}'
      )
  ) b ON a.CONFLICTID <> b.CONFLICTID 
  AND a."GroupID" = b."GroupID" 
GROUP BY 
  a."APID", 
  a."Contract", 
  a.CRDATEUNIQUE;
