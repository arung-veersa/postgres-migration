UPDATE 
  CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS AS CVM 
SET 
  CVM."UpdateFlag" = NULL, 
  CVM."StatusFlag" = ' D ', 
  CVM."ResolveDate" = COALESCE(
    CVM."ResolveDate", CURRENT_TIMESTAMP
  ), 
  CVM."ResolvedBy" = COALESCE(
    CVM."ConAgencyContact", CVM."ConProviderName"
  ) 
FROM 
  ANALYTICS.BI.FACTVISITCALLPERFORMANCE_DELETED_CR AS DELETECR 
WHERE 
  CVM."ConVisitID" = DELETECR."Visit Id" 
  AND CVM."StatusFlag" != ' D ' 
  AND DATE(CVM."VisitDate") BETWEEN DATE(
    DATEADD(
      year, 
      -2, 
      GETDATE()
    )
  ) 
  AND DATE(
    DATEADD(
      day, 
      45, 
      GETDATE()
    )
  );
UPDATE 
  CONFLICTREPORT.PUBLIC.CONFLICTS CF 
SET 
  CF."StatusFlag" = ' D ', 
  CF."ResolveDate" = COALESCE(
    CF."ResolveDate", CURRENT_TIMESTAMP
  ), 
  CF."ResolvedBy" = COALESCE(
    CVM."AgencyContact", CVM."ProviderName"
  ) 
FROM 
  ANALYTICS.BI.FACTVISITCALLPERFORMANCE_DELETED_CR DELETECR 
  INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM ON CVM."VisitID" = DELETECR."Visit Id" 
WHERE 
  CF."StatusFlag" != ' D ' 
  AND CF."CONFLICTID" = CVM."CONFLICTID" 
  AND DATE(CVM."VisitDate") BETWEEN DATE(
    DATEADD(
      year, 
      -2, 
      GETDATE()
    )
  ) 
  AND DATE(
    DATEADD(
      day, 
      45, 
      GETDATE()
    )
  );
UPDATE 
  CONFLICTREPORT.PUBLIC.CONFLICTS CF 
SET 
  CF."StatusFlag" = CASE WHEN CF."StatusFlag" = ' D ' THEN ' D ' WHEN CVM."IsMissed" = TRUE THEN ' R ' ELSE CF."StatusFlag" END, 
  CF."ResolveDate" = CASE WHEN CF."StatusFlag" = ' D ' THEN COALESCE(
    CF."ResolveDate", CURRENT_TIMESTAMP
  ) WHEN CVM."IsMissed" = TRUE THEN COALESCE(
    CF."ResolveDate", CURRENT_TIMESTAMP
  ) ELSE COALESCE(
    CF."ResolveDate", CURRENT_TIMESTAMP
  ) END, 
  CF."ResolvedBy" = CASE WHEN CF."StatusFlag" = ' D ' THEN COALESCE(
    CVM."AgencyContact", CVM."ProviderName"
  ) WHEN CVM."IsMissed" = TRUE THEN COALESCE(
    CVM."AgencyContact", CVM."ProviderName"
  ) ELSE COALESCE(
    CF."ResolvedBy", CVM."AgencyContact", 
    CVM."ProviderName"
  ) END 
FROM 
  CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM 
WHERE 
  CVM.CONFLICTID = CF.CONFLICTID 
  AND DATE(CVM."VisitDate") BETWEEN DATE(
    DATEADD(
      year, 
      -2, 
      GETDATE()
    )
  ) 
  AND DATE(
    DATEADD(
      day, 
      45, 
      GETDATE()
    )
  );
UPDATE 
  CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM 
SET 
  CVM."StatusFlag" = CASE WHEN CVM."StatusFlag" = ' D ' THEN ' D ' WHEN CVM."ConIsMissed" = TRUE THEN ' R ' ELSE CVM."StatusFlag" END, 
  CVM."ResolveDate" = CASE WHEN CVM."StatusFlag" = ' D ' THEN COALESCE(
    CVM."ResolveDate", CURRENT_TIMESTAMP
  ) WHEN CVM."ConIsMissed" = TRUE THEN COALESCE(
    CVM."ResolveDate", CURRENT_TIMESTAMP
  ) ELSE COALESCE(
    CVM."ResolveDate", CURRENT_TIMESTAMP
  ) END, 
  CVM."ResolvedBy" = CASE WHEN CVM."StatusFlag" = ' D ' THEN COALESCE(
    CVM."ConAgencyContact", CVM."ConProviderName"
  ) WHEN CVM."ConIsMissed" = TRUE THEN COALESCE(
    CVM."ConAgencyContact", CVM."ConProviderName"
  ) ELSE COALESCE(
    CVM."ResolvedBy", CVM."ConAgencyContact", 
    CVM."ConProviderName"
  ) END 
WHERE 
  DATE(CVM."VisitDate") BETWEEN DATE(
    DATEADD(
      year, 
      -2, 
      GETDATE()
    )
  ) 
  AND DATE(
    DATEADD(
      day, 
      45, 
      GETDATE()
    )
  );
UPDATE 
  CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS AS CVM 
SET 
  CVM."UpdateFlag" = NULL, 
  CVM."StatusFlag" = CASE WHEN CVM."StatusFlag" = ' D ' THEN ' D ' WHEN CVM."ConIsMissed" = TRUE THEN ' R ' ELSE ' R ' END, 
  CVM."ResolveDate" = COALESCE(
    CVM."ResolveDate", CURRENT_TIMESTAMP
  ), 
  CVM."ResolvedBy" = CASE WHEN CVM."StatusFlag" = ' D ' THEN COALESCE(
    CVM."ConAgencyContact", CVM."ConProviderName"
  ) WHEN CVM."ConIsMissed" = TRUE THEN COALESCE(
    CVM."ConAgencyContact", CVM."ConProviderName"
  ) ELSE COALESCE(
    CVM."ResolvedBy", CVM."ConAgencyContact", 
    CVM."ConProviderName"
  ) END 
WHERE 
  CVM."UpdateFlag" = 1 
  AND DATE(CVM."VisitDate") BETWEEN DATE(
    DATEADD(
      year, 
      -2, 
      GETDATE()
    )
  ) 
  AND DATE(
    DATEADD(
      day, 
      45, 
      GETDATE()
    )
  );
UPDATE 
  CONFLICTREPORT.PUBLIC.CONFLICTS CF 
SET 
  CF."UpdatedRFlag" = ' 1 ' 
FROM 
  CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM 
WHERE 
  CVM.CONFLICTID = CF.CONFLICTID 
  AND DATE(CVM."VisitDate") BETWEEN DATE(
    DATEADD(
      year, 
      -2, 
      GETDATE()
    )
  ) 
  AND DATE(
    DATEADD(
      day, 
      45, 
      GETDATE()
    )
  );
UPDATE 
  CONFLICTREPORT.PUBLIC.CONFLICTS CF 
SET 
  CF."StatusFlag" = ' U ', 
  CF."UpdatedRFlag" = NULL 
WHERE 
  CF.CONFLICTID IN (
    SELECT 
      CF.CONFLICTID 
    FROM 
      CONFLICTREPORT.PUBLIC.CONFLICTS CF 
      INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM ON CVM.CONFLICTID = CF.CONFLICTID 
    WHERE 
      CF."StatusFlag" NOT IN (' D ', ' I ', ' W ', ' U ') 
      AND CVM."StatusFlag" IN(' U ') 
      AND DATE(CVM."VisitDate") BETWEEN DATE(
        DATEADD(
          year, 
          -2, 
          GETDATE()
        )
      ) 
      AND DATE(
        DATEADD(
          day, 
          45, 
          GETDATE()
        )
      ) 
    GROUP BY 
      CF.CONFLICTID
  );
UPDATE 
  CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM 
SET 
  CVM."StatusFlag" = CASE WHEN CVM."StatusFlag" = ' D ' THEN ' D ' WHEN CVM."ConIsMissed" = TRUE THEN ' R ' ELSE ' R ' END, 
  CVM."ResolveDate" = CASE WHEN CVM."StatusFlag" = ' D ' THEN COALESCE(
    CVM."ResolveDate", CURRENT_TIMESTAMP
  ) WHEN CVM."ConIsMissed" = TRUE THEN COALESCE(
    CVM."ResolveDate", CURRENT_TIMESTAMP
  ) ELSE COALESCE(
    CVM."ResolveDate", CURRENT_TIMESTAMP
  ) END, 
  CVM."ResolvedBy" = CASE WHEN CVM."StatusFlag" = ' D ' THEN COALESCE(
    CVM."ConAgencyContact", CVM."ConProviderName"
  ) WHEN CVM."ConIsMissed" = TRUE THEN COALESCE(
    CVM."ConAgencyContact", CVM."ConProviderName"
  ) ELSE COALESCE(
    CVM."ResolvedBy", CVM."ConAgencyContact", 
    CVM."ConProviderName"
  ) END 
WHERE 
  CVM.CONFLICTID IN (
    SELECT 
      CF.CONFLICTID 
    FROM 
      CONFLICTREPORT.PUBLIC.CONFLICTS CF 
      INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM ON CVM.CONFLICTID = CF.CONFLICTID 
    WHERE 
      CF."StatusFlag" IN (' R ', ' D ') 
      AND DATE(CVM."VisitDate") BETWEEN DATE(
        DATEADD(
          year, 
          -2, 
          GETDATE()
        )
      ) 
      AND DATE(
        DATEADD(
          day, 
          45, 
          GETDATE()
        )
      ) 
    GROUP BY 
      CF.CONFLICTID 
    HAVING 
      COUNT(CVM.ID) = 1
  ) 
  AND DATE(CVM."VisitDate") BETWEEN DATE(
    DATEADD(
      year, 
      -2, 
      GETDATE()
    )
  ) 
  AND DATE(
    DATEADD(
      day, 
      45, 
      GETDATE()
    )
  );
UPDATE 
  CONFLICTREPORT.PUBLIC.CONFLICTS CF 
SET 
  CF."StatusFlag" = CASE WHEN CF."StatusFlag" = ' D ' THEN ' D ' WHEN CVM."IsMissed" = TRUE THEN ' R ' ELSE ' R ' END, 
  CF."ResolveDate" = CASE WHEN CF."StatusFlag" = ' D ' THEN COALESCE(
    CF."ResolveDate", CURRENT_TIMESTAMP
  ) WHEN CVM."IsMissed" = TRUE THEN COALESCE(
    CF."ResolveDate", CURRENT_TIMESTAMP
  ) ELSE COALESCE(
    CF."ResolveDate", CURRENT_TIMESTAMP
  ) END, 
  CF."ResolvedBy" = CASE WHEN CF."StatusFlag" = ' D ' THEN COALESCE(
    CVM."AgencyContact", CVM."ProviderName"
  ) WHEN CVM."IsMissed" = TRUE THEN COALESCE(
    CVM."AgencyContact", CVM."ProviderName"
  ) ELSE COALESCE(
    CF."ResolvedBy", CVM."AgencyContact", 
    CVM."ProviderName"
  ) END 
FROM 
  CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM 
WHERE 
  CVM.CONFLICTID = CF.CONFLICTID 
  AND CF.CONFLICTID IN(
    SELECT 
      DISTINCT CVM.CONFLICTID 
    FROM 
      CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM 
    WHERE 
      CVM.CONFLICTID IN (
        SELECT 
          DISTINCT CVM.CONFLICTID 
        FROM 
          CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM 
        WHERE 
          CVM."StatusFlag" IN(' R ', ' D ') 
          AND DATE(CVM."VisitDate") BETWEEN DATE(
            DATEADD(
              year, 
              -2, 
              GETDATE()
            )
          ) 
          AND DATE(
            DATEADD(
              day, 
              45, 
              GETDATE()
            )
          )
      ) 
      AND DATE(CVM."VisitDate") BETWEEN DATE(
        DATEADD(
          year, 
          -2, 
          GETDATE()
        )
      ) 
      AND DATE(
        DATEADD(
          day, 
          45, 
          GETDATE()
        )
      ) 
    GROUP BY 
      CVM.CONFLICTID 
    HAVING 
      COUNT(CVM.ID) = 1
  ) 
  AND DATE(CVM."VisitDate") BETWEEN DATE(
    DATEADD(
      year, 
      -2, 
      GETDATE()
    )
  ) 
  AND DATE(
    DATEADD(
      day, 
      45, 
      GETDATE()
    )
  );
UPDATE 
  CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM 
SET 
  CVM."StatusFlag" = CASE WHEN CVM."StatusFlag" = ' D ' THEN ' D ' WHEN CVM."ConIsMissed" = TRUE THEN ' R ' ELSE ' R ' END, 
  CVM."ResolveDate" = CASE WHEN CVM."StatusFlag" = ' D ' THEN COALESCE(
    CVM."ResolveDate", CURRENT_TIMESTAMP
  ) WHEN CVM."ConIsMissed" = TRUE THEN COALESCE(
    CVM."ResolveDate", CURRENT_TIMESTAMP
  ) ELSE COALESCE(
    CVM."ResolveDate", CURRENT_TIMESTAMP
  ) END, 
  CVM."ResolvedBy" = CASE WHEN CVM."StatusFlag" = ' D ' THEN COALESCE(
    CVM."ConAgencyContact", CVM."ConProviderName"
  ) WHEN CVM."ConIsMissed" = TRUE THEN COALESCE(
    CVM."ConAgencyContact", CVM."ConProviderName"
  ) ELSE COALESCE(
    CVM."ResolvedBy", CVM."ConAgencyContact", 
    CVM."ConProviderName"
  ) END 
WHERE 
  CVM.CONFLICTID IN (
    SELECT 
      CF.CONFLICTID 
    FROM 
      CONFLICTREPORT.PUBLIC.CONFLICTS CF 
      LEFT JOIN CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM ON CVM.CONFLICTID = CF.CONFLICTID 
      AND DATE(CVM."VisitDate") BETWEEN DATE(
        DATEADD(
          year, 
          -2, 
          GETDATE()
        )
      ) 
      AND DATE(
        DATEADD(
          day, 
          45, 
          GETDATE()
        )
      ) 
      LEFT JOIN CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM1 ON CVM1.CONFLICTID = CF.CONFLICTID 
      AND CVM1."StatusFlag" IN(' R ', ' D ') 
      AND DATE(CVM1."VisitDate") BETWEEN DATE(
        DATEADD(
          year, 
          -2, 
          GETDATE()
        )
      ) 
      AND DATE(
        DATEADD(
          day, 
          45, 
          GETDATE()
        )
      ) 
    WHERE 
      CF."StatusFlag" IN(' R ', ' D ') 
    GROUP BY 
      CF.CONFLICTID 
    HAVING 
      COUNT(DISTINCT CVM.ID) = COUNT(DISTINCT CVM1.ID)
  ) 
  AND DATE(CVM."VisitDate") BETWEEN DATE(
    DATEADD(
      year, 
      -2, 
      GETDATE()
    )
  ) 
  AND DATE(
    DATEADD(
      day, 
      45, 
      GETDATE()
    )
  );
UPDATE 
  CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM 
SET 
  CVM."StatusFlag" = CASE WHEN CVM."StatusFlag" = ' D ' THEN ' D ' WHEN CVM."ConIsMissed" = TRUE THEN ' R ' ELSE ' R ' END, 
  CVM."ResolveDate" = CASE WHEN CVM."StatusFlag" = ' D ' THEN COALESCE(
    CVM."ResolveDate", CURRENT_TIMESTAMP
  ) WHEN CVM."ConIsMissed" = TRUE THEN COALESCE(
    CVM."ResolveDate", CURRENT_TIMESTAMP
  ) ELSE COALESCE(
    CVM."ResolveDate", CURRENT_TIMESTAMP
  ) END, 
  CVM."ResolvedBy" = CASE WHEN CVM."StatusFlag" = ' D ' THEN COALESCE(
    CVM."ConAgencyContact", CVM."ConProviderName"
  ) WHEN CVM."ConIsMissed" = TRUE THEN COALESCE(
    CVM."ConAgencyContact", CVM."ConProviderName"
  ) ELSE COALESCE(
    CVM."ResolvedBy", CVM."ConAgencyContact", 
    CVM."ConProviderName"
  ) END 
WHERE 
  CVM.CONFLICTID IN (
    SELECT 
      CF.CONFLICTID 
    FROM 
      CONFLICTREPORT.PUBLIC.CONFLICTS CF 
      LEFT JOIN CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM ON CVM.CONFLICTID = CF.CONFLICTID 
      AND DATE(CVM."VisitDate") BETWEEN DATE(
        DATEADD(
          year, 
          -2, 
          GETDATE()
        )
      ) 
      AND DATE(
        DATEADD(
          day, 
          45, 
          GETDATE()
        )
      ) 
      LEFT JOIN CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM1 ON CVM1.CONFLICTID = CF.CONFLICTID 
      AND CVM1."StatusFlag" IN(' R ', ' D ') 
      AND DATE(CVM1."VisitDate") BETWEEN DATE(
        DATEADD(
          year, 
          -2, 
          GETDATE()
        )
      ) 
      AND DATE(
        DATEADD(
          day, 
          45, 
          GETDATE()
        )
      ) 
    WHERE 
      CF."StatusFlag" IN(' R ', ' D ') 
    GROUP BY 
      CF.CONFLICTID 
    HAVING 
      COUNT(DISTINCT CVM.ID) = COUNT(DISTINCT CVM1.ID) 
      OR (
        COUNT(DISTINCT CVM.ID)-1
      ) = COUNT(DISTINCT CVM1.ID)
  ) 
  AND DATE(CVM."VisitDate") BETWEEN DATE(
    DATEADD(
      year, 
      -2, 
      GETDATE()
    )
  ) 
  AND DATE(
    DATEADD(
      day, 
      45, 
      GETDATE()
    )
  );
UPDATE 
  CONFLICTREPORT.PUBLIC.CONFLICTS CF 
SET 
  CF."StatusFlag" = CASE WHEN CF."StatusFlag" = ' D ' THEN ' D ' WHEN CVM."IsMissed" = TRUE THEN ' R ' ELSE ' R ' END, 
  CF."ResolveDate" = CASE WHEN CF."StatusFlag" = ' D ' THEN COALESCE(
    CF."ResolveDate", CURRENT_TIMESTAMP
  ) WHEN CVM."IsMissed" = TRUE THEN COALESCE(
    CF."ResolveDate", CURRENT_TIMESTAMP
  ) ELSE COALESCE(
    CF."ResolveDate", CURRENT_TIMESTAMP
  ) END, 
  CF."ResolvedBy" = CASE WHEN CF."StatusFlag" = ' D ' THEN COALESCE(
    CVM."AgencyContact", CVM."ProviderName"
  ) WHEN CVM."IsMissed" = TRUE THEN COALESCE(
    CVM."AgencyContact", CVM."ProviderName"
  ) ELSE COALESCE(
    CF."ResolvedBy", CVM."AgencyContact", 
    CVM."ProviderName"
  ) END 
FROM 
  CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM 
WHERE 
  CVM.CONFLICTID = CF.CONFLICTID 
  AND CF.CONFLICTID IN (
    SELECT 
      CF.CONFLICTID 
    FROM 
      CONFLICTREPORT.PUBLIC.CONFLICTS CF 
      LEFT JOIN CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM ON CVM.CONFLICTID = CF.CONFLICTID 
      AND DATE(CVM."VisitDate") BETWEEN DATE(
        DATEADD(
          year, 
          -2, 
          GETDATE()
        )
      ) 
      AND DATE(
        DATEADD(
          day, 
          45, 
          GETDATE()
        )
      ) 
      LEFT JOIN CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM1 ON CVM1.CONFLICTID = CF.CONFLICTID 
      AND CVM1."StatusFlag" IN(' R ', ' D ') 
      AND DATE(CVM1."VisitDate") BETWEEN DATE(
        DATEADD(
          year, 
          -2, 
          GETDATE()
        )
      ) 
      AND DATE(
        DATEADD(
          day, 
          45, 
          GETDATE()
        )
      ) 
    WHERE 
      CF."StatusFlag" NOT IN(' R ', ' D ') 
    GROUP BY 
      CF.CONFLICTID 
    HAVING 
      COUNT(DISTINCT CVM.ID) = COUNT(DISTINCT CVM1.ID) 
      AND COUNT(DISTINCT CVM.ID) > 0 
      AND COUNT(DISTINCT CVM1.ID) > 0
  ) 
  AND DATE(CVM."VisitDate") BETWEEN DATE(
    DATEADD(
      year, 
      -2, 
      GETDATE()
    )
  ) 
  AND DATE(
    DATEADD(
      day, 
      45, 
      GETDATE()
    )
  );
UPDATE 
  CONFLICTREPORT.PUBLIC.CONFLICTS CF 
SET 
  CF."StatusFlag" = CASE WHEN CF."NoResponseFlag" = ' Yes ' THEN ' N ' ELSE CF."StatusFlag" END, 
  CF."ResolveDate" = NULL, 
  CF."ResolvedBy" = NULL 
FROM 
  CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM 
WHERE 
  CVM.CONFLICTID = CF.CONFLICTID 
  AND CF."StatusFlag" IN (' U ', ' N ', ' W ', ' I ') 
  AND DATE(CVM."VisitDate") BETWEEN DATE(
    DATEADD(
      year, 
      -2, 
      GETDATE()
    )
  ) 
  AND DATE(
    DATEADD(
      day, 
      45, 
      GETDATE()
    )
  );
UPDATE 
  CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM 
SET 
  CVM."StatusFlag" = CASE WHEN CVM."ConNoResponseFlag" = ' Yes ' THEN ' N ' ELSE CVM."StatusFlag" END, 
  CVM."ResolveDate" = NULL, 
  CVM."ResolvedBy" = NULL 
WHERE 
  CVM."StatusFlag" IN (' U ', ' N ', ' W ', ' I ') 
  AND DATE(CVM."VisitDate") BETWEEN DATE(
    DATEADD(
      year, 
      -2, 
      GETDATE()
    )
  ) 
  AND DATE(
    DATEADD(
      day, 
      45, 
      GETDATE()
    )
  );
UPDATE 
  CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS 
SET 
  "ShVTSTTime" = COALESCE(
    "VisitStartTime", "SchStartTime", 
    "InserviceStartDate"
  ), 
  "ShVTENTime" = COALESCE(
    "VisitEndTime", "SchEndTime", "InserviceEndDate"
  ), 
  "CShVTSTTime" = COALESCE(
    "ConVisitStartTime", "ConSchStartTime", 
    "ConInserviceStartDate"
  ), 
  "CShVTENTime" = COALESCE(
    "ConVisitEndTime", "ConSchEndTime", 
    "ConInserviceEndDate"
  ) 
WHERE 
  DATE("VisitDate") BETWEEN DATE(
    DATEADD(
      year, 
      -2, 
      GETDATE()
    )
  ) 
  AND DATE(
    DATEADD(
      day, 
      45, 
      GETDATE()
    )
  );
UPDATE 
  CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS 
SET 
  "BilledRateMinute" = (
    CASE WHEN "Billed" = ' yes ' 
    AND "RateType" = ' Hourly ' 
    AND "BillRateBoth" > 0 THEN "BillRateBoth" / 60 WHEN "Billed" = ' yes ' 
    AND "RateType" = ' Daily ' 
    AND "BillRateBoth" > 0 
    AND "BilledHours" > 0 THEN ("BillRateBoth" / "BilledHours")/ 60 WHEN "Billed" = ' yes ' 
    AND "RateType" = ' Visit ' 
    AND "BillRateBoth" > 0 
    AND "BilledHours" > 0 THEN ("BillRateBoth" / "BilledHours")/ 60 WHEN "Billed" != ' yes ' 
    AND "RateType" = ' Hourly ' 
    AND "BillRateBoth" > 0 THEN "BillRateBoth" / 60 WHEN "Billed" != ' yes ' 
    AND "RateType" = ' Daily ' 
    AND "BillRateBoth" > 0 
    AND "SchStartTime" IS NOT NULL 
    AND "SchEndTime" IS NOT NULL 
    AND "SchStartTime" != "SchEndTime" THEN (
      "BillRateBoth" /(
        TIMESTAMPDIFF(
          MINUTE, "SchStartTime", "SchEndTime"
        )/ 60
      )
    )/ 60 WHEN "Billed" != ' yes ' 
    AND "RateType" = ' Visit ' 
    AND "BillRateBoth" > 0 
    AND "SchStartTime" IS NOT NULL 
    AND "SchEndTime" IS NOT NULL 
    AND "SchStartTime" != "SchEndTime" THEN (
      "BillRateBoth" /(
        TIMESTAMPDIFF(
          MINUTE, "SchStartTime", "SchEndTime"
        )/ 60
      )
    )/ 60 ELSE 0 END
  ), 
  "ConBilledRateMinute" = (
    CASE WHEN "ConBilled" = ' yes ' 
    AND "ConRateType" = ' Hourly ' 
    AND "ConBillRateBoth" > 0 THEN "ConBillRateBoth" / 60 WHEN "ConBilled" = ' yes ' 
    AND "ConRateType" = ' Daily ' 
    AND "ConBillRateBoth" > 0 
    AND "ConBilledHours" > 0 THEN (
      "ConBillRateBoth" / "ConBilledHours"
    )/ 60 WHEN "ConBilled" = ' yes ' 
    AND "ConRateType" = ' Visit ' 
    AND "ConBillRateBoth" > 0 
    AND "ConBilledHours" > 0 THEN (
      "ConBillRateBoth" / "ConBilledHours"
    )/ 60 WHEN "ConBilled" != ' yes ' 
    AND "ConRateType" = ' Hourly ' 
    AND "ConBillRateBoth" > 0 THEN "ConBillRateBoth" / 60 WHEN "ConBilled" != ' yes ' 
    AND "ConRateType" = ' Daily ' 
    AND "ConBillRateBoth" > 0 
    AND "ConSchStartTime" IS NOT NULL 
    AND "ConSchEndTime" IS NOT NULL 
    AND "ConSchStartTime" != "ConSchEndTime" THEN (
      "ConBillRateBoth" /(
        TIMESTAMPDIFF(
          MINUTE, "ConSchStartTime", "ConSchEndTime"
        )/ 60
      )
    )/ 60 WHEN "ConBilled" != ' yes ' 
    AND "ConRateType" = ' Visit ' 
    AND "ConBillRateBoth" > 0 
    AND "ConSchStartTime" IS NOT NULL 
    AND "ConSchEndTime" IS NOT NULL 
    AND "ConSchStartTime" != "ConSchEndTime" THEN (
      "ConBillRateBoth" /(
        TIMESTAMPDIFF(
          MINUTE, "ConSchStartTime", "ConSchEndTime"
        )/ 60
      )
    )/ 60 ELSE 0 END
  ) 
WHERE 
  DATE("VisitDate") BETWEEN DATE(
    DATEADD(
      year, 
      -2, 
      GETDATE()
    )
  ) 
  AND DATE(
    DATEADD(
      day, 
      45, 
      GETDATE()
    )
  );
UPDATE 
  CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS AS CVM 
SET 
  CVM.SSN = ALLDATA.SSN, 
  CVM."ProviderID" = ALLDATA."ProviderID", 
  CVM."AppProviderID" = ALLDATA."AppProviderID", 
  CVM."ProviderName" = ALLDATA."ProviderName", 
  CVM."VisitDate" = ALLDATA."VisitDate", 
  CVM."SchStartTime" = ALLDATA."SchStartTime", 
  CVM."SchEndTime" = ALLDATA."SchEndTime", 
  CVM."VisitStartTime" = ALLDATA."VisitStartTime", 
  CVM."VisitEndTime" = ALLDATA."VisitEndTime", 
  CVM."EVVStartTime" = ALLDATA."EVVStartTime", 
  CVM."EVVEndTime" = ALLDATA."EVVEndTime", 
  CVM."CaregiverID" = ALLDATA."CaregiverID", 
  CVM."AppCaregiverID" = ALLDATA."AppCaregiverID", 
  CVM."AideCode" = ALLDATA."AideCode", 
  CVM."AideName" = ALLDATA."AideName", 
  CVM."AideSSN" = ALLDATA."AideSSN", 
  CVM."OfficeID" = ALLDATA."OfficeID", 
  CVM."AppOfficeID" = ALLDATA."AppOfficeID", 
  CVM."Office" = ALLDATA."Office", 
  CVM."PatientID" = ALLDATA."PatientID", 
  CVM."AppPatientID" = ALLDATA."AppPatientID", 
  CVM."PAdmissionID" = ALLDATA."PAdmissionID", 
  CVM."PName" = ALLDATA."PName", 
  CVM."PAddressID" = ALLDATA."PAddressID", 
  CVM."PAppAddressID" = ALLDATA."PAppAddressID", 
  CVM."PAddressL1" = ALLDATA."PAddressL1", 
  CVM."PAddressL2" = ALLDATA."PAddressL2", 
  CVM."PCity" = ALLDATA."PCity", 
  CVM."PAddressState" = ALLDATA."PAddressState", 
  CVM."PZipCode" = ALLDATA."PZipCode", 
  CVM."PCounty" = ALLDATA."PCounty", 
  CVM."PLongitude" = ALLDATA."PLongitude", 
  CVM."PLatitude" = ALLDATA."PLatitude", 
  CVM."PayerID" = ALLDATA."PayerID", 
  CVM."AppPayerID" = ALLDATA."AppPayerID", 
  CVM."BilledDate" = ALLDATA."BilledDate", 
  CVM."BilledHours" = ALLDATA."BilledHours", 
  CVM."Billed" = ALLDATA."Billed", 
  CVM."ServiceCodeID" = ALLDATA."ServiceCodeID", 
  CVM."AppServiceCodeID" = ALLDATA."AppServiceCodeID", 
  CVM."RateType" = ALLDATA."RateType", 
  CVM."ServiceCode" = ALLDATA."ServiceCode", 
  CVM."AideFName" = ALLDATA."AideFName", 
  CVM."AideLName" = ALLDATA."AideLName", 
  CVM."PFName" = ALLDATA."PFName", 
  CVM."PLName" = ALLDATA."PLName", 
  CVM."PMedicaidNumber" = ALLDATA."PMedicaidNumber", 
  CVM."PayerState" = ALLDATA."PayerState", 
  CVM."LastUpdatedBy" = ALLDATA."LastUpdatedBy", 
  CVM."LastUpdatedDate" = ALLDATA."LastUpdatedDate", 
  CVM."BilledRate" = ALLDATA."BilledRate", 
  CVM."TotalBilledAmount" = ALLDATA."TotalBilledAmount", 
  CVM."IsMissed" = ALLDATA."IsMissed", 
  CVM."MissedVisitReason" = ALLDATA."MissedVisitReason", 
  CVM."EVVType" = ALLDATA."EVVType", 
  CVM."PStatus" = ALLDATA."PStatus", 
  CVM."AideStatus" = ALLDATA."AideStatus", 
  CVM."P_PatientID" = ALLDATA."P_PatientID", 
  CVM."P_AppPatientID" = ALLDATA."P_AppPatientID", 
  CVM."PA_PatientID" = ALLDATA."PA_PatientID", 
  CVM."PA_AppPatientID" = ALLDATA."PA_AppPatientID", 
  CVM."P_PAdmissionID" = ALLDATA."P_PAdmissionID", 
  CVM."P_PName" = ALLDATA."P_PName", 
  CVM."P_PAddressID" = ALLDATA."P_PAddressID", 
  CVM."P_PAppAddressID" = ALLDATA."P_PAppAddressID", 
  CVM."P_PAddressL1" = ALLDATA."P_PAddressL1", 
  CVM."P_PAddressL2" = ALLDATA."P_PAddressL2", 
  CVM."P_PCity" = ALLDATA."P_PCity", 
  CVM."P_PAddressState" = ALLDATA."P_PAddressState", 
  CVM."P_PZipCode" = ALLDATA."P_PZipCode", 
  CVM."P_PCounty" = ALLDATA."P_PCounty", 
  CVM."P_PFName" = ALLDATA."P_PFName", 
  CVM."P_PLName" = ALLDATA."P_PLName", 
  CVM."P_PMedicaidNumber" = ALLDATA."P_PMedicaidNumber", 
  CVM."PA_PAdmissionID" = ALLDATA."PA_PAdmissionID", 
  CVM."PA_PName" = ALLDATA."PA_PName", 
  CVM."PA_PAddressID" = ALLDATA."PA_PAddressID", 
  CVM."PA_PAppAddressID" = ALLDATA."PA_PAppAddressID", 
  CVM."PA_PAddressL1" = ALLDATA."PA_PAddressL1", 
  CVM."PA_PAddressL2" = ALLDATA."PA_PAddressL2", 
  CVM."PA_PCity" = ALLDATA."PA_PCity", 
  CVM."PA_PAddressState" = ALLDATA."PA_PAddressState", 
  CVM."PA_PZipCode" = ALLDATA."PA_PZipCode", 
  CVM."PA_PCounty" = ALLDATA."PA_PCounty", 
  CVM."PA_PFName" = ALLDATA."PA_PFName", 
  CVM."PA_PLName" = ALLDATA."PA_PLName", 
  CVM."PA_PMedicaidNumber" = ALLDATA."PA_PMedicaidNumber", 
  CVM."BillRateNonBilled" = ALLDATA."BillRateNonBilled", 
  CVM."BillRateBoth" = ALLDATA."BillRateBoth", 
  CVM."FederalTaxNumber" = ALLDATA."FederalTaxNumber" 
FROM 
  (
    SELECT 
      V1."SSN", 
      V1."ProviderID" AS "ProviderID", 
      V1."AppProviderID" AS "AppProviderID", 
      V1."ProviderName" AS "ProviderName", 
      V1."VisitID" AS "VisitID", 
      V1."AppVisitID" AS "AppVisitID", 
      V1."VisitDate" AS "VisitDate", 
      V1."SchStartTime" AS "SchStartTime", 
      V1."SchEndTime" AS "SchEndTime", 
      V1."VisitStartTime" AS "VisitStartTime", 
      V1."VisitEndTime" AS "VisitEndTime", 
      V1."EVVStartTime" AS "EVVStartTime", 
      V1."EVVEndTime" AS "EVVEndTime", 
      V1."CaregiverID" AS "CaregiverID", 
      V1."AppCaregiverID" AS "AppCaregiverID", 
      V1."AideCode" AS "AideCode", 
      V1."AideName" AS "AideName", 
      V1."AideSSN" AS "AideSSN", 
      V1."OfficeID" AS "OfficeID", 
      V1."AppOfficeID" AS "AppOfficeID", 
      V1."Office" AS "Office", 
      V1."PatientID" AS "PatientID", 
      V1."AppPatientID" AS "AppPatientID", 
      V1."PAdmissionID" AS "PAdmissionID", 
      V1."PName" AS "PName", 
      V1."PAddressID" AS "PAddressID", 
      V1."PAppAddressID" AS "PAppAddressID", 
      V1."PAddressL1" AS "PAddressL1", 
      V1."PAddressL2" AS "PAddressL2", 
      V1."PCity" AS "PCity", 
      V1."PAddressState" AS "PAddressState", 
      V1."PZipCode" AS "PZipCode", 
      V1."PCounty" AS "PCounty", 
      V1."Longitude" AS "PLongitude", 
      V1."Latitude" AS "PLatitude", 
      V1."PayerID" AS "PayerID", 
      V1."AppPayerID" AS "AppPayerID", 
      V1."Contract" AS "Contract", 
      V1."BilledDate" AS "BilledDate", 
      V1."BilledHours" AS "BilledHours", 
      V1."Billed" AS "Billed", 
      V1."ServiceCodeID" AS "ServiceCodeID", 
      V1."AppServiceCodeID" AS "AppServiceCodeID", 
      V1."RateType" AS "RateType", 
      V1."ServiceCode" AS "ServiceCode", 
      V1."AideFName" AS "AideFName", 
      V1."AideLName" AS "AideLName", 
      V1."PFName" AS "PFName", 
      V1."PLName" AS "PLName", 
      V1."PMedicaidNumber" AS "PMedicaidNumber", 
      V1."PayerState" AS "PayerState", 
      V1."AgencyContact" AS "AgencyContact", 
      V1."AgencyPhone" AS "AgencyPhone", 
      V1."LastUpdatedBy" AS "LastUpdatedBy", 
      V1."LastUpdatedDate" AS "LastUpdatedDate", 
      V1."BilledRate" AS "BilledRate", 
      V1."TotalBilledAmount" AS "TotalBilledAmount", 
      V1."IsMissed" AS "IsMissed", 
      V1."MissedVisitReason" AS "MissedVisitReason", 
      V1."EVVType" AS "EVVType", 
      V1."PStatus" AS "PStatus", 
      V1."AideStatus" AS "AideStatus", 
      V1."P_PatientID" AS "P_PatientID", 
      V1."P_AppPatientID" AS "P_AppPatientID", 
      V1."PA_PatientID" AS "PA_PatientID", 
      V1."PA_AppPatientID" AS "PA_AppPatientID", 
      V1."P_PAdmissionID" AS "P_PAdmissionID", 
      V1."P_PName" AS "P_PName", 
      V1."P_PAddressID" AS "P_PAddressID", 
      V1."P_PAppAddressID" AS "P_PAppAddressID", 
      V1."P_PAddressL1" AS "P_PAddressL1", 
      V1."P_PAddressL2" AS "P_PAddressL2", 
      V1."P_PCity" AS "P_PCity", 
      V1."P_PAddressState" AS "P_PAddressState", 
      V1."P_PZipCode" AS "P_PZipCode", 
      V1."P_PCounty" AS "P_PCounty", 
      V1."P_PFName" AS "P_PFName", 
      V1."P_PLName" AS "P_PLName", 
      V1."P_PMedicaidNumber" AS "P_PMedicaidNumber", 
      V1."PA_PAdmissionID" AS "PA_PAdmissionID", 
      V1."PA_PName" AS "PA_PName", 
      V1."PA_PAddressID" AS "PA_PAddressID", 
      V1."PA_PAppAddressID" AS "PA_PAppAddressID", 
      V1."PA_PAddressL1" AS "PA_PAddressL1", 
      V1."PA_PAddressL2" AS "PA_PAddressL2", 
      V1."PA_PCity" AS "PA_PCity", 
      V1."PA_PAddressState" AS "PA_PAddressState", 
      V1."PA_PZipCode" AS "PA_PZipCode", 
      V1."PA_PCounty" AS "PA_PCounty", 
      V1."PA_PFName" AS "PA_PFName", 
      V1."PA_PLName" AS "PA_PLName", 
      V1."PA_PMedicaidNumber" AS "PA_PMedicaidNumber", 
      V1."ContractType" AS "ContractType", 
      V1."P_PStatus" AS "P_PStatus", 
      V1."PA_PStatus" AS "PA_PStatus", 
      V1."BillRateNonBilled" AS "BillRateNonBilled", 
      V1."BillRateBoth" AS "BillRateBoth", 
      V1."FederalTaxNumber" AS "FederalTaxNumber" 
    FROM 
      (
        SELECT 
          DISTINCT CR1."Bill Rate Non-Billed" AS "BillRateNonBilled", 
          CASE WHEN CR1."Billed" = ' yes ' THEN CR1."Billed Rate" ELSE CR1."Bill Rate Non-Billed" END AS "BillRateBoth", 
          TRIM(CAR."SSN") as "SSN", 
          CAST(NULL AS STRING) "PStatus", 
          CAR."Status" AS "AideStatus", 
          CR1."Missed Visit Reason" AS "MissedVisitReason", 
          CR1."Is Missed" AS "IsMissed", 
          CR1."Call Out Device Type" AS "EVVType", 
          CR1."Billed Rate" AS "BilledRate", 
          CR1."Total Billed Amount" AS "TotalBilledAmount", 
          CR1."Provider Id" as "ProviderID", 
          CR1."Application Provider Id" as "AppProviderID", 
          DPR."Provider Name" AS "ProviderName", 
          CAST(NULL AS STRING) "AgencyContact", 
          DPR."Phone Number 1" AS "AgencyPhone", 
          DPR."Federal Tax Number" AS "FederalTaxNumber", 
          CR1."Visit Id" as "VisitID", 
          CR1."Application Visit Id" as "AppVisitID", 
          DATE(CR1."Visit Date") AS "VisitDate", 
          CAST(
            CR1."Scheduled Start Time" AS timestamp
          ) AS "SchStartTime", 
          CAST(
            CR1."Scheduled End Time" AS timestamp
          ) AS "SchEndTime", 
          CAST(
            CR1."Visit Start Time" AS timestamp
          ) AS "VisitStartTime", 
          CAST(
            CR1."Visit End Time" AS timestamp
          ) AS "VisitEndTime", 
          CAST(CR1."Call In Time" AS timestamp) AS "EVVStartTime", 
          CAST(CR1."Call Out Time" AS timestamp) AS "EVVEndTime", 
          CR1."Caregiver Id" as "CaregiverID", 
          CR1."Application Caregiver Id" as "AppCaregiverID", 
          CAR."Caregiver Code" as "AideCode", 
          CAR."Caregiver Fullname" as "AideName", 
          CAR."Caregiver Firstname" as "AideFName", 
          CAR."Caregiver Lastname" as "AideLName", 
          TRIM(CAR."SSN") as "AideSSN", 
          CR1."Office Id" as "OfficeID", 
          CR1."Application Office Id" as "AppOfficeID", 
          DOF."Office Name" as "Office", 
          CR1."Payer Patient Id" as "PA_PatientID", 
          CR1."Application Payer Patient Id" as "PA_AppPatientID", 
          CR1."Provider Patient Id" as "P_PatientID", 
          CR1."Application Provider Patient Id" as "P_AppPatientID", 
          CR1."Patient Id" as "PatientID", 
          CR1."Application Patient Id" as "AppPatientID", 
          CAST(NULL AS STRING) "PAdmissionID", 
          CAST(NULL AS STRING) "PName", 
          CAST(NULL AS STRING) "PFName", 
          CAST(NULL AS STRING) "PLName", 
          CAST(NULL AS STRING) "PMedicaidNumber", 
          CAST(NULL AS STRING) "PAddressID", 
          CAST(NULL AS STRING) "PAppAddressID", 
          CAST(NULL AS STRING) "PAddressL1", 
          CAST(NULL AS STRING) "PAddressL2", 
          CAST(NULL AS STRING) "PCity", 
          CAST(NULL AS STRING) "PAddressState", 
          CAST(NULL AS STRING) "PZipCode", 
          CAST(NULL AS STRING) "PCounty", 
          CASE WHEN CR1."Call Out GPS Coordinates" IS NOT NULL 
          AND CR1."Call Out GPS Coordinates" != ', 
          ' THEN REPLACE(
            SPLIT(
              CR1."Call Out GPS Coordinates", 
              ', '
            ) [1], 
            ' "', CAST(NULL AS NUMBER)) WHEN CR1." Call In GPS Coordinates " IS NOT NULL AND CR1." Call In GPS Coordinates " != ',' THEN REPLACE(SPLIT(CR1." Call In GPS Coordinates ", ',')[1], '" ', 
            CAST(NULL AS NUMBER)
          ) ELSE DPAD_P."Provider_Longitude" END AS "Longitude", 
          CASE WHEN CR1."Call Out GPS Coordinates" IS NOT NULL 
          AND CR1."Call Out GPS Coordinates" != ', 
          ' THEN REPLACE(
            SPLIT(
              CR1."Call Out GPS Coordinates", 
              ', '
            ) [0], 
            ' "', CAST(NULL AS NUMBER)) WHEN CR1." Call In GPS Coordinates " IS NOT NULL AND CR1." Call In GPS Coordinates " != ',' THEN REPLACE(SPLIT(CR1." Call In GPS Coordinates ", ',')[0], '" ', 
            CAST(NULL AS NUMBER)
          ) ELSE DPAD_P."Provider_Latitude" END AS "Latitude", 
          CR1."Payer Id" as "PayerID", 
          CR1."Application Payer Id" as "AppPayerID", 
          COALESCE(
            SPA."Payer Name", DCON."Contract Name"
          ) AS "Contract", 
          SPA."Payer State" AS "PayerState", 
          CAST(CR1."Invoice Date" AS timestamp) AS "BilledDate", 
          CR1."Billed Hours" AS "BilledHours", 
          CR1."Billed" AS "Billed", 
          DSC."Service Code Id" AS "ServiceCodeID", 
          DSC."Application Service Code Id" AS "AppServiceCodeID", 
          CR1."Bill Type" as "RateType", 
          DSC."Service Code" as "ServiceCode", 
          CAST(
            CR1."Visit Updated Timestamp" AS timestamp
          ) as "LastUpdatedDate", 
          DUSR."User Fullname" AS "LastUpdatedBy", 
          DPA_P."Admission Id" as "P_PAdmissionID", 
          DPA_P."Patient Name" as "P_PName", 
          DPA_P."Patient Firstname" as "P_PFName", 
          DPA_P."Patient Lastname" as "P_PLName", 
          DPA_P."Medicaid Number" as "P_PMedicaidNumber", 
          DPA_P."Status" AS "P_PStatus", 
          DPAD_P."Patient Address Id" as "P_PAddressID", 
          DPAD_P."Application Patient Address Id" as "P_PAppAddressID", 
          DPAD_P."Address Line 1" as "P_PAddressL1", 
          DPAD_P."Address Line 2" as "P_PAddressL2", 
          DPAD_P."City" as "P_PCity", 
          DPAD_P."Address State" as "P_PAddressState", 
          DPAD_P."Zip Code" as "P_PZipCode", 
          DPAD_P."County" as "P_PCounty", 
          DPA_PA."Admission Id" as "PA_PAdmissionID", 
          DPA_PA."Patient Name" as "PA_PName", 
          DPA_PA."Patient Firstname" as "PA_PFName", 
          DPA_PA."Patient Lastname" as "PA_PLName", 
          DPA_PA."Medicaid Number" as "PA_PMedicaidNumber", 
          DPA_PA."Status" AS "PA_PStatus", 
          DPAD_PA."Patient Address Id" as "PA_PAddressID", 
          DPAD_PA."Application Patient Address Id" as "PA_PAppAddressID", 
          DPAD_PA."Address Line 1" as "PA_PAddressL1", 
          DPAD_PA."Address Line 2" as "PA_PAddressL2", 
          DPAD_PA."City" as "PA_PCity", 
          DPAD_PA."Address State" as "PA_PAddressState", 
          DPAD_PA."Zip Code" as "PA_PZipCode", 
          DPAD_PA."County" as "PA_PCounty", 
          CASE WHEN (
            CR1."Application Payer Id" = ' 0 ' 
            AND CR1."Application Contract Id" != ' 0 '
          ) THEN ' Internal ' WHEN (
            CR1."Application Payer Id" != ' 0 ' 
            AND CR1."Application Contract Id" != ' 0 '
          ) THEN ' UPR ' WHEN (
            CR1."Application Payer Id" != ' 0 ' 
            AND CR1."Application Contract Id" = ' 0 '
          ) THEN ' Payer ' END AS "ContractType" 
        FROM 
          ANALYTICS.BI.FACTVISITCALLPERFORMANCE_CR AS CR1 
          INNER JOIN ANALYTICS.BI.DIMCAREGIVER AS CAR ON CAR."Caregiver Id" = CR1."Caregiver Id" 
          AND TRIM(CAR."SSN") IS NOT NULL 
          AND TRIM(CAR."SSN")!= '' 
          LEFT JOIN ANALYTICS.BI.DIMOFFICE AS DOF ON DOF."Office Id" = CR1."Office Id" 
          AND DOF."Is Active" = TRUE 
          LEFT JOIN ANALYTICS.BI.DIMPATIENT AS DPA_P ON DPA_P."Patient Id" = CR1."Provider Patient Id" 
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
                ORDER BY 
                  DDD."Application Created UTC Timestamp" DESC
              ) AS rn 
            FROM 
              ANALYTICS.BI.DIMPATIENTADDRESS AS DDD 
            WHERE 
              DDD."Primary Address" = TRUE 
              AND DDD."Address Type" LIKE ' % GPS % '
          ) AS DPAD_P ON DPAD_P."Patient Id" = DPA_P."Patient Id" 
          AND DPAD_P."RN" = 1 
          LEFT JOIN ANALYTICS.BI.DIMPATIENT AS DPA_PA ON DPA_PA."Patient Id" = CR1."Payer Patient Id" 
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
                ORDER BY 
                  DDD."Application Created UTC Timestamp" DESC
              ) AS rn 
            FROM 
              ANALYTICS.BI.DIMPATIENTADDRESS AS DDD 
            WHERE 
              DDD."Primary Address" = TRUE 
              AND DDD."Address Type" LIKE ' % GPS % '
          ) AS DPAD_PA ON DPAD_PA."Patient Id" = DPA_PA."Patient Id" 
          AND DPAD_PA."RN" = 1 
          LEFT JOIN ANALYTICS.BI.DIMPAYER AS SPA ON SPA."Payer Id" = CR1."Payer Id" 
          AND SPA."Is Active" = TRUE 
          AND SPA."Is Demo" = FALSE 
          LEFT JOIN ANALYTICS.BI.DIMCONTRACT AS DCON ON DCON."Contract Id" = CR1."Contract Id" 
          AND DCON."Is Active" = TRUE 
          INNER JOIN ANALYTICS.BI.DIMPROVIDER AS DPR ON DPR."Provider Id" = CR1."Provider Id" 
          AND DPR."Is Active" = TRUE 
          AND DPR."Is Demo" = FALSE 
          LEFT JOIN ANALYTICS.BI.DIMSERVICECODE AS DSC ON DSC."Service Code Id" = CR1."Service Code Id" 
          LEFT JOIN ANALYTICS.BI.DIMUSER AS DUSR ON DUSR."User Id" = CR1."Visit Updated User Id" 
          INNER JOIN CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS AS CVM ON CVM."VisitID" = CR1."Visit Id" 
          INNER JOIN CONFLICTREPORT."PUBLIC".CONFLICTS AS C ON C.CONFLICTID = CVM.CONFLICTID 
          AND C."StatusFlag" = ' R ' 
        WHERE 
          DATE(CR1."Visit Date") BETWEEN DATE(
            DATEADD(
              year, 
              -2, 
              GETDATE()
            )
          ) 
          AND DATE(
            DATEADD(
              day, 
              45, 
              GETDATE()
            )
          )
      ) AS V1
  ) AS ALLDATA 
WHERE 
  CVM."VisitID" = ALLDATA."VisitID";
UPDATE 
  CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS AS CVM 
SET 
  CVM.SSN = ALLDATA.SSN, 
  CVM."ProviderID" = ALLDATA."ProviderID", 
  CVM."AppProviderID" = ALLDATA."AppProviderID", 
  CVM."ProviderName" = ALLDATA."ProviderName", 
  CVM."VisitDate" = ALLDATA."VisitDate", 
  CVM."SchStartTime" = ALLDATA."SchStartTime", 
  CVM."SchEndTime" = ALLDATA."SchEndTime", 
  CVM."VisitStartTime" = ALLDATA."VisitStartTime", 
  CVM."VisitEndTime" = ALLDATA."VisitEndTime", 
  CVM."EVVStartTime" = ALLDATA."EVVStartTime", 
  CVM."EVVEndTime" = ALLDATA."EVVEndTime", 
  CVM."CaregiverID" = ALLDATA."CaregiverID", 
  CVM."AppCaregiverID" = ALLDATA."AppCaregiverID", 
  CVM."AideCode" = ALLDATA."AideCode", 
  CVM."AideName" = ALLDATA."AideName", 
  CVM."AideSSN" = ALLDATA."AideSSN", 
  CVM."OfficeID" = ALLDATA."OfficeID", 
  CVM."AppOfficeID" = ALLDATA."AppOfficeID", 
  CVM."Office" = ALLDATA."Office", 
  CVM."PatientID" = ALLDATA."PatientID", 
  CVM."AppPatientID" = ALLDATA."AppPatientID", 
  CVM."PAdmissionID" = ALLDATA."PAdmissionID", 
  CVM."PName" = ALLDATA."PName", 
  CVM."PAddressID" = ALLDATA."PAddressID", 
  CVM."PAppAddressID" = ALLDATA."PAppAddressID", 
  CVM."PAddressL1" = ALLDATA."PAddressL1", 
  CVM."PAddressL2" = ALLDATA."PAddressL2", 
  CVM."PCity" = ALLDATA."PCity", 
  CVM."PAddressState" = ALLDATA."PAddressState", 
  CVM."PZipCode" = ALLDATA."PZipCode", 
  CVM."PCounty" = ALLDATA."PCounty", 
  CVM."PLongitude" = ALLDATA."PLongitude", 
  CVM."PLatitude" = ALLDATA."PLatitude", 
  CVM."PayerID" = ALLDATA."PayerID", 
  CVM."AppPayerID" = ALLDATA."AppPayerID", 
  CVM."BilledDate" = ALLDATA."BilledDate", 
  CVM."BilledHours" = ALLDATA."BilledHours", 
  CVM."Billed" = ALLDATA."Billed", 
  CVM."ServiceCodeID" = ALLDATA."ServiceCodeID", 
  CVM."AppServiceCodeID" = ALLDATA."AppServiceCodeID", 
  CVM."RateType" = ALLDATA."RateType", 
  CVM."ServiceCode" = ALLDATA."ServiceCode", 
  CVM."AideFName" = ALLDATA."AideFName", 
  CVM."AideLName" = ALLDATA."AideLName", 
  CVM."PFName" = ALLDATA."PFName", 
  CVM."PLName" = ALLDATA."PLName", 
  CVM."PMedicaidNumber" = ALLDATA."PMedicaidNumber", 
  CVM."PayerState" = ALLDATA."PayerState", 
  CVM."LastUpdatedBy" = ALLDATA."LastUpdatedBy", 
  CVM."LastUpdatedDate" = ALLDATA."LastUpdatedDate", 
  CVM."BilledRate" = ALLDATA."BilledRate", 
  CVM."TotalBilledAmount" = ALLDATA."TotalBilledAmount", 
  CVM."IsMissed" = ALLDATA."IsMissed", 
  CVM."MissedVisitReason" = ALLDATA."MissedVisitReason", 
  CVM."EVVType" = ALLDATA."EVVType", 
  CVM."PStatus" = ALLDATA."PStatus", 
  CVM."AideStatus" = ALLDATA."AideStatus", 
  CVM."P_PatientID" = ALLDATA."P_PatientID", 
  CVM."P_AppPatientID" = ALLDATA."P_AppPatientID", 
  CVM."PA_PatientID" = ALLDATA."PA_PatientID", 
  CVM."PA_AppPatientID" = ALLDATA."PA_AppPatientID", 
  CVM."P_PAdmissionID" = ALLDATA."P_PAdmissionID", 
  CVM."P_PName" = ALLDATA."P_PName", 
  CVM."P_PAddressID" = ALLDATA."P_PAddressID", 
  CVM."P_PAppAddressID" = ALLDATA."P_PAppAddressID", 
  CVM."P_PAddressL1" = ALLDATA."P_PAddressL1", 
  CVM."P_PAddressL2" = ALLDATA."P_PAddressL2", 
  CVM."P_PCity" = ALLDATA."P_PCity", 
  CVM."P_PAddressState" = ALLDATA."P_PAddressState", 
  CVM."P_PZipCode" = ALLDATA."P_PZipCode", 
  CVM."P_PCounty" = ALLDATA."P_PCounty", 
  CVM."P_PFName" = ALLDATA."P_PFName", 
  CVM."P_PLName" = ALLDATA."P_PLName", 
  CVM."P_PMedicaidNumber" = ALLDATA."P_PMedicaidNumber", 
  CVM."PA_PAdmissionID" = ALLDATA."PA_PAdmissionID", 
  CVM."PA_PName" = ALLDATA."PA_PName", 
  CVM."PA_PAddressID" = ALLDATA."PA_PAddressID", 
  CVM."PA_PAppAddressID" = ALLDATA."PA_PAppAddressID", 
  CVM."PA_PAddressL1" = ALLDATA."PA_PAddressL1", 
  CVM."PA_PAddressL2" = ALLDATA."PA_PAddressL2", 
  CVM."PA_PCity" = ALLDATA."PA_PCity", 
  CVM."PA_PAddressState" = ALLDATA."PA_PAddressState", 
  CVM."PA_PZipCode" = ALLDATA."PA_PZipCode", 
  CVM."PA_PCounty" = ALLDATA."PA_PCounty", 
  CVM."PA_PFName" = ALLDATA."PA_PFName", 
  CVM."PA_PLName" = ALLDATA."PA_PLName", 
  CVM."PA_PMedicaidNumber" = ALLDATA."PA_PMedicaidNumber", 
  CVM."BillRateNonBilled" = ALLDATA."BillRateNonBilled", 
  CVM."BillRateBoth" = ALLDATA."BillRateBoth", 
  CVM."FederalTaxNumber" = ALLDATA."FederalTaxNumber" 
FROM 
  (
    SELECT 
      V1."SSN", 
      V1."ProviderID" AS "ProviderID", 
      V1."AppProviderID" AS "AppProviderID", 
      V1."ProviderName" AS "ProviderName", 
      V1."VisitID" AS "VisitID", 
      V1."AppVisitID" AS "AppVisitID", 
      V1."VisitDate" AS "VisitDate", 
      V1."SchStartTime" AS "SchStartTime", 
      V1."SchEndTime" AS "SchEndTime", 
      V1."VisitStartTime" AS "VisitStartTime", 
      V1."VisitEndTime" AS "VisitEndTime", 
      V1."EVVStartTime" AS "EVVStartTime", 
      V1."EVVEndTime" AS "EVVEndTime", 
      V1."CaregiverID" AS "CaregiverID", 
      V1."AppCaregiverID" AS "AppCaregiverID", 
      V1."AideCode" AS "AideCode", 
      V1."AideName" AS "AideName", 
      V1."AideSSN" AS "AideSSN", 
      V1."OfficeID" AS "OfficeID", 
      V1."AppOfficeID" AS "AppOfficeID", 
      V1."Office" AS "Office", 
      V1."PatientID" AS "PatientID", 
      V1."AppPatientID" AS "AppPatientID", 
      V1."PAdmissionID" AS "PAdmissionID", 
      V1."PName" AS "PName", 
      V1."PAddressID" AS "PAddressID", 
      V1."PAppAddressID" AS "PAppAddressID", 
      V1."PAddressL1" AS "PAddressL1", 
      V1."PAddressL2" AS "PAddressL2", 
      V1."PCity" AS "PCity", 
      V1."PAddressState" AS "PAddressState", 
      V1."PZipCode" AS "PZipCode", 
      V1."PCounty" AS "PCounty", 
      V1."Longitude" AS "PLongitude", 
      V1."Latitude" AS "PLatitude", 
      V1."PayerID" AS "PayerID", 
      V1."AppPayerID" AS "AppPayerID", 
      V1."Contract" AS "Contract", 
      V1."BilledDate" AS "BilledDate", 
      V1."BilledHours" AS "BilledHours", 
      V1."Billed" AS "Billed", 
      V1."ServiceCodeID" AS "ServiceCodeID", 
      V1."AppServiceCodeID" AS "AppServiceCodeID", 
      V1."RateType" AS "RateType", 
      V1."ServiceCode" AS "ServiceCode", 
      V1."AideFName" AS "AideFName", 
      V1."AideLName" AS "AideLName", 
      V1."PFName" AS "PFName", 
      V1."PLName" AS "PLName", 
      V1."PMedicaidNumber" AS "PMedicaidNumber", 
      V1."PayerState" AS "PayerState", 
      V1."AgencyContact" AS "AgencyContact", 
      V1."AgencyPhone" AS "AgencyPhone", 
      V1."LastUpdatedBy" AS "LastUpdatedBy", 
      V1."LastUpdatedDate" AS "LastUpdatedDate", 
      V1."BilledRate" AS "BilledRate", 
      V1."TotalBilledAmount" AS "TotalBilledAmount", 
      V1."IsMissed" AS "IsMissed", 
      V1."MissedVisitReason" AS "MissedVisitReason", 
      V1."EVVType" AS "EVVType", 
      V1."PStatus" AS "PStatus", 
      V1."AideStatus" AS "AideStatus", 
      V1."P_PatientID" AS "P_PatientID", 
      V1."P_AppPatientID" AS "P_AppPatientID", 
      V1."PA_PatientID" AS "PA_PatientID", 
      V1."PA_AppPatientID" AS "PA_AppPatientID", 
      V1."P_PAdmissionID" AS "P_PAdmissionID", 
      V1."P_PName" AS "P_PName", 
      V1."P_PAddressID" AS "P_PAddressID", 
      V1."P_PAppAddressID" AS "P_PAppAddressID", 
      V1."P_PAddressL1" AS "P_PAddressL1", 
      V1."P_PAddressL2" AS "P_PAddressL2", 
      V1."P_PCity" AS "P_PCity", 
      V1."P_PAddressState" AS "P_PAddressState", 
      V1."P_PZipCode" AS "P_PZipCode", 
      V1."P_PCounty" AS "P_PCounty", 
      V1."P_PFName" AS "P_PFName", 
      V1."P_PLName" AS "P_PLName", 
      V1."P_PMedicaidNumber" AS "P_PMedicaidNumber", 
      V1."PA_PAdmissionID" AS "PA_PAdmissionID", 
      V1."PA_PName" AS "PA_PName", 
      V1."PA_PAddressID" AS "PA_PAddressID", 
      V1."PA_PAppAddressID" AS "PA_PAppAddressID", 
      V1."PA_PAddressL1" AS "PA_PAddressL1", 
      V1."PA_PAddressL2" AS "PA_PAddressL2", 
      V1."PA_PCity" AS "PA_PCity", 
      V1."PA_PAddressState" AS "PA_PAddressState", 
      V1."PA_PZipCode" AS "PA_PZipCode", 
      V1."PA_PCounty" AS "PA_PCounty", 
      V1."PA_PFName" AS "PA_PFName", 
      V1."PA_PLName" AS "PA_PLName", 
      V1."PA_PMedicaidNumber" AS "PA_PMedicaidNumber", 
      V1."ContractType" AS "ContractType", 
      V1."P_PStatus" AS "P_PStatus", 
      V1."PA_PStatus" AS "PA_PStatus", 
      V1."BillRateNonBilled" AS "BillRateNonBilled", 
      V1."BillRateBoth" AS "BillRateBoth", 
      V1."FederalTaxNumber" AS "FederalTaxNumber" 
    FROM 
      (
        SELECT 
          DISTINCT CR1."Bill Rate Non-Billed" AS "BillRateNonBilled", 
          CASE WHEN CR1."Billed" = ' yes ' THEN CR1."Billed Rate" ELSE CR1."Bill Rate Non-Billed" END AS "BillRateBoth", 
          TRIM(CAR."SSN") as "SSN", 
          CAST(NULL AS STRING) "PStatus", 
          CAR."Status" AS "AideStatus", 
          CR1."Missed Visit Reason" AS "MissedVisitReason", 
          CR1."Is Missed" AS "IsMissed", 
          CR1."Call Out Device Type" AS "EVVType", 
          CR1."Billed Rate" AS "BilledRate", 
          CR1."Total Billed Amount" AS "TotalBilledAmount", 
          CR1."Provider Id" as "ProviderID", 
          CR1."Application Provider Id" as "AppProviderID", 
          DPR."Provider Name" AS "ProviderName", 
          CAST(NULL AS STRING) "AgencyContact", 
          DPR."Phone Number 1" AS "AgencyPhone", 
          DPR."Federal Tax Number" AS "FederalTaxNumber", 
          CR1."Visit Id" as "VisitID", 
          CR1."Application Visit Id" as "AppVisitID", 
          DATE(CR1."Visit Date") AS "VisitDate", 
          CAST(
            CR1."Scheduled Start Time" AS timestamp
          ) AS "SchStartTime", 
          CAST(
            CR1."Scheduled End Time" AS timestamp
          ) AS "SchEndTime", 
          CAST(
            CR1."Visit Start Time" AS timestamp
          ) AS "VisitStartTime", 
          CAST(
            CR1."Visit End Time" AS timestamp
          ) AS "VisitEndTime", 
          CAST(CR1."Call In Time" AS timestamp) AS "EVVStartTime", 
          CAST(CR1."Call Out Time" AS timestamp) AS "EVVEndTime", 
          CR1."Caregiver Id" as "CaregiverID", 
          CR1."Application Caregiver Id" as "AppCaregiverID", 
          CAR."Caregiver Code" as "AideCode", 
          CAR."Caregiver Fullname" as "AideName", 
          CAR."Caregiver Firstname" as "AideFName", 
          CAR."Caregiver Lastname" as "AideLName", 
          TRIM(CAR."SSN") as "AideSSN", 
          CR1."Office Id" as "OfficeID", 
          CR1."Application Office Id" as "AppOfficeID", 
          DOF."Office Name" as "Office", 
          CR1."Payer Patient Id" as "PA_PatientID", 
          CR1."Application Payer Patient Id" as "PA_AppPatientID", 
          CR1."Provider Patient Id" as "P_PatientID", 
          CR1."Application Provider Patient Id" as "P_AppPatientID", 
          CR1."Patient Id" as "PatientID", 
          CR1."Application Patient Id" as "AppPatientID", 
          CAST(NULL AS STRING) "PAdmissionID", 
          CAST(NULL AS STRING) "PName", 
          CAST(NULL AS STRING) "PFName", 
          CAST(NULL AS STRING) "PLName", 
          CAST(NULL AS STRING) "PMedicaidNumber", 
          CAST(NULL AS STRING) "PAddressID", 
          CAST(NULL AS STRING) "PAppAddressID", 
          CAST(NULL AS STRING) "PAddressL1", 
          CAST(NULL AS STRING) "PAddressL2", 
          CAST(NULL AS STRING) "PCity", 
          CAST(NULL AS STRING) "PAddressState", 
          CAST(NULL AS STRING) "PZipCode", 
          CAST(NULL AS STRING) "PCounty", 
          CASE WHEN CR1."Call Out GPS Coordinates" IS NOT NULL 
          AND CR1."Call Out GPS Coordinates" != ', 
          ' THEN REPLACE(
            SPLIT(
              CR1."Call Out GPS Coordinates", 
              ', '
            ) [1], 
            ' "', CAST(NULL AS NUMBER)) WHEN CR1." Call In GPS Coordinates " IS NOT NULL AND CR1." Call In GPS Coordinates " != ',' THEN REPLACE(SPLIT(CR1." Call In GPS Coordinates ", ',')[1], '" ', 
            CAST(NULL AS NUMBER)
          ) ELSE DPAD_P."Provider_Longitude" END AS "Longitude", 
          CASE WHEN CR1."Call Out GPS Coordinates" IS NOT NULL 
          AND CR1."Call Out GPS Coordinates" != ', 
          ' THEN REPLACE(
            SPLIT(
              CR1."Call Out GPS Coordinates", 
              ', '
            ) [0], 
            ' "', CAST(NULL AS NUMBER)) WHEN CR1." Call In GPS Coordinates " IS NOT NULL AND CR1." Call In GPS Coordinates " != ',' THEN REPLACE(SPLIT(CR1." Call In GPS Coordinates ", ',')[0], '" ', 
            CAST(NULL AS NUMBER)
          ) ELSE DPAD_P."Provider_Latitude" END AS "Latitude", 
          CR1."Payer Id" as "PayerID", 
          CR1."Application Payer Id" as "AppPayerID", 
          COALESCE(
            SPA."Payer Name", DCON."Contract Name"
          ) AS "Contract", 
          SPA."Payer State" AS "PayerState", 
          CAST(CR1."Invoice Date" AS timestamp) AS "BilledDate", 
          CR1."Billed Hours" AS "BilledHours", 
          CR1."Billed" AS "Billed", 
          DSC."Service Code Id" AS "ServiceCodeID", 
          DSC."Application Service Code Id" AS "AppServiceCodeID", 
          CR1."Bill Type" as "RateType", 
          DSC."Service Code" as "ServiceCode", 
          CAST(
            CR1."Visit Updated Timestamp" AS timestamp
          ) as "LastUpdatedDate", 
          DUSR."User Fullname" AS "LastUpdatedBy", 
          DPA_P."Admission Id" as "P_PAdmissionID", 
          DPA_P."Patient Name" as "P_PName", 
          DPA_P."Patient Firstname" as "P_PFName", 
          DPA_P."Patient Lastname" as "P_PLName", 
          DPA_P."Medicaid Number" as "P_PMedicaidNumber", 
          DPA_P."Status" AS "P_PStatus", 
          DPAD_P."Patient Address Id" as "P_PAddressID", 
          DPAD_P."Application Patient Address Id" as "P_PAppAddressID", 
          DPAD_P."Address Line 1" as "P_PAddressL1", 
          DPAD_P."Address Line 2" as "P_PAddressL2", 
          DPAD_P."City" as "P_PCity", 
          DPAD_P."Address State" as "P_PAddressState", 
          DPAD_P."Zip Code" as "P_PZipCode", 
          DPAD_P."County" as "P_PCounty", 
          DPA_PA."Admission Id" as "PA_PAdmissionID", 
          DPA_PA."Patient Name" as "PA_PName", 
          DPA_PA."Patient Firstname" as "PA_PFName", 
          DPA_PA."Patient Lastname" as "PA_PLName", 
          DPA_PA."Medicaid Number" as "PA_PMedicaidNumber", 
          DPA_PA."Status" AS "PA_PStatus", 
          DPAD_PA."Patient Address Id" as "PA_PAddressID", 
          DPAD_PA."Application Patient Address Id" as "PA_PAppAddressID", 
          DPAD_PA."Address Line 1" as "PA_PAddressL1", 
          DPAD_PA."Address Line 2" as "PA_PAddressL2", 
          DPAD_PA."City" as "PA_PCity", 
          DPAD_PA."Address State" as "PA_PAddressState", 
          DPAD_PA."Zip Code" as "PA_PZipCode", 
          DPAD_PA."County" as "PA_PCounty", 
          CASE WHEN (
            CR1."Application Payer Id" = ' 0 ' 
            AND CR1."Application Contract Id" != ' 0 '
          ) THEN ' Internal ' WHEN (
            CR1."Application Payer Id" != ' 0 ' 
            AND CR1."Application Contract Id" != ' 0 '
          ) THEN ' UPR ' WHEN (
            CR1."Application Payer Id" != ' 0 ' 
            AND CR1."Application Contract Id" = ' 0 '
          ) THEN ' Payer ' END AS "ContractType" 
        FROM 
          ANALYTICS.BI.FACTVISITCALLPERFORMANCE_DELETED_CR AS CR1 
          INNER JOIN ANALYTICS.BI.DIMCAREGIVER AS CAR ON CAR."Caregiver Id" = CR1."Caregiver Id" 
          AND TRIM(CAR."SSN") IS NOT NULL 
          AND TRIM(CAR."SSN")!= '' 
          LEFT JOIN ANALYTICS.BI.DIMOFFICE AS DOF ON DOF."Office Id" = CR1."Office Id" 
          AND DOF."Is Active" = TRUE 
          LEFT JOIN ANALYTICS.BI.DIMPATIENT AS DPA_P ON DPA_P."Patient Id" = CR1."Provider Patient Id" 
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
                ORDER BY 
                  DDD."Application Created UTC Timestamp" DESC
              ) AS rn 
            FROM 
              ANALYTICS.BI.DIMPATIENTADDRESS AS DDD 
            WHERE 
              DDD."Primary Address" = TRUE 
              AND DDD."Address Type" LIKE ' % GPS % '
          ) AS DPAD_P ON DPAD_P."Patient Id" = DPA_P."Patient Id" 
          AND DPAD_P."RN" = 1 
          LEFT JOIN ANALYTICS.BI.DIMPATIENT AS DPA_PA ON DPA_PA."Patient Id" = CR1."Payer Patient Id" 
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
                ORDER BY 
                  DDD."Application Created UTC Timestamp" DESC
              ) AS rn 
            FROM 
              ANALYTICS.BI.DIMPATIENTADDRESS AS DDD 
            WHERE 
              DDD."Primary Address" = TRUE 
              AND DDD."Address Type" LIKE ' % GPS % '
          ) AS DPAD_PA ON DPAD_PA."Patient Id" = DPA_PA."Patient Id" 
          AND DPAD_PA."RN" = 1 
          LEFT JOIN ANALYTICS.BI.DIMPAYER AS SPA ON SPA."Payer Id" = CR1."Payer Id" 
          AND SPA."Is Active" = TRUE 
          AND SPA."Is Demo" = FALSE 
          LEFT JOIN ANALYTICS.BI.DIMCONTRACT AS DCON ON DCON."Contract Id" = CR1."Contract Id" 
          AND DCON."Is Active" = TRUE 
          INNER JOIN ANALYTICS.BI.DIMPROVIDER AS DPR ON DPR."Provider Id" = CR1."Provider Id" 
          AND DPR."Is Active" = TRUE 
          AND DPR."Is Demo" = FALSE 
          LEFT JOIN ANALYTICS.BI.DIMSERVICECODE AS DSC ON DSC."Service Code Id" = CR1."Service Code Id" 
          LEFT JOIN ANALYTICS.BI.DIMUSER AS DUSR ON DUSR."User Id" = CR1."Visit Updated User Id" 
          INNER JOIN CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS AS CVM ON CVM."VisitID" = CR1."Visit Id" 
          INNER JOIN CONFLICTREPORT."PUBLIC".CONFLICTS AS C ON C.CONFLICTID = CVM.CONFLICTID 
          AND C."StatusFlag" = ' D ' 
        WHERE 
          DATE(CR1."Visit Date") BETWEEN DATE(
            DATEADD(
              year, 
              -2, 
              GETDATE()
            )
          ) 
          AND DATE(
            DATEADD(
              day, 
              45, 
              GETDATE()
            )
          )
      ) AS V1
  ) AS ALLDATA 
WHERE 
  CVM."VisitID" = ALLDATA."VisitID";
UPDATE 
  CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS AS CVM 
SET 
  CVM.SSN = ALLDATA.SSN, 
  CVM."ConProviderID" = ALLDATA."ProviderID", 
  CVM."ConAppProviderID" = ALLDATA."AppProviderID", 
  CVM."ConProviderName" = ALLDATA."ProviderName", 
  CVM."VisitDate" = ALLDATA."VisitDate", 
  CVM."ConSchStartTime" = ALLDATA."SchStartTime", 
  CVM."ConSchEndTime" = ALLDATA."SchEndTime", 
  CVM."ConVisitStartTime" = ALLDATA."VisitStartTime", 
  CVM."ConVisitEndTime" = ALLDATA."VisitEndTime", 
  CVM."ConEVVStartTime" = ALLDATA."EVVStartTime", 
  CVM."ConEVVEndTime" = ALLDATA."EVVEndTime", 
  CVM."ConCaregiverID" = ALLDATA."CaregiverID", 
  CVM."ConAppCaregiverID" = ALLDATA."AppCaregiverID", 
  CVM."ConAideCode" = ALLDATA."AideCode", 
  CVM."ConAideName" = ALLDATA."AideName", 
  CVM."ConAideSSN" = ALLDATA."AideSSN", 
  CVM."ConOfficeID" = ALLDATA."OfficeID", 
  CVM."ConAppOfficeID" = ALLDATA."AppOfficeID", 
  CVM."ConOffice" = ALLDATA."Office", 
  CVM."ConPatientID" = ALLDATA."PatientID", 
  CVM."ConAppPatientID" = ALLDATA."AppPatientID", 
  CVM."ConPAdmissionID" = ALLDATA."PAdmissionID", 
  CVM."ConPName" = ALLDATA."PName", 
  CVM."ConPAddressID" = ALLDATA."PAddressID", 
  CVM."ConPAppAddressID" = ALLDATA."PAppAddressID", 
  CVM."ConPAddressL1" = ALLDATA."PAddressL1", 
  CVM."ConPAddressL2" = ALLDATA."PAddressL2", 
  CVM."ConPCity" = ALLDATA."PCity", 
  CVM."ConPAddressState" = ALLDATA."PAddressState", 
  CVM."ConPZipCode" = ALLDATA."PZipCode", 
  CVM."ConPCounty" = ALLDATA."PCounty", 
  CVM."ConPLongitude" = ALLDATA."PLongitude", 
  CVM."ConPLatitude" = ALLDATA."PLatitude", 
  CVM."ConPayerID" = ALLDATA."PayerID", 
  CVM."ConAppPayerID" = ALLDATA."AppPayerID", 
  CVM."ConBilledDate" = ALLDATA."BilledDate", 
  CVM."ConBilledHours" = ALLDATA."BilledHours", 
  CVM."ConBilled" = ALLDATA."Billed", 
  CVM."ConServiceCodeID" = ALLDATA."ServiceCodeID", 
  CVM."ConAppServiceCodeID" = ALLDATA."AppServiceCodeID", 
  CVM."ConRateType" = ALLDATA."RateType", 
  CVM."ConServiceCode" = ALLDATA."ServiceCode", 
  CVM."ConAideFName" = ALLDATA."AideFName", 
  CVM."ConAideLName" = ALLDATA."AideLName", 
  CVM."ConPFName" = ALLDATA."PFName", 
  CVM."ConPLName" = ALLDATA."PLName", 
  CVM."ConPMedicaidNumber" = ALLDATA."PMedicaidNumber", 
  CVM."ConPayerState" = ALLDATA."PayerState", 
  CVM."ConLastUpdatedBy" = ALLDATA."LastUpdatedBy", 
  CVM."ConLastUpdatedDate" = ALLDATA."LastUpdatedDate", 
  CVM."ConBilledRate" = ALLDATA."BilledRate", 
  CVM."ConTotalBilledAmount" = ALLDATA."TotalBilledAmount", 
  CVM."ConIsMissed" = ALLDATA."IsMissed", 
  CVM."ConMissedVisitReason" = ALLDATA."MissedVisitReason", 
  CVM."ConEVVType" = ALLDATA."EVVType", 
  CVM."ConPStatus" = ALLDATA."PStatus", 
  CVM."ConAideStatus" = ALLDATA."AideStatus", 
  CVM."ConP_PatientID" = ALLDATA."P_PatientID", 
  CVM."ConP_AppPatientID" = ALLDATA."P_AppPatientID", 
  CVM."ConPA_PatientID" = ALLDATA."PA_PatientID", 
  CVM."ConPA_AppPatientID" = ALLDATA."PA_AppPatientID", 
  CVM."ConP_PAdmissionID" = ALLDATA."P_PAdmissionID", 
  CVM."ConP_PName" = ALLDATA."P_PName", 
  CVM."ConP_PAddressID" = ALLDATA."P_PAddressID", 
  CVM."ConP_PAppAddressID" = ALLDATA."P_PAppAddressID", 
  CVM."ConP_PAddressL1" = ALLDATA."P_PAddressL1", 
  CVM."ConP_PAddressL2" = ALLDATA."P_PAddressL2", 
  CVM."ConP_PCity" = ALLDATA."P_PCity", 
  CVM."ConP_PAddressState" = ALLDATA."P_PAddressState", 
  CVM."ConP_PZipCode" = ALLDATA."P_PZipCode", 
  CVM."ConP_PCounty" = ALLDATA."P_PCounty", 
  CVM."ConP_PFName" = ALLDATA."P_PFName", 
  CVM."ConP_PLName" = ALLDATA."P_PLName", 
  CVM."ConP_PMedicaidNumber" = ALLDATA."P_PMedicaidNumber", 
  CVM."ConPA_PAdmissionID" = ALLDATA."PA_PAdmissionID", 
  CVM."ConPA_PName" = ALLDATA."PA_PName", 
  CVM."ConPA_PAddressID" = ALLDATA."PA_PAddressID", 
  CVM."ConPA_PAppAddressID" = ALLDATA."PA_PAppAddressID", 
  CVM."ConPA_PAddressL1" = ALLDATA."PA_PAddressL1", 
  CVM."ConPA_PAddressL2" = ALLDATA."PA_PAddressL2", 
  CVM."ConPA_PCity" = ALLDATA."PA_PCity", 
  CVM."ConPA_PAddressState" = ALLDATA."PA_PAddressState", 
  CVM."ConPA_PZipCode" = ALLDATA."PA_PZipCode", 
  CVM."ConPA_PCounty" = ALLDATA."PA_PCounty", 
  CVM."ConPA_PFName" = ALLDATA."PA_PFName", 
  CVM."ConPA_PLName" = ALLDATA."PA_PLName", 
  CVM."ConPA_PMedicaidNumber" = ALLDATA."PA_PMedicaidNumber", 
  CVM."ConBillRateNonBilled" = ALLDATA."BillRateNonBilled", 
  CVM."ConBillRateBoth" = ALLDATA."BillRateBoth", 
  CVM."ConFederalTaxNumber" = ALLDATA."FederalTaxNumber" 
FROM 
  (
    SELECT 
      V1."SSN", 
      V1."ProviderID" AS "ProviderID", 
      V1."AppProviderID" AS "AppProviderID", 
      V1."ProviderName" AS "ProviderName", 
      V1."VisitID" AS "VisitID", 
      V1."AppVisitID" AS "AppVisitID", 
      V1."VisitDate" AS "VisitDate", 
      V1."SchStartTime" AS "SchStartTime", 
      V1."SchEndTime" AS "SchEndTime", 
      V1."VisitStartTime" AS "VisitStartTime", 
      V1."VisitEndTime" AS "VisitEndTime", 
      V1."EVVStartTime" AS "EVVStartTime", 
      V1."EVVEndTime" AS "EVVEndTime", 
      V1."CaregiverID" AS "CaregiverID", 
      V1."AppCaregiverID" AS "AppCaregiverID", 
      V1."AideCode" AS "AideCode", 
      V1."AideName" AS "AideName", 
      V1."AideSSN" AS "AideSSN", 
      V1."OfficeID" AS "OfficeID", 
      V1."AppOfficeID" AS "AppOfficeID", 
      V1."Office" AS "Office", 
      V1."PatientID" AS "PatientID", 
      V1."AppPatientID" AS "AppPatientID", 
      V1."PAdmissionID" AS "PAdmissionID", 
      V1."PName" AS "PName", 
      V1."PAddressID" AS "PAddressID", 
      V1."PAppAddressID" AS "PAppAddressID", 
      V1."PAddressL1" AS "PAddressL1", 
      V1."PAddressL2" AS "PAddressL2", 
      V1."PCity" AS "PCity", 
      V1."PAddressState" AS "PAddressState", 
      V1."PZipCode" AS "PZipCode", 
      V1."PCounty" AS "PCounty", 
      V1."Longitude" AS "PLongitude", 
      V1."Latitude" AS "PLatitude", 
      V1."PayerID" AS "PayerID", 
      V1."AppPayerID" AS "AppPayerID", 
      V1."Contract" AS "Contract", 
      V1."BilledDate" AS "BilledDate", 
      V1."BilledHours" AS "BilledHours", 
      V1."Billed" AS "Billed", 
      V1."ServiceCodeID" AS "ServiceCodeID", 
      V1."AppServiceCodeID" AS "AppServiceCodeID", 
      V1."RateType" AS "RateType", 
      V1."ServiceCode" AS "ServiceCode", 
      V1."AideFName" AS "AideFName", 
      V1."AideLName" AS "AideLName", 
      V1."PFName" AS "PFName", 
      V1."PLName" AS "PLName", 
      V1."PMedicaidNumber" AS "PMedicaidNumber", 
      V1."PayerState" AS "PayerState", 
      V1."AgencyContact" AS "AgencyContact", 
      V1."AgencyPhone" AS "AgencyPhone", 
      V1."LastUpdatedBy" AS "LastUpdatedBy", 
      V1."LastUpdatedDate" AS "LastUpdatedDate", 
      V1."BilledRate" AS "BilledRate", 
      V1."TotalBilledAmount" AS "TotalBilledAmount", 
      V1."IsMissed" AS "IsMissed", 
      V1."MissedVisitReason" AS "MissedVisitReason", 
      V1."EVVType" AS "EVVType", 
      V1."PStatus" AS "PStatus", 
      V1."AideStatus" AS "AideStatus", 
      V1."P_PatientID" AS "P_PatientID", 
      V1."P_AppPatientID" AS "P_AppPatientID", 
      V1."PA_PatientID" AS "PA_PatientID", 
      V1."PA_AppPatientID" AS "PA_AppPatientID", 
      V1."P_PAdmissionID" AS "P_PAdmissionID", 
      V1."P_PName" AS "P_PName", 
      V1."P_PAddressID" AS "P_PAddressID", 
      V1."P_PAppAddressID" AS "P_PAppAddressID", 
      V1."P_PAddressL1" AS "P_PAddressL1", 
      V1."P_PAddressL2" AS "P_PAddressL2", 
      V1."P_PCity" AS "P_PCity", 
      V1."P_PAddressState" AS "P_PAddressState", 
      V1."P_PZipCode" AS "P_PZipCode", 
      V1."P_PCounty" AS "P_PCounty", 
      V1."P_PFName" AS "P_PFName", 
      V1."P_PLName" AS "P_PLName", 
      V1."P_PMedicaidNumber" AS "P_PMedicaidNumber", 
      V1."PA_PAdmissionID" AS "PA_PAdmissionID", 
      V1."PA_PName" AS "PA_PName", 
      V1."PA_PAddressID" AS "PA_PAddressID", 
      V1."PA_PAppAddressID" AS "PA_PAppAddressID", 
      V1."PA_PAddressL1" AS "PA_PAddressL1", 
      V1."PA_PAddressL2" AS "PA_PAddressL2", 
      V1."PA_PCity" AS "PA_PCity", 
      V1."PA_PAddressState" AS "PA_PAddressState", 
      V1."PA_PZipCode" AS "PA_PZipCode", 
      V1."PA_PCounty" AS "PA_PCounty", 
      V1."PA_PFName" AS "PA_PFName", 
      V1."PA_PLName" AS "PA_PLName", 
      V1."PA_PMedicaidNumber" AS "PA_PMedicaidNumber", 
      V1."ContractType" AS "ContractType", 
      V1."P_PStatus" AS "P_PStatus", 
      V1."PA_PStatus" AS "PA_PStatus", 
      V1."BillRateNonBilled" AS "BillRateNonBilled", 
      V1."BillRateBoth" AS "BillRateBoth", 
      V1."FederalTaxNumber" AS "FederalTaxNumber" 
    FROM 
      (
        SELECT 
          DISTINCT CR1."Bill Rate Non-Billed" AS "BillRateNonBilled", 
          CASE WHEN CR1."Billed" = ' yes ' THEN CR1."Billed Rate" ELSE CR1."Bill Rate Non-Billed" END AS "BillRateBoth", 
          TRIM(CAR."SSN") as "SSN", 
          CAST(NULL AS STRING) "PStatus", 
          CAR."Status" AS "AideStatus", 
          CR1."Missed Visit Reason" AS "MissedVisitReason", 
          CR1."Is Missed" AS "IsMissed", 
          CR1."Call Out Device Type" AS "EVVType", 
          CR1."Billed Rate" AS "BilledRate", 
          CR1."Total Billed Amount" AS "TotalBilledAmount", 
          CR1."Provider Id" as "ProviderID", 
          CR1."Application Provider Id" as "AppProviderID", 
          DPR."Provider Name" AS "ProviderName", 
          CAST(NULL AS STRING) "AgencyContact", 
          DPR."Phone Number 1" AS "AgencyPhone", 
          DPR."Federal Tax Number" AS "FederalTaxNumber", 
          CR1."Visit Id" as "VisitID", 
          CR1."Application Visit Id" as "AppVisitID", 
          DATE(CR1."Visit Date") AS "VisitDate", 
          CAST(
            CR1."Scheduled Start Time" AS timestamp
          ) AS "SchStartTime", 
          CAST(
            CR1."Scheduled End Time" AS timestamp
          ) AS "SchEndTime", 
          CAST(
            CR1."Visit Start Time" AS timestamp
          ) AS "VisitStartTime", 
          CAST(
            CR1."Visit End Time" AS timestamp
          ) AS "VisitEndTime", 
          CAST(CR1."Call In Time" AS timestamp) AS "EVVStartTime", 
          CAST(CR1."Call Out Time" AS timestamp) AS "EVVEndTime", 
          CR1."Caregiver Id" as "CaregiverID", 
          CR1."Application Caregiver Id" as "AppCaregiverID", 
          CAR."Caregiver Code" as "AideCode", 
          CAR."Caregiver Fullname" as "AideName", 
          CAR."Caregiver Firstname" as "AideFName", 
          CAR."Caregiver Lastname" as "AideLName", 
          TRIM(CAR."SSN") as "AideSSN", 
          CR1."Office Id" as "OfficeID", 
          CR1."Application Office Id" as "AppOfficeID", 
          DOF."Office Name" as "Office", 
          CR1."Payer Patient Id" as "PA_PatientID", 
          CR1."Application Payer Patient Id" as "PA_AppPatientID", 
          CR1."Provider Patient Id" as "P_PatientID", 
          CR1."Application Provider Patient Id" as "P_AppPatientID", 
          CR1."Patient Id" as "PatientID", 
          CR1."Application Patient Id" as "AppPatientID", 
          CAST(NULL AS STRING) "PAdmissionID", 
          CAST(NULL AS STRING) "PName", 
          CAST(NULL AS STRING) "PFName", 
          CAST(NULL AS STRING) "PLName", 
          CAST(NULL AS STRING) "PMedicaidNumber", 
          CAST(NULL AS STRING) "PAddressID", 
          CAST(NULL AS STRING) "PAppAddressID", 
          CAST(NULL AS STRING) "PAddressL1", 
          CAST(NULL AS STRING) "PAddressL2", 
          CAST(NULL AS STRING) "PCity", 
          CAST(NULL AS STRING) "PAddressState", 
          CAST(NULL AS STRING) "PZipCode", 
          CAST(NULL AS STRING) "PCounty", 
          CASE WHEN CR1."Call Out GPS Coordinates" IS NOT NULL 
          AND CR1."Call Out GPS Coordinates" != ', 
          ' THEN REPLACE(
            SPLIT(
              CR1."Call Out GPS Coordinates", 
              ', '
            ) [1], 
            ' "', CAST(NULL AS NUMBER)) WHEN CR1." Call In GPS Coordinates " IS NOT NULL AND CR1." Call In GPS Coordinates " != ',' THEN REPLACE(SPLIT(CR1." Call In GPS Coordinates ", ',')[1], '" ', 
            CAST(NULL AS NUMBER)
          ) ELSE DPAD_P."Provider_Longitude" END AS "Longitude", 
          CASE WHEN CR1."Call Out GPS Coordinates" IS NOT NULL 
          AND CR1."Call Out GPS Coordinates" != ', 
          ' THEN REPLACE(
            SPLIT(
              CR1."Call Out GPS Coordinates", 
              ', '
            ) [0], 
            ' "', CAST(NULL AS NUMBER)) WHEN CR1." Call In GPS Coordinates " IS NOT NULL AND CR1." Call In GPS Coordinates " != ',' THEN REPLACE(SPLIT(CR1." Call In GPS Coordinates ", ',')[0], '" ', 
            CAST(NULL AS NUMBER)
          ) ELSE DPAD_P."Provider_Latitude" END AS "Latitude", 
          CR1."Payer Id" as "PayerID", 
          CR1."Application Payer Id" as "AppPayerID", 
          COALESCE(
            SPA."Payer Name", DCON."Contract Name"
          ) AS "Contract", 
          SPA."Payer State" AS "PayerState", 
          CAST(CR1."Invoice Date" AS timestamp) AS "BilledDate", 
          CR1."Billed Hours" AS "BilledHours", 
          CR1."Billed" AS "Billed", 
          DSC."Service Code Id" AS "ServiceCodeID", 
          DSC."Application Service Code Id" AS "AppServiceCodeID", 
          CR1."Bill Type" as "RateType", 
          DSC."Service Code" as "ServiceCode", 
          CAST(
            CR1."Visit Updated Timestamp" AS timestamp
          ) as "LastUpdatedDate", 
          DUSR."User Fullname" AS "LastUpdatedBy", 
          DPA_P."Admission Id" as "P_PAdmissionID", 
          DPA_P."Patient Name" as "P_PName", 
          DPA_P."Patient Firstname" as "P_PFName", 
          DPA_P."Patient Lastname" as "P_PLName", 
          DPA_P."Medicaid Number" as "P_PMedicaidNumber", 
          DPA_P."Status" AS "P_PStatus", 
          DPAD_P."Patient Address Id" as "P_PAddressID", 
          DPAD_P."Application Patient Address Id" as "P_PAppAddressID", 
          DPAD_P."Address Line 1" as "P_PAddressL1", 
          DPAD_P."Address Line 2" as "P_PAddressL2", 
          DPAD_P."City" as "P_PCity", 
          DPAD_P."Address State" as "P_PAddressState", 
          DPAD_P."Zip Code" as "P_PZipCode", 
          DPAD_P."County" as "P_PCounty", 
          DPA_PA."Admission Id" as "PA_PAdmissionID", 
          DPA_PA."Patient Name" as "PA_PName", 
          DPA_PA."Patient Firstname" as "PA_PFName", 
          DPA_PA."Patient Lastname" as "PA_PLName", 
          DPA_PA."Medicaid Number" as "PA_PMedicaidNumber", 
          DPA_PA."Status" AS "PA_PStatus", 
          DPAD_PA."Patient Address Id" as "PA_PAddressID", 
          DPAD_PA."Application Patient Address Id" as "PA_PAppAddressID", 
          DPAD_PA."Address Line 1" as "PA_PAddressL1", 
          DPAD_PA."Address Line 2" as "PA_PAddressL2", 
          DPAD_PA."City" as "PA_PCity", 
          DPAD_PA."Address State" as "PA_PAddressState", 
          DPAD_PA."Zip Code" as "PA_PZipCode", 
          DPAD_PA."County" as "PA_PCounty", 
          CASE WHEN (
            CR1."Application Payer Id" = ' 0 ' 
            AND CR1."Application Contract Id" != ' 0 '
          ) THEN ' Internal ' WHEN (
            CR1."Application Payer Id" != ' 0 ' 
            AND CR1."Application Contract Id" != ' 0 '
          ) THEN ' UPR ' WHEN (
            CR1."Application Payer Id" != ' 0 ' 
            AND CR1."Application Contract Id" = ' 0 '
          ) THEN ' Payer ' END AS "ContractType" 
        FROM 
          ANALYTICS.BI.FACTVISITCALLPERFORMANCE_CR AS CR1 
          INNER JOIN ANALYTICS.BI.DIMCAREGIVER AS CAR ON CAR."Caregiver Id" = CR1."Caregiver Id" 
          AND TRIM(CAR."SSN") IS NOT NULL 
          AND TRIM(CAR."SSN")!= '' 
          LEFT JOIN ANALYTICS.BI.DIMOFFICE AS DOF ON DOF."Office Id" = CR1."Office Id" 
          AND DOF."Is Active" = TRUE 
          LEFT JOIN ANALYTICS.BI.DIMPATIENT AS DPA_P ON DPA_P."Patient Id" = CR1."Provider Patient Id" 
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
                ORDER BY 
                  DDD."Application Created UTC Timestamp" DESC
              ) AS rn 
            FROM 
              ANALYTICS.BI.DIMPATIENTADDRESS AS DDD 
            WHERE 
              DDD."Primary Address" = TRUE 
              AND DDD."Address Type" LIKE ' % GPS % '
          ) AS DPAD_P ON DPAD_P."Patient Id" = DPA_P."Patient Id" 
          AND DPAD_P."RN" = 1 
          LEFT JOIN ANALYTICS.BI.DIMPATIENT AS DPA_PA ON DPA_PA."Patient Id" = CR1."Payer Patient Id" 
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
                ORDER BY 
                  DDD."Application Created UTC Timestamp" DESC
              ) AS rn 
            FROM 
              ANALYTICS.BI.DIMPATIENTADDRESS AS DDD 
            WHERE 
              DDD."Primary Address" = TRUE 
              AND DDD."Address Type" LIKE ' % GPS % '
          ) AS DPAD_PA ON DPAD_PA."Patient Id" = DPA_PA."Patient Id" 
          AND DPAD_PA."RN" = 1 
          LEFT JOIN ANALYTICS.BI.DIMPAYER AS SPA ON SPA."Payer Id" = CR1."Payer Id" 
          AND SPA."Is Active" = TRUE 
          AND SPA."Is Demo" = FALSE 
          LEFT JOIN ANALYTICS.BI.DIMCONTRACT AS DCON ON DCON."Contract Id" = CR1."Contract Id" 
          AND DCON."Is Active" = TRUE 
          INNER JOIN ANALYTICS.BI.DIMPROVIDER AS DPR ON DPR."Provider Id" = CR1."Provider Id" 
          AND DPR."Is Active" = TRUE 
          AND DPR."Is Demo" = FALSE 
          LEFT JOIN ANALYTICS.BI.DIMSERVICECODE AS DSC ON DSC."Service Code Id" = CR1."Service Code Id" 
          LEFT JOIN ANALYTICS.BI.DIMUSER AS DUSR ON DUSR."User Id" = CR1."Visit Updated User Id" 
          INNER JOIN CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS AS CVM ON CVM."ConVisitID" = CR1."Visit Id" 
          AND CVM."StatusFlag" = ' R ' 
        WHERE 
          DATE(CR1."Visit Date") BETWEEN DATE(
            DATEADD(
              year, 
              -2, 
              GETDATE()
            )
          ) 
          AND DATE(
            DATEADD(
              day, 
              45, 
              GETDATE()
            )
          )
      ) AS V1
  ) AS ALLDATA 
WHERE 
  CVM."ConVisitID" = ALLDATA."VisitID";
UPDATE 
  CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS AS CVM 
SET 
  CVM.SSN = ALLDATA.SSN, 
  CVM."ConProviderID" = ALLDATA."ProviderID", 
  CVM."ConAppProviderID" = ALLDATA."AppProviderID", 
  CVM."ConProviderName" = ALLDATA."ProviderName", 
  CVM."VisitDate" = ALLDATA."VisitDate", 
  CVM."ConSchStartTime" = ALLDATA."SchStartTime", 
  CVM."ConSchEndTime" = ALLDATA."SchEndTime", 
  CVM."ConVisitStartTime" = ALLDATA."VisitStartTime", 
  CVM."ConVisitEndTime" = ALLDATA."VisitEndTime", 
  CVM."ConEVVStartTime" = ALLDATA."EVVStartTime", 
  CVM."ConEVVEndTime" = ALLDATA."EVVEndTime", 
  CVM."ConCaregiverID" = ALLDATA."CaregiverID", 
  CVM."ConAppCaregiverID" = ALLDATA."AppCaregiverID", 
  CVM."ConAideCode" = ALLDATA."AideCode", 
  CVM."ConAideName" = ALLDATA."AideName", 
  CVM."ConAideSSN" = ALLDATA."AideSSN", 
  CVM."ConOfficeID" = ALLDATA."OfficeID", 
  CVM."ConAppOfficeID" = ALLDATA."AppOfficeID", 
  CVM."ConOffice" = ALLDATA."Office", 
  CVM."ConPatientID" = ALLDATA."PatientID", 
  CVM."ConAppPatientID" = ALLDATA."AppPatientID", 
  CVM."ConPAdmissionID" = ALLDATA."PAdmissionID", 
  CVM."ConPName" = ALLDATA."PName", 
  CVM."ConPAddressID" = ALLDATA."PAddressID", 
  CVM."ConPAppAddressID" = ALLDATA."PAppAddressID", 
  CVM."ConPAddressL1" = ALLDATA."PAddressL1", 
  CVM."ConPAddressL2" = ALLDATA."PAddressL2", 
  CVM."ConPCity" = ALLDATA."PCity", 
  CVM."ConPAddressState" = ALLDATA."PAddressState", 
  CVM."ConPZipCode" = ALLDATA."PZipCode", 
  CVM."ConPCounty" = ALLDATA."PCounty", 
  CVM."ConPLongitude" = ALLDATA."PLongitude", 
  CVM."ConPLatitude" = ALLDATA."PLatitude", 
  CVM."ConPayerID" = ALLDATA."PayerID", 
  CVM."ConAppPayerID" = ALLDATA."AppPayerID", 
  CVM."ConBilledDate" = ALLDATA."BilledDate", 
  CVM."ConBilledHours" = ALLDATA."BilledHours", 
  CVM."ConBilled" = ALLDATA."Billed", 
  CVM."ConServiceCodeID" = ALLDATA."ServiceCodeID", 
  CVM."ConAppServiceCodeID" = ALLDATA."AppServiceCodeID", 
  CVM."ConRateType" = ALLDATA."RateType", 
  CVM."ConServiceCode" = ALLDATA."ServiceCode", 
  CVM."ConAideFName" = ALLDATA."AideFName", 
  CVM."ConAideLName" = ALLDATA."AideLName", 
  CVM."ConPFName" = ALLDATA."PFName", 
  CVM."ConPLName" = ALLDATA."PLName", 
  CVM."ConPMedicaidNumber" = ALLDATA."PMedicaidNumber", 
  CVM."ConPayerState" = ALLDATA."PayerState", 
  CVM."ConLastUpdatedBy" = ALLDATA."LastUpdatedBy", 
  CVM."ConLastUpdatedDate" = ALLDATA."LastUpdatedDate", 
  CVM."ConBilledRate" = ALLDATA."BilledRate", 
  CVM."ConTotalBilledAmount" = ALLDATA."TotalBilledAmount", 
  CVM."ConIsMissed" = ALLDATA."IsMissed", 
  CVM."ConMissedVisitReason" = ALLDATA."MissedVisitReason", 
  CVM."ConEVVType" = ALLDATA."EVVType", 
  CVM."ConPStatus" = ALLDATA."PStatus", 
  CVM."ConAideStatus" = ALLDATA."AideStatus", 
  CVM."ConP_PatientID" = ALLDATA."P_PatientID", 
  CVM."ConP_AppPatientID" = ALLDATA."P_AppPatientID", 
  CVM."ConPA_PatientID" = ALLDATA."PA_PatientID", 
  CVM."ConPA_AppPatientID" = ALLDATA."PA_AppPatientID", 
  CVM."ConP_PAdmissionID" = ALLDATA."P_PAdmissionID", 
  CVM."ConP_PName" = ALLDATA."P_PName", 
  CVM."ConP_PAddressID" = ALLDATA."P_PAddressID", 
  CVM."ConP_PAppAddressID" = ALLDATA."P_PAppAddressID", 
  CVM."ConP_PAddressL1" = ALLDATA."P_PAddressL1", 
  CVM."ConP_PAddressL2" = ALLDATA."P_PAddressL2", 
  CVM."ConP_PCity" = ALLDATA."P_PCity", 
  CVM."ConP_PAddressState" = ALLDATA."P_PAddressState", 
  CVM."ConP_PZipCode" = ALLDATA."P_PZipCode", 
  CVM."ConP_PCounty" = ALLDATA."P_PCounty", 
  CVM."ConP_PFName" = ALLDATA."P_PFName", 
  CVM."ConP_PLName" = ALLDATA."P_PLName", 
  CVM."ConP_PMedicaidNumber" = ALLDATA."P_PMedicaidNumber", 
  CVM."ConPA_PAdmissionID" = ALLDATA."PA_PAdmissionID", 
  CVM."ConPA_PName" = ALLDATA."PA_PName", 
  CVM."ConPA_PAddressID" = ALLDATA."PA_PAddressID", 
  CVM."ConPA_PAppAddressID" = ALLDATA."PA_PAppAddressID", 
  CVM."ConPA_PAddressL1" = ALLDATA."PA_PAddressL1", 
  CVM."ConPA_PAddressL2" = ALLDATA."PA_PAddressL2", 
  CVM."ConPA_PCity" = ALLDATA."PA_PCity", 
  CVM."ConPA_PAddressState" = ALLDATA."PA_PAddressState", 
  CVM."ConPA_PZipCode" = ALLDATA."PA_PZipCode", 
  CVM."ConPA_PCounty" = ALLDATA."PA_PCounty", 
  CVM."ConPA_PFName" = ALLDATA."PA_PFName", 
  CVM."ConPA_PLName" = ALLDATA."PA_PLName", 
  CVM."ConPA_PMedicaidNumber" = ALLDATA."PA_PMedicaidNumber", 
  CVM."ConBillRateNonBilled" = ALLDATA."BillRateNonBilled", 
  CVM."ConBillRateBoth" = ALLDATA."BillRateBoth", 
  CVM."ConFederalTaxNumber" = ALLDATA."FederalTaxNumber" 
FROM 
  (
    SELECT 
      V1."SSN", 
      V1."ProviderID" AS "ProviderID", 
      V1."AppProviderID" AS "AppProviderID", 
      V1."ProviderName" AS "ProviderName", 
      V1."VisitID" AS "VisitID", 
      V1."AppVisitID" AS "AppVisitID", 
      V1."VisitDate" AS "VisitDate", 
      V1."SchStartTime" AS "SchStartTime", 
      V1."SchEndTime" AS "SchEndTime", 
      V1."VisitStartTime" AS "VisitStartTime", 
      V1."VisitEndTime" AS "VisitEndTime", 
      V1."EVVStartTime" AS "EVVStartTime", 
      V1."EVVEndTime" AS "EVVEndTime", 
      V1."CaregiverID" AS "CaregiverID", 
      V1."AppCaregiverID" AS "AppCaregiverID", 
      V1."AideCode" AS "AideCode", 
      V1."AideName" AS "AideName", 
      V1."AideSSN" AS "AideSSN", 
      V1."OfficeID" AS "OfficeID", 
      V1."AppOfficeID" AS "AppOfficeID", 
      V1."Office" AS "Office", 
      V1."PatientID" AS "PatientID", 
      V1."AppPatientID" AS "AppPatientID", 
      V1."PAdmissionID" AS "PAdmissionID", 
      V1."PName" AS "PName", 
      V1."PAddressID" AS "PAddressID", 
      V1."PAppAddressID" AS "PAppAddressID", 
      V1."PAddressL1" AS "PAddressL1", 
      V1."PAddressL2" AS "PAddressL2", 
      V1."PCity" AS "PCity", 
      V1."PAddressState" AS "PAddressState", 
      V1."PZipCode" AS "PZipCode", 
      V1."PCounty" AS "PCounty", 
      V1."Longitude" AS "PLongitude", 
      V1."Latitude" AS "PLatitude", 
      V1."PayerID" AS "PayerID", 
      V1."AppPayerID" AS "AppPayerID", 
      V1."Contract" AS "Contract", 
      V1."BilledDate" AS "BilledDate", 
      V1."BilledHours" AS "BilledHours", 
      V1."Billed" AS "Billed", 
      V1."ServiceCodeID" AS "ServiceCodeID", 
      V1."AppServiceCodeID" AS "AppServiceCodeID", 
      V1."RateType" AS "RateType", 
      V1."ServiceCode" AS "ServiceCode", 
      V1."AideFName" AS "AideFName", 
      V1."AideLName" AS "AideLName", 
      V1."PFName" AS "PFName", 
      V1."PLName" AS "PLName", 
      V1."PMedicaidNumber" AS "PMedicaidNumber", 
      V1."PayerState" AS "PayerState", 
      V1."AgencyContact" AS "AgencyContact", 
      V1."AgencyPhone" AS "AgencyPhone", 
      V1."LastUpdatedBy" AS "LastUpdatedBy", 
      V1."LastUpdatedDate" AS "LastUpdatedDate", 
      V1."BilledRate" AS "BilledRate", 
      V1."TotalBilledAmount" AS "TotalBilledAmount", 
      V1."IsMissed" AS "IsMissed", 
      V1."MissedVisitReason" AS "MissedVisitReason", 
      V1."EVVType" AS "EVVType", 
      V1."PStatus" AS "PStatus", 
      V1."AideStatus" AS "AideStatus", 
      V1."P_PatientID" AS "P_PatientID", 
      V1."P_AppPatientID" AS "P_AppPatientID", 
      V1."PA_PatientID" AS "PA_PatientID", 
      V1."PA_AppPatientID" AS "PA_AppPatientID", 
      V1."P_PAdmissionID" AS "P_PAdmissionID", 
      V1."P_PName" AS "P_PName", 
      V1."P_PAddressID" AS "P_PAddressID", 
      V1."P_PAppAddressID" AS "P_PAppAddressID", 
      V1."P_PAddressL1" AS "P_PAddressL1", 
      V1."P_PAddressL2" AS "P_PAddressL2", 
      V1."P_PCity" AS "P_PCity", 
      V1."P_PAddressState" AS "P_PAddressState", 
      V1."P_PZipCode" AS "P_PZipCode", 
      V1."P_PCounty" AS "P_PCounty", 
      V1."P_PFName" AS "P_PFName", 
      V1."P_PLName" AS "P_PLName", 
      V1."P_PMedicaidNumber" AS "P_PMedicaidNumber", 
      V1."PA_PAdmissionID" AS "PA_PAdmissionID", 
      V1."PA_PName" AS "PA_PName", 
      V1."PA_PAddressID" AS "PA_PAddressID", 
      V1."PA_PAppAddressID" AS "PA_PAppAddressID", 
      V1."PA_PAddressL1" AS "PA_PAddressL1", 
      V1."PA_PAddressL2" AS "PA_PAddressL2", 
      V1."PA_PCity" AS "PA_PCity", 
      V1."PA_PAddressState" AS "PA_PAddressState", 
      V1."PA_PZipCode" AS "PA_PZipCode", 
      V1."PA_PCounty" AS "PA_PCounty", 
      V1."PA_PFName" AS "PA_PFName", 
      V1."PA_PLName" AS "PA_PLName", 
      V1."PA_PMedicaidNumber" AS "PA_PMedicaidNumber", 
      V1."ContractType" AS "ContractType", 
      V1."P_PStatus" AS "P_PStatus", 
      V1."PA_PStatus" AS "PA_PStatus", 
      V1."BillRateNonBilled" AS "BillRateNonBilled", 
      V1."BillRateBoth" AS "BillRateBoth", 
      V1."FederalTaxNumber" AS "FederalTaxNumber" 
    FROM 
      (
        SELECT 
          DISTINCT CR1."Bill Rate Non-Billed" AS "BillRateNonBilled", 
          CASE WHEN CR1."Billed" = ' yes ' THEN CR1."Billed Rate" ELSE CR1."Bill Rate Non-Billed" END AS "BillRateBoth", 
          TRIM(CAR."SSN") as "SSN", 
          CAST(NULL AS STRING) "PStatus", 
          CAR."Status" AS "AideStatus", 
          CR1."Missed Visit Reason" AS "MissedVisitReason", 
          CR1."Is Missed" AS "IsMissed", 
          CR1."Call Out Device Type" AS "EVVType", 
          CR1."Billed Rate" AS "BilledRate", 
          CR1."Total Billed Amount" AS "TotalBilledAmount", 
          CR1."Provider Id" as "ProviderID", 
          CR1."Application Provider Id" as "AppProviderID", 
          DPR."Provider Name" AS "ProviderName", 
          CAST(NULL AS STRING) "AgencyContact", 
          DPR."Phone Number 1" AS "AgencyPhone", 
          DPR."Federal Tax Number" AS "FederalTaxNumber", 
          CR1."Visit Id" as "VisitID", 
          CR1."Application Visit Id" as "AppVisitID", 
          DATE(CR1."Visit Date") AS "VisitDate", 
          CAST(
            CR1."Scheduled Start Time" AS timestamp
          ) AS "SchStartTime", 
          CAST(
            CR1."Scheduled End Time" AS timestamp
          ) AS "SchEndTime", 
          CAST(
            CR1."Visit Start Time" AS timestamp
          ) AS "VisitStartTime", 
          CAST(
            CR1."Visit End Time" AS timestamp
          ) AS "VisitEndTime", 
          CAST(CR1."Call In Time" AS timestamp) AS "EVVStartTime", 
          CAST(CR1."Call Out Time" AS timestamp) AS "EVVEndTime", 
          CR1."Caregiver Id" as "CaregiverID", 
          CR1."Application Caregiver Id" as "AppCaregiverID", 
          CAR."Caregiver Code" as "AideCode", 
          CAR."Caregiver Fullname" as "AideName", 
          CAR."Caregiver Firstname" as "AideFName", 
          CAR."Caregiver Lastname" as "AideLName", 
          TRIM(CAR."SSN") as "AideSSN", 
          CR1."Office Id" as "OfficeID", 
          CR1."Application Office Id" as "AppOfficeID", 
          DOF."Office Name" as "Office", 
          CR1."Payer Patient Id" as "PA_PatientID", 
          CR1."Application Payer Patient Id" as "PA_AppPatientID", 
          CR1."Provider Patient Id" as "P_PatientID", 
          CR1."Application Provider Patient Id" as "P_AppPatientID", 
          CR1."Patient Id" as "PatientID", 
          CR1."Application Patient Id" as "AppPatientID", 
          CAST(NULL AS STRING) "PAdmissionID", 
          CAST(NULL AS STRING) "PName", 
          CAST(NULL AS STRING) "PFName", 
          CAST(NULL AS STRING) "PLName", 
          CAST(NULL AS STRING) "PMedicaidNumber", 
          CAST(NULL AS STRING) "PAddressID", 
          CAST(NULL AS STRING) "PAppAddressID", 
          CAST(NULL AS STRING) "PAddressL1", 
          CAST(NULL AS STRING) "PAddressL2", 
          CAST(NULL AS STRING) "PCity", 
          CAST(NULL AS STRING) "PAddressState", 
          CAST(NULL AS STRING) "PZipCode", 
          CAST(NULL AS STRING) "PCounty", 
          CASE WHEN CR1."Call Out GPS Coordinates" IS NOT NULL 
          AND CR1."Call Out GPS Coordinates" != ', 
          ' THEN REPLACE(
            SPLIT(
              CR1."Call Out GPS Coordinates", 
              ', '
            ) [1], 
            ' "', CAST(NULL AS NUMBER)) WHEN CR1." Call In GPS Coordinates " IS NOT NULL AND CR1." Call In GPS Coordinates " != ',' THEN REPLACE(SPLIT(CR1." Call In GPS Coordinates ", ',')[1], '" ', 
            CAST(NULL AS NUMBER)
          ) ELSE DPAD_P."Provider_Longitude" END AS "Longitude", 
          CASE WHEN CR1."Call Out GPS Coordinates" IS NOT NULL 
          AND CR1."Call Out GPS Coordinates" != ', 
          ' THEN REPLACE(
            SPLIT(
              CR1."Call Out GPS Coordinates", 
              ', '
            ) [0], 
            ' "', CAST(NULL AS NUMBER)) WHEN CR1." Call In GPS Coordinates " IS NOT NULL AND CR1." Call In GPS Coordinates " != ',' THEN REPLACE(SPLIT(CR1." Call In GPS Coordinates ", ',')[0], '" ', 
            CAST(NULL AS NUMBER)
          ) ELSE DPAD_P."Provider_Latitude" END AS "Latitude", 
          CR1."Payer Id" as "PayerID", 
          CR1."Application Payer Id" as "AppPayerID", 
          COALESCE(
            SPA."Payer Name", DCON."Contract Name"
          ) AS "Contract", 
          SPA."Payer State" AS "PayerState", 
          CAST(CR1."Invoice Date" AS timestamp) AS "BilledDate", 
          CR1."Billed Hours" AS "BilledHours", 
          CR1."Billed" AS "Billed", 
          DSC."Service Code Id" AS "ServiceCodeID", 
          DSC."Application Service Code Id" AS "AppServiceCodeID", 
          CR1."Bill Type" as "RateType", 
          DSC."Service Code" as "ServiceCode", 
          CAST(
            CR1."Visit Updated Timestamp" AS timestamp
          ) as "LastUpdatedDate", 
          DUSR."User Fullname" AS "LastUpdatedBy", 
          DPA_P."Admission Id" as "P_PAdmissionID", 
          DPA_P."Patient Name" as "P_PName", 
          DPA_P."Patient Firstname" as "P_PFName", 
          DPA_P."Patient Lastname" as "P_PLName", 
          DPA_P."Medicaid Number" as "P_PMedicaidNumber", 
          DPA_P."Status" AS "P_PStatus", 
          DPAD_P."Patient Address Id" as "P_PAddressID", 
          DPAD_P."Application Patient Address Id" as "P_PAppAddressID", 
          DPAD_P."Address Line 1" as "P_PAddressL1", 
          DPAD_P."Address Line 2" as "P_PAddressL2", 
          DPAD_P."City" as "P_PCity", 
          DPAD_P."Address State" as "P_PAddressState", 
          DPAD_P."Zip Code" as "P_PZipCode", 
          DPAD_P."County" as "P_PCounty", 
          DPA_PA."Admission Id" as "PA_PAdmissionID", 
          DPA_PA."Patient Name" as "PA_PName", 
          DPA_PA."Patient Firstname" as "PA_PFName", 
          DPA_PA."Patient Lastname" as "PA_PLName", 
          DPA_PA."Medicaid Number" as "PA_PMedicaidNumber", 
          DPA_PA."Status" AS "PA_PStatus", 
          DPAD_PA."Patient Address Id" as "PA_PAddressID", 
          DPAD_PA."Application Patient Address Id" as "PA_PAppAddressID", 
          DPAD_PA."Address Line 1" as "PA_PAddressL1", 
          DPAD_PA."Address Line 2" as "PA_PAddressL2", 
          DPAD_PA."City" as "PA_PCity", 
          DPAD_PA."Address State" as "PA_PAddressState", 
          DPAD_PA."Zip Code" as "PA_PZipCode", 
          DPAD_PA."County" as "PA_PCounty", 
          CASE WHEN (
            CR1."Application Payer Id" = ' 0 ' 
            AND CR1."Application Contract Id" != ' 0 '
          ) THEN ' Internal ' WHEN (
            CR1."Application Payer Id" != ' 0 ' 
            AND CR1."Application Contract Id" != ' 0 '
          ) THEN ' UPR ' WHEN (
            CR1."Application Payer Id" != ' 0 ' 
            AND CR1."Application Contract Id" = ' 0 '
          ) THEN ' Payer ' END AS "ContractType" 
        FROM 
          ANALYTICS.BI.FACTVISITCALLPERFORMANCE_DELETED_CR AS CR1 
          INNER JOIN ANALYTICS.BI.DIMCAREGIVER AS CAR ON CAR."Caregiver Id" = CR1."Caregiver Id" 
          AND TRIM(CAR."SSN") IS NOT NULL 
          AND TRIM(CAR."SSN")!= '' 
          LEFT JOIN ANALYTICS.BI.DIMOFFICE AS DOF ON DOF."Office Id" = CR1."Office Id" 
          AND DOF."Is Active" = TRUE 
          LEFT JOIN ANALYTICS.BI.DIMPATIENT AS DPA_P ON DPA_P."Patient Id" = CR1."Provider Patient Id" 
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
                ORDER BY 
                  DDD."Application Created UTC Timestamp" DESC
              ) AS rn 
            FROM 
              ANALYTICS.BI.DIMPATIENTADDRESS AS DDD 
            WHERE 
              DDD."Primary Address" = TRUE 
              AND DDD."Address Type" LIKE ' % GPS % '
          ) AS DPAD_P ON DPAD_P."Patient Id" = DPA_P."Patient Id" 
          AND DPAD_P."RN" = 1 
          LEFT JOIN ANALYTICS.BI.DIMPATIENT AS DPA_PA ON DPA_PA."Patient Id" = CR1."Payer Patient Id" 
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
                ORDER BY 
                  DDD."Application Created UTC Timestamp" DESC
              ) AS rn 
            FROM 
              ANALYTICS.BI.DIMPATIENTADDRESS AS DDD 
            WHERE 
              DDD."Primary Address" = TRUE 
              AND DDD."Address Type" LIKE ' % GPS % '
          ) AS DPAD_PA ON DPAD_PA."Patient Id" = DPA_PA."Patient Id" 
          AND DPAD_PA."RN" = 1 
          LEFT JOIN ANALYTICS.BI.DIMPAYER AS SPA ON SPA."Payer Id" = CR1."Payer Id" 
          AND SPA."Is Active" = TRUE 
          AND SPA."Is Demo" = FALSE 
          LEFT JOIN ANALYTICS.BI.DIMCONTRACT AS DCON ON DCON."Contract Id" = CR1."Contract Id" 
          AND DCON."Is Active" = TRUE 
          INNER JOIN ANALYTICS.BI.DIMPROVIDER AS DPR ON DPR."Provider Id" = CR1."Provider Id" 
          AND DPR."Is Active" = TRUE 
          AND DPR."Is Demo" = FALSE 
          LEFT JOIN ANALYTICS.BI.DIMSERVICECODE AS DSC ON DSC."Service Code Id" = CR1."Service Code Id" 
          LEFT JOIN ANALYTICS.BI.DIMUSER AS DUSR ON DUSR."User Id" = CR1."Visit Updated User Id" 
          INNER JOIN CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS AS CVM ON CVM."ConVisitID" = CR1."Visit Id" 
          AND CVM."StatusFlag" = ' D ' 
        WHERE 
          DATE(CR1."Visit Date") BETWEEN DATE(
            DATEADD(
              year, 
              -2, 
              GETDATE()
            )
          ) 
          AND DATE(
            DATEADD(
              day, 
              45, 
              GETDATE()
            )
          )
      ) AS V1
  ) AS ALLDATA 
WHERE 
  CVM."ConVisitID" = ALLDATA."VisitID";

--error case
UPDATE 
  CONFLICTREPORT."PUBLIC".SETTINGS 
SET 
  "InProgressFlag" = 2;
