CREATE OR REPLACE PROCEDURE CONFLICTREPORT.PUBLIC.LOAD_PAYER_CONFLICT_SUMMARY()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
	// Section 1: Truncate All Target Tables
    var SQL_TRUNCATE_COUNT = `TRUNCATE TABLE CONFLICTREPORT.PUBLIC.PAYER_CONFLICT_SUMMARY_COUNT;`;
	var SQL_TRUNCATE_IMPACT = `TRUNCATE TABLE CONFLICTREPORT.PUBLIC.PAYER_CONFLICT_SUMMARY_IMPACT;`;
	
	// Section 2: Load COUNT Data
    var SQL_INSERT_COUNT = `
		INSERT INTO CONFLICTREPORT.PUBLIC.PAYER_CONFLICT_SUMMARY_COUNT (
			PAYERID, PROVIDERID,CONPAYERID,PROVIDER_NAME, TIN, CONTRACT, PATIENT_FNAME, PATIENT_LNAME, ADMISSIONID,
			CAREGIVER_NAME, CONTYPE, CONTYPEDESC, VISITDATE, CRDATEUNIQUE, STATUSFLAG, VISIT_KEY
		)
		SELECT 
			"PayerID" AS PAYERID,
			"ProviderID" AS PROVIDERID,
			"ConPayerID" AS CONPAYERID,
			"ProviderName" AS PROVIDER_NAME,
			"FederalTaxNumber" AS TIN,
			"ConContract" AS CONTRACT,
			"PA_PFName" AS PATIENT_FNAME,
			"PA_PLName" AS PATIENT_LNAME,
			"PA_PAdmissionID" AS ADMISSIONID,
			MAX(CONCAT("AideFName",'' '', "AideLName")) AS CAREGIVER_NAME,
			CONTYPE,
			CASE 
				WHEN CONTYPE = ''only_to'' THEN ''Time Overlap Only''
				WHEN CONTYPE = ''only_td'' THEN ''Time Distance Only''
				WHEN CONTYPE = ''only_is'' THEN ''In Service Only''
				WHEN CONTYPE = ''both_to_td'' THEN ''Time Overlap and Time Distance''
				WHEN CONTYPE = ''both_to_is'' THEN ''Time Overlap and In Service''
				WHEN CONTYPE = ''both_td_is'' THEN ''Time Distance and In Service''
				WHEN CONTYPE = ''all_to_td_is'' THEN ''All Three (Time Overlap, Time Distance, and In Service)''
				ELSE NULL
			END AS CONTYPEDESC,
			"VisitDate" AS VISITDATE,
			"G_CRDATEUNIQUE"::DATE AS CRDATEUNIQUE,
			"StatusFlag" AS STATUSFLAG,
			VISIT_KEY
		FROM CONFLICTREPORT.PUBLIC.DT_PAYER_CONFLICTS_COMMON
		WHERE  "ProviderName" IS NOT NULL AND  "PA_PFName" IS NOT NULL AND "PA_PLName" IS NOT NULL AND "Contract" IS NOT NULL
		GROUP BY 
			"PayerID",
			"ProviderID",
			"ConPayerID",
			"ProviderName",
			"FederalTaxNumber",
			"ConContract",
			"PA_PFName",
			"PA_PLName",
			"PA_PAdmissionID",
			CONTYPE,
			"VisitDate",
			"G_CRDATEUNIQUE",
			"StatusFlag",
			VISIT_KEY;
    `;
	
	// Section 2: Load IMPACT Data
    var SQL_INSERT_IMPACT = `
		INSERT INTO CONFLICTREPORT.PUBLIC.PAYER_CONFLICT_SUMMARY_IMPACT (
			PAYERID, PROVIDERID,CONPAYERID,PROVIDER_NAME, TIN, CONTRACT, PATIENT_FNAME, PATIENT_LNAME, ADMISSIONID,
			CAREGIVER_NAME, CONTYPE, CONTYPEDESC, VISITDATE, CRDATEUNIQUE, STATUSFLAG, CON_SP, CON_OP, CON_FP
		)
		SELECT 
			"PayerID" AS PAYERID,
			"ProviderID" AS PROVIDERID,
			"ConPayerID" AS CONPAYERID,
			"ProviderName" AS PROVIDER_NAME,
			"FederalTaxNumber" AS TIN,
			"ConContract" AS CONTRACT,
			"PA_PFName" AS PATIENT_FNAME,
			"PA_PLName" AS PATIENT_LNAME,
			"PA_PAdmissionID" AS ADMISSIONID,
			MAX(CONCAT("AideFName",'' '', "AideLName")) AS CAREGIVER_NAME,
			CONTYPE,
			CASE 
				WHEN CONTYPE = ''only_to'' THEN ''Time Overlap Only''
				WHEN CONTYPE = ''only_td'' THEN ''Time Distance Only''
				WHEN CONTYPE = ''only_is'' THEN ''In Service Only''
				WHEN CONTYPE = ''both_to_td'' THEN ''Time Overlap and Time Distance''
				WHEN CONTYPE = ''both_to_is'' THEN ''Time Overlap and In Service''
				WHEN CONTYPE = ''both_td_is'' THEN ''Time Distance and In Service''
				WHEN CONTYPE = ''all_to_td_is'' THEN ''All Three (Time Overlap, Time Distance, and In Service)''
				ELSE NULL
			END AS CONTYPEDESC,
			"VisitDate" AS VISITDATE,
			"G_CRDATEUNIQUE"::DATE AS CRDATEUNIQUE,
			"StatusFlag" AS STATUSFLAG,
			SUM(FULL_SHIFT_AMOUNT) AS CON_SP,
			SUM(OVERLAP_AMOUNT) AS CON_OP,
			SUM(CASE WHEN "StatusFlag" = ''R'' THEN OVERLAP_AMOUNT ELSE 0 END) AS CON_FP			
		FROM CONFLICTREPORT.PUBLIC.DT_PAYER_CONFLICTS_COMMON
		WHERE  "ProviderName" IS NOT NULL AND  "PA_PFName" IS NOT NULL AND "PA_PLName" IS NOT NULL AND "Contract" IS NOT NULL
		GROUP BY 
			"PayerID",
			"ProviderID",
			"ConPayerID",
			"ProviderName",
			"FederalTaxNumber",
			"ConContract",
			"PA_PFName",
			"PA_PLName",
			"PA_PAdmissionID",
			CONTYPE,
			"VisitDate",
			"G_CRDATEUNIQUE",
			"StatusFlag";
    `;
			
    try {
        // Execute all truncates first
        snowflake.execute({ sqlText: SQL_TRUNCATE_COUNT });
		snowflake.execute({ sqlText: SQL_TRUNCATE_IMPACT });
		
        // Execute all inserts
        snowflake.execute({ sqlText: SQL_INSERT_COUNT });
		snowflake.execute({ sqlText: SQL_INSERT_IMPACT });

        return "PAYER Conflict Summary Data Loaded Successfully.";
    } catch (err) {
        throw "ERROR: " + err.message;
    }
';