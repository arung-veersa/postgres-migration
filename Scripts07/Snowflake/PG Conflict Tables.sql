-- Schema-agnostic Postgres DDL translated from SF Conflict Tables.sql
-- Usage:
--   - Change the value of target_schema in the DO block below (default: conflict_dev)
--   - Run the script in psql/pgAdmin/DBeaver; it will create the schema if missing and set search_path
--   - All objects are created unqualified, honoring the configured search_path

DO $$
DECLARE
	target_schema text := 'conflict_dev'; -- change this to your desired schema name
BEGIN
	IF NOT EXISTS (
		SELECT 1 FROM pg_namespace WHERE nspname = target_schema
	) THEN
		EXECUTE format('CREATE SCHEMA %I', target_schema);
	END IF;
	-- Set search_path for this session to the target schema
	PERFORM set_config('search_path', quote_ident(target_schema), false);
END $$;

-- ==========================
-- Sequences for autoincrement columns
-- ==========================
CREATE SEQUENCE IF NOT EXISTS conflicts_conflictid_seq START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE IF NOT EXISTS conflictvisitmaps_id_seq START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE IF NOT EXISTS conflict_commu_inters_id_seq START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE IF NOT EXISTS contact_maintenance_id_seq START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE IF NOT EXISTS excluded_agency_id_seq START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE IF NOT EXISTS excluded_ssn_id_seq START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE IF NOT EXISTS govbodiespayers_id_seq START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE IF NOT EXISTS log_fields_id_seq START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE IF NOT EXISTS log_history_id_seq START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE IF NOT EXISTS log_history_values_id_seq START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE IF NOT EXISTS mph_id_seq START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE IF NOT EXISTS noresponsereasons_id_seq START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE IF NOT EXISTS notifications_id_seq START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE IF NOT EXISTS payer_provider_reminders_id_seq START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE IF NOT EXISTS task_history_list_id_seq START WITH 1 INCREMENT BY 1;

-- ==========================
-- Tables
-- ==========================

-- CONFLICTS
CREATE TABLE IF NOT EXISTS conflicts (
	"CONFLICTID" numeric(38,0) NOT NULL DEFAULT nextval('conflicts_conflictid_seq'::regclass),
	"RECORDEDDATETIME" timestamp DEFAULT CURRENT_TIMESTAMP,
	"StatusFlag" varchar(5) DEFAULT 'U',
	"NoResponseFlag" varchar(10),
	"NoResponseReasonID" numeric(38,0),
	"NoResponseTitle" varchar(500),
	"NoResponseNotes" varchar(500),
	"ResolveDate" timestamp,
	"CreatedDate" timestamp,
	"ResolvedBy" varchar(200),
	"NoResponseDate" timestamp,
	"FlagForReview" varchar(5),
	"FlagForReviewDate" timestamp,
	"UpdatedRFlag" varchar(5),
	PRIMARY KEY ("CONFLICTID")
);
ALTER SEQUENCE conflicts_conflictid_seq OWNED BY conflicts."CONFLICTID";

-- CONFLICTVISITMAPS
CREATE TABLE IF NOT EXISTS conflictvisitmaps (
	"ID" numeric(38,0) NOT NULL DEFAULT nextval('conflictvisitmaps_id_seq'::regclass),
	"CONFLICTID" numeric(38,0),
	"GroupID" numeric(38,0),
	"SSN" varchar(50),
	"ProviderID" varchar(50),
	"AppProviderID" varchar(50),
	"ProviderName" varchar(100),
	"ConProviderID" varchar(50),
	"ConAppProviderID" varchar(50),
	"ConProviderName" varchar(100),
	"VisitID" varchar(50),
	"AppVisitID" varchar(50),
	"ConVisitID" varchar(50),
	"ConAppVisitID" varchar(50),
	"VisitDate" date,
	"SchStartTime" timestamp,
	"SchEndTime" timestamp,
	"ConSchStartTime" timestamp,
	"ConSchEndTime" timestamp,
	"VisitStartTime" timestamp,
	"VisitEndTime" timestamp,
	"ConVisitStartTime" timestamp,
	"ConVisitEndTime" timestamp,
	"EVVStartTime" timestamp,
	"EVVEndTime" timestamp,
	"ConEVVStartTime" timestamp,
	"ConEVVEndTime" timestamp,
	"ShVTSTTime" timestamp,
	"ShVTENTime" timestamp,
	"CShVTSTTime" timestamp,
	"CShVTENTime" timestamp,
	"CaregiverID" varchar(50),
	"AppCaregiverID" numeric(38,0),
	"AideCode" varchar(50),
	"AideName" varchar(101),
	"AideFName" varchar(50),
	"AideLName" varchar(50),
	"AideSSN" varchar(50),
	"AideStatus" varchar(50),
	"ConCaregiverID" varchar(50),
	"ConAppCaregiverID" numeric(38,0),
	"ConAideCode" varchar(50),
	"ConAideName" varchar(101),
	"ConAideFName" varchar(50),
	"ConAideLName" varchar(50),
	"ConAideSSN" varchar(50),
	"ConAideStatus" varchar(20),
	"OfficeID" varchar(50),
	"AppOfficeID" varchar(50),
	"Office" varchar(100),
	"ConOfficeID" varchar(50),
	"ConAppOfficeID" varchar(50),
	"ConOffice" varchar(100),
	"PatientID" varchar(50),
	"AppPatientID" numeric(38,5),
	"PAdmissionID" varchar(500),
	"PName" varchar(201),
	"PFName" varchar(100),
	"PLName" varchar(100),
	"PMedicaidNumber" varchar(100),
	"PStatus" varchar(50),
	"PAddressID" varchar(50),
	"PAppAddressID" numeric(38,5),
	"PAddressL1" varchar(500),
	"PAddressL2" varchar(100),
	"PCity" varchar(255),
	"PAddressState" varchar(100),
	"PZipCode" varchar(100),
	"PCounty" varchar(100),
	"PLongitude" varchar(50),
	"PLatitude" varchar(50),
	"ConPatientID" varchar(50),
	"ConAppPatientID" numeric(38,5),
	"ConPAdmissionID" varchar(500),
	"ConPName" varchar(201),
	"ConPFName" varchar(100),
	"ConPLName" varchar(100),
	"ConPMedicaidNumber" varchar(100),
	"ConPStatus" varchar(20),
	"ConPAddressID" varchar(50),
	"ConPAppAddressID" numeric(38,5),
	"ConPAddressL1" varchar(500),
	"ConPAddressL2" varchar(100),
	"ConPCity" varchar(255),
	"ConPAddressState" varchar(100),
	"ConPZipCode" varchar(100),
	"ConPCounty" varchar(100),
	"ConPLongitude" varchar(50),
	"ConPLatitude" varchar(50),
	"PayerID" varchar(50),
	"AppPayerID" varchar(50),
	"Contract" varchar(50),
	"PayerState" varchar(100),
	"ConPayerID" varchar(50),
	"ConAppPayerID" varchar(50),
	"ConContract" varchar(50),
	"ConPayerState" varchar(100),
	"Billed" varchar(3),
	"BilledDate" timestamp,
	"BilledHours" numeric(38,3),
	"BilledRate" numeric(19,3),
	"TotalBilledAmount" numeric(19,3),
	"BillRateNonBilled" numeric(22,6),
	"BillRateBoth" numeric(22,6),
	"BilledRateMinute" numeric(38,15),
	"RateType" varchar(50),
	"ConBilled" varchar(3),
	"ConBilledDate" timestamp,
	"ConBilledHours" numeric(38,3),
	"ConBilledRate" numeric(19,3),
	"ConTotalBilledAmount" numeric(19,3),
	"ConBillRateNonBilled" numeric(22,6),
	"ConBillRateBoth" numeric(22,6),
	"ConBilledRateMinute" numeric(38,15),
	"ConRateType" varchar(50),
	"MinuteDiffBetweenSch" numeric(38,0),
	"DistanceMilesFromLatLng" numeric(38,2),
	"AverageMilesPerHour" numeric(38,2),
	"ETATravleMinutes" numeric(38,0),
	"InserviceStartDate" timestamp,
	"InserviceEndDate" timestamp,
	"PTOStartDate" timestamp,
	"PTOEndDate" timestamp,
	"ServiceCodeID" varchar(50),
	"AppServiceCodeID" numeric(38,0),
	"ServiceCode" varchar(50),
	"ConServiceCodeID" varchar(50),
	"ConAppServiceCodeID" numeric(38,0),
	"ConServiceCode" varchar(50),
	"SameSchTimeFlag" varchar(5),
	"SameVisitTimeFlag" varchar(5),
	"SchAndVisitTimeSameFlag" varchar(5),
	"SchOverAnotherSchTimeFlag" varchar(5),
	"VisitTimeOverAnotherVisitTimeFlag" varchar(5),
	"SchTimeOverVisitTimeFlag" varchar(5),
	"DistanceFlag" varchar(5),
	"InServiceFlag" varchar(5),
	"PTOFlag" varchar(5),
	"AgencyContact" varchar(100),
	"AgencyPhone" varchar(30),
	"ConAgencyContact" varchar(100),
	"ConAgencyPhone" varchar(30),
	"IsMissed" boolean,
	"MissedVisitReason" varchar(500),
	"EVVType" varchar(20),
	"ConIsMissed" boolean,
	"ConMissedVisitReason" varchar(500),
	"ConEVVType" varchar(20),
	"ConNoResponseFlag" varchar(10),
	"ConNoResponseReasonID" numeric(38,0),
	"ConNoResponseTitle" varchar(500),
	"ConNoResponseNotes" varchar(500),
	"P_PatientID" varchar(50),
	"P_AppPatientID" numeric(38,5),
	"P_PAdmissionID" varchar(500),
	"P_PName" varchar(201),
	"P_PAddressID" varchar(50),
	"P_PAppAddressID" numeric(38,5),
	"P_PAddressL1" varchar(500),
	"P_PAddressL2" varchar(100),
	"P_PCity" varchar(255),
	"P_PAddressState" varchar(100),
	"P_PZipCode" varchar(100),
	"P_PCounty" varchar(100),
	"P_PFName" varchar(100),
	"P_PLName" varchar(100),
	"P_PMedicaidNumber" varchar(100),
	"P_PStatus" varchar(50),
	"ConP_PatientID" varchar(50),
	"ConP_AppPatientID" numeric(38,5),
	"ConP_PAdmissionID" varchar(500),
	"ConP_PName" varchar(201),
	"ConP_PAddressID" varchar(50),
	"ConP_PAppAddressID" numeric(38,5),
	"ConP_PAddressL1" varchar(500),
	"ConP_PAddressL2" varchar(100),
	"ConP_PCity" varchar(255),
	"ConP_PAddressState" varchar(100),
	"ConP_PZipCode" varchar(100),
	"ConP_PCounty" varchar(100),
	"ConP_PFName" varchar(100),
	"ConP_PLName" varchar(100),
	"ConP_PMedicaidNumber" varchar(100),
	"ConP_PStatus" varchar(20),
	"PA_PatientID" varchar(50),
	"PA_AppPatientID" numeric(38,5),
	"PA_PAdmissionID" varchar(500),
	"PA_PName" varchar(201),
	"PA_PAddressID" varchar(50),
	"PA_PAppAddressID" numeric(38,5),
	"PA_PAddressL1" varchar(500),
	"PA_PAddressL2" varchar(100),
	"PA_PCity" varchar(255),
	"PA_PAddressState" varchar(100),
	"PA_PZipCode" varchar(100),
	"PA_PCounty" varchar(100),
	"PA_PFName" varchar(100),
	"PA_PLName" varchar(100),
	"PA_PMedicaidNumber" varchar(100),
	"PA_PStatus" varchar(20),
	"ConPA_PatientID" varchar(50),
	"ConPA_AppPatientID" numeric(38,5),
	"ConPA_PAdmissionID" varchar(500),
	"ConPA_PName" varchar(201),
	"ConPA_PAddressID" varchar(50),
	"ConPA_PAppAddressID" numeric(38,5),
	"ConPA_PAddressL1" varchar(500),
	"ConPA_PAddressL2" varchar(100),
	"ConPA_PCity" varchar(255),
	"ConPA_PAddressState" varchar(100),
	"ConPA_PZipCode" varchar(100),
	"ConPA_PCounty" varchar(100),
	"ConPA_PFName" varchar(100),
	"ConPA_PLName" varchar(100),
	"ConPA_PMedicaidNumber" varchar(100),
	"ConPA_PStatus" varchar(20),
	"ContractType" varchar(30),
	"ConContractType" varchar(30),
	"LastUpdatedBy" varchar(100),
	"LastUpdatedDate" timestamp,
	"ConLastUpdatedBy" varchar(100),
	"ConLastUpdatedDate" timestamp,
	"CreatedDate" timestamp DEFAULT CURRENT_TIMESTAMP,
	"ResolveDate" timestamp,
	"CRDATEUNIQUE" timestamp,
	"G_CRDATEUNIQUE" timestamp,
	"UpdateFlag" numeric(38,0),
	"UpdatedDate" timestamp,
	"StatusFlag" varchar(5) DEFAULT 'U',
	"ResolvedBy" varchar(200),
	"TempGroupID" numeric(38,0),
	"ReverseUUID" varchar(100),
	"ConNoResponseDate" timestamp,
	"FederalTaxNumber" varchar(100),
	"ConFederalTaxNumber" varchar(100),
	"ConInserviceStartDate" timestamp,
	"ConInserviceEndDate" timestamp,
	"ConPTOStartDate" timestamp,
	"ConPTOEndDate" timestamp,
	"FlagForReview" varchar(5),
	"FlagForReviewDate" timestamp,
	"AggFlagForReview" varchar(5),
	"AggFlagForReviewDate" timestamp,
	"BILLABLEMINUTESFULLSHIFT" numeric(8,4),
	"BILLABLEUNITSFULLSHIFT" numeric(8,4),
	"BILLABLEMINUTESOVERLAP" numeric(8,4),
	"BILLABLEUNITSOVERLAP" numeric(8,4),
	"FAILEDON" timestamp,
	PRIMARY KEY ("ID")
);
ALTER SEQUENCE conflictvisitmaps_id_seq OWNED BY conflictvisitmaps."ID";

-- CONFLICTVISITMAPS_TEMP
CREATE TABLE IF NOT EXISTS conflictvisitmaps_temp (
	"ID" numeric(38,0),
	"CONFLICTID" numeric(38,0),
	"SSN" varchar(50),
	"ProviderID" varchar(50),
	"AppProviderID" varchar(50),
	"ProviderName" varchar(100),
	"FederalTaxNumber" varchar(100),
	"VisitID" varchar(50),
	"AppVisitID" varchar(50),
	"ConProviderID" varchar(50),
	"ConAppProviderID" varchar(50),
	"ConProviderName" varchar(100),
	"ConFederalTaxNumber" varchar(100),
	"ConVisitID" varchar(50),
	"ConAppVisitID" varchar(50),
	"VisitDate" date,
	"SchStartTime" timestamp,
	"SchEndTime" timestamp,
	"ConSchStartTime" timestamp,
	"ConSchEndTime" timestamp,
	"VisitStartTime" timestamp,
	"VisitEndTime" timestamp,
	"ConVisitStartTime" timestamp,
	"ConVisitEndTime" timestamp,
	"EVVStartTime" timestamp,
	"EVVEndTime" timestamp,
	"ConEVVStartTime" timestamp,
	"ConEVVEndTime" timestamp,
	"CaregiverID" varchar(50),
	"AppCaregiverID" numeric(38,0),
	"AideCode" varchar(50),
	"AideName" varchar(101),
	"AideSSN" varchar(50),
	"ConCaregiverID" varchar(50),
	"ConAppCaregiverID" numeric(38,0),
	"ConAideCode" varchar(50),
	"ConAideName" varchar(101),
	"ConAideSSN" varchar(50),
	"OfficeID" varchar(50),
	"AppOfficeID" varchar(50),
	"Office" varchar(100),
	"ConOfficeID" varchar(50),
	"ConAppOfficeID" varchar(50),
	"ConOffice" varchar(100),
	"PatientID" varchar(50),
	"AppPatientID" numeric(38,5),
	"PAdmissionID" varchar(500),
	"PName" varchar(201),
	"PAddressID" varchar(50),
	"PAppAddressID" numeric(38,5),
	"PAddressL1" varchar(500),
	"PAddressL2" varchar(100),
	"PCity" varchar(255),
	"PAddressState" varchar(100),
	"PZipCode" varchar(100),
	"PCounty" varchar(100),
	"PLongitude" varchar(50),
	"PLatitude" varchar(50),
	"ConPatientID" varchar(50),
	"ConAppPatientID" numeric(38,5),
	"ConPAdmissionID" varchar(500),
	"ConPName" varchar(201),
	"ConPAddressID" varchar(50),
	"ConPAppAddressID" numeric(38,5),
	"ConPAddressL1" varchar(500),
	"ConPAddressL2" varchar(100),
	"ConPCity" varchar(255),
	"ConPAddressState" varchar(100),
	"ConPZipCode" varchar(100),
	"ConPCounty" varchar(100),
	"ConPLongitude" varchar(50),
	"ConPLatitude" varchar(50),
	"PayerID" varchar(50),
	"AppPayerID" varchar(50),
	"Contract" varchar(50),
	"ConPayerID" varchar(50),
	"ConAppPayerID" varchar(50),
	"ConContract" varchar(50),
	"BilledDate" timestamp,
	"ConBilledDate" timestamp,
	"BilledHours" numeric(38,3),
	"ConBilledHours" numeric(38,3),
	"Billed" varchar(3),
	"ConBilled" varchar(3),
	"MinuteDiffBetweenSch" numeric(38,0),
	"DistanceMilesFromLatLng" numeric(38,2),
	"AverageMilesPerHour" numeric(38,2),
	"ETATravleMinutes" numeric(38,0),
	"InserviceStartDate" timestamp,
	"InserviceEndDate" timestamp,
	"PTOStartDate" timestamp,
	"PTOEndDate" timestamp,
	"ConInserviceStartDate" timestamp,
	"ConInserviceEndDate" timestamp,
	"ConPTOStartDate" timestamp,
	"ConPTOEndDate" timestamp,
	"ServiceCodeID" varchar(50),
	"AppServiceCodeID" numeric(38,0),
	"RateType" varchar(50),
	"ServiceCode" varchar(50),
	"ConServiceCodeID" varchar(50),
	"ConAppServiceCodeID" numeric(38,0),
	"ConRateType" varchar(50),
	"ConServiceCode" varchar(50),
	"SameSchTimeFlag" varchar(5),
	"SameVisitTimeFlag" varchar(5),
	"SchAndVisitTimeSameFlag" varchar(5),
	"SchOverAnotherSchTimeFlag" varchar(5),
	"VisitTimeOverAnotherVisitTimeFlag" varchar(5),
	"SchTimeOverVisitTimeFlag" varchar(5),
	"DistanceFlag" varchar(5),
	"InServiceFlag" varchar(5),
	"PTOFlag" varchar(5),
	"StatusFlag" varchar(5) DEFAULT 'U',
	"ConStatusFlag" varchar(5) DEFAULT 'U',
	"AideFName" varchar(50),
	"AideLName" varchar(50),
	"ConAideFName" varchar(50),
	"ConAideLName" varchar(50),
	"PFName" varchar(100),
	"PLName" varchar(100),
	"ConPFName" varchar(100),
	"ConPLName" varchar(100),
	"PMedicaidNumber" varchar(100),
	"ConPMedicaidNumber" varchar(100)
);

-- CONFLICT_COMMU_INTERS
CREATE TABLE IF NOT EXISTS conflict_commu_inters (
	"id" numeric(38,0) NOT NULL DEFAULT nextval('conflict_commu_inters_id_seq'::regclass),
	"CONFLICTID" numeric(38,0),
	"GroupID" numeric(38,0),
	"ReverseUUID" varchar(100),
	"Description" varchar(1000),
	"CommentType" numeric(3,0),
	"Attachmenturl" varchar(500),
	"communications_type" numeric(3,0),
	"created_at" timestamp,
	"updated_at" timestamp,
	"created_by" numeric(38,0),
	"updated_by" numeric(38,0),
	"created_by_name" varchar(200),
	"updated_by_name" varchar(200),
	"OriginalFileName" varchar(200),
	"FileSize" numeric(38,0),
	PRIMARY KEY ("id")
);
ALTER SEQUENCE conflict_commu_inters_id_seq OWNED BY conflict_commu_inters."id";
COMMENT ON COLUMN conflict_commu_inters."CONFLICTID" IS 'For provider internal notes';
COMMENT ON COLUMN conflict_commu_inters."GroupID" IS 'for payer login internalnotes';
COMMENT ON COLUMN conflict_commu_inters."ReverseUUID" IS 'For communication';
COMMENT ON COLUMN conflict_commu_inters."CommentType" IS '1 = Communications 2 = Internal Notes';
COMMENT ON COLUMN conflict_commu_inters."communications_type" IS '1 = Provider 2 = Payer';

-- CONTACT_MAINTENANCE
CREATE TABLE IF NOT EXISTS contact_maintenance (
	"ID" numeric(38,0) NOT NULL DEFAULT nextval('contact_maintenance_id_seq'::regclass),
	"RECORDEDDATETIME" timestamp DEFAULT CURRENT_TIMESTAMP,
	"CONTACT_NAME" varchar(255) DEFAULT 'U',
	"PHONE" varchar(20),
	"ProviderID" varchar(50),
	"AppProviderID" varchar(50),
	"UPDATED_BY" numeric(38,0),
	"UPDATED_AT" timestamp DEFAULT CURRENT_TIMESTAMP,
	"PID" varchar(50),
	"APPLICATIONPID" varchar(50),
	PRIMARY KEY ("ID")
);
ALTER SEQUENCE contact_maintenance_id_seq OWNED BY contact_maintenance."ID";

-- EXCLUDED_AGENCY
CREATE TABLE IF NOT EXISTS excluded_agency (
	"ID" numeric(38,0) NOT NULL DEFAULT nextval('excluded_agency_id_seq'::regclass),
	"AgencyName" varchar(100),
	"ProviderID" varchar(50),
	"AppProviderID" varchar(50),
	PRIMARY KEY ("ID")
);
ALTER SEQUENCE excluded_agency_id_seq OWNED BY excluded_agency."ID";

-- EXCLUDED_SSN
CREATE TABLE IF NOT EXISTS excluded_ssn (
	"ID" numeric(38,0) NOT NULL DEFAULT nextval('excluded_ssn_id_seq'::regclass),
	"SSN" varchar(50),
	PRIMARY KEY ("ID")
);
ALTER SEQUENCE excluded_ssn_id_seq OWNED BY excluded_ssn."ID";

-- FAILED_API_LOGS
CREATE TABLE IF NOT EXISTS failed_api_logs (
	"APPVISITID" numeric(38,0),
	"APPPAYERID" numeric(38,0),
	"CONTRACT_ID_INTERNAL" numeric(38,0),
	"PAYLOAD" text,
	"RESPONSE" text,
	"FAILURE_TYPE" text,
	"ERROR_MESSAGE" text,
	"CONFLICTID" numeric(38,0) NOT NULL,
	PRIMARY KEY ("CONFLICTID")
);

-- GOVBODIESPAYERS
CREATE TABLE IF NOT EXISTS govbodiespayers (
	"ID" numeric(38,0) NOT NULL DEFAULT nextval('govbodiespayers_id_seq'::regclass),
	"PayerID" varchar(50),
	"AppPayerID" varchar(50),
	"UserID" numeric(38,0),
	"PAYER_NAME" varchar(100),
	"STATUS_NAME" varchar(50),
	"CREATED_AT" timestamp DEFAULT CURRENT_TIMESTAMP,
	"UPDATED_AT" timestamp DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY ("ID")
);
ALTER SEQUENCE govbodiespayers_id_seq OWNED BY govbodiespayers."ID";

-- LOG_FIELDS
CREATE TABLE IF NOT EXISTS log_fields (
	"ID" numeric(38,0) NOT NULL DEFAULT nextval('log_fields_id_seq'::regclass),
	"FieldName" varchar(50),
	"FieldDisplayValue" varchar(50),
	"FieldFor" varchar(10),
	"FieldType" varchar(10),
	"RestrictedFlag" numeric(2,0),
	"NotShowInDropDown" numeric(2,0),
	"HideColumnFlag" numeric(2,0),
	"HideHidePayerFlag" numeric(2,0),
	"HideForProviderFlag" numeric(2,0),
	PRIMARY KEY ("ID")
);
ALTER SEQUENCE log_fields_id_seq OWNED BY log_fields."ID";

-- LOG_HISTORY
CREATE TABLE IF NOT EXISTS log_history (
	"ID" numeric(38,0) NOT NULL DEFAULT nextval('log_history_id_seq'::regclass),
	"CONID" numeric(38,0),
	"CreatedDateTime" timestamp DEFAULT CURRENT_TIMESTAMP,
	"LogTypeFlag" varchar(10),
	PRIMARY KEY ("ID")
);
ALTER SEQUENCE log_history_id_seq OWNED BY log_history."ID";

-- LOG_HISTORY_VALUES
CREATE TABLE IF NOT EXISTS log_history_values (
	"ID" numeric(38,0) NOT NULL DEFAULT nextval('log_history_values_id_seq'::regclass),
	"LHID" numeric(38,0),
	"LogID" numeric(38,0),
	"OldValue" text,
	"NewValue" text,
	"VisitID" varchar(50),
	"AppVisitID" varchar(50),
	PRIMARY KEY ("ID")
);
ALTER SEQUENCE log_history_values_id_seq OWNED BY log_history_values."ID";

-- LOG_HISTORY_VALUES_TEMP
CREATE TABLE IF NOT EXISTS log_history_values_temp (
	"CONID" numeric(38,0) NOT NULL,
	"LogID" numeric(38,0),
	"OldValue" text,
	"NewValue" text,
	"VisitID" varchar(50),
	"AppVisitID" varchar(50)
);

-- MPH
CREATE TABLE IF NOT EXISTS mph (
	"ID" numeric(38,0) NOT NULL DEFAULT nextval('mph_id_seq'::regclass),
	"RECORDEDDATETIME" timestamp DEFAULT CURRENT_TIMESTAMP,
	"TYPE" varchar(50),
	"From" numeric(38,0),
	"To" numeric(38,0),
	"AverageMilesPerHour" numeric(38,0),
	PRIMARY KEY ("ID")
);
ALTER SEQUENCE mph_id_seq OWNED BY mph."ID";

-- NORESPONSEREASONS
CREATE TABLE IF NOT EXISTS noresponsereasons (
	"ID" numeric(38,0) NOT NULL DEFAULT nextval('noresponsereasons_id_seq'::regclass),
	"Title" varchar(500),
	"Description" varchar(500),
	PRIMARY KEY ("ID")
);
ALTER SEQUENCE noresponsereasons_id_seq OWNED BY noresponsereasons."ID";

-- NOTIFICATIONS
CREATE TABLE IF NOT EXISTS notifications (
	"ID" numeric(38,0) NOT NULL DEFAULT nextval('notifications_id_seq'::regclass),
	"CONFLICTID" numeric(38,0),
	"ProviderID" varchar(50),
	"AppProviderID" varchar(50),
	"NotificationType" varchar(50),
	"CreatedDate" date,
	"CreatedDateTime" timestamp,
	"ReadUnreadFlag" numeric(3,0),
	"Contract" varchar(100),
	PRIMARY KEY ("ID")
);
ALTER SEQUENCE notifications_id_seq OWNED BY notifications."ID";

-- PAYER_DASHBOARD_AGENCY_COUNT
CREATE TABLE payer_dashboard_agency_count (
	"PAYERID" varchar(50) NULL,
	"VISITDATE" date NULL,
	"PROVIDERID" varchar(50) NULL,	
	"P_NAME" varchar(100) NULL,
	"TIN" varchar(100) NULL,
	"VISIT_KEY" varchar(100) NULL	
);
CREATE INDEX idx_payer_dashboard_agency_count_cluster ON payer_dashboard_agency_count USING btree ("PAYERID","VISITDATE","PROVIDERID","P_NAME","TIN","VISIT_KEY"  );

-- PAYER_DASHBOARD_AGENCY_IMPACT
CREATE TABLE payer_dashboard_agency_impact (
	"PAYERID" varchar(50) NULL,	
	"VISITDATE" date NULL,
	"PROVIDERID" varchar(50) NULL,
	"P_NAME" varchar(100) NULL,
	"TIN" varchar(100) NULL,
	"CON_SP" numeric(38, 2) NULL,
	"CON_OP" numeric(38, 2) NULL,
	"CON_FP" numeric(38, 2) NULL
);
CREATE INDEX idx_payer_dashboard_agency_impact_cluster ON payer_dashboard_agency_impact USING btree ("PAYERID","VISITDATE","PROVIDERID","P_NAME","TIN");

-- PAYER_DASHBOARD_CAREGIVER_COUNT
CREATE TABLE payer_dashboard_caregiver_count (
	"PAYERID" varchar(50) NULL,
	"VISITDATE" date NULL,
	"SSN" varchar(50) NULL,		
	"C_NAME" varchar(100) NULL,
	"VISIT_KEY" varchar(100) NULL
);
CREATE INDEX idx_payer_dashboard_caregiver_count_cluster ON payer_dashboard_caregiver_count USING btree ("VISITDATE","PAYERID","SSN","C_NAME","VISIT_KEY"  );

CREATE TABLE payer_dashboard_caregiver_impact (
	"PAYERID" varchar(50) NULL,	
	"VISITDATE" date NULL,
	"SSN" varchar(50) NULL,		
	"C_NAME" varchar(100) NULL,
	"CON_SP" numeric(38, 2) NULL,
	"CON_OP" numeric(38, 2) NULL,
	"CON_FP" numeric(38, 2) NULL
);
CREATE INDEX idx_payer_dashboard_caregiver_impact_cluster ON payer_dashboard_caregiver_impact USING btree ("VISITDATE","PAYERID","SSN","C_NAME");

-- PAYER_DASHBOARD_CON_TYP_COUNT
CREATE TABLE payer_dashboard_con_typ_count (
	"PAYERID" varchar(50) NULL,
	"VISITDATE" date NULL,
	"CONTYPE" varchar(50) NULL,
	"CONTYPEDESC" varchar(100) NULL,
	"VISIT_KEY" varchar(100) NULL	
);
CREATE INDEX idx_payer_dashboard_con_typ_count_cluster ON payer_dashboard_con_typ_count USING btree ("PAYERID","VISITDATE","CONTYPE","CONTYPEDESC","VISIT_KEY"  );

-- PAYER_DASHBOARD_CON_TYP_IMPACT
CREATE TABLE payer_dashboard_con_typ_impact (
	"PAYERID" varchar(50) NULL,
	"VISITDATE" date NULL,
	"CONTYPE" varchar(50) NULL,
	"CONTYPEDESC" varchar(100) NULL,
	"CON_SP" numeric(38, 2) NULL,
	"CON_OP" numeric(38, 2) NULL,
	"CON_FP" numeric(38, 2) NULL
);
CREATE INDEX idx_payer_dashboard_con_typ_impact_cluster ON payer_dashboard_con_typ_impact USING btree ("PAYERID","VISITDATE","CONTYPE","CONTYPEDESC" );

-- PAYER_DASHBOARD_PATIENT_COUNT
CREATE TABLE payer_dashboard_patient_count (
	"PAYERID" varchar(50) NULL,
	"VISITDATE" date NULL,
	"PATIENTID" varchar(50) NULL,
	"PFNAME" varchar(100) NULL,
	"PLNAME" varchar(100) NULL,
	"PNAME" varchar(100) NULL,
	"ADMISSIONID" varchar(100) NULL,
	"VISIT_KEY" varchar(100) NULL	
);
CREATE INDEX idx_payer_dashboard_patient_count_cluster ON payer_dashboard_patient_count USING btree ("PAYERID","VISITDATE","PATIENTID", "PFNAME","PLNAME","PNAME","ADMISSIONID","VISIT_KEY"  );

-- PAYER_DASHBOARD_PATIENT_IMPACT
CREATE TABLE payer_dashboard_patient_impact (
	"PAYERID" varchar(50) NULL,
	"VISITDATE" date NULL,
	"PATIENTID" varchar(50) NULL,
	"PFNAME" varchar(100) NULL,
	"PLNAME" varchar(100) NULL,
	"PNAME" varchar(100) NULL,
	"ADMISSIONID" varchar(100) NULL,
	"CON_SP" numeric(38, 2) NULL,
	"CON_OP" numeric(38, 2) NULL,
	"CON_FP" numeric(38, 2) NULL
);
CREATE INDEX idx_payer_dashboard_patient_impact_cluster ON payer_dashboard_patient_impact USING btree ("PAYERID","VISITDATE","PATIENTID", "PFNAME","PLNAME","PNAME","ADMISSIONID");

-- PAYER_DASHBOARD_PAYER_COUNT
CREATE TABLE payer_dashboard_payer_count (
	"PAYERID" varchar(50) NULL,
	"VISITDATE" date NULL,
	"CONPAYERID" varchar(50) NULL,
	"PNAME" varchar(100) NULL,
	"VISIT_KEY" varchar(100) NULL	
);
CREATE INDEX idx_payer_dashboard_payer_count_cluster ON payer_dashboard_payer_count USING btree ("PAYERID","VISITDATE","CONPAYERID","PNAME", "VISIT_KEY"  );

-- PAYER_DASHBOARD_PAYER_IMPACT
CREATE TABLE payer_dashboard_payer_impact (
	"PAYERID" varchar(50) NULL,
	"VISITDATE" date NULL,
	"CONPAYERID" varchar(50) NULL,
	"PNAME" varchar(100) NULL,
	"CON_SP" numeric(38, 2) NULL,
	"CON_OP" numeric(38, 2) NULL,
	"CON_FP" numeric(38, 2) NULL	
);
CREATE INDEX idx_payer_dashboard_payer_impact_cluster ON payer_dashboard_payer_impact USING btree ("PAYERID","VISITDATE","CONPAYERID","PNAME");

-- PAYER_DASHBOARD_PAYER_CHART_COUNT
CREATE TABLE payer_dashboard_payer_chart_count (
	"PAYERID" varchar(50) NULL,
	"VISITDATE" date NULL,
	"CONPAYERID" varchar(50) NULL,
	"PNAME" varchar(100) NULL,
	"STATUSFLAG" varchar(5) NULL,
	"COSTTYPE" varchar(20) NULL,
	"VISITTYPE" varchar(20) NULL,
	"VISIT_KEY" varchar(100) NULL	
);
CREATE INDEX idx_payer_dashboard_payer_chart_count_cluster ON payer_dashboard_payer_chart_count USING btree ("VISITDATE","PAYERID","CONPAYERID","PNAME", "STATUSFLAG","COSTTYPE","VISITTYPE","VISIT_KEY"  );

-- PAYER_DASHBOARD_PAYER_CHART_IMPACT
CREATE TABLE payer_dashboard_payer_chart_impact (
	"PAYERID" varchar(50) NULL,
	"VISITDATE" date NULL,
	"CONPAYERID" varchar(50) NULL,
	"PNAME" varchar(100) NULL,
	"STATUSFLAG" varchar(5) NULL,
	"COSTTYPE" varchar(20) NULL,
	"VISITTYPE" varchar(20) NULL,
	"CON_SP" numeric(38, 2) NULL,
	"CON_OP" numeric(38, 2) NULL,
	"CON_FP" numeric(38, 2) NULL	
);
CREATE INDEX idx_payer_dashboard_payer_chart_impact_cluster ON payer_dashboard_payer_chart_impact USING btree ("VISITDATE","PAYERID","CONPAYERID","PNAME", "STATUSFLAG","COSTTYPE","VISITTYPE"  );

-- PAYER_PROVIDER_REMINDERS
CREATE TABLE IF NOT EXISTS payer_provider_reminders (
	"ID" numeric(38,0) NOT NULL DEFAULT nextval('payer_provider_reminders_id_seq'::regclass),
	"PayerID" varchar(50),
	"AppPayerID" varchar(50),
	"Contract" varchar(100),
	"ProviderID" varchar(50),
	"AppProviderID" varchar(50),
	"ProviderName" varchar(100),
	"CreatedDateTime" timestamp DEFAULT CURRENT_TIMESTAMP,
	"NumberOfDays" numeric(38,0),
	PRIMARY KEY ("ID")
);
ALTER SEQUENCE payer_provider_reminders_id_seq OWNED BY payer_provider_reminders."ID";

-- PROVIDER_DASHBOARD_AGENCY
CREATE TABLE IF NOT EXISTS provider_dashboard_agency (
	"PROVIDERID" varchar(50),
	"OFFICEID" varchar(50),
	"CRDATEUNIQUE" date,
	"CONPROVIDERID" varchar(50),
	"CON_P_NAME" varchar(100),
	"CON_TIN" varchar(100),
	"CON_TO" numeric(38,0),
	"CON_SP" numeric(38,2),
	"CON_OP" numeric(38,2),
	"CON_FP" numeric(38,2)
);
CREATE INDEX IF NOT EXISTS idx_provider_dashboard_agency_cluster ON provider_dashboard_agency ("PROVIDERID", "OFFICEID", "CRDATEUNIQUE", "CONPROVIDERID", "CON_P_NAME", "CON_TIN");

-- PROVIDER_DASHBOARD_CAREGIVER
CREATE TABLE IF NOT EXISTS provider_dashboard_caregiver (
	"PROVIDERID" varchar(50),
	"OFFICEID" varchar(50),
	"CRDATEUNIQUE" date,
	"CAREGIVERID" varchar(50),
	"C_CODE" varchar(100),
	"C_NAME" varchar(100),
	"CON_TO" numeric(38,0),
	"CON_SP" numeric(38,2),
	"CON_OP" numeric(38,2),
	"CON_FP" numeric(38,2)
);
CREATE INDEX IF NOT EXISTS idx_provider_dashboard_caregiver_cluster ON provider_dashboard_caregiver ("PROVIDERID", "OFFICEID", "CRDATEUNIQUE", "CAREGIVERID", "C_CODE", "C_NAME");

-- PROVIDER_DASHBOARD_CON_TYP
CREATE TABLE IF NOT EXISTS provider_dashboard_con_typ (
	"PROVIDERID" varchar(50),
	"OFFICEID" varchar(50),
	"CRDATEUNIQUE" date,
	"EX_ST_MATCH_TO" numeric(38,0),
	"EX_ST_MATCH_SP" numeric(38,2),
	"EX_ST_MATCH_OP" numeric(38,2),
	"EX_ST_MATCH_FP" numeric(38,2),
	"EX_VT_MATCH_TO" numeric(38,0),
	"EX_VT_MATCH_SP" numeric(38,2),
	"EX_VT_MATCH_OP" numeric(38,2),
	"EX_VT_MATCH_FP" numeric(38,2),
	"EX_ST_VT_MATCH_TO" numeric(38,0),
	"EX_ST_VT_MATCH_SP" numeric(38,2),
	"EX_ST_VT_MATCH_OP" numeric(38,2),
	"EX_ST_VT_MATCH_FP" numeric(38,2),
	"ST_OVR_TO" numeric(38,0),
	"ST_OVR_SP" numeric(38,2),
	"ST_OVR_OP" numeric(38,2),
	"ST_OVR_FP" numeric(38,2),
	"VT_OVR_TO" numeric(38,0),
	"VT_OVR_SP" numeric(38,2),
	"VT_OVR_OP" numeric(38,2),
	"VT_OVR_FP" numeric(38,2),
	"ST_VT_OVR_TO" numeric(38,0),
	"ST_VT_OVR_SP" numeric(38,2),
	"ST_VT_OVR_OP" numeric(38,2),
	"ST_VT_OVR_FP" numeric(38,2),
	"TD_TO" numeric(38,0),
	"TD_SP" numeric(38,2),
	"TD_OP" numeric(38,2),
	"TD_FP" numeric(38,2),
	"IN_TO" numeric(38,0),
	"IN_SP" numeric(38,2),
	"IN_OP" numeric(38,2),
	"IN_FP" numeric(38,2),
	"PT_TO" numeric(38,0),
	"PT_SP" numeric(38,2),
	"PT_OP" numeric(38,2),
	"PT_FP" numeric(38,2)
);
CREATE INDEX IF NOT EXISTS idx_provider_dashboard_con_typ_cluster ON provider_dashboard_con_typ ("PROVIDERID", "OFFICEID", "CRDATEUNIQUE");

-- PROVIDER_DASHBOARD_PATIENT
CREATE TABLE IF NOT EXISTS provider_dashboard_patient (
	"PROVIDERID" varchar(50),
	"OFFICEID" varchar(50),
	"CRDATEUNIQUE" date,
	"PATIENTID" varchar(50),
	"PFNAME" varchar(100),
	"PLNAME" varchar(100),
	"PNAME" varchar(100),
	"CON_TO" numeric(38,0),
	"CON_SP" numeric(38,2),
	"CON_OP" numeric(38,2),
	"CON_FP" numeric(38,2)
);
CREATE INDEX IF NOT EXISTS idx_provider_dashboard_patient_cluster ON provider_dashboard_patient ("PROVIDERID", "OFFICEID", "CRDATEUNIQUE", "PATIENTID", "PFNAME", "PLNAME", "PNAME");

-- PROVIDER_DASHBOARD_PAYER
CREATE TABLE IF NOT EXISTS provider_dashboard_payer (
	"PROVIDERID" varchar(50),
	"OFFICEID" varchar(50),
	"CRDATEUNIQUE" date,
	"PAYERID" varchar(50),
	"PNAME" varchar(100),
	"CON_TO" numeric(38,0),
	"CON_SP" numeric(38,2),
	"CON_OP" numeric(38,2),
	"CON_FP" numeric(38,2)
);
CREATE INDEX IF NOT EXISTS idx_provider_dashboard_payer_cluster ON provider_dashboard_payer ("PROVIDERID", "OFFICEID", "CRDATEUNIQUE", "PAYERID", "PNAME");

-- PROVIDER_DASHBOARD_TOP
CREATE TABLE IF NOT EXISTS provider_dashboard_top (
	"PROVIDERID" varchar(50),
	"OFFICEID" varchar(50),
	"TODAYTOTAL" numeric(38,0),
	"TODAYSHIFTPRICE" numeric(38,2),
	"TODAYOVERLAPPRICE" numeric(38,2),
	"SEVENTOTAL" numeric(38,0),
	"SEVENFINALPRICE" numeric(38,2),
	"THIRTYTOTAL" numeric(38,0),
	"THIRTYFINALPRICE" numeric(38,2)
);
CREATE INDEX IF NOT EXISTS idx_provider_dashboard_top_cluster ON provider_dashboard_top ("PROVIDERID", "OFFICEID");

-- SETTINGS
CREATE TABLE IF NOT EXISTS settings (
	"ExtraDistance" numeric(38,0),
	"ExtraDistancePer" numeric(38,2),
	"NORESPONSELIMITTIME" numeric(38,0),
	"ID" numeric(38,0) DEFAULT 0,
	"UpdateCronFlag" numeric(3,0),
	"InsertCronFlag" numeric(3,0),
	"ConflictIDFlag" numeric(3,0),
	"GroupIDFlag" numeric(3,0),
	"VisitHistoryFlag" numeric(3,0),
	"LastLoadDate" timestamp,
	"InProgressFlag" numeric(38,0),
	CONSTRAINT "SETTINGS_PK" PRIMARY KEY ("ID")
);

-- TASK_HISTORY_LIST
CREATE TABLE IF NOT EXISTS task_history_list (
	"ID" numeric(38,0) NOT NULL DEFAULT nextval('task_history_list_id_seq'::regclass),
	"NAME" varchar(100),
	"UNNAME" varchar(100),
	"EMAILSENT" varchar(10),
	"CREATED_AT" timestamp DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY ("ID")
);
ALTER SEQUENCE task_history_list_id_seq OWNED BY task_history_list."ID";

-- TMP_BILLABLE_UPDATES
CREATE TABLE IF NOT EXISTS tmp_billable_updates (
	"ID" numeric(38,0) NOT NULL,
	"BMF" numeric(38,0) NOT NULL,
	"BUF" text NOT NULL,
	"BMO" numeric(38,0) NOT NULL,
	"BUO" text NOT NULL
);

-- STATE_DASHBOARD_AGENCY_COUNT
CREATE TABLE state_dashboard_agency_count (
	"PAYERID" varchar(50) NULL,
	"PROVIDERID" varchar(50) NULL,
	"VISITDATE" date NULL,
	"P_NAME" varchar(100) NULL,
	"STATUSFLAG" varchar(5) NULL,
	"COSTTYPE" varchar(20) NULL,
	"VISITTYPE" varchar(20) NULL,
	"COUNTY" varchar(100) NULL,
	"SERVICECODE" varchar(50) NULL,
	"VISIT_KEY" varchar(100) NULL
);
CREATE INDEX idx_state_dashboard_agency_count_cluster ON state_dashboard_agency_count USING btree ("VISITDATE","PAYERID","PROVIDERID","STATUSFLAG","COSTTYPE","VISITTYPE","COUNTY","SERVICECODE","VISIT_KEY"  );

-- STATE_DASHBOARD_AGENCY_IMPACT
CREATE TABLE state_dashboard_agency_impact (
	"PAYERID" varchar(50) NULL,
	"PROVIDERID" varchar(50) NULL,
	"VISITDATE" date NULL,
	"P_NAME" varchar(100) NULL,
	"STATUSFLAG" varchar(5) NULL,
	"COSTTYPE" varchar(20) NULL,
	"VISITTYPE" varchar(20) NULL,
	"COUNTY" varchar(100) NULL,
	"SERVICECODE" varchar(50) NULL,
	"CON_SP" numeric(38, 2) NULL,
	"CON_OP" numeric(38, 2) NULL,
	"CON_FP" numeric(38, 2) NULL
);
CREATE INDEX idx_state_dashboard_agency_impact_cluster ON state_dashboard_agency_impact USING btree ("VISITDATE","PAYERID","PROVIDERID","STATUSFLAG","COSTTYPE","VISITTYPE","COUNTY","SERVICECODE");


-- STATE_DASHBOARD_CAREGIVER_COUNT
CREATE TABLE state_dashboard_caregiver_count (
	"PAYERID" varchar(50) NULL,
	"PROVIDERID" varchar(50) NULL,
	"VISITDATE" date NULL,
	"SSN" varchar(50) NULL,
	"C_NAME" varchar(100) NULL,
	"STATUSFLAG" varchar(5) NULL,
	"COSTTYPE" varchar(20) NULL,
	"VISITTYPE" varchar(20) NULL,
	"COUNTY" varchar(100) NULL,
	"SERVICECODE" varchar(50) NULL,
	"VISIT_KEY" varchar(100) NULL
);
CREATE INDEX idx_state_dashboard_caregiver_count_cluster ON state_dashboard_caregiver_count USING btree ("VISITDATE","PAYERID","PROVIDERID","STATUSFLAG","COSTTYPE","VISITTYPE","COUNTY","SERVICECODE","VISIT_KEY"  );

-- STATE_DASHBOARD_CAREGIVER_IMPACT
CREATE TABLE state_dashboard_caregiver_impact (
	"PAYERID" varchar(50) NULL,
	"PROVIDERID" varchar(50) NULL,
	"VISITDATE" date NULL,
	"SSN" varchar(50) NULL,
	"C_NAME" varchar(100) NULL,
	"STATUSFLAG" varchar(5) NULL,
	"COSTTYPE" varchar(20) NULL,
	"VISITTYPE" varchar(20) NULL,
	"COUNTY" varchar(100) NULL,
	"SERVICECODE" varchar(50) NULL,
	"CON_SP" numeric(38, 2) NULL,
	"CON_OP" numeric(38, 2) NULL,
	"CON_FP" numeric(38, 2) NULL
);
CREATE INDEX idx_state_dashboard_caregiver_impact_cluster ON state_dashboard_caregiver_impact USING btree ("VISITDATE","PAYERID","PROVIDERID","STATUSFLAG","COSTTYPE","VISITTYPE","COUNTY","SERVICECODE");

-- STATE_DASHBOARD_CON_TYP_COUNT
CREATE TABLE state_dashboard_con_type_count (
	"PAYERID" varchar(50) NULL,
	"PROVIDERID" varchar(50) NULL,
	"VISITDATE" date NULL,
	"CONTYPE" varchar(50) NULL,
	"CONTYPEDESC" varchar(100) NULL,
	"STATUSFLAG" varchar(5) NULL,
	"COSTTYPE" varchar(20) NULL,
	"VISITTYPE" varchar(20) NULL,
	"COUNTY" varchar(100) NULL,
	"SERVICECODE" varchar(50) NULL,
	"VISIT_KEY" varchar(100) NULL
);
CREATE INDEX idx_state_dashboard_con_type_count_cluster ON state_dashboard_con_type_count USING btree ("VISITDATE","PAYERID","PROVIDERID","STATUSFLAG","COSTTYPE","VISITTYPE","COUNTY","SERVICECODE","VISIT_KEY"  );

-- STATE_DASHBOARD_CON_TYP_IMPACT
CREATE TABLE state_dashboard_con_type_impact (
	"PAYERID" varchar(50) NULL,
	"PROVIDERID" varchar(50) NULL,
	"VISITDATE" date NULL,
	"CONTYPE" varchar(50) NULL,
	"CONTYPEDESC" varchar(100) NULL,
	"STATUSFLAG" varchar(5) NULL,
	"COSTTYPE" varchar(20) NULL,
	"VISITTYPE" varchar(20) NULL,
	"COUNTY" varchar(100) NULL,
	"SERVICECODE" varchar(50) NULL,
	"CON_SP" numeric(38, 2) NULL,
	"CON_OP" numeric(38, 2) NULL,
	"CON_FP" numeric(38, 2) NULL
);
CREATE INDEX idx_state_dashboard_con_type_impact_cluster ON state_dashboard_con_type_impact USING btree ("VISITDATE","PAYERID","PROVIDERID","STATUSFLAG","COSTTYPE","VISITTYPE","COUNTY","SERVICECODE");

-- STATE_DASHBOARD_PATIENT_COUNT
CREATE TABLE state_dashboard_patient_count (
	"PAYERID" varchar(50) NULL,
	"PROVIDERID" varchar(50) NULL,
	"VISITDATE" date NULL,
	"PATIENTID" varchar(50) NULL,
	"PNAME" varchar(100) NULL,
	"STATUSFLAG" varchar(5) NULL,
	"COSTTYPE" varchar(20) NULL,
	"VISITTYPE" varchar(20) NULL,
	"COUNTY" varchar(100) NULL,
	"SERVICECODE" varchar(50) NULL,
	"VISIT_KEY" varchar(100) NULL	
);
CREATE INDEX idx_state_dashboard_patient_count_cluster ON state_dashboard_patient_count USING btree ("VISITDATE","PAYERID","PROVIDERID","STATUSFLAG","COSTTYPE","VISITTYPE","COUNTY","SERVICECODE","VISIT_KEY"  );

-- STATE_DASHBOARD_PATIENT_IMPACT
CREATE TABLE state_dashboard_patient_impact (
	"PAYERID" varchar(50) NULL,
	"PROVIDERID" varchar(50) NULL,
	"VISITDATE" date NULL,
	"PATIENTID" varchar(50) NULL,
	"PNAME" varchar(100) NULL,
	"STATUSFLAG" varchar(5) NULL,
	"COSTTYPE" varchar(20) NULL,
	"VISITTYPE" varchar(20) NULL,
	"COUNTY" varchar(100) NULL,
	"SERVICECODE" varchar(50) NULL,
	"CON_SP" numeric(38, 2) NULL,
	"CON_OP" numeric(38, 2) NULL,
	"CON_FP" numeric(38, 2) NULL
);
CREATE INDEX idx_state_dashboard_patient_impact_cluster ON state_dashboard_patient_impact USING btree ("VISITDATE","PAYERID","PROVIDERID","STATUSFLAG","COSTTYPE","VISITTYPE","COUNTY","SERVICECODE");

-- STATE_DASHBOARD_PAYER_COUNT
CREATE TABLE state_dashboard_payer_count (
	"PAYERID" varchar(50) NULL,
	"PROVIDERID" varchar(50) NULL,
	"VISITDATE" date NULL,
	"PNAME" varchar(100) NULL,
	"STATUSFLAG" varchar(5) NULL,
	"COSTTYPE" varchar(20) NULL,
	"VISITTYPE" varchar(20) NULL,
	"COUNTY" varchar(100) NULL,
	"SERVICECODE" varchar(50) NULL,
	"VISIT_KEY" varchar(100) NULL	
);
CREATE INDEX idx_state_dashboard_payer_count_cluster ON state_dashboard_payer_count USING btree ("VISITDATE","PAYERID","PROVIDERID","STATUSFLAG","COSTTYPE","VISITTYPE","COUNTY","SERVICECODE","VISIT_KEY"  );

-- STATE_DASHBOARD_PAYER_IMPACT
CREATE TABLE state_dashboard_payer_impact (
	"PAYERID" varchar(50) NULL,
	"PROVIDERID" varchar(50) NULL,
	"VISITDATE" date NULL,
	"PNAME" varchar(100) NULL,
	"STATUSFLAG" varchar(5) NULL,
	"COSTTYPE" varchar(20) NULL,
	"VISITTYPE" varchar(20) NULL,
	"COUNTY" varchar(100) NULL,
	"SERVICECODE" varchar(50) NULL,
	"CON_SP" numeric(38, 2) NULL,
	"CON_OP" numeric(38, 2) NULL,
	"CON_FP" numeric(38, 2) NULL
);
CREATE INDEX idx_state_dashboard_payer_impact_cluster ON state_dashboard_payer_impact USING btree ("VISITDATE","PAYERID","PROVIDERID","STATUSFLAG","COSTTYPE","VISITTYPE","COUNTY","SERVICECODE");


--PAYER_CONFLICT_SUMMARY_COUNT
CREATE TABLE payer_conflict_summary_count (
	"PAYERID" varchar(50) NULL,
	"PROVIDERID" varchar(50) NULL,
	"CONPAYERID" varchar(50) NULL,
	"PROVIDER_NAME" varchar(100) NULL,
	"TIN" varchar(100) NULL,
	"CONTRACT" varchar(100) NULL,
	"PATIENT_FNAME" varchar(100) NULL,
	"PATIENT_LNAME" varchar(100) NULL,
	"ADMISSIONID" varchar(100) NULL,
	"CAREGIVER_NAME" varchar(100) NULL,
	"CONTYPE" varchar(50) NULL,
	"CONTYPEDESC" varchar(100) NULL,
	"VISITDATE" date NULL,
	"CRDATEUNIQUE" date NULL,		
	"STATUSFLAG" varchar(5) NULL,
	"VISIT_KEY" varchar(100) NULL	
);
CREATE INDEX idx_payer_conflict_summary_count_cluster ON payer_conflict_summary_count USING btree ("PAYERID","PROVIDERID","PROVIDER_NAME","TIN","CONTRACT","PATIENT_FNAME","PATIENT_LNAME", "ADMISSIONID",
 "CAREGIVER_NAME", "CONTYPE","VISITDATE","CRDATEUNIQUE","STATUSFLAG","VISIT_KEY"  );

--PAYER_CONFLICT_SUMMARY_IMPACT
CREATE TABLE payer_conflict_summary_impact (
	"PAYERID" varchar(50) NULL,
	"PROVIDERID" varchar(50) NULL,
	"CONPAYERID" varchar(50) NULL,
	"PROVIDER_NAME" varchar(100) NULL,
	"TIN" varchar(100) NULL,
	"CONTRACT" varchar(100) NULL,
	"PATIENT_FNAME" varchar(100) NULL,
	"PATIENT_LNAME" varchar(100) NULL,
	"ADMISSIONID" varchar(100) NULL,
	"CAREGIVER_NAME" varchar(100) NULL,
	"CONTYPE" varchar(50) NULL,
	"CONTYPEDESC" varchar(100) NULL,
	"VISITDATE" date NULL,
	"CRDATEUNIQUE" date NULL,		
	"STATUSFLAG" varchar(5) NULL,
	"CON_SP" numeric(38, 2) NULL,
	"CON_OP" numeric(38, 2) NULL,
	"CON_FP" numeric(38, 2) NULL
		
);
CREATE INDEX idx_payer_conflict_summary_impact_cluster ON payer_conflict_summary_impact USING btree ("PAYERID","PROVIDERID","PROVIDER_NAME","TIN","CONTRACT","PATIENT_FNAME","PATIENT_LNAME", "ADMISSIONID",
 "CAREGIVER_NAME", "CONTYPE","VISITDATE","CRDATEUNIQUE","STATUSFLAG" );
