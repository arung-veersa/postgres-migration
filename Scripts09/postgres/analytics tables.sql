
-- =====================================================
-- Analytics Schema Tables DDL Script
-- =====================================================

-- =====================================================
-- 1. DIMCAREGIVER TABLE
-- =====================================================

DROP TABLE IF EXISTS dimcaregiver;
CREATE TABLE dimcaregiver (
	"Caregiver Id" uuid NOT NULL,
	"Application Caregiver Id" int8 NULL,
	"Caregiver Code" varchar(50) NULL,
	"Caregiver Firstname" varchar(50) NULL,
	"Caregiver Lastname" varchar(50) NULL,
	"Caregiver Fullname" varchar(101) NULL,
	"SSN" varchar(20) NULL,
	"Status" varchar(50) NULL,
	"Updated Datatimestamp" timestamptz NULL,
	CONSTRAINT PRIMARY KEY ("Caregiver Id")
);

-- =====================================================
-- 2. DIMCONTRACT TABLE
-- =====================================================
DROP TABLE IF EXISTS dimcontract;
CREATE TABLE dimcontract (
	"Contract Id" uuid NOT NULL,
	"Application Contract Id" int8 NULL,
	"Contract Name" varchar(250) NULL,
	"Is Active" bool NULL,
	"Updated Datatimestamp" timestamptz NULL,
	CONSTRAINT PRIMARY KEY ("Contract Id")
);
-- =====================================================
-- 3. DIMOFFICE TABLE 
-- =====================================================
DROP TABLE IF EXISTS dimoffice;
CREATE TABLE dimoffice (
	"Office Id" uuid NOT NULL,
	"Application Office Id" int8 NULL,
	"Office Name" varchar(100) NULL,
	"Is Active" bool NULL,
	"Updated Datatimestamp" timestamptz NULL,
	"Federal Tax Number" varchar(100) NULL,
	"NPI" varchar(100) NULL,
	CONSTRAINT PRIMARY KEY ("Office Id")
);


-- =====================================================
-- 4. DIMPATIENT TABLE
-- =====================================================
DROP TABLE IF EXISTS dimpatient;
CREATE TABLE dimpatient (
	"Patient Id" uuid NOT NULL,
	"Payer Id" uuid NULL,
	"Application Patient Id" int8 NULL,
	"Patient Firstname" varchar(100) NULL,
	"Patient Lastname" varchar(100) NULL,
	"Patient Name" varchar(201) NULL,
	"Admission Id" varchar(500) NULL,
	"Medicaid Number" varchar(100) NULL,
	"Status" varchar(50) NULL,
	"Has Visit" bool NULL,
	"Updated Datatimestamp" timestamptz NULL,
    "Conflict Report Hash" int8,
	CONSTRAINT PRIMARY KEY ("Patient Id")
);
-- =====================================================
-- 5. DIMPATIENTADDRESS TABLE
-- =====================================================
DROP TABLE IF EXISTS dimpatientaddress;
CREATE TABLE dimpatientaddress (
    "Patient Address Id" uuid NOT NULL,
	"Application Patient Address Id" int8 NULL,
	"Patient Id" uuid NULL,
	"Application Patient Id" int8 NULL,
	"Address Type" varchar(50) NULL,
	"Address Line 1" varchar(500) NULL,
	"Address Line 2" varchar(100) NULL,
	"City" varchar(255) NULL,
	"County" varchar(100) NULL,
	"Address State" varchar(100) NULL,
	"Zip Code" varchar(100) NULL,
	"Primary Address" bool NULL,
	"Latitude" real NULL,
	"Longitude" real NULL,
	"Application Created UTC Timestamp" timestamptz NULL,
	"Updated Datatimestamp" timestamptz NULL,
	CONSTRAINT PRIMARY KEY ("Patient Address Id")
);

-- =====================================================
-- 6. DIMPAYER TABLE
-- =====================================================
DROP TABLE IF EXISTS dimpayer;
CREATE TABLE dimpayer (
    "Payer Id" uuid NOT NULL,
	"Application Payer Id" int8 NULL,
	"Payer Name" varchar(200) NULL,
	"Payer State" varchar(100) NULL,
	"Is Active" bool NULL,
	"Is Demo" bool NULL,
	"Updated Datatimestamp" timestamptz NULL,
	CONSTRAINT PRIMARY KEY ("Payer Id")
);

-- =====================================================
-- 7. DIMPAYERPROVIDER TABLE
-- =====================================================
DROP TABLE IF EXISTS dimpayerprovider;
CREATE TABLE dimpayerprovider (
    "Payer Id" varchar(50) NOT NULL,
	"Application Payer Id" numeric(38) NULL,
	"Provider Id" varchar(50) NOT NULL,
	"Application Provider Id" numeric(38) NULL,
    "Conflict Report Hash" int8,
	CONSTRAINT PRIMARY KEY ("Provider Id", "Payer Id")
);

-- =====================================================
-- 8. DIMPROVIDER TABLE
-- =====================================================
DROP TABLE IF EXISTS dimprovider;
CREATE TABLE dimprovider (
    "Provider Id" uuid NOT NULL,
	"Application Provider Id" int8 NULL,
	"Provider Name" varchar(200) NULL,
	"Address State" varchar(100) NULL,
	"Federal Tax Number" varchar(100) NULL,
	"Phone Number 1" varchar(30) NULL,
	"Is Active" bool NULL,
	"Is Demo" bool NULL,
	"Updated Datatimestamp" timestamptz NULL,
	CONSTRAINT PRIMARY KEY ("Provider Id")
);

-- =====================================================
-- 9. DIMSERVICECODE TABLE
-- =====================================================
DROP TABLE IF EXISTS dimservicecode;
CREATE TABLE dimservicecode (
    "Service Code Id" uuid NOT NULL,
	"Application Service Code Id" int8 NULL,
	"Service Code" varchar(50) NULL,
	"Updated Datatimestamp" timestamptz NULL,
	CONSTRAINT PRIMARY KEY ("Service Code Id")
);

-- =====================================================
-- 10. DIMUSER TABLE
-- =====================================================
DROP TABLE IF EXISTS dimuser;
CREATE TABLE dimuser (
    "User Id" varchar(50) NOT NULL,
	"Application User Id" int8 NULL,
	"User Fullname" varchar(200) NULL,
	"User Email Address" varchar(100) NULL,
	"Vendor Id" varchar(50) NULL,
	"Application Vendor Id" int8 NULL,
	"Aggregator Database Name" varchar(50) NULL,
    "Conflict Report Hash" int8,
	CONSTRAINT PRIMARY KEY ("User Id")  
);


-- =====================================================
-- 11. DIMUSEROFFICES TABLE
-- =====================================================
DROP TABLE IF EXISTS dimuseroffices;
CREATE TABLE dimuseroffices (
    "User Id" varchar(50) NOT NULL,
	"Office Id" uuid NOT NULL,
	"Vendor Id" varchar(50) NULL,
	"Vendor Type" varchar(50) NULL,
    "Conflict Report Hash" int8,
	CONSTRAINT  PRIMARY KEY ("User Id", "Office Id")
    
);

-- =====================================================
-- 12. FACTCAREGIVERABSENCE TABLE
-- =====================================================
DROP TABLE IF EXISTS factcaregiverabsence;
CREATE TABLE factcaregiverabsence (
    "Caregiver Vacation Id" int8 NOT NULL,
	"Environment" varchar(50) NOT NULL,
	"Global Caregiver Id" uuid NULL,
	"Office Id" uuid NULL,
	"Provider Id" uuid NULL,
	"Start Date" timestamptz NULL,
	"End Date" timestamptz NULL,
	"Updated Datatimestamp" timestamptz NULL,
	CONSTRAINT  PRIMARY KEY ("Caregiver Vacation Id", "Environment")
);

-- =====================================================
-- 13. FACTCAREGIVERINSERVICE TABLE
-- =====================================================
DROP TABLE IF EXISTS factcaregiverinservice;
CREATE TABLE factcaregiverinservice (
    "Application Caregiver Inservice Id" int8 NOT NULL,
	"Environment" varchar(50) NOT NULL,
	"Caregiver Id" uuid NULL,
	"Provider Id" uuid NULL,
	"Office Id" uuid NULL,
	"Inservice start date" timestamptz NULL,
	"Inservice end date" timestamptz NULL,
	"Updated Datatimestamp" timestamptz NULL,
	CONSTRAINT PRIMARY KEY ("Application Caregiver Inservice Id", "Environment")
);

-- =====================================================
-- 14. FACTVISITCALLPERFORMANCE_CR TABLE
-- =====================================================
DROP TABLE IF EXISTS factvisitcallperformance_cr;
CREATE TABLE factvisitcallperformance_cr (
    "Visit Id" uuid NOT NULL,
	"Application Visit Id" int8 NULL,
	"Patient Id" uuid NULL,
	"Application Patient Id" int8 NULL,
	"Caregiver Id" uuid NULL,
	"Application Caregiver Id" int8 NULL,
	"Provider Id" uuid NULL,
	"Application Provider Id" int8 NULL,
	"Office Id" uuid NULL,
	"Application Office Id" int8 NULL,
	"Payer Id" uuid NULL,
	"Application Payer Id" int8 NULL,
	"Contract Id" uuid NULL,
	"Application Contract Id" int8 NULL,
	"Service Code Id" uuid NULL,
	"Application Service Code Id" int8 NULL,
	"Payer Patient Id" uuid NULL,
	"Application Payer Patient Id" int8 NULL,
	"Provider Patient Id" uuid NULL,
	"Application Provider Patient Id" int8 NULL,
	"Visit Date" date NULL,
	"Scheduled Start Time" timestamptz NULL,
	"Scheduled End Time" timestamptz NULL,
	"Visit Start Time" timestamptz NULL,
	"Visit End Time" timestamptz NULL,
	"Call In Time" timestamptz NULL,
	"Call In GPS Coordinates" varchar(100) NULL,
	"Call Out Time" timestamptz NULL,
	"Call Out GPS Coordinates" varchar(100) NULL,
	"Call Out Device Type" varchar(100) NULL,
	"Bill Type" varchar(50) NULL,
	"Billed" varchar(3) NULL,
	"Billed Hours" real NULL,
	"Billed Rate" real NULL,
	"Total Billed Amount" real NULL,
	"Bill Rate Non-Billed" real NULL,
	"Invoice Date" date NULL,
	"Is Missed" bool NULL,
	"Missed Visit Reason" varchar(500) NULL,
	"Visit Updated User Id" uuid NULL,
	"Application Visit Updated User Id" int8 NULL,
	"Visit Updated Timestamp" timestamptz NULL,
	"Updated Datatimestamp" timestamptz NULL,
    "Conflict Report Hash" int8,
    CONSTRAINT PRIMARY KEY ("Visit Id")
);

-- =====================================================
-- 15. FACTVISITCALLPERFORMANCE_DELETED_CR TABLE
-- =====================================================
DROP TABLE IF EXISTS factvisitcallperformance_deleted_cr;
CREATE TABLE factvisitcallperformance_deleted_cr (
    "Visit Id" uuid NOT NULL,
	"Application Visit Id" int8 NULL,
	"Patient Id" uuid NULL,
	"Application Patient Id" int8 NULL,
	"Caregiver Id" uuid NULL,
	"Application Caregiver Id" int8 NULL,
	"Provider Id" uuid NULL,
	"Application Provider Id" int8 NULL,
	"Office Id" uuid NULL,
	"Application Office Id" int8 NULL,
	"Payer Id" uuid NULL,
	"Application Payer Id" int8 NULL,
	"Contract Id" uuid NULL,
	"Application Contract Id" int8 NULL,
	"Service Code Id" uuid NULL,
	"Application Service Code Id" int8 NULL,
	"Payer Patient Id" uuid NULL,
	"Application Payer Patient Id" int8 NULL,
	"Provider Patient Id" uuid NULL,
	"Application Provider Patient Id" int8 NULL,
	"Visit Date" date NULL,
	"Scheduled Start Time" timestamptz NULL,
	"Scheduled End Time" timestamptz NULL,
	"Visit Start Time" timestamp NULL,
	"Visit End Time" timestamp NULL,
	"Call In Time" timestamp NULL,
	"Call In GPS Coordinates" varchar(100) NULL,
	"Call Out Time" timestamp NULL,
	"Call Out GPS Coordinates" varchar(100) NULL,
	"Call Out Device Type" varchar(100) NULL,
	"Bill Type" varchar(50) NULL,
	"Billed" varchar(3) NULL,
	"Billed Hours" real NULL,
	"Billed Rate" real NULL,
	"Total Billed Amount" real NULL,
	"Bill Rate Non-Billed" real NULL,
	"Invoice Date" date NULL,
	"Is Missed" bool NULL,
	"Missed Visit Reason" varchar(500) NULL,
	"Visit Updated User Id" uuid NULL,
	"Application Visit Updated User Id" int8 NULL,
	"Visit Updated Timestamp" timestamptz NULL,
	"Updated Datatimestamp" timestamptz NULL,
    "Conflict Report Hash" int8,
	CONSTRAINT PRIMARY KEY ("Visit Id")
);

-- =====================================================
-- 16. aggnyprod TABLE
-- =====================================================

DROP TABLE analytics.aggnyprod;
CREATE TABLE analytics.aggnyprod (
	payerid int8 NOT NULL,
	payername varchar(50) NULL,
	"Global Payer ID" uuid NULL
);