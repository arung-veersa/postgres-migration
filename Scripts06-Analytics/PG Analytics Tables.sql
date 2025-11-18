-- =====================================================
-- Analytics Schema Tables DDL Script
-- =====================================================

-- =====================================================
-- 1. DIMCAREGIVER TABLE
-- =====================================================
DROP TABLE IF EXISTS dimcaregiver;
CREATE TABLE dimcaregiver (
    "Caregiver Id" VARCHAR(50) PRIMARY KEY,
    "Application Caregiver Id" NUMERIC(38,0),
    "Caregiver Code" VARCHAR(50),
    "Caregiver Firstname" VARCHAR(50),
    "Caregiver Lastname" VARCHAR(50),
    "Caregiver Fullname" VARCHAR(101),
    "SSN" VARCHAR(50),
    "Status" VARCHAR(50),
    "Updated Datatimestamp" TIMESTAMPTZ
);

-- =====================================================
-- 2. DIMCONTRACT TABLE
-- =====================================================
DROP TABLE IF EXISTS dimcontract;
CREATE TABLE dimcontract (
    "Contract Id" VARCHAR(50) PRIMARY KEY,
    "Application Contract Id" NUMERIC(38,0),
    "Contract Name" VARCHAR(250),
    "Is Active" BOOLEAN,
    "Updated Datatimestamp" TIMESTAMPTZ
);

-- =====================================================
-- 3. DIMOFFICE TABLE
-- =====================================================
DROP TABLE IF EXISTS dimoffice;
CREATE TABLE dimoffice (
    "Office Id" VARCHAR(50) PRIMARY KEY,
    "Application Office Id" NUMERIC(38,0),
    "Office Name" VARCHAR(100),
    "Is Active" BOOLEAN,
    "Updated Datatimestamp" TIMESTAMPTZ
);

-- =====================================================
-- 4. DIMPATIENT TABLE
-- =====================================================
DROP TABLE IF EXISTS dimpatient;
CREATE TABLE dimpatient (
    "Patient Id" VARCHAR(50) PRIMARY KEY,
    "Application Patient Id" NUMERIC(38,0),
    "Patient Firstname" VARCHAR(100),
    "Patient Lastname" VARCHAR(100),
    "Patient Name" VARCHAR(201),
    "Admission Id" VARCHAR(500),
    "Medicaid Number" VARCHAR(100),
    "Status" VARCHAR(50),
    "Updated Datatimestamp" TIMESTAMPTZ
);

-- =====================================================
-- 5. DIMPATIENTADDRESS TABLE
-- =====================================================
DROP TABLE IF EXISTS dimpatientaddress;
CREATE TABLE dimpatientaddress (
    "Patient Address Id" VARCHAR(50) PRIMARY KEY,
    "Application Patient Address Id" NUMERIC(38,0),
    "Application Patient Id" NUMERIC(38,0),
    "Address Type" VARCHAR(50),
    "Address Line 1" VARCHAR(500),
    "Address Line 2" VARCHAR(100),
    "City" VARCHAR(255),
    "County" VARCHAR(100),
    "Address State" VARCHAR(100),
    "Zip Code" VARCHAR(100),
    "Primary Address" BOOLEAN,
    "Latitude" NUMERIC(12, 8),
    "Longitude" NUMERIC(12, 8),
    "Application Created UTC Timestamp" TIMESTAMPTZ,
    "Updated Datatimestamp" TIMESTAMPTZ
);

-- =====================================================
-- 6. DIMPAYER TABLE
-- =====================================================
DROP TABLE IF EXISTS dimpayer;
CREATE TABLE dimpayer (
    "Payer Id" VARCHAR(50) PRIMARY KEY,
    "Application Payer Id" NUMERIC(38,0),
    "Payer Name" VARCHAR(50),
    "Payer State" VARCHAR(100),
    "Is Active" BOOLEAN,
    "Is Demo" BOOLEAN,
    "Updated Datatimestamp" TIMESTAMPTZ
);

-- =====================================================
-- 7. DIMPAYERPROVIDER TABLE
-- =====================================================
DROP TABLE IF EXISTS dimpayerprovider;
CREATE TABLE dimpayerprovider (
    "Payer Id" VARCHAR(50),
    "Application Payer Id" NUMERIC(38,0),
    "Provider Id" VARCHAR(50),
    "Application Provider Id" NUMERIC(38,0),
    PRIMARY KEY ("Provider Id", "Payer Id")
);

-- =====================================================
-- 8. DIMPROVIDER TABLE
-- =====================================================
DROP TABLE IF EXISTS dimprovider;
CREATE TABLE dimprovider (
    "Provider Id" VARCHAR(50) PRIMARY KEY,
    "Application Provider Id" NUMERIC(38,0),
    "Provider Name" VARCHAR(200),
    "Address State" VARCHAR(100),
    "Federal Tax Number" VARCHAR(100),
    "Phone Number 1" VARCHAR(30),
    "Is Active" BOOLEAN,
    "Is Demo" BOOLEAN,
    "Updated Datatimestamp" TIMESTAMPTZ
);

-- =====================================================
-- 9. DIMSERVICECODE TABLE
-- =====================================================
DROP TABLE IF EXISTS dimservicecode;
CREATE TABLE dimservicecode (
    "Service Code Id" VARCHAR(50) PRIMARY KEY,
    "Application Service Code Id" NUMERIC(38,0),
    "Service Code" VARCHAR(50),
    "Updated Datatimestamp" TIMESTAMPTZ
);

-- =====================================================
-- 10. DIMUSER TABLE
-- =====================================================
DROP TABLE IF EXISTS dimuser;
CREATE TABLE dimuser (
    "User Id" VARCHAR(50) PRIMARY KEY,
    "Application User Id" NUMERIC(38,0),
    "User Fullname" VARCHAR(511),
    "User Email Address" VARCHAR(320),
    "Vendor Id" VARCHAR(50),
    "Application Vendor Id" NUMERIC(38,0)
);

-- =====================================================
-- 11. DIMUSEROFFICES TABLE
-- =====================================================
DROP TABLE IF EXISTS dimuseroffices;
CREATE TABLE dimuseroffices (
    "User Id" VARCHAR(50),
    "Office Id" VARCHAR(50),
    "Vendor Id" VARCHAR(50),
    "Vendor Type" VARCHAR(50),
    PRIMARY KEY ("User Id", "Office Id")
);

-- =====================================================
-- 12. FACTCAREGIVERABSENCE TABLE
-- =====================================================
DROP TABLE IF EXISTS factcaregiverabsence;
CREATE TABLE factcaregiverabsence (
    "Caregiver Vacation Id" NUMERIC(38,0),
    "Environment" VARCHAR(50),
    "Global Caregiver Id" VARCHAR(50),
    "Office Id" VARCHAR(50),
    "Provider Id" VARCHAR(50),
    "Start Date" TIMESTAMP,
    "End Date" TIMESTAMP,
    "Updated Datatimestamp" TIMESTAMPTZ,
    PRIMARY KEY ("Caregiver Vacation Id", "Environment")
);

-- =====================================================
-- 13. FACTCAREGIVERINSERVICE TABLE
-- =====================================================
DROP TABLE IF EXISTS factcaregiverinservice;
CREATE TABLE factcaregiverinservice (
    "Application Caregiver Inservice Id" NUMERIC(38,0),
    "Environment" VARCHAR(50),
    "Caregiver Id" VARCHAR(50),
    "Provider Id" VARCHAR(50),
    "Office Id" VARCHAR(50),
    "Inservice start date" TIMESTAMPTZ,
    "Inservice end date" TIMESTAMPTZ,
    "Updated Datatimestamp" TIMESTAMPTZ,
    PRIMARY KEY ("Application Caregiver Inservice Id", "Environment")
);

-- =====================================================
-- 14. FACTVISITCALLPERFORMANCE_CR TABLE
-- =====================================================
DROP TABLE IF EXISTS factvisitcallperformance_cr;
CREATE TABLE factvisitcallperformance_cr (
    "Visit Id" VARCHAR(50) PRIMARY KEY,
    "Application Visit Id" NUMERIC(38,0),
    "Patient Id" VARCHAR(50),
    "Application Patient Id" NUMERIC(38,0),
    "Caregiver Id" VARCHAR(50),
    "Application Caregiver Id" NUMERIC(38,0),
    "Provider Id" VARCHAR(50),
    "Application Provider Id" NUMERIC(38,0),
    "Office Id" VARCHAR(50),
    "Application Office Id" NUMERIC(38,0),
    "Payer Id" VARCHAR(50),
    "Application Payer Id" NUMERIC(38,0),
    "Contract Id" VARCHAR(50),
    "Application Contract Id" NUMERIC(38,0),
    "Service Code Id" VARCHAR(50),
    "Application Service Code Id" NUMERIC(38,0),
    "Payer Patient Id" VARCHAR(50),
    "Application Payer Patient Id" NUMERIC(38,0),
    "Provider Patient Id" VARCHAR(50),
    "Application Provider Patient Id" NUMERIC(38,0),
    "Visit Date" DATE,
    "Scheduled Start Time" TIMESTAMPTZ,
    "Scheduled End Time" TIMESTAMPTZ,
    "Visit Start Time" TIMESTAMP,
    "Visit End Time" TIMESTAMP,
    "Call In Time" TIMESTAMP,
    "Call In GPS Coordinates" VARCHAR(100),
    "Call Out Time" TIMESTAMP,
    "Call Out GPS Coordinates" VARCHAR(100),
    "Call Out Device Type" VARCHAR(100),
    "Bill Type" VARCHAR(50),
    "Billed" VARCHAR(3),
    "Billed Hours" NUMERIC(8, 2),
    "Billed Rate" NUMERIC(19, 3),
    "Total Billed Amount" NUMERIC(19, 3),
    "Bill Rate Non-Billed" NUMERIC(12, 2),
    "Invoice Date" DATE,
    "Is Missed" BOOLEAN,
    "Missed Visit Reason" VARCHAR(500),
    "Visit Updated User Id" VARCHAR(50),
    "Application Visit Updated User Id" NUMERIC(38,0),
    "Visit Updated Timestamp" TIMESTAMPTZ,
    "Updated Datatimestamp" TIMESTAMPTZ
);

-- =====================================================
-- 15. FACTVISITCALLPERFORMANCE_DELETED_CR TABLE
-- =====================================================
DROP TABLE IF EXISTS factvisitcallperformance_deleted_cr;
CREATE TABLE factvisitcallperformance_deleted_cr (
    "Visit Id" VARCHAR(50) PRIMARY KEY,
    "Application Visit Id" NUMERIC(38,0),
    "Patient Id" VARCHAR(50),
    "Application Patient Id" NUMERIC(38,0),
    "Caregiver Id" VARCHAR(50),
    "Application Caregiver Id" NUMERIC(38,0),
    "Provider Id" VARCHAR(50),
    "Application Provider Id" NUMERIC(38,0),
    "Office Id" VARCHAR(50),
    "Application Office Id" NUMERIC(38,0),
    "Payer Id" VARCHAR(50),
    "Application Payer Id" NUMERIC(38,0),
    "Contract Id" VARCHAR(50),
    "Application Contract Id" NUMERIC(38,0),
    "Service Code Id" VARCHAR(50),
    "Application Service Code Id" NUMERIC(38,0),
    "Payer Patient Id" VARCHAR(50),
    "Application Payer Patient Id" NUMERIC(38,0),
    "Provider Patient Id" VARCHAR(50),
    "Application Provider Patient Id" NUMERIC(38,0),
    "Visit Date" DATE,
    "Scheduled Start Time" TIMESTAMPTZ,
    "Scheduled End Time" TIMESTAMPTZ,
    "Visit Start Time" TIMESTAMP,
    "Visit End Time" TIMESTAMP,
    "Call In Time" TIMESTAMP,
    "Call In GPS Coordinates" VARCHAR(100),
    "Call Out Time" TIMESTAMP,
    "Call Out GPS Coordinates" VARCHAR(100),
    "Call Out Device Type" VARCHAR(100),
    "Bill Type" VARCHAR(50),
    "Billed" VARCHAR(3),
    "Billed Hours" NUMERIC(8, 2),
    "Billed Rate" NUMERIC(19, 3),
    "Total Billed Amount" NUMERIC(19, 3),
    "Bill Rate Non-Billed" NUMERIC(12, 2),
    "Invoice Date" DATE,
    "Is Missed" BOOLEAN,
    "Missed Visit Reason" VARCHAR(500),
    "Visit Updated User Id" VARCHAR(50),
    "Application Visit Updated User Id" NUMERIC(38,0),
    "Visit Updated Timestamp" TIMESTAMPTZ,
    "Updated Datatimestamp" TIMESTAMPTZ
);