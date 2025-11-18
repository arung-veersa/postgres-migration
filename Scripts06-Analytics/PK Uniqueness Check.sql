-- =====================================================
-- Primary Key Uniqueness Check Script for Snowflake Views
-- =====================================================
-- For each query, a result with no rows indicates that the
-- primary key is unique in the Snowflake view.
-- =====================================================

-- =====================================================
-- 1. DIMCAREGIVER
-- =====================================================
SELECT
    "Caregiver Id",
    COUNT(*)
FROM
    dimcaregiver
GROUP BY
    "Caregiver Id"
HAVING
    COUNT(*) > 1;

-- =====================================================
-- 2. DIMCONTRACT
-- =====================================================
SELECT
    "Contract Id",
    COUNT(*)
FROM
    dimcontract
GROUP BY
    "Contract Id"
HAVING
    COUNT(*) > 1;

-- =====================================================
-- 3. DIMOFFICE
-- =====================================================
SELECT
    "Office Id",
    COUNT(*)
FROM
    dimoffice
GROUP BY
    "Office Id"
HAVING
    COUNT(*) > 1;

-- =====================================================
-- 4. DIMPATIENT
-- =====================================================
SELECT
    "Patient Id",
    COUNT(*)
FROM
    dimpatient
GROUP BY
    "Patient Id"
HAVING
    COUNT(*) > 1;

-- =====================================================
-- 5. DIMPATIENTADDRESS
-- =====================================================
SELECT
    "Patient Address Id",
    COUNT(*)
FROM
    dimpatientaddress
GROUP BY
    "Patient Address Id"
HAVING
    COUNT(*) > 1;

-- =====================================================
-- 6. DIMPAYER
-- =====================================================
SELECT
    "Payer Id",
    COUNT(*)
FROM
    dimpayer
GROUP BY
    "Payer Id"
HAVING
    COUNT(*) > 1;

-- =====================================================
-- 7. DIMPAYERPROVIDER
-- =====================================================
SELECT
    "Provider Id",
    "Payer Id",
    COUNT(*)
FROM
    dimpayerprovider
GROUP BY
    "Provider Id",
    "Payer Id"
HAVING
    COUNT(*) > 1;

-- =====================================================
-- 8. DIMPROVIDER
-- =====================================================
SELECT
    "Provider Id",
    COUNT(*)
FROM
    dimprovider
GROUP BY
    "Provider Id"
HAVING
    COUNT(*) > 1;

-- =====================================================
-- 9. DIMSERVICECODE
-- =====================================================
SELECT
    "Service Code Id",
    COUNT(*)
FROM
    dimservicecode
GROUP BY
    "Service Code Id"
HAVING
    COUNT(*) > 1;

-- =====================================================
-- 10. DIMUSER
-- =====================================================
SELECT
    "User Id",
    COUNT(*)
FROM
    dimuser
GROUP BY
    "User Id"
HAVING
    COUNT(*) > 1;

-- =====================================================
-- 11. DIMUSEROFFICES
-- =====================================================
SELECT
    "User Id",
    "Office Id",
    COUNT(*)
FROM
    dimuseroffices
GROUP BY
    "User Id",
    "Office Id"
HAVING
    COUNT(*) > 1;

-- =====================================================
-- 12. FACTCAREGIVERABSENCE >>>>
-- =====================================================
SELECT
    "Caregiver Vacation Id",
    "Environment",
    COUNT(*)
FROM
    factcaregiverabsence
GROUP BY
    "Caregiver Vacation Id",
    "Environment"
HAVING
    COUNT(*) > 1;

-- =====================================================
-- 13. FACTCAREGIVERINSERVICE >>>
-- =====================================================
SELECT
    "Application Caregiver Inservice Id",
    "Environment",
    COUNT(*)
FROM
    factcaregiverinservice
GROUP BY
    "Application Caregiver Inservice Id",
    "Environment"
HAVING
    COUNT(*) > 1;

-- =====================================================
-- 14. FACTVISITCALLPERFORMANCE_CR
-- =====================================================
SELECT
    "Visit Id",
    COUNT(*)
FROM
    factvisitcallperformance_cr
GROUP BY
    "Visit Id"
HAVING
    COUNT(*) > 1;

-- =====================================================
-- 15. FACTVISITCALLPERFORMANCE_DELETED_CR
-- =====================================================
SELECT
    "Visit Id",
    COUNT(*)
FROM
    factvisitcallperformance_deleted_cr
GROUP BY
    "Visit Id"
HAVING
    COUNT(*) > 1;
