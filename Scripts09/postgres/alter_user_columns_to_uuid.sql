-- Script to convert User Id and Vendor Id columns from VARCHAR to UUID
-- This ensures type consistency across all analytics tables

-- ============================================================================
-- TABLE: dimuser
-- ============================================================================

-- Step 1: Drop the primary key constraint (will be recreated after type change)
ALTER TABLE analytics_dev2.dimuser 
    DROP CONSTRAINT IF EXISTS dimuser_pkey;

-- Step 2: Convert "User Id" from varchar(50) to uuid
ALTER TABLE analytics_dev2.dimuser 
    ALTER COLUMN "User Id" TYPE uuid USING "User Id"::uuid;

-- Step 3: Convert "Vendor Id" from varchar(50) to uuid (nullable column)
-- Note: This will fail if there are non-UUID formatted values or NULLs
-- NULLs are fine, but invalid UUID strings will cause an error
ALTER TABLE analytics_dev2.dimuser 
    ALTER COLUMN "Vendor Id" TYPE uuid USING 
        CASE 
            WHEN "Vendor Id" IS NULL THEN NULL 
            ELSE "Vendor Id"::uuid 
        END;

-- Step 4: Recreate the primary key constraint
ALTER TABLE analytics_dev2.dimuser 
    ADD CONSTRAINT dimuser_pkey PRIMARY KEY ("User Id");

-- ============================================================================
-- TABLE: dimuseroffices
-- ============================================================================

-- Step 1: Drop the composite primary key constraint
ALTER TABLE analytics_dev2.dimuseroffices 
    DROP CONSTRAINT IF EXISTS dimuseroffices_pkey;

-- Step 2: Convert "User Id" from varchar(50) to uuid
ALTER TABLE analytics_dev2.dimuseroffices 
    ALTER COLUMN "User Id" TYPE uuid USING "User Id"::uuid;

-- Step 3: "Office Id" is already UUID - no change needed

-- Step 4: Convert "Vendor Id" from varchar(50) to uuid (nullable column)
ALTER TABLE analytics_dev2.dimuseroffices 
    ALTER COLUMN "Vendor Id" TYPE uuid USING 
        CASE 
            WHEN "Vendor Id" IS NULL THEN NULL 
            ELSE "Vendor Id"::uuid 
        END;

-- Step 5: Recreate the composite primary key constraint
ALTER TABLE analytics_dev2.dimuseroffices 
    ADD CONSTRAINT dimuseroffices_pkey PRIMARY KEY ("User Id", "Office Id");

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================

-- Verify column types after conversion
SELECT 
    table_name,
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns
WHERE table_schema = 'analytics_dev2'
    AND table_name IN ('dimuser', 'dimuseroffices')
    AND column_name IN ('User Id', 'Vendor Id', 'Office Id')
ORDER BY table_name, ordinal_position;
