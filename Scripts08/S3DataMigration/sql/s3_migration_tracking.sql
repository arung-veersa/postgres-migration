-- ====================================================================
-- S3 Migration Tracking Tables
-- ====================================================================
-- These tables extend the existing migration_status schema
-- to support S3-staged migrations
-- ====================================================================

-- Run this in your PostgreSQL database after creating the
-- base migration_status schema (from sql/migration_status_schema.sql)

-- Switch to the migration status schema
SET search_path TO migration_status;

-- ====================================================================
-- Table: s3_unload_files
-- Tracks files created by Snowflake UNLOAD operations
-- ====================================================================
CREATE TABLE IF NOT EXISTS s3_unload_files (
    file_id SERIAL PRIMARY KEY,
    run_id VARCHAR(100) NOT NULL,
    source_database VARCHAR(100) NOT NULL,
    source_schema VARCHAR(100) NOT NULL,
    source_table VARCHAR(255) NOT NULL,
    chunk_id INTEGER NOT NULL,
    
    -- S3 file details
    s3_bucket VARCHAR(255) NOT NULL,
    s3_key VARCHAR(1000) NOT NULL,
    s3_url VARCHAR(1500) NOT NULL,
    file_size_bytes BIGINT,
    file_format VARCHAR(50) DEFAULT 'parquet',
    compression VARCHAR(50) DEFAULT 'snappy',
    
    -- Snowflake UNLOAD details
    snowflake_query_id VARCHAR(100),
    row_count BIGINT,
    
    -- Status tracking
    status VARCHAR(50) DEFAULT 'created',  -- created, loading, loaded, failed, deleted
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    loaded_at TIMESTAMP,
    deleted_at TIMESTAMP,
    
    -- Error tracking
    error_message TEXT,
    
    -- Metadata
    metadata JSONB,
    
    CONSTRAINT s3_unload_files_unique UNIQUE (run_id, source_database, source_schema, source_table, chunk_id, s3_key)
);

CREATE INDEX IF NOT EXISTS idx_s3_unload_files_run_id ON s3_unload_files(run_id);
CREATE INDEX IF NOT EXISTS idx_s3_unload_files_status ON s3_unload_files(status);
CREATE INDEX IF NOT EXISTS idx_s3_unload_files_table ON s3_unload_files(source_database, source_schema, source_table);
CREATE INDEX IF NOT EXISTS idx_s3_unload_files_s3_key ON s3_unload_files(s3_bucket, s3_key);

COMMENT ON TABLE s3_unload_files IS 'Tracks S3 files created by Snowflake UNLOAD operations';
COMMENT ON COLUMN s3_unload_files.status IS 'File status: created, loading, loaded, failed, deleted';
COMMENT ON COLUMN s3_unload_files.snowflake_query_id IS 'Snowflake query ID for the UNLOAD operation';

-- ====================================================================
-- Table: s3_load_progress
-- Tracks progress of loading S3 files into PostgreSQL
-- ====================================================================
CREATE TABLE IF NOT EXISTS s3_load_progress (
    load_id SERIAL PRIMARY KEY,
    file_id INTEGER NOT NULL REFERENCES s3_unload_files(file_id),
    
    -- Load details
    load_method VARCHAR(50) NOT NULL,  -- aws_s3, psycopg2_copy
    rows_loaded BIGINT DEFAULT 0,
    
    -- Timing
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    duration_seconds INTEGER,
    
    -- Status
    status VARCHAR(50) DEFAULT 'in_progress',  -- in_progress, completed, failed
    error_message TEXT,
    
    -- Metadata
    metadata JSONB
);

CREATE INDEX IF NOT EXISTS idx_s3_load_progress_file_id ON s3_load_progress(file_id);
CREATE INDEX IF NOT EXISTS idx_s3_load_progress_status ON s3_load_progress(status);

COMMENT ON TABLE s3_load_progress IS 'Tracks progress of loading S3 files into PostgreSQL';
COMMENT ON COLUMN s3_load_progress.load_method IS 'Method used: aws_s3 extension or psycopg2 COPY';

-- ====================================================================
-- View: s3_migration_summary
-- Summary view of S3 migrations
-- ====================================================================
CREATE OR REPLACE VIEW s3_migration_summary AS
SELECT 
    f.run_id,
    f.source_database,
    f.source_schema,
    f.source_table,
    COUNT(DISTINCT f.file_id) AS total_files,
    COUNT(DISTINCT CASE WHEN f.status = 'loaded' THEN f.file_id END) AS files_loaded,
    COUNT(DISTINCT CASE WHEN f.status = 'failed' THEN f.file_id END) AS files_failed,
    SUM(f.file_size_bytes) AS total_file_size_bytes,
    SUM(f.row_count) AS total_rows,
    MIN(f.created_at) AS unload_started_at,
    MAX(f.loaded_at) AS last_file_loaded_at,
    ROUND(
        COUNT(DISTINCT CASE WHEN f.status = 'loaded' THEN f.file_id END)::NUMERIC 
        / NULLIF(COUNT(DISTINCT f.file_id), 0) * 100, 
        1
    ) AS progress_percent
FROM s3_unload_files f
GROUP BY f.run_id, f.source_database, f.source_schema, f.source_table;

COMMENT ON VIEW s3_migration_summary IS 'Summary view of S3-staged migrations by run and table';

-- ====================================================================
-- View: s3_file_details
-- Detailed view of S3 files with load status
-- ====================================================================
CREATE OR REPLACE VIEW s3_file_details AS
SELECT 
    f.file_id,
    f.run_id,
    f.source_database || '.' || f.source_schema || '.' || f.source_table AS full_table_name,
    f.chunk_id,
    f.s3_url,
    f.file_size_bytes,
    ROUND(f.file_size_bytes / 1024.0 / 1024.0, 2) AS file_size_mb,
    f.row_count,
    f.status AS file_status,
    f.created_at AS unloaded_at,
    f.loaded_at,
    l.load_method,
    l.rows_loaded,
    l.duration_seconds AS load_duration_seconds,
    l.status AS load_status,
    CASE 
        WHEN f.status = 'loaded' THEN '✅ Loaded'
        WHEN f.status = 'loading' THEN '⏳ Loading'
        WHEN f.status = 'failed' THEN '❌ Failed'
        WHEN f.status = 'created' THEN '📦 Ready'
        ELSE f.status
    END AS status_icon
FROM s3_unload_files f
LEFT JOIN s3_load_progress l ON f.file_id = l.file_id AND l.status = 'completed'
ORDER BY f.run_id, f.source_table, f.chunk_id, f.created_at;

COMMENT ON VIEW s3_file_details IS 'Detailed view of S3 files with load status';

-- ====================================================================
-- Grant permissions (adjust as needed for your setup)
-- ====================================================================
-- GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA migration_status TO your_migration_user;
-- GRANT USAGE ON ALL SEQUENCES IN SCHEMA migration_status TO your_migration_user;
-- GRANT SELECT ON ALL TABLES IN SCHEMA migration_status TO your_readonly_user;

-- ====================================================================
-- Success message
-- ====================================================================
DO $$
BEGIN
    RAISE NOTICE '✅ S3 migration tracking tables created successfully!';
    RAISE NOTICE '';
    RAISE NOTICE 'Tables created:';
    RAISE NOTICE '  - s3_unload_files (tracks Snowflake UNLOAD files)';
    RAISE NOTICE '  - s3_load_progress (tracks PostgreSQL load progress)';
    RAISE NOTICE '';
    RAISE NOTICE 'Views created:';
    RAISE NOTICE '  - s3_migration_summary (summary by run and table)';
    RAISE NOTICE '  - s3_file_details (detailed file status)';
    RAISE NOTICE '';
    RAISE NOTICE 'You can now proceed with S3-staged migrations!';
END $$;


