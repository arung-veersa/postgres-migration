-- Migration Status Tracking Tables
-- These tables track the progress of data migration operations
-- to enable resume capability and monitoring

-- Drop existing tables if re-initializing (be careful in production!)
-- DROP TABLE IF EXISTS migration_status.migration_chunk_status CASCADE;
-- DROP TABLE IF EXISTS migration_status.migration_table_status CASCADE;
-- DROP TABLE IF EXISTS migration_status.migration_runs CASCADE;
-- DROP SCHEMA IF EXISTS migration_status CASCADE;

-- Create dedicated schema for migration tracking
CREATE SCHEMA IF NOT EXISTS migration_status;

-- Overall migration run tracking
CREATE TABLE IF NOT EXISTS migration_status.migration_runs (
    run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    status VARCHAR(20) NOT NULL DEFAULT 'running' CHECK (status IN ('running', 'completed', 'failed', 'partial')),
    config_hash VARCHAR(64),  -- MD5 hash of config for tracking changes
    total_sources INTEGER DEFAULT 0,
    total_tables INTEGER DEFAULT 0,
    completed_tables INTEGER DEFAULT 0,
    failed_tables INTEGER DEFAULT 0,
    total_rows_copied BIGINT DEFAULT 0,
    error_message TEXT,
    metadata JSONB,  -- Store additional context (Lambda request ID, user, etc.)
    created_by VARCHAR(255),
    CONSTRAINT valid_completed_at CHECK (completed_at IS NULL OR completed_at >= started_at)
);

-- Per-table migration tracking
CREATE TABLE IF NOT EXISTS migration_status.migration_table_status (
    run_id UUID NOT NULL REFERENCES migration_status.migration_runs(run_id) ON DELETE CASCADE,
    source_name VARCHAR(255) NOT NULL,
    source_database VARCHAR(255) NOT NULL,
    source_schema VARCHAR(255) NOT NULL,
    source_table VARCHAR(255) NOT NULL,
    target_database VARCHAR(255) NOT NULL,
    target_schema VARCHAR(255) NOT NULL,
    target_table VARCHAR(255) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'in_progress', 'completed', 'failed', 'skipped')),
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    total_chunks INTEGER DEFAULT 0,
    completed_chunks INTEGER DEFAULT 0,
    failed_chunks INTEGER DEFAULT 0,
    total_rows_copied BIGINT DEFAULT 0,
    indexes_disabled BOOLEAN DEFAULT FALSE,
    indexes_restored BOOLEAN DEFAULT FALSE,
    error_message TEXT,
    metadata JSONB,
    PRIMARY KEY (run_id, source_database, source_schema, source_table),
    CONSTRAINT valid_table_completed_at CHECK (completed_at IS NULL OR completed_at >= started_at)
);

-- Per-chunk migration tracking for granular resume capability
CREATE TABLE IF NOT EXISTS migration_status.migration_chunk_status (
    run_id UUID NOT NULL,
    source_database VARCHAR(255) NOT NULL,
    source_schema VARCHAR(255) NOT NULL,
    source_table VARCHAR(255) NOT NULL,
    chunk_id INTEGER NOT NULL,
    chunk_range JSONB NOT NULL,  -- Store chunk boundaries/filter as JSON
    status VARCHAR(20) NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'in_progress', 'completed', 'failed')),
    rows_copied INTEGER DEFAULT 0,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    retry_count INTEGER DEFAULT 0,
    error_message TEXT,
    PRIMARY KEY (run_id, source_database, source_schema, source_table, chunk_id),
    FOREIGN KEY (run_id, source_database, source_schema, source_table) 
        REFERENCES migration_status.migration_table_status(run_id, source_database, source_schema, source_table) 
        ON DELETE CASCADE,
    CONSTRAINT valid_chunk_completed_at CHECK (completed_at IS NULL OR completed_at >= started_at)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_migration_runs_status ON migration_status.migration_runs(status, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_migration_runs_completed ON migration_status.migration_runs(completed_at DESC) WHERE completed_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_table_status_run_status ON migration_status.migration_table_status(run_id, status);
CREATE INDEX IF NOT EXISTS idx_table_status_source ON migration_status.migration_table_status(source_database, source_schema, source_table);
CREATE INDEX IF NOT EXISTS idx_table_status_target ON migration_status.migration_table_status(target_database, target_schema, target_table);

CREATE INDEX IF NOT EXISTS idx_chunk_status_run_status ON migration_status.migration_chunk_status(run_id, status);
CREATE INDEX IF NOT EXISTS idx_chunk_status_pending ON migration_status.migration_chunk_status(run_id, source_database, source_schema, source_table, status) 
    WHERE status IN ('pending', 'failed');

-- View for monitoring active migrations
CREATE OR REPLACE VIEW migration_status.v_active_migrations AS
SELECT 
    r.run_id,
    r.status as run_status,
    r.started_at,
    EXTRACT(EPOCH FROM (COALESCE(r.completed_at, CURRENT_TIMESTAMP) - r.started_at)) as duration_seconds,
    r.total_tables,
    r.completed_tables,
    r.failed_tables,
    r.total_rows_copied,
    COUNT(DISTINCT t.source_table) FILTER (WHERE t.status = 'in_progress') as tables_in_progress,
    COUNT(DISTINCT c.chunk_id) FILTER (WHERE c.status = 'in_progress') as chunks_in_progress,
    COUNT(DISTINCT c.chunk_id) FILTER (WHERE c.status = 'failed') as chunks_failed
FROM migration_status.migration_runs r
LEFT JOIN migration_status.migration_table_status t ON r.run_id = t.run_id
LEFT JOIN migration_status.migration_chunk_status c ON r.run_id = c.run_id
WHERE r.status IN ('running', 'partial')
GROUP BY r.run_id, r.status, r.started_at, r.completed_at, r.total_tables, r.completed_tables, r.failed_tables, r.total_rows_copied;

-- View for detailed table progress
CREATE OR REPLACE VIEW migration_status.v_table_progress AS
SELECT 
    t.run_id,
    t.source_name,
    t.source_table,
    t.target_table,
    t.status,
    t.total_chunks,
    t.completed_chunks,
    t.failed_chunks,
    CASE 
        WHEN t.total_chunks > 0 THEN ROUND((t.completed_chunks::NUMERIC / t.total_chunks::NUMERIC) * 100, 2)
        ELSE 0 
    END as completion_percentage,
    t.total_rows_copied,
    EXTRACT(EPOCH FROM (COALESCE(t.completed_at, CURRENT_TIMESTAMP) - t.started_at)) as duration_seconds,
    t.started_at,
    t.completed_at
FROM migration_status.migration_table_status t
WHERE t.status IN ('in_progress', 'completed', 'failed')
ORDER BY t.started_at DESC;

COMMENT ON SCHEMA migration_status IS 'Schema for tracking data migration progress and enabling resume capability';
COMMENT ON TABLE migration_status.migration_runs IS 'Tracks overall migration run execution';
COMMENT ON TABLE migration_status.migration_table_status IS 'Tracks individual table migration progress';
COMMENT ON TABLE migration_status.migration_chunk_status IS 'Tracks chunk-level migration progress for granular resume';

