#!/usr/bin/env python3
"""
PostgreSQL Migration Tool
Migrates data from Snowflake to PostgreSQL using configuration-driven approach
"""

import sys
import os
import argparse
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Any, List

# Add lib to path
sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv

from lib.config_loader import ConfigLoader
from lib.config_validator import validate_config
from lib.connections import ConnectionFactory
from lib.chunking import ChunkingStrategyFactory
from lib.status_tracker import StatusTracker
from lib.index_manager import IndexManager
from lib.migration_worker import MigrationWorker
from lib.utils import setup_logging, Timer, format_number, format_duration, logger


class MigrationOrchestrator:
    """Orchestrates the entire migration process"""
    
    def __init__(self, config_path: str, log_level: str = "INFO", args=None):
        self.config_path = config_path
        self.logger = setup_logging(log_level)
        self.args = args  # Store CLI arguments
        
        # Load configuration
        self.config_loader = ConfigLoader(config_path)
        self.config = self.config_loader.load()
        self.global_config = self.config_loader.get_global_config()
        
        # Validate configuration
        self.logger.info("Validating configuration...")
        if not validate_config(self.config):
            raise ValueError("Configuration validation failed. Please fix errors and try again.")
        self.logger.info("✓ Configuration validation passed")
        
        # Initialize connection factory
        self.conn_factory = ConnectionFactory(self.config)
        self.sf_manager = self.conn_factory.get_snowflake_manager()
        self.pg_manager = self.conn_factory.get_postgres_manager()
        
        # Will be initialized during run
        self.status_tracker: StatusTracker = None
        self.run_id = None
        self.start_time = None
        self.resuming = False  # Track if this is a resume operation
    
    def run(self):
        """Execute the migration"""
        self.start_time = time.time()
        
        try:
            self.logger.info("=" * 80)
            self.logger.info("PostgreSQL Migration Tool - Starting")
            self.logger.info("=" * 80)
            
            # Get enabled sources
            sources = self.config_loader.get_enabled_sources()
            if not sources:
                self.logger.warning("No enabled sources found in configuration")
                return
            
            # Count total tables
            total_tables = sum(
                len(self.config_loader.get_enabled_tables(source))
                for source in sources
            )
            
            self.logger.info(f"Configuration:")
            self.logger.info(f"  Sources: {len(sources)}")
            self.logger.info(f"  Total tables: {total_tables}")
            self.logger.info(f"  Parallel threads: {self.global_config['parallel_threads']}")
            self.logger.info(f"  Batch size: {format_number(self.global_config['batch_size'])}")
            
            # Initialize status tracking (use first target database)
            first_target_db = sources[0]['target_pg_database']
            self.status_tracker = StatusTracker(self.pg_manager, first_target_db)
            
            # Initialize status schema
            self.logger.info("Initializing migration status schema...")
            self.pg_manager.initialize_status_schema(first_target_db, "schema.sql")
            
            # Check for resumable run (unless --no-resume specified)
            config_hash = self.config_loader.get_config_hash()
            resumable_run = None
            
            if self.args and self.args.resume_run_id:
                # User specified run ID to resume
                self.run_id = uuid.UUID(self.args.resume_run_id)
                self.resuming = True
                self.logger.info(f"Resuming specified run: {self.run_id}")
            elif not (self.args and self.args.no_resume):
                # Auto-detect resumable run
                max_age = self.args.resume_max_age if self.args else 12
                resumable_run = self.status_tracker.find_resumable_run(config_hash, max_age)
                
                if resumable_run:
                    self._display_resume_warning(resumable_run)
                    self.run_id = resumable_run['run_id']
                    self.resuming = True
            
            # Create new run if not resuming
            if not self.resuming:
                self.run_id = self.status_tracker.create_migration_run(
                    config_hash=config_hash,
                    total_sources=len(sources),
                    total_tables=total_tables,
                    metadata={'config_path': self.config_path}
                )
            
            self.logger.info(f"Migration Run ID: {self.run_id}")
            self.logger.info("=" * 80)
            
            # Process each source
            completed_tables = 0
            failed_tables = 0
            total_rows = 0
            
            for source in sources:
                source_name = source.get('source_name', 'unnamed')
                self.logger.info(f"\nProcessing source: {source_name}")
                self.logger.info("-" * 80)
                
                tables = self.config_loader.get_enabled_tables(source)
                
                for table in tables:
                    try:
                        rows = self._process_table(source, table)
                        total_rows += rows
                        completed_tables += 1
                    except Exception as e:
                        self.logger.error(f"Failed to process table {table['source']}: {e}")
                        failed_tables += 1
            
            # Update final status
            final_status = 'completed' if failed_tables == 0 else 'partial'
            self.status_tracker.update_run_status(
                self.run_id,
                status=final_status,
                completed_tables=completed_tables,
                failed_tables=failed_tables,
                total_rows_copied=total_rows
            )
            
            # Print summary
            self._print_summary(completed_tables, failed_tables, total_rows)
            
        except Exception as e:
            self.logger.error(f"Migration failed: {e}", exc_info=True)
            if self.status_tracker and self.run_id:
                self.status_tracker.update_run_status(
                    self.run_id, status='failed', error_message=str(e)
                )
            raise
        finally:
            # Close connections
            self.conn_factory.close_all()
    
    def _display_resume_warning(self, resumable_run: Dict):
        """Display warning message before resuming with pause for user cancellation"""
        import time
        
        age_hours = (time.time() - resumable_run['started_at'].timestamp()) / 3600
        
        self.logger.warning("=" * 80)
        self.logger.warning("⚠️  RESUMING INCOMPLETE MIGRATION")
        self.logger.warning("=" * 80)
        self.logger.warning(f"  Run ID: {resumable_run['run_id']}")
        self.logger.warning(f"  Status: {resumable_run['status']}")
        self.logger.warning(f"  Started: {resumable_run['started_at']} ({age_hours:.1f} hours ago)")
        self.logger.warning(f"  Progress:")
        self.logger.warning(f"    - Total tables: {resumable_run['total_tables']}")
        self.logger.warning(f"    - Completed: {resumable_run['completed_tables']}")
        self.logger.warning(f"    - Failed: {resumable_run['failed_tables']}")
        self.logger.warning(f"    - Rows copied: {format_number(resumable_run['total_rows_copied'])}")
        self.logger.warning("")
        self.logger.warning("  The migration will resume from where it left off.")
        self.logger.warning("  - Completed tables will be skipped")
        self.logger.warning("  - Partial tables will continue from pending chunks")
        self.logger.warning("")
        self.logger.warning("  Press Ctrl+C to cancel")
        self.logger.warning("  Use --no-resume flag to force fresh start")
        self.logger.warning("")
        self.logger.warning("  Continuing in 5 seconds...")
        self.logger.warning("=" * 80)
        
        try:
            time.sleep(5)
        except KeyboardInterrupt:
            self.logger.info("\n\n⚠️  Resume cancelled by user")
            raise
    
    def _check_is_initial_full_load(self, source: Dict[str, Any], table: Dict[str, Any]) -> bool:
        """
        Check if this is an initial full load (empty table + no watermark).
        CRITICAL: Must be called ONCE before any threads start to avoid race conditions.
        
        Returns:
            bool: True if table is empty AND no watermark exists (initial full load)
        """
        # If truncate_onstart, it's not a "natural" initial load
        if table.get('truncate_onstart', False):
            return False
        
        # If no uniqueness columns, can't use UPSERT anyway
        if not table.get('uniqueness_columns'):
            return False
        
        target_database = source['target_pg_database']
        target_schema = source['target_pg_schema']
        target_table = table['target']
        target_watermark = table.get('target_watermark')
        
        try:
            # Check if table is empty
            conn = self.pg_manager.get_connection(target_database)
            cursor = conn.cursor()
            try:
                cursor.execute(
                    f"SELECT COUNT(*) = 0 FROM {target_schema}.{target_table}"
                )
                is_empty = cursor.fetchone()[0]
            finally:
                cursor.close()
                self.pg_manager.return_connection(conn)
            
            if not is_empty:
                # Table has data - not an initial load
                return False
            
            # Table is empty - check if watermark exists
            if target_watermark:
                max_watermark = self.pg_manager.get_max_watermark(
                    target_database, target_schema, target_table, target_watermark
                )
                if max_watermark:
                    # Has watermark but empty table - unusual, play it safe with UPSERT
                    return False
            
            # Table is empty AND no watermark - initial full load!
            return True
            
        except Exception as e:
            self.logger.warning(
                f"Could not determine initial load status: {e}. Defaulting to UPSERT mode."
            )
            return False
    
    def _reconstruct_chunks_from_status(self, pending_chunk_data: List[Dict]) -> List:
        """Reconstruct ChunkInfo objects from stored chunk status data"""
        from lib.chunking import ChunkInfo
        
        chunks = []
        for chunk_data in pending_chunk_data:
            metadata = chunk_data['chunk_range']
            
            # Get filter_sql from metadata or reconstruct it
            if 'filter_sql' in metadata:
                filter_sql = metadata['filter_sql']
            else:
                # Fallback: reconstruct based on strategy (for old data)
                strategy = metadata.get('strategy', '')
                if strategy == 'grouped_values':
                    # Reconstruct from values array
                    values = metadata.get('values', [])
                    id_column = metadata.get('id_column', '')
                    if values and id_column:
                        values_str = ", ".join([f"'{v}'" for v in values])
                        filter_sql = f"{id_column} IN ({values_str})"
                    else:
                        filter_sql = "1=1"  # Fallback
                else:
                    # For other strategies, use generic filter
                    filter_sql = "1=1"
            
            chunks.append(ChunkInfo(
                chunk_id=chunk_data['chunk_id'],
                filter_sql=filter_sql,
                estimated_rows=metadata.get('estimated_rows', 0),
                metadata=metadata
            ))
        
        return chunks
    
    def _create_fresh_chunks(self, source: Dict[str, Any], table: Dict[str, Any], 
                             source_table: str) -> List:
        """Create fresh chunks using chunking strategy"""
        from lib.chunking import ChunkingStrategyFactory
        
        # OPTIMIZATION: Get max watermark from target for incremental loads
        max_target_watermark = None
        source_watermark = table.get('source_watermark')
        target_watermark = table.get('target_watermark')
        truncate_onstart = table.get('truncate_onstart', False)
        
        if source_watermark and target_watermark and not truncate_onstart:
            # This is an incremental load - get max watermark to pre-filter chunking
            try:
                self.logger.debug(
                    f"[{source_table}] Querying max watermark from "
                    f"{source['target_pg_database']}.{source['target_pg_schema']}.{table['target']}"
                )
                max_target_watermark = self.pg_manager.get_max_watermark(
                    source['target_pg_database'],
                    source['target_pg_schema'],
                    table['target'],
                    target_watermark
                )
                self.logger.debug(f"[{source_table}] Max target watermark: {max_target_watermark}")
                
                if max_target_watermark:
                    self.logger.info(
                        f"[{source_table}] Incremental load optimization: "
                        f"Will only chunk data after {max_target_watermark}"
                    )
                else:
                    self.logger.info(
                        f"[{source_table}] No existing watermark found - performing full load"
                    )
            except Exception as e:
                self.logger.warning(
                    f"[{source_table}] Could not get max watermark for optimization: {e}"
                )
        
        self.logger.info(f"[{source_table}] Determining chunking strategy...")
        
        # Determine batch size: Use larger batch for full loads when COPY mode will be used
        # For initial full loads (no watermark), use batch_size_copy_mode if configured
        batch_size = self.global_config['batch_size']
        if not max_target_watermark and not truncate_onstart:
            # This is a full load on empty table - will use COPY mode
            batch_size_copy = self.global_config.get('batch_size_copy_mode')
            if batch_size_copy and batch_size_copy > batch_size:
                self.logger.info(
                    f"[{source_table}] Initial full load detected - using larger batch size "
                    f"({format_number(batch_size_copy)} vs {format_number(batch_size)}) for faster COPY"
                )
                batch_size = batch_size_copy
        
        chunking_strategy = ChunkingStrategyFactory.create_strategy(
            self.sf_manager,
            source['source_sf_database'],
            source['source_sf_schema'],
            source_table,
            table,
            batch_size,
            max_target_watermark
        )
        
        chunks = chunking_strategy.create_chunks()
        
        if not chunks:
            self.logger.warning(f"[{source_table}] No chunks created (no data?)")
            return []
        
        # Create chunk statuses
        for chunk in chunks:
            # Enhance metadata with filter_sql (smart conditional storage)
            strategy = chunk.metadata.get('strategy', '')
            if strategy == 'grouped_values':
                # Don't store filter_sql for grouped_values (already has 'values' array)
                enhanced_metadata = chunk.metadata
            else:
                # Store filter_sql for efficient reconstruction on resume
                enhanced_metadata = {
                    **chunk.metadata,
                    'filter_sql': chunk.filter_sql,
                    'estimated_rows': chunk.estimated_rows
                }
            
            self.status_tracker.create_chunk_status(
                self.run_id,
                source['source_sf_database'],
                source['source_sf_schema'],
                source_table,
                chunk.chunk_id,
                enhanced_metadata
            )
        
        return chunks
    
    def _process_table(self, source: Dict[str, Any], table: Dict[str, Any]) -> int:
        """Process a single table"""
        source_table = table['source']
        target_table = table['target']
        
        # Check if resuming and table already completed
        if self.resuming:
            table_progress = self.status_tracker.get_table_status(
                self.run_id,
                source['source_sf_database'],
                source['source_sf_schema'],
                source_table
            )
            
            if table_progress and table_progress['status'] == 'completed':
                self.logger.info(f"\n[{source_table}] ✓ Already completed, skipping...")
                return table_progress['total_rows_copied']
        
        self.logger.info(f"\n[{source_table}] Starting migration...")
        
        with Timer(f"Table migration: {source_table}", self.logger):
            # Create or update table status
            self.status_tracker.create_table_status(
                run_id=self.run_id,
                source_name=source.get('source_name', 'unnamed'),
                source_database=source['source_sf_database'],
                source_schema=source['source_sf_schema'],
                source_table=source_table,
                target_database=source['target_pg_database'],
                target_schema=source['target_pg_schema'],
                target_table=target_table,
                total_chunks=0  # Will update after chunking
            )
            
            # Mark table as in progress
            self.status_tracker.update_table_status(
                self.run_id,
                source['source_sf_database'],
                source['source_sf_schema'],
                source_table,
                'in_progress'
            )
            
            # Handle truncate (skip on resume to preserve existing data)
            if table.get('truncate_onstart', False) and not self.resuming:
                worker = MigrationWorker(
                    self.sf_manager, self.pg_manager, self.status_tracker,
                    source, table, self.global_config['max_retry_attempts']
                )
                worker.truncate_table()
            elif table.get('truncate_onstart', False) and self.resuming:
                self.logger.info(f"[{source_table}] Skipping truncate on resume (preserving existing data)")
            
            # Handle index disabling
            indexes = []
            constraints = []
            if table.get('disable_index', False):
                index_manager = IndexManager(self.pg_manager, source['target_pg_database'])
                indexes, constraints = index_manager.disable_indexes(
                    source['target_pg_schema'], target_table
                )
                self.status_tracker.mark_indexes_disabled(
                    self.run_id,
                    source['source_sf_database'],
                    source['source_sf_schema'],
                    source_table
                )
            
            try:
                # Check if we're resuming this table
                if self.resuming:
                    pending_chunk_data = self.status_tracker.get_pending_chunks(
                        self.run_id,
                        source['source_sf_database'],
                        source['source_sf_schema'],
                        source_table
                    )
                    
                    if pending_chunk_data:
                        # Resume with pending chunks
                        self.logger.info(
                            f"[{source_table}] Resuming with {len(pending_chunk_data)} pending chunks..."
                        )
                        chunks = self._reconstruct_chunks_from_status(pending_chunk_data)
                    else:
                        # No pending chunks, table might be complete or needs fresh start
                        self.logger.info(f"[{source_table}] No pending chunks, starting fresh chunking...")
                        chunks = self._create_fresh_chunks(source, table, source_table)
                else:
                    # Fresh start: create chunks normally
                    chunks = self._create_fresh_chunks(source, table, source_table)
                
                if not chunks:
                    self.logger.warning(f"[{source_table}] No chunks created (no data?)")
                    self.status_tracker.update_table_status(
                        self.run_id,
                        source['source_sf_database'],
                        source['source_sf_schema'],
                        source_table,
                        'completed',
                        completed_chunks=0,
                        total_rows_copied=0
                    )
                    return 0
                
                # Update table status with chunk count
                self.status_tracker.update_table_status(
                    self.run_id,
                    source['source_sf_database'],
                    source['source_sf_schema'],
                    source_table,
                    'in_progress'
                )
                
                # Update total chunks
                conn = self.pg_manager.get_connection(source['target_pg_database'])
                cursor = conn.cursor()
                try:
                    cursor.execute(
                        """UPDATE migration_status.migration_table_status 
                           SET total_chunks = %s 
                           WHERE run_id = %s AND source_database = %s 
                           AND source_schema = %s AND source_table = %s""",
                        (len(chunks), str(self.run_id), source['source_sf_database'],
                         source['source_sf_schema'], source_table)
                    )
                    conn.commit()
                finally:
                    cursor.close()
                    self.pg_manager.return_connection(conn)
                
                self.logger.info(
                    f"[{source_table}] Processing {len(chunks)} chunks with "
                    f"{self.global_config['parallel_threads']} threads..."
                )
                
                # Process chunks in parallel
                total_rows = self._process_chunks_parallel(source, table, chunks)
                
                # Mark table as completed
                self.status_tracker.update_table_status(
                    self.run_id,
                    source['source_sf_database'],
                    source['source_sf_schema'],
                    source_table,
                    'completed',
                    completed_chunks=len(chunks),
                    total_rows_copied=total_rows
                )
                
                self.logger.info(
                    f"✓ [{source_table}] Completed: {format_number(total_rows)} rows migrated"
                )
                
                return total_rows
                
            finally:
                # Restore indexes if they were disabled
                if table.get('disable_index', False) and (indexes or constraints):
                    index_manager = IndexManager(self.pg_manager, source['target_pg_database'])
                    index_manager.restore_indexes(
                        source['target_pg_schema'], target_table, indexes, constraints
                    )
                    index_manager.analyze_table(source['target_pg_schema'], target_table)
                    self.status_tracker.mark_indexes_restored(
                        self.run_id,
                        source['source_sf_database'],
                        source['source_sf_schema'],
                        source_table
                    )
    
    def _process_chunks_parallel(self, source: Dict[str, Any], 
                                table: Dict[str, Any], chunks: List) -> int:
        """Process chunks in parallel using thread pool"""
        total_rows = 0
        completed = 0
        failed = 0
        
        # CRITICAL: Determine if this is an initial full load BEFORE any threads start
        # This must happen ONCE to avoid race conditions between threads
        is_initial_full_load = self._check_is_initial_full_load(source, table)
        
        worker = MigrationWorker(
            self.sf_manager, self.pg_manager, self.status_tracker,
            source, table, self.global_config['max_retry_attempts'],
            is_initial_full_load=is_initial_full_load  # Pass the decision to worker
        )
        
        with ThreadPoolExecutor(max_workers=self.global_config['parallel_threads']) as executor:
            # Submit all chunks
            future_to_chunk = {
                executor.submit(
                    worker.process_chunk,
                    str(self.run_id),
                    chunk.chunk_id,
                    chunk.filter_sql,
                    chunk.metadata  # Pass chunk metadata for chunk-scoped watermark
                ): chunk
                for chunk in chunks
            }
            
            # Process completed chunks
            for future in as_completed(future_to_chunk):
                chunk = future_to_chunk[future]
                try:
                    rows = future.result()
                    total_rows += rows
                    completed += 1
                    
                    if completed % 10 == 0 or completed == len(chunks):
                        self.logger.info(
                            f"  Progress: {completed}/{len(chunks)} chunks "
                            f"({format_number(total_rows)} rows)"
                        )
                except Exception as e:
                    failed += 1
                    self.logger.error(f"  Chunk {chunk.chunk_id} failed: {e}")
        
        if failed > 0:
            raise Exception(f"{failed}/{len(chunks)} chunks failed")
        
        return total_rows
    
    def _print_summary(self, completed: int, failed: int, total_rows: int):
        """Print migration summary"""
        duration = time.time() - self.start_time
        
        self.logger.info("\n" + "=" * 80)
        self.logger.info("Migration Summary")
        self.logger.info("=" * 80)
        self.logger.info(f"Run ID: {self.run_id}")
        self.logger.info(f"Duration: {format_duration(duration)}")
        self.logger.info(f"Tables completed: {completed}")
        self.logger.info(f"Tables failed: {failed}")
        self.logger.info(f"Total rows migrated: {format_number(total_rows)}")
        
        if total_rows > 0 and duration > 0:
            rate = total_rows / duration
            self.logger.info(f"Average rate: {format_number(int(rate))} rows/second")
        
        if failed == 0:
            self.logger.info("\n✓ Migration completed successfully!")
        else:
            self.logger.warning(f"\n⚠ Migration completed with {failed} failures")
        
        self.logger.info("=" * 80)
    
    def dry_run(self):
        """
        Dry run: validate configuration and show what would be migrated
        """
        self.logger.info("=" * 80)
        self.logger.info("DRY RUN MODE - No data will be migrated")
        self.logger.info("=" * 80)
        
        # Configuration summary
        sources = self.config.get('sources', [])
        enabled_sources = [s for s in sources if s.get('enabled', False)]
        
        total_tables = 0
        enabled_tables = 0
        
        self.logger.info(f"\nConfiguration Summary:")
        self.logger.info(f"  Total sources: {len(sources)}")
        self.logger.info(f"  Enabled sources: {len(enabled_sources)}")
        self.logger.info(f"  Parallel threads: {self.global_config.get('parallel_threads')}")
        self.logger.info(f"  Batch size: {self.global_config.get('batch_size'):,}")
        
        for source in sources:
            source_name = source['source_name']
            source_enabled = source.get('enabled', False)
            tables = source.get('tables', [])
            source_enabled_tables = [t for t in tables if t.get('enabled', False)]
            
            total_tables += len(tables)
            if source_enabled:
                enabled_tables += len(source_enabled_tables)
            
            status = "✓ ENABLED" if source_enabled else "✗ DISABLED"
            self.logger.info(f"\n  Source: {source_name} [{status}]")
            self.logger.info(f"    Database: {source['source_sf_database']}.{source['source_sf_schema']}")
            self.logger.info(f"    → {source['target_pg_database']}.{source['target_pg_schema']}")
            self.logger.info(f"    Tables: {len(source_enabled_tables)}/{len(tables)} enabled")
            
            if source_enabled and source_enabled_tables:
                for table in source_enabled_tables[:5]:  # Show first 5
                    source_table = table['source']
                    target_table = table['target']
                    self.logger.info(f"      • {source_table} → {target_table}")
                
                if len(source_enabled_tables) > 5:
                    self.logger.info(f"      ... and {len(source_enabled_tables) - 5} more")
        
        self.logger.info(f"\n{'=' * 80}")
        self.logger.info(f"Summary: {enabled_tables} tables will be migrated from {len(enabled_sources)} sources")
        self.logger.info(f"{'=' * 80}")
        
        if enabled_tables == 0:
            self.logger.warning("⚠️  No tables are enabled for migration!")
        else:
            self.logger.info(f"\n✓ Ready to migrate! Run without --dry-run to start migration.")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Migrate data from Snowflake to PostgreSQL')
    parser.add_argument(
        '--config',
        default='config.json',
        help='Path to configuration file (default: config.json)'
    )
    parser.add_argument(
        '--log-level',
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging level (default: INFO)'
    )
    parser.add_argument(
        '--env-file',
        default='.env',
        help='Path to .env file (default: .env)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Validate configuration and show what would be migrated without actually migrating'
    )
    parser.add_argument(
        '--no-resume',
        action='store_true',
        help='Force fresh start, ignore existing incomplete runs'
    )
    parser.add_argument(
        '--resume-max-age',
        type=int,
        default=12,
        help='Maximum age (hours) of run to resume (default: 12)'
    )
    parser.add_argument(
        '--resume-run-id',
        type=str,
        help='Resume specific run ID (overrides auto-detection)'
    )
    
    args = parser.parse_args()
    
    # Load environment variables
    if os.path.exists(args.env_file):
        load_dotenv(args.env_file)
        print(f"✓ Loaded environment variables from {args.env_file}")
    else:
        print(f"ℹ No .env file found at {args.env_file}, using system environment")
    
    # Run migration
    orchestrator = MigrationOrchestrator(args.config, args.log_level, args)
    
    if args.dry_run:
        orchestrator.dry_run()
    else:
        orchestrator.run()


if __name__ == "__main__":
    main()

