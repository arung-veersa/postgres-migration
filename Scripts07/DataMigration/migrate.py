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
    
    def __init__(self, config_path: str, log_level: str = "INFO"):
        self.config_path = config_path
        self.logger = setup_logging(log_level)
        
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
            
            # Create migration run
            config_hash = self.config_loader.get_config_hash()
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
    
    def _process_table(self, source: Dict[str, Any], table: Dict[str, Any]) -> int:
        """Process a single table"""
        source_table = table['source']
        target_table = table['target']
        
        self.logger.info(f"\n[{source_table}] Starting migration...")
        
        with Timer(f"Table migration: {source_table}", self.logger):
            # Create table status
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
            
            # Handle truncate
            if table.get('truncate_onstart', False):
                worker = MigrationWorker(
                    self.sf_manager, self.pg_manager, self.status_tracker,
                    source, table, self.global_config['max_retry_attempts']
                )
                worker.truncate_table()
            
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
                # Determine chunking strategy
                self.logger.info(f"[{source_table}] Determining chunking strategy...")
                chunking_strategy = ChunkingStrategyFactory.create_strategy(
                    self.sf_manager,
                    source['source_sf_database'],
                    source['source_sf_schema'],
                    source_table,
                    table,
                    self.global_config['batch_size']
                )
                
                chunks = chunking_strategy.create_chunks()
                
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
                
                # Create chunk statuses
                for chunk in chunks:
                    self.status_tracker.create_chunk_status(
                        self.run_id,
                        source['source_sf_database'],
                        source['source_sf_schema'],
                        source_table,
                        chunk.chunk_id,
                        chunk.metadata
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
                    f"[{source_table}] Created {len(chunks)} chunks, "
                    f"processing with {self.global_config['parallel_threads']} threads..."
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
        
        worker = MigrationWorker(
            self.sf_manager, self.pg_manager, self.status_tracker,
            source, table, self.global_config['max_retry_attempts']
        )
        
        with ThreadPoolExecutor(max_workers=self.global_config['parallel_threads']) as executor:
            # Submit all chunks
            future_to_chunk = {
                executor.submit(
                    worker.process_chunk,
                    str(self.run_id),
                    chunk.chunk_id,
                    chunk.filter_sql
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
    
    args = parser.parse_args()
    
    # Load environment variables
    if os.path.exists(args.env_file):
        load_dotenv(args.env_file)
        print(f"✓ Loaded environment variables from {args.env_file}")
    else:
        print(f"ℹ No .env file found at {args.env_file}, using system environment")
    
    # Run migration
    orchestrator = MigrationOrchestrator(args.config, args.log_level)
    
    if args.dry_run:
        orchestrator.dry_run()
    else:
        orchestrator.run()


if __name__ == "__main__":
    main()

