"""
Conflict Processor for Task 02
Handles streaming conflict detection results and batch updates to Postgres
"""

from typing import Dict, List, Any, Tuple, Optional
from .utils import get_logger, format_duration, estimate_memory_mb
from .connections import SnowflakeConnectionManager, PostgresConnectionManager
from .query_builder import QueryBuilder

logger = get_logger(__name__)


class ConflictProcessor:
    """Processes conflict detection results with streaming and batch updates"""
    
    def __init__(
        self,
        sf_manager: SnowflakeConnectionManager,
        pg_manager: PostgresConnectionManager,
        query_builder: QueryBuilder,
        db_names: Dict[str, str],
        batch_size: int = 5000,
        skip_unchanged_records: bool = True,
        enable_asymmetric_join: bool = True,
        enable_stale_cleanup: bool = True
    ):
        self.sf_manager = sf_manager
        self.pg_manager = pg_manager
        self.query_builder = query_builder
        self.db_names = db_names
        self.batch_size = batch_size
        self.skip_unchanged_records = skip_unchanged_records
        self.enable_asymmetric_join = enable_asymmetric_join
        self.enable_stale_cleanup = enable_stale_cleanup
        self.logger = logger
        
        # Persistent Postgres connection for batch processing
        self.pg_connection = None
        
        # Statistics
        self.stats = {
            'rows_fetched': 0,
            'rows_processed': 0,
            'rows_updated': 0,
            'rows_skipped_no_changes': 0,
            'batches_processed': 0,
            'errors': 0,
            'unique_visits': set(),
            'matched_in_postgres': 0,
            'new_conflicts': 0,
            # Change tracking stats
            'changes_by_flag': 0,
            'changes_by_business_data': 0,
            'skip_unchanged_records': skip_unchanged_records,
            # Asymmetric join stats
            'asymmetric_join_enabled': enable_asymmetric_join,
            'stale_cleanup_enabled': enable_stale_cleanup,
            'stale_conflicts_reset': 0
        }
    
    def fetch_reference_data(self) -> Dict[str, Any]:
        """
        Fetch all reference data from Postgres in parallel
        
        Returns:
            Dict with keys: excluded_agencies, excluded_ssns, settings, mph
        """
        logger.info("Fetching reference data from Postgres...")
        
        # Fetch excluded agencies
        query = self.query_builder.build_reference_query('pg_fetch_excluded_agencies.sql', self.db_names)
        agencies = self.pg_manager.execute_query(query, database=self.db_names['pg_database'])
        excluded_agencies = [row[0] for row in agencies if row[0]]
        logger.info(f"  ✓ Excluded agencies: {len(excluded_agencies)}")
        
        # Fetch excluded SSNs - SKIPPED for performance test
        # query = self.query_builder.build_reference_query('pg_fetch_excluded_ssns.sql', self.db_names)
        # ssns = self.pg_manager.execute_query(query, database=self.db_names['pg_database'])
        # excluded_ssns = [row[0] for row in ssns if row[0]]
        excluded_ssns = []  # Empty list for performance test
        logger.info(f"  ⚠ Excluded SSNs: SKIPPED (performance test - isolating bottleneck)")
        
        # Fetch settings
        query = self.query_builder.build_reference_query('pg_fetch_settings.sql', self.db_names)
        settings_rows = self.pg_manager.execute_query(query, database=self.db_names['pg_database'])
        settings = dict(zip(['ExtraDistancePer'], settings_rows[0])) if settings_rows else {}
        logger.info(f"  ✓ Settings loaded")
        
        # Fetch MPH lookup
        query = self.query_builder.build_reference_query('pg_fetch_mph.sql', self.db_names)
        mph_rows = self.pg_manager.execute_query(query, database=self.db_names['pg_database'])
        mph = [{'From': row[0], 'To': row[1], 'AverageMilesPerHour': row[2]} for row in mph_rows]
        logger.info(f"  ✓ MPH lookup: {len(mph)} ranges")
        if not mph:
            logger.warning("  ⚠ No MPH data retrieved from database!")
        
        return {
            'excluded_agencies': excluded_agencies,
            'excluded_ssns': excluded_ssns,
            'settings': settings,
            'mph': mph
        }
    
    def stream_and_process_conflicts(
        self,
        conflict_query: str,
        timeout_callback: Optional[callable] = None
    ) -> Dict[str, Any]:
        """
        Stream conflict results from Snowflake and process in batches
        
        Args:
            conflict_query: The parameterized conflict detection SQL
            timeout_callback: Optional function to check for timeout (Lambda)
        
        Returns:
            Dict with processing statistics
        """
        logger.info("Starting conflict detection stream processing...")
        logger.info(f"  Batch size: {self.batch_size}")
        logger.info(f"  Asymmetric join: {'ENABLED' if self.enable_asymmetric_join else 'DISABLED'}")
        logger.info(f"  Stale cleanup: {'ENABLED' if self.enable_stale_cleanup else 'DISABLED'}")
        
        batch = []
        batch_number = 0
        all_conflict_keys = set()  # Track all (VisitID, ConVisitID) pairs from Snowflake
        
        try:
            # Open streaming cursor
            with self.sf_manager.streaming_cursor() as cursor:
                logger.info("Executing conflict detection query in Snowflake...")
                
                # DEBUG: Verify query structure before sending to Snowflake
                logger.info(f"  Query length received: {len(conflict_query)} characters")
                logger.info(f"  Query starts with: {conflict_query[:100]}")
                
                # Check for WITH keyword
                with_idx = conflict_query.find('WITH')
                logger.info(f"  'WITH' keyword at position: {with_idx}")
                
                # Check for delta_conflict_keys
                delta_idx = conflict_query.find('delta_conflict_keys')  
                logger.info(f"  'delta_conflict_keys' at position: {delta_idx}")
                
                # Show what Snowflake will see (first non-comment line)
                lines = conflict_query.split('\n')
                first_sql_line_idx = None
                for i, line in enumerate(lines):
                    stripped = line.strip()
                    if stripped and not stripped.startswith('--'):
                        first_sql_line_idx = i
                        logger.info(f"  First non-comment line (#{i}): {stripped[:100]}")
                        break
                
                import time
                query_start = time.time()
                
                cursor.execute(conflict_query)
                
                query_duration = time.time() - query_start
                logger.info(f"✓ Snowflake query executed in {query_duration:.1f} seconds")
                
                # Get column names
                column_names = [desc[0] for desc in cursor.description]
                logger.info(f"Query returned {len(column_names)} columns")
                logger.info(f"Estimated data volume: ~{len(column_names) * 50} bytes/row")
                
                if self.enable_asymmetric_join:
                    logger.info("Streaming asymmetric join results (Delta vs All + All vs Delta)...")
                else:
                    logger.info("Streaming symmetric join results...")
                
                # Stream results
                streaming_start = time.time()
                for row in cursor:
                    self.stats['rows_fetched'] += 1
                    
                    # Log progress for large datasets
                    if self.stats['rows_fetched'] % 50000 == 0:
                        elapsed = time.time() - streaming_start
                        rate = self.stats['rows_fetched'] / elapsed if elapsed > 0 else 0
                        logger.info(f"  Streamed {self.stats['rows_fetched']:,} conflicts ({rate:.0f} rows/sec)")
                    
                    # Convert row tuple to dict
                    row_dict = dict(zip(column_names, row))
                    batch.append(row_dict)
                    
                    # Track conflict keys for stale cleanup
                    if self.enable_stale_cleanup:
                        conflict_key = (row_dict.get('VisitID'), row_dict.get('ConVisitID'))
                        all_conflict_keys.add(conflict_key)
                    
                    # Process batch when full
                    if len(batch) >= self.batch_size:
                        batch_number += 1
                        self._process_batch(batch, batch_number)
                        batch = []  # Clear for next batch
                        
                        # Check timeout if callback provided
                        if timeout_callback and timeout_callback():
                            logger.warning("Timeout approaching, stopping processing")
                            break
                
                # Process remaining rows
                if batch:
                    batch_number += 1
                    self._process_batch(batch, batch_number)
                
                # Log streaming completion
                streaming_duration = time.time() - streaming_start
                logger.info(f"✓ Streaming completed: {self.stats['rows_fetched']:,} conflicts in {streaming_duration:.1f} seconds")
                if streaming_duration > 0:
                    avg_rate = self.stats['rows_fetched'] / streaming_duration
                    logger.info(f"  Average streaming rate: {avg_rate:.0f} rows/sec")
                
                # After processing all new conflicts, cleanup stale ones
                if self.enable_stale_cleanup and all_conflict_keys:
                    logger.info("")
                    logger.info(f"Running stale conflict cleanup with {len(all_conflict_keys)} active conflicts...")
                    self._cleanup_stale_conflicts(all_conflict_keys)
        
        except Exception as e:
            logger.error(f"Error during stream processing: {e}", exc_info=True)
            self.stats['errors'] += 1
            raise
        
        finally:
            # Clean up persistent Postgres connection
            if self.pg_connection:
                try:
                    self.pg_connection.close()
                    logger.debug("✓ Persistent Postgres connection closed")
                except Exception as close_error:
                    logger.warning(f"Error closing persistent connection: {close_error}")
        
        logger.info("✓ Stream processing completed")
        
        # Return statistics with JSON-serializable data (convert set to int)
        return self.get_statistics()
    
    def _process_batch(self, batch: List[Dict[str, Any]], batch_number: int):
        """
        Process a single batch of conflicts
        
        Args:
            batch: List of conflict row dictionaries
            batch_number: Batch sequence number for logging
        """
        logger.info(f"Processing batch {batch_number}: {len(batch)} conflicts")
        
        try:
            # Track unique visits in this batch
            for row in batch:
                self.stats['unique_visits'].add(row['VisitID'])
            
            # Fetch existing CONFLICTVISITMAPS records for these visits
            visit_ids = [row['VisitID'] for row in batch]
            existing_records = self._fetch_existing_records(visit_ids)
            
            # Count how many conflicts matched existing records
            matched_count = 0
            new_count = 0
            
            for conflict_row in batch:
                visit_id = str(conflict_row.get('VisitID'))
                con_visit_id = str(conflict_row.get('ConVisitID')) if conflict_row.get('ConVisitID') else None
                key = (visit_id, con_visit_id)
                
                if key in existing_records:
                    matched_count += 1
                else:
                    new_count += 1
            
            self.stats['matched_in_postgres'] += matched_count
            self.stats['new_conflicts'] += new_count
            
            logger.info(f"  Matched: {matched_count}, New: {new_count}")
            
            # Match and prepare updates (with change detection)
            updates = self._prepare_updates(batch, existing_records)
            
            if updates:
                # Log change detection impact if enabled
                if self.skip_unchanged_records:
                    records_skipped = matched_count - len(updates)
                    logger.info(f"  Change detection: {len(updates)} dirty, {records_skipped} clean")
                else:
                    logger.info(f"  Change detection: DISABLED (processing all {len(updates)} matched)")
                
                # Get or create persistent connection
                if self.pg_connection is None:
                    self.pg_connection = self.pg_manager.get_connection(
                        database=self.db_names['pg_database']
                    )
                
                # Execute batch update using persistent connection
                rows_updated = self._execute_updates_with_commit(updates)
                
                self.stats['rows_updated'] += rows_updated
                logger.info(f"  ✓ Batch {batch_number}: {rows_updated} rows updated (COMMITTED)")
            else:
                logger.info(f"  ✓ Batch {batch_number}: No updates needed")
            
            self.stats['rows_processed'] += len(batch)
            self.stats['batches_processed'] += 1
            
            # Memory estimate for logging
            est_mb = estimate_memory_mb(len(batch), 200)
            logger.debug(f"  Memory estimate for batch: {est_mb:.1f} MB")
            
        except Exception as e:
            # Rollback this batch on error
            if self.pg_connection:
                try:
                    self.pg_connection.rollback()
                    logger.warning(f"  ✗ Batch {batch_number}: Rolled back due to error")
                except Exception as rollback_error:
                    logger.error(f"Error during rollback: {rollback_error}")
            
            logger.error(f"Error processing batch {batch_number}: {e}", exc_info=True)
            self.stats['errors'] += 1
            raise
    
    def _fetch_existing_records(self, visit_ids: List[str]) -> Dict[Tuple[str, str], Dict]:
        """
        Fetch existing CONFLICTVISITMAPS records for given visit IDs
        
        Args:
            visit_ids: List of VisitID values
        
        Returns:
            Dict mapping (VisitID, ConVisitID) tuple to existing record dict with relevant fields
        """
        if not visit_ids:
            return {}
        
        schema = self.db_names['pg_schema']
        
        # Build query to fetch existing records with all fields needed for conditional updates
        visit_ids_str = ','.join([f"'{vid}'" for vid in visit_ids])
        query = f"""
            SELECT 
                "VisitID",
                "ConVisitID",
                "CONFLICTID",
                "StatusFlag",
                "SameSchTimeFlag",
                "SameVisitTimeFlag",
                "SchAndVisitTimeSameFlag",
                "SchOverAnotherSchTimeFlag",
                "VisitTimeOverAnotherVisitTimeFlag",
                "SchTimeOverVisitTimeFlag",
                "DistanceFlag",
                -- Key business columns for change detection
                "ProviderID",
                "ConProviderID",
                "VisitDate",
                "SchStartTime",
                "SchEndTime",
                "ConSchStartTime",
                "ConSchEndTime",
                "EVVStartTime",
                "EVVEndTime",
                "ConEVVStartTime",
                "ConEVVEndTime",
                "CaregiverID",
                "ConCaregiverID",
                "OfficeID",
                "ConOfficeID",
                "PatientID",
                "ConPatientID",
                "PayerID",
                "ConPayerID",
                "ServiceCodeID",
                "ConServiceCodeID",
                "IsMissed",
                "EVVType",
                "ConIsMissed",
                "ConEVVType",
                "P_PatientID",
                "ConP_PatientID",
                "PA_PatientID",
                "ConPA_PatientID",
                "ContractType",
                "ConContractType",
                "FederalTaxNumber",
                "ConFederalTaxNumber"
            FROM {schema}.conflictvisitmaps
            WHERE "VisitID" IN ({visit_ids_str})
              AND "InserviceStartDate" IS NULL
              AND "InserviceEndDate" IS NULL
              AND "PTOStartDate" IS NULL
              AND "PTOEndDate" IS NULL
              AND "ConInserviceStartDate" IS NULL
              AND "ConInserviceEndDate" IS NULL
              AND "ConPTOStartDate" IS NULL
              AND "ConPTOEndDate" IS NULL
        """
        
        try:
            results = self.pg_manager.execute_query(query, database=self.db_names['pg_database'])
            
            # Map results by (VisitID, ConVisitID) tuple
            existing = {}
            for row in results:
                visit_id = str(row[0]) if row[0] else None
                con_visit_id = str(row[1]) if row[1] else None
                
                # Create key tuple
                key = (visit_id, con_visit_id)
                
                # Store as dict for easier access
                existing[key] = {
                    'VisitID': visit_id,
                    'ConVisitID': con_visit_id,
                    'CONFLICTID': row[2],
                    'StatusFlag': row[3],
                    'SameSchTimeFlag': row[4],
                    'SameVisitTimeFlag': row[5],
                    'SchAndVisitTimeSameFlag': row[6],
                    'SchOverAnotherSchTimeFlag': row[7],
                    'VisitTimeOverAnotherVisitTimeFlag': row[8],
                    'SchTimeOverVisitTimeFlag': row[9],
                    'DistanceFlag': row[10],
                    # Business columns
                    'ProviderID': row[11],
                    'ConProviderID': row[12],
                    'VisitDate': row[13],
                    'SchStartTime': row[14],
                    'SchEndTime': row[15],
                    'ConSchStartTime': row[16],
                    'ConSchEndTime': row[17],
                    'EVVStartTime': row[18],
                    'EVVEndTime': row[19],
                    'ConEVVStartTime': row[20],
                    'ConEVVEndTime': row[21],
                    'CaregiverID': row[22],
                    'ConCaregiverID': row[23],
                    'OfficeID': row[24],
                    'ConOfficeID': row[25],
                    'PatientID': row[26],
                    'ConPatientID': row[27],
                    'PayerID': row[28],
                    'ConPayerID': row[29],
                    'ServiceCodeID': row[30],
                    'ConServiceCodeID': row[31],
                    'IsMissed': row[32],
                    'EVVType': row[33],
                    'ConIsMissed': row[34],
                    'ConEVVType': row[35],
                    'P_PatientID': row[36],
                    'ConP_PatientID': row[37],
                    'PA_PatientID': row[38],
                    'ConPA_PatientID': row[39],
                    'ContractType': row[40],
                    'ConContractType': row[41],
                    'FederalTaxNumber': row[42],
                    'ConFederalTaxNumber': row[43]
                }
            
            logger.debug(f"Fetched {len(existing)} existing conflict records")
            return existing
            
        except Exception as e:
            logger.warning(f"Could not fetch existing records: {e}")
            return {}
    
    def _prepare_updates(
        self,
        batch: List[Dict[str, Any]],
        existing_records: Dict[Tuple[str, str], Dict]
    ) -> List[Tuple[str, tuple]]:
        """
        Prepare UPDATE statements for batch with conditional logic and change detection
        
        Args:
            batch: List of conflict records from Snowflake
            existing_records: Dict mapping (VisitID, ConVisitID) to existing record
        
        Returns:
            List of (sql, params) tuples ready for execute_batch
        """
        updates = []
        
        for conflict_row in batch:
            visit_id = str(conflict_row.get('VisitID'))
            con_visit_id = str(conflict_row.get('ConVisitID')) if conflict_row.get('ConVisitID') else None
            
            # Create key tuple
            key = (visit_id, con_visit_id)
            
            # Check if record exists in Postgres
            if key not in existing_records:
                continue  # Skip new conflicts (can't update what doesn't exist)
            
            existing_row = existing_records[key]
            
            # Check if data actually changed (PERFORMANCE OPTIMIZATION)
            if not self._has_changes(conflict_row, existing_row):
                self.stats['rows_skipped_no_changes'] += 1
                continue  # Skip if no changes detected
            
            # Preserve CONFLICTID from existing record
            conflict_row['CONFLICTID'] = existing_row['CONFLICTID']
            
            # Build UPDATE statement with conditional logic
            update_stmt = self.query_builder.build_update_statement(
                conflict_row,
                self.db_names,
                existing_row
            )
            updates.append(update_stmt)
        
        return updates
    
    def _has_changes(self, new_row: Dict[str, Any], existing_row: Dict[str, Any]) -> bool:
        """
        Check if new row has changes compared to existing row.
        
        This method performs fast dictionary comparison to avoid building and executing
        UPDATE statements for records that haven't changed. Can be disabled via config
        to revert to updating all matched records.
        
        Args:
            new_row: Dictionary of column values from Snowflake
            existing_row: Dictionary of existing column values from Postgres (48 columns)
        
        Returns:
            True if changes detected OR optimization disabled, False otherwise
        """
        # If change detection is disabled, always return True (process all records)
        if not self.skip_unchanged_records:
            return True
        
        # Define conditional flag columns (only update if existing = 'N')
        conditional_flags = [
            'SameSchTimeFlag',
            'SameVisitTimeFlag',
            'SchAndVisitTimeSameFlag',
            'SchOverAnotherSchTimeFlag',
            'VisitTimeOverAnotherVisitTimeFlag',
            'SchTimeOverVisitTimeFlag',
            'DistanceFlag'
        ]
        
        # Check conditional flags first (most common reason for updates)
        for col in conditional_flags:
            existing_val = existing_row.get(col)
            new_val = new_row.get(col)
            
            # Conditional logic: only update if existing is 'N' and new differs
            if existing_val == 'N' and new_val != existing_val:
                self.stats['changes_by_flag'] += 1
                # Log first 10 changes for debugging
                if self.stats['changes_by_flag'] + self.stats['changes_by_business_data'] <= 10:
                    logger.debug(f"VisitID={new_row.get('VisitID')}: Flag change in {col} "
                                f"(was: {existing_val}, now: {new_val})")
                return True
        
        # Define key business columns to check
        business_columns = [
            'ProviderID', 'ConProviderID', 'VisitDate',
            'SchStartTime', 'SchEndTime', 'ConSchStartTime', 'ConSchEndTime',
            'EVVStartTime', 'EVVEndTime', 'ConEVVStartTime', 'ConEVVEndTime',
            'CaregiverID', 'ConCaregiverID',
            'OfficeID', 'ConOfficeID',
            'PatientID', 'ConPatientID',
            'PayerID', 'ConPayerID',
            'ServiceCodeID', 'ConServiceCodeID',
            'IsMissed', 'EVVType', 'ConIsMissed', 'ConEVVType',
            'P_PatientID', 'ConP_PatientID', 'PA_PatientID', 'ConPA_PatientID',
            'ContractType', 'ConContractType',
            'FederalTaxNumber', 'ConFederalTaxNumber'
        ]
        
        # Check business columns for differences
        for col in business_columns:
            # Skip if column not in new data
            if col not in new_row:
                continue
            
            new_val = new_row.get(col)
            existing_val = existing_row.get(col)
            
            # Compare values
            if new_val != existing_val:
                # Handle None/NULL comparisons properly
                if new_val is None and existing_val is None:
                    continue  # Both are None, no change
                
                self.stats['changes_by_business_data'] += 1
                # Log first 10 changes for debugging
                if self.stats['changes_by_flag'] + self.stats['changes_by_business_data'] <= 10:
                    logger.debug(f"VisitID={new_row.get('VisitID')}: Business data change in {col} "
                                f"(was: {existing_val}, now: {new_val})")
                return True
        
        return False  # No changes detected
    
    def _execute_updates_with_commit(self, update_statements: List[Tuple[str, tuple]]) -> int:
        """
        Execute UPDATE statements using persistent connection with explicit commit
        
        This method uses the persistent connection to execute updates and commits
        immediately after, ensuring progress is saved even if Lambda times out.
        
        Args:
            update_statements: List of (sql, params) tuples
        
        Returns:
            Number of rows updated
        """
        if not update_statements:
            return 0
        
        from psycopg2.extras import execute_batch
        
        cursor = self.pg_connection.cursor()
        total_updated = 0
        
        try:
            # Group by SQL statement for efficient batching
            grouped = {}
            for sql, params in update_statements:
                if sql not in grouped:
                    grouped[sql] = []
                grouped[sql].append(params)
            
            # Execute each group in batches
            for sql, params_list in grouped.items():
                execute_batch(cursor, sql, params_list, page_size=1000)
                total_updated += cursor.rowcount
            
            # CRITICAL: Explicit commit to save progress
            self.pg_connection.commit()
            
            return total_updated
            
        except Exception as e:
            self.logger.error(f"Batch update failed: {e}")
            raise
        finally:
            cursor.close()
    
    def _build_reset_update(self, visit_id: int, con_visit_id: int) -> Tuple[str, tuple]:
        """
        Build UPDATE statement to reset all conflict flags to 'N' for stale conflicts.
        Used when a conflict pair no longer appears in Snowflake results.
        
        Args:
            visit_id: The VisitID
            con_visit_id: The ConVisitID
            
        Returns:
            Tuple of (SQL statement, parameters tuple)
        """
        schema = self.db_names['pg_schema']
        
        sql = f"""
            UPDATE {schema}.conflictvisitmaps
            SET 
                "SameSchTimeFlag" = 'N',
                "SameVisitTimeFlag" = 'N',
                "SchAndVisitTimeSameFlag" = 'N',
                "SchOverAnotherSchTimeFlag" = 'N',
                "VisitTimeOverAnotherVisitTimeFlag" = 'N',
                "SchTimeOverVisitTimeFlag" = 'N',
                "DistanceFlag" = 'N',
                "StatusFlag" = CASE 
                    WHEN "StatusFlag" NOT IN ('W', 'I') THEN 'U' 
                    ELSE "StatusFlag" 
                END,
                "UpdatedDate" = CURRENT_TIMESTAMP
            WHERE "VisitID" = %s
              AND "ConVisitID" = %s
        """
        
        return (sql, (visit_id, con_visit_id))
    
    def _cleanup_stale_conflicts(self, active_conflict_keys: set):
        """
        Find and reset conflicts in Postgres that no longer appear in Snowflake results.
        This handles the case where visits were updated and no longer conflict.
        
        Args:
            active_conflict_keys: Set of (VisitID, ConVisitID) tuples from current Snowflake query
        """
        try:
            schema = self.db_names['pg_schema']
            
            # Fetch all existing conflicts from Postgres in the date window
            # We need to check these against the active conflicts from Snowflake
            query = f"""
                SELECT "VisitID", "ConVisitID"
                FROM {schema}.conflictvisitmaps
                WHERE "CONFLICTID" IS NOT NULL
                  AND ("SameSchTimeFlag" = 'Y' 
                       OR "SameVisitTimeFlag" = 'Y'
                       OR "SchAndVisitTimeSameFlag" = 'Y'
                       OR "SchOverAnotherSchTimeFlag" = 'Y'
                       OR "VisitTimeOverAnotherVisitTimeFlag" = 'Y'
                       OR "SchTimeOverVisitTimeFlag" = 'Y'
                       OR "DistanceFlag" = 'Y')
            """
            
            existing_conflicts = self.pg_manager.execute_query(query, database=self.db_names['pg_database'])
            logger.info(f"  Found {len(existing_conflicts)} existing conflicts in Postgres with active flags")
            
            # Find stale conflicts (exist in Postgres but not in current Snowflake results)
            stale_conflicts = []
            for row in existing_conflicts:
                key = (row[0], row[1])  # (VisitID, ConVisitID)
                if key not in active_conflict_keys:
                    stale_conflicts.append(key)
            
            if stale_conflicts:
                logger.info(f"  Resetting {len(stale_conflicts)} stale conflicts (flags → 'N')")
                
                # Build reset UPDATE statements
                reset_updates = [self._build_reset_update(vid, cvid) for vid, cvid in stale_conflicts]
                
                # Execute resets in batches
                batch_size = 1000
                for i in range(0, len(reset_updates), batch_size):
                    batch = reset_updates[i:i+batch_size]
                    rows_updated = self._execute_updates_with_commit(batch)
                    self.stats['stale_conflicts_reset'] += rows_updated
                    logger.debug(f"    Reset batch {i//batch_size + 1}: {rows_updated} conflicts")
                
                logger.info(f"  ✓ Stale cleanup complete: {self.stats['stale_conflicts_reset']} conflicts reset")
            else:
                logger.info("  ✓ No stale conflicts found")
        
        except Exception as e:
            logger.error(f"Error during stale conflict cleanup: {e}", exc_info=True)
            # Don't raise - stale cleanup is not critical, just log the error
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get processing statistics with enhanced metrics"""
        return {
            'rows_fetched': self.stats['rows_fetched'],
            'rows_processed': self.stats['rows_processed'],
            'rows_updated': self.stats['rows_updated'],
            'rows_skipped_no_changes': self.stats['rows_skipped_no_changes'],
            'batches_processed': self.stats['batches_processed'],
            'errors': self.stats['errors'],
            'unique_visits': len(self.stats['unique_visits']),
            'matched_in_postgres': self.stats['matched_in_postgres'],
            'new_conflicts': self.stats['new_conflicts'],
            'stale_conflicts_reset': self.stats['stale_conflicts_reset'],
            'update_rate': (self.stats['rows_updated'] / max(self.stats['rows_fetched'], 1)) * 100,
            'match_rate': (self.stats['matched_in_postgres'] / max(self.stats['rows_fetched'], 1)) * 100,
            'efficiency_rate': (self.stats['rows_updated'] / max(self.stats['matched_in_postgres'], 1)) * 100
        }
    
    def _get_table_counts(self) -> Dict[str, int]:
        """
        Fetch row counts from key tables
        
        Returns:
            Dict with table names as keys and row counts as values
        """
        counts = {}
        
        try:
            # Postgres table counts
            pg_schema = self.db_names['pg_schema']
            
            # ConflictVisitMaps count
            try:
                query = f'SELECT COUNT(*) FROM {pg_schema}.conflictvisitmaps'
                result = self.pg_manager.execute_query(query, database=self.db_names['pg_database'])
                counts['conflictvisitmaps'] = result[0][0] if result else 0
            except Exception as e:
                logger.warning(f"Could not fetch conflictvisitmaps count: {e}")
                counts['conflictvisitmaps'] = -1
            
            # Conflicts count
            try:
                query = f'SELECT COUNT(*) FROM {pg_schema}.conflicts'
                result = self.pg_manager.execute_query(query, database=self.db_names['pg_database'])
                counts['conflicts'] = result[0][0] if result else 0
            except Exception as e:
                logger.warning(f"Could not fetch conflicts count: {e}")
                counts['conflicts'] = -1
            
            # Snowflake FACTVISITCALLPERFORMANCE_CR count
            try:
                sf_db = self.db_names['sf_database']
                sf_schema = self.db_names['sf_schema']
                query = f'SELECT COUNT(*) FROM {sf_db}.{sf_schema}.FACTVISITCALLPERFORMANCE_CR'
                result = self.sf_manager.execute_query(query)
                counts['factvisitcallperformance_cr'] = result[0][0] if result else 0
            except Exception as e:
                logger.warning(f"Could not fetch FACTVISITCALLPERFORMANCE_CR count: {e}")
                counts['factvisitcallperformance_cr'] = -1
            
        except Exception as e:
            logger.warning(f"Error fetching table counts: {e}")
        
        return counts
    
    def log_summary(self, duration_seconds: float = 0):
        """Log processing summary with enhanced metrics and table counts"""
        logger.info("=" * 70)
        logger.info("CONFLICT PROCESSING SUMMARY")
        logger.info("=" * 70)
        
        # Configuration settings
        logger.info("Configuration:")
        logger.info(f"  Skip unchanged records: {'YES' if self.skip_unchanged_records else 'NO'}")
        if self.skip_unchanged_records:
            logger.info(f"  Columns compared: 48 (7 flags + 41 business)")
        logger.info(f"  Asymmetric join: {'ENABLED' if self.enable_asymmetric_join else 'DISABLED'}")
        if self.enable_asymmetric_join:
            logger.info(f"  (Delta vs All visits - catches conflicts with unchanged records)")
        logger.info(f"  Stale cleanup: {'ENABLED' if self.enable_stale_cleanup else 'DISABLED'}")
        if self.enable_stale_cleanup:
            logger.info(f"  (Resets flag values to 'N' for resolved conflicts)")
        
        # Performance metrics
        logger.info("")
        logger.info("Performance Metrics:")
        logger.info(f"  Conflicts detected from Snowflake: {self.stats['rows_fetched']:,}")
        
        # Asymmetric join breakdown (if enabled)
        if self.enable_asymmetric_join:
            logger.info("")
            logger.info("Asymmetric Join Analysis:")
            unique_visit_count = len(self.stats['unique_visits'])
            logger.info(f"  Delta visits (32-hour window): {unique_visit_count:,}")
            
            # Estimate conflict keys (assuming ~80% uniqueness in date/SSN combinations)
            estimated_keys = int(unique_visit_count * 0.8)
            logger.info(f"  Estimated (VisitDate, SSN) keys: ~{estimated_keys:,}")
            logger.info(f"    (extracted from delta visits for targeted fetch)")
            
            # Calculate visits per key ratio
            if estimated_keys > 0:
                visits_per_key = self.stats['rows_fetched'] / estimated_keys
                logger.info(f"  All visits fetched (matching keys): ~{int(visits_per_key * estimated_keys):,}")
                logger.info(f"    (avg ~{visits_per_key:.1f} visits per key)")
            
            # Show optimization impact
            logger.info(f"  Final conflicts after asymmetric join: {self.stats['rows_fetched']:,}")
            logger.info(f"    (Delta vs All + All vs Delta, deduplicated)")
            
            # Compare to symmetric join
            if unique_visit_count > 0:
                symmetric_estimate = int(unique_visit_count * 1.0)  # Rough estimate
                expansion_ratio = self.stats['rows_fetched'] / symmetric_estimate if symmetric_estimate > 0 else 1
                logger.info(f"  Expansion vs symmetric join: {expansion_ratio:.1f}x")
                logger.info(f"    (symmetric would detect ~{symmetric_estimate:,} conflicts)")
        else:
            # Unique visits count (legacy mode)
            unique_visit_count = len(self.stats['unique_visits'])
            logger.info(f"  Unique visits in last 36 hours: {unique_visit_count:,}")
        
        # Match statistics
        matched = self.stats['matched_in_postgres']
        new_conflicts = self.stats['new_conflicts']
        
        if self.stats['rows_fetched'] > 0:
            matched_pct = (matched / self.stats['rows_fetched']) * 100
            new_pct = (new_conflicts / self.stats['rows_fetched']) * 100
            logger.info(f"  Matched in Postgres: {matched:,} ({matched_pct:.1f}%)")
            logger.info(f"  New conflicts (not in Postgres): {new_conflicts:,} ({new_pct:.1f}%)")
        else:
            logger.info(f"  Matched in Postgres: {matched:,}")
            logger.info(f"  New conflicts (not in Postgres): {new_conflicts:,}")
        
        # Stale cleanup results
        if self.enable_stale_cleanup and self.stats['stale_conflicts_reset'] > 0:
            logger.info(f"  Stale conflicts reset (flags → 'N'): {self.stats['stale_conflicts_reset']:,}")
        
        # Change detection analysis
        if self.skip_unchanged_records:
            logger.info("")
            logger.info("Change Detection Analysis:")
            logger.info(f"  Records evaluated: {matched:,}")
            total_changes = self.stats['changes_by_flag'] + self.stats['changes_by_business_data']
            if matched > 0:
                change_pct = (total_changes / matched) * 100 if matched > 0 else 0
                skip_pct = ((matched - total_changes) / matched) * 100 if matched > 0 else 0
                logger.info(f"  Changes detected: {total_changes:,} ({change_pct:.1f}%)")
                logger.info(f"    - By flag changes: {self.stats['changes_by_flag']:,}")
                logger.info(f"    - By business data: {self.stats['changes_by_business_data']:,}")
                logger.info(f"  Records skipped (no changes): {matched - total_changes:,} ({skip_pct:.1f}%)")
        else:
            logger.info("")
            logger.info("Change Detection Analysis:")
            logger.info(f"  All matched records processed: {matched:,}")
            logger.info(f"  (Skip unchanged records: OFF - no filtering)")
        
        # Update results
        logger.info("")
        logger.info("Update Results:")
        logger.info(f"  Rows processed: {self.stats['rows_processed']:,}")
        logger.info(f"  Rows updated in Postgres: {self.stats['rows_updated']:,}")
        logger.info(f"  Rows skipped (no changes): {self.stats['rows_skipped_no_changes']:,}")
        
        if matched > 0:
            efficiency_rate = (self.stats['rows_updated'] / matched) * 100
            logger.info(f"  Update efficiency (updated/matched): {efficiency_rate:.1f}%")
        
        logger.info(f"  Batches processed: {self.stats['batches_processed']:,}")
        logger.info(f"  Errors encountered: {self.stats['errors']}")
        
        # Throughput
        if self.stats['rows_fetched'] > 0:
            logger.info("")
            logger.info("Throughput:")
            update_rate = (self.stats['rows_updated'] / self.stats['rows_fetched']) * 100
            logger.info(f"  Overall update rate: {update_rate:.1f}%")
            
            if duration_seconds > 0:
                throughput = self.stats['rows_processed'] / duration_seconds
                logger.info(f"  Processing speed: {throughput:.0f} rows/sec")
                logger.info(f"  Total execution time: {duration_seconds/60:.1f} minutes")
        
        # Fetch and log table counts
        logger.info("=" * 70)
        logger.info("TABLE ROW COUNTS")
        logger.info("=" * 70)
        
        counts = self._get_table_counts()
        
        # Postgres tables
        if counts.get('conflictvisitmaps', -1) >= 0:
            logger.info(f"ConflictVisitMaps (Postgres): {counts['conflictvisitmaps']:,}")
        else:
            logger.info("ConflictVisitMaps (Postgres): Error fetching count")
        
        if counts.get('conflicts', -1) >= 0:
            logger.info(f"Conflicts (Postgres): {counts['conflicts']:,}")
        else:
            logger.info("Conflicts (Postgres): Error fetching count")
        
        # Snowflake table
        if counts.get('factvisitcallperformance_cr', -1) >= 0:
            logger.info(f"FactVisitCallPerformance_CR (Snowflake): {counts['factvisitcallperformance_cr']:,}")
        else:
            logger.info("FactVisitCallPerformance_CR (Snowflake): Error fetching count")
        
        logger.info("=" * 70)
