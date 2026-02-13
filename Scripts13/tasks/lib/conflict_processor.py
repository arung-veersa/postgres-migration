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
        enable_stale_cleanup: bool = True,
        enable_insert: bool = True
    ):
        self.sf_manager = sf_manager
        self.pg_manager = pg_manager
        self.query_builder = query_builder
        self.db_names = db_names
        self.batch_size = batch_size
        self.skip_unchanged_records = skip_unchanged_records
        self.enable_asymmetric_join = enable_asymmetric_join
        self.enable_stale_cleanup = enable_stale_cleanup
        self.enable_insert = enable_insert
        self.logger = logger
        
        # Persistent Postgres connection for batch processing
        self.pg_connection = None
        
        # INSERT template (built once, reused for all batches)
        self._insert_sql: Optional[str] = None
        self._insert_sf_columns: Optional[List[str]] = None
        
        # Statistics
        self.stats = {
            'rows_fetched': 0,
            'rows_processed': 0,
            'rows_updated': 0,
            'rows_inserted': 0,
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
            # INSERT stats
            'insert_enabled': enable_insert,
            'insert_batches': 0,
            # Asymmetric join stats
            'asymmetric_join_enabled': enable_asymmetric_join,
            'stale_cleanup_enabled': enable_stale_cleanup,
            'stale_conflicts_reset': 0,
            # Scoped stale cleanup stats
            'delta_ssns_count': 0,
            'delta_dates_count': 0,
            'delta_pairs_count': 0,
            'delta_keys_count': 0,  # backward compat alias
            'modified_visit_ids_count': 0,  # backward compat alias
            'records_marked_for_update': 0,
            'stale_conflicts_resolved': 0
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
        
        # Fetch excluded SSNs
        query = self.query_builder.build_reference_query('pg_fetch_excluded_ssns.sql', self.db_names)
        ssns = self.pg_manager.execute_query(query, database=self.db_names['pg_database'])
        excluded_ssns = [row[0] for row in ssns if row[0]]
        logger.info(f"  ✓ Excluded SSNs: {len(excluded_ssns)}")
        
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
    
    def stream_and_process_conflicts_v3(
        self,
        queries: Dict[str, str],
        timeout_callback: Optional[callable] = None
    ) -> Dict[str, Any]:
        """
        Stream conflict results from Snowflake using v3 temp table approach
        with pair-precise seen-based anti-join for accurate stale conflict detection.
        
        V3 Execution Flow:
        1. Execute step1 (delta_keys temp table) - always created for stale cleanup scoping
        2. Execute step2 (base_visits temp table with materialization)
        2d. Stream actual (visit_date, ssn) pairs from delta_keys to Postgres _tmp_delta_pairs
            (precise scope for stale resolve -- avoids cross-product of separate SSN/date lists)
        3. Execute step3 (final conflict detection query) and stream results
           - During streaming, collect all seen (VisitID, ConVisitID) pairs
        4. Resolve stale conflicts via pair-precise seen-based anti-join
        
        PERFORMANCE: Step 2d streams actual (visit_date, ssn) pairs from delta_keys 
        directly into Postgres via chunked COPY. This gives precise stale scoping --
        only records matching an exact (visit_date, ssn) pair from delta_keys are
        candidates for stale resolution, eliminating the cross-product problem.
        
        Args:
            queries: Dict with keys 'step1', 'step2', 'step2d', 'step3' containing SQL
            timeout_callback: Optional function to check for shutdown (e.g. SIGTERM)
        
        Returns:
            Dict with processing statistics
        """
        logger.info("Starting v3 conflict detection with temp tables...")
        logger.info(f"  Batch size: {self.batch_size}")
        logger.info(f"  Asymmetric join: {'ENABLED' if self.enable_asymmetric_join else 'DISABLED'}")
        logger.info(f"  Stale cleanup: {'ENABLED' if self.enable_stale_cleanup else 'DISABLED'}")
        logger.info(f"  Insert new conflicts: {'ENABLED' if self.enable_insert else 'DISABLED'}")
        
        # Build INSERT template once (reused for every batch)
        if self.enable_insert:
            self._insert_sql, self._insert_sf_columns = \
                self.query_builder.build_insert_template(self.db_names)
        
        batch = []
        batch_number = 0
        delta_pairs_loaded = False  # Whether (visit_date, ssn) pairs were loaded into Postgres
        
        try:
            # Open a connection to Snowflake
            with self.sf_manager.streaming_cursor() as cursor:
                # STEP 0: Create and populate excluded_ssns temp table
                if queries.get('step0_create'):
                    logger.info("STEP 0: Creating excluded_ssns temp table...")
                    cursor.execute(queries['step0_create'])
                    insert_stmts = queries.get('step0_inserts', [])
                    if insert_stmts:
                        for stmt in insert_stmts:
                            cursor.execute(stmt)
                        logger.info(f"  ✓ Loaded {len(insert_stmts)} batch(es) of excluded SSNs")
                    else:
                        logger.info("  ✓ No excluded SSNs to load (table created empty)")
                
                # STEP 1: Create delta_keys temp table (always created now)
                if queries.get('step1'):
                    logger.info("STEP 1: Creating delta_keys temp table...")
                    cursor.execute(queries['step1'])
                    logger.info("  ✓ delta_keys temp table created")
                else:
                    logger.info("STEP 1: SKIPPED (no step1 query)")
                
                # STEP 2: Create base_visits temp table
                # Part A: Delta rows only (~70K, fast with partition pruning)
                import time as _time
                step2a_start = _time.time()
                logger.info("STEP 2 (Part A): Creating base_visits with delta rows only...")
                cursor.execute(queries['step2'])
                step2a_duration = _time.time() - step2a_start
                logger.info(f"  ✓ base_visits created with delta rows ({step2a_duration:.1f}s)")
                
                # Part B (asymmetric only): INSERT related non-delta rows via delta_keys JOIN
                if queries.get('step2_asym_insert'):
                    step2b_start = _time.time()
                    logger.info("STEP 2 (Part B): Inserting related non-delta visits via delta_keys JOIN...")
                    cursor.execute(queries['step2_asym_insert'])
                    step2b_duration = _time.time() - step2b_start
                    logger.info(f"  ✓ Related non-delta rows inserted ({step2b_duration:.1f}s)")
                    logger.info(f"  Total Step 2: {step2a_duration + step2b_duration:.1f}s")
                
                # STEP 2d: Stream actual (visit_date, ssn) pairs from delta_keys into Postgres
                # These pairs define the EXACT scope Snowflake scanned for conflict detection.
                # Using actual pairs avoids the cross-product problem of separate SSN/date lists.
                # Streamed in chunks of 100K via COPY for low memory usage.
                if self.enable_stale_cleanup and queries.get('step2d'):
                    import io
                    step2d_start = _time.time()
                    logger.info("STEP 2d: Streaming (visit_date, ssn) pairs from delta_keys to Postgres...")
                    
                    # Create _tmp_delta_pairs in Postgres
                    if self.pg_connection is None:
                        self.pg_connection = self.pg_manager.get_connection(
                            database=self.db_names['pg_database']
                        )
                    pg_cursor = self.pg_connection.cursor()
                    pg_cursor.execute("DROP TABLE IF EXISTS _tmp_delta_pairs")
                    pg_cursor.execute("""
                        CREATE TEMP TABLE _tmp_delta_pairs (
                            visit_date DATE,
                            ssn VARCHAR(20)
                        )
                    """)
                    
                    # Stream from Snowflake cursor and COPY in chunks
                    COPY_CHUNK_SIZE = 100000
                    cursor.execute(queries['step2d'])
                    buffer = io.StringIO()
                    pair_count = 0
                    chunk_count = 0
                    distinct_ssns = set()
                    distinct_dates = set()
                    
                    for row in cursor:
                        visit_date = row[0]
                        ssn = str(row[1]).strip() if row[1] else None
                        if visit_date and ssn:
                            buffer.write(f"{visit_date}\t{ssn}\n")
                            pair_count += 1
                            distinct_ssns.add(ssn)
                            distinct_dates.add(visit_date)
                            
                            if pair_count % COPY_CHUNK_SIZE == 0:
                                buffer.seek(0)
                                pg_cursor.copy_from(buffer, '_tmp_delta_pairs', columns=('visit_date', 'ssn'))
                                chunk_count += 1
                                buffer = io.StringIO()
                                if pair_count % 1000000 == 0:
                                    logger.info(f"    Streamed {pair_count:,} pairs...")
                    
                    # Flush remaining rows
                    if buffer.tell() > 0:
                        buffer.seek(0)
                        pg_cursor.copy_from(buffer, '_tmp_delta_pairs', columns=('visit_date', 'ssn'))
                        chunk_count += 1
                    
                    copy_duration = _time.time() - step2d_start
                    logger.info(f"  ✓ Loaded {pair_count:,} pairs into _tmp_delta_pairs ({chunk_count} chunks, {copy_duration:.1f}s)")
                    logger.info(f"    Distinct SSNs: {len(distinct_ssns):,}, Distinct dates: {len(distinct_dates):,}")
                    
                    # Index for Phase 1 JOIN performance
                    idx_start = _time.time()
                    pg_cursor.execute("CREATE INDEX ON _tmp_delta_pairs (ssn, visit_date)")
                    pg_cursor.execute("ANALYZE _tmp_delta_pairs")
                    idx_duration = _time.time() - idx_start
                    logger.info(f"  ✓ Indexed and analyzed _tmp_delta_pairs ({idx_duration:.1f}s)")
                    
                    pg_cursor.close()
                    delta_pairs_loaded = True
                    
                    step2d_duration = _time.time() - step2d_start
                    self.stats['delta_ssns_count'] = len(distinct_ssns)
                    self.stats['delta_keys_count'] = pair_count
                    self.stats['delta_dates_count'] = len(distinct_dates)
                    self.stats['delta_pairs_count'] = pair_count
                    logger.info(f"  Total step 2d: {step2d_duration:.1f}s")
                    logger.info("  (No upfront marking -- will use seen-based resolve after streaming)")
                else:
                    logger.info("STEP 2d: SKIPPED (stale cleanup disabled)")
                
                # STEP 3: Execute final conflict detection query and stream results
                logger.info("STEP 3: Executing final conflict detection query...")
                cursor.execute(queries['step3'])
                logger.info("  ✓ Query started, streaming results...")
                
                # Get column names
                column_names = [desc[0] for desc in cursor.description]
                logger.info(f"  Result columns: {len(column_names)}")
                
                # Track all (VisitID, ConVisitID) pairs from Snowflake for seen-based stale resolve
                seen_conflict_keys = set()
                
                # Stream and process results
                for row in cursor:
                    # Check timeout
                    if timeout_callback and timeout_callback():
                        logger.warning("Timeout detected, stopping processing")
                        break
                    
                    self.stats['rows_fetched'] += 1
                    
                    # Convert row to dict
                    conflict_row = dict(zip(column_names, row))
                    
                    # Track unique visits
                    self.stats['unique_visits'].add(conflict_row['VisitID'])
                    
                    # Track seen conflict keys for stale resolve
                    if self.enable_stale_cleanup:
                        visit_id = str(conflict_row.get('VisitID'))
                        con_visit_id = str(conflict_row.get('ConVisitID')) if conflict_row.get('ConVisitID') else None
                        seen_conflict_keys.add((visit_id, con_visit_id))
                    
                    batch.append(conflict_row)
                    
                    # Process batch when full
                    if len(batch) >= self.batch_size:
                        batch_number += 1
                        self._process_batch(batch, batch_number)
                        batch = []
                
                # Process remaining records
                if batch:
                    batch_number += 1
                    self._process_batch(batch, batch_number)
            
            logger.info(f"✓ Streaming complete: {self.stats['rows_fetched']} conflicts fetched from Snowflake")
            if self.enable_stale_cleanup:
                logger.info(f"  Seen conflict keys collected: {len(seen_conflict_keys):,}")
            
            # STEP 4: Resolve stale conflicts (seen-based anti-join approach)
            # _tmp_delta_pairs was already loaded in Step 2d and persists in the PG session
            if self.enable_stale_cleanup and delta_pairs_loaded:
                self._resolve_stale_conflicts_seen_based(seen_conflict_keys)
            
            # Finalize stats
            self.stats['batches_processed'] = batch_number
            self.stats['unique_visits'] = len(self.stats['unique_visits'])
            
            return self.stats
            
        except Exception as e:
            logger.error(f"Error during v3 conflict processing: {str(e)}")
            self.stats['errors'] += 1
            raise
        finally:
            # Close persistent connection
            if self.pg_connection:
                try:
                    self.pg_connection.close()
                    logger.info("Closed persistent Postgres connection")
                except Exception as e:
                    logger.warning(f"Error closing Postgres connection: {e}")
    
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
            
            # --- UPDATES: Match and prepare updates (with change detection) ---
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
            
            # --- INSERTS: Insert new conflict rows ---
            if self.enable_insert and new_count > 0:
                # Collect new rows (those not matched in PG)
                new_rows = []
                for conflict_row in batch:
                    visit_id = str(conflict_row.get('VisitID'))
                    con_visit_id = str(conflict_row.get('ConVisitID')) if conflict_row.get('ConVisitID') else None
                    key = (visit_id, con_visit_id)
                    if key not in existing_records:
                        new_rows.append(conflict_row)
                
                if new_rows:
                    # Get or create persistent connection
                    if self.pg_connection is None:
                        self.pg_connection = self.pg_manager.get_connection(
                            database=self.db_names['pg_database']
                        )
                    
                    rows_inserted = self._execute_inserts_with_commit(new_rows)
                    self.stats['rows_inserted'] += rows_inserted
                    self.stats['insert_batches'] += 1
                    logger.info(f"  ✓ Batch {batch_number}: {rows_inserted} rows inserted (COMMITTED)")
            
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
            existing_row: Dictionary of existing column values from Postgres (40 columns)
        
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
        immediately after, ensuring progress is saved even if the container is stopped.
        
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
    
    def _execute_inserts_with_commit(self, new_rows: List[Dict[str, Any]]) -> int:
        """
        INSERT new conflict rows using persistent connection with explicit commit.
        
        Uses the pre-built INSERT template (self._insert_sql) and extracts values
        from each Snowflake row dict in the column order defined by
        self._insert_sf_columns.  Executed via psycopg2 ``execute_batch`` for
        efficient batching.
        
        Args:
            new_rows: List of Snowflake conflict row dicts to insert
        
        Returns:
            Number of rows inserted
        """
        if not new_rows or not self._insert_sql:
            return 0
        
        from psycopg2.extras import execute_batch
        import psycopg2.errors
        
        # Build param tuples from row dicts (done once, reusable on retry)
        params_list = []
        for row in new_rows:
            params = tuple(row.get(col) for col in self._insert_sf_columns)
            params_list.append(params)
        
        schema = self.db_names['pg_schema']
        max_attempts = 2  # original + 1 retry after sequence fix
        
        for attempt in range(1, max_attempts + 1):
            cursor = self.pg_connection.cursor()
            try:
                execute_batch(cursor, self._insert_sql, params_list, page_size=1000)
                # NOTE: cursor.rowcount after execute_batch only reflects the
                # last statement in the semicolon-joined batch, so it always
                # returns 1.  Use len(params_list) instead -- if execute_batch
                # raises no exception, all rows were inserted successfully.
                inserted = len(params_list)
                
                # CRITICAL: Explicit commit to save progress
                self.pg_connection.commit()
                cursor.close()
                return inserted
            
            except psycopg2.errors.UniqueViolation as e:
                self.pg_connection.rollback()
                cursor.close()
                
                if attempt < max_attempts and 'conflictvisitmaps_pkey' in str(e):
                    # Identity sequence is behind MAX(ID) -- auto-fix and retry
                    self.logger.warning(
                        f"Identity sequence collision detected (attempt {attempt}). "
                        f"Advancing sequence to MAX(ID) and retrying..."
                    )
                    fix_cursor = self.pg_connection.cursor()
                    try:
                        fix_cursor.execute(
                            "SELECT pg_get_serial_sequence(%s, 'ID')",
                            (f'{schema}.conflictvisitmaps',),
                        )
                        seq_name = fix_cursor.fetchone()[0]
                        fix_cursor.execute(
                            f'SELECT setval(%s, (SELECT MAX("ID") FROM {schema}.conflictvisitmaps))',
                            (seq_name,),
                        )
                        new_val = fix_cursor.fetchone()[0]
                        self.pg_connection.commit()
                        self.logger.warning(
                            f"  Sequence {seq_name} advanced to {new_val:,}. Retrying insert..."
                        )
                    except Exception as fix_err:
                        self.pg_connection.rollback()
                        self.logger.error(f"  Failed to fix sequence: {fix_err}")
                        raise e from fix_err
                    finally:
                        fix_cursor.close()
                    continue  # retry the insert with a fresh cursor
                
                self.logger.error(f"Batch insert failed: {e}")
                raise
            
            except Exception as e:
                self.pg_connection.rollback()
                cursor.close()
                self.logger.error(f"Batch insert failed: {e}")
                raise
    
    def _resolve_stale_conflicts_seen_based(self, seen_conflict_keys: set):
        """
        Resolve stale conflicts using precise pair-based anti-join approach.
        
        Optimizations applied (v5 - precise pair scope):
        1. Pair-precise scope: Uses _tmp_delta_pairs (visit_date, ssn) loaded in Step 2d
           with actual pairs from delta_keys, eliminating the cross-product problem
           that caused 3.9M false stale records with separate SSN + date filters.
        2. UUID type match: _tmp_seen_conflicts uses UUID columns (enables index usage)
        3. Minimal UPDATE: Only sets StatusFlag='R' and UpdatedDate
        4. Two-phase resolve: SELECT stale IDs first, then UPDATE by PK
        5. Batched Phase 2: UPDATEs in chunks of 100K to avoid giant transactions
        
        Prerequisite: _tmp_delta_pairs must already exist in the PG session (created in Step 2d).
        
        Args:
            seen_conflict_keys: Set of (VisitID, ConVisitID) tuples seen during streaming
        """
        import io
        import time as _time
        
        BATCH_SIZE = 100000  # Phase 2 UPDATE batch size (reduced from 500K for faster commits)
        
        try:
            schema = self.db_names['pg_schema']
            step4_start = _time.time()
            
            # Get or create persistent connection
            if self.pg_connection is None:
                self.pg_connection = self.pg_manager.get_connection(
                    database=self.db_names['pg_database']
                )
            
            cursor = self.pg_connection.cursor()
            
            logger.info(f"STEP 4: Resolving stale conflicts (pair-precise seen-based approach)...")
            logger.info(f"  Delta pairs scope: _tmp_delta_pairs (loaded in Step 2d)")
            logger.info(f"  Seen conflict pairs: {len(seen_conflict_keys):,}")
            
            # 1. Create and load _tmp_seen_conflicts temp table (UUID columns for index match)
            cursor.execute("DROP TABLE IF EXISTS _tmp_seen_conflicts")
            cursor.execute("""
                CREATE TEMP TABLE _tmp_seen_conflicts (
                    visit_id UUID,
                    con_visit_id UUID
                )
            """)
            
            copy_start = _time.time()
            buffer = io.StringIO()
            for visit_id, con_visit_id in seen_conflict_keys:
                vid = visit_id if visit_id else '\\N'
                cvid = con_visit_id if con_visit_id else '\\N'
                buffer.write(f"{vid}\t{cvid}\n")
            buffer.seek(0)
            cursor.copy_from(buffer, '_tmp_seen_conflicts', columns=('visit_id', 'con_visit_id'))
            copy_duration = _time.time() - copy_start
            logger.info(f"  ✓ Loaded {len(seen_conflict_keys):,} seen pairs into _tmp_seen_conflicts ({copy_duration:.1f}s)")
            
            # 2. Index and analyze temp tables
            idx_start = _time.time()
            cursor.execute("CREATE INDEX ON _tmp_seen_conflicts (visit_id, con_visit_id)")
            cursor.execute("ANALYZE _tmp_seen_conflicts")
            # _tmp_delta_pairs was already indexed in Step 2d
            idx_duration = _time.time() - idx_start
            logger.info(f"  ✓ _tmp_seen_conflicts indexed and analyzed ({idx_duration:.1f}s)")
            
            # 3. Phase 1: Identify stale records into temp table (read-only scan)
            # JOIN on _tmp_delta_pairs gives precise (visit_date, ssn) scope -- no cross-product.
            # Records that: (a) match an actual delta (visit_date, ssn) pair,
            # (b) are NOT in seen pairs (stale),
            # (c) are non-InService/PTO, (d) are not already resolved/whitelisted/ignored
            phase1_start = _time.time()
            cursor.execute("DROP TABLE IF EXISTS _tmp_stale_ids")
            cursor.execute(f"""
                CREATE TEMP TABLE _tmp_stale_ids AS
                SELECT cvm."VisitID", cvm."ConVisitID"
                FROM {schema}.conflictvisitmaps cvm
                INNER JOIN _tmp_delta_pairs dp
                  ON cvm."SSN" = dp.ssn
                  AND cvm."VisitDate" = dp.visit_date
                WHERE cvm."CONFLICTID" IS NOT NULL
                  AND cvm."StatusFlag" NOT IN ('R', 'W', 'I')
                  AND cvm."InserviceStartDate" IS NULL
                  AND cvm."InserviceEndDate" IS NULL
                  AND cvm."PTOStartDate" IS NULL
                  AND cvm."PTOEndDate" IS NULL
                  AND cvm."ConInserviceStartDate" IS NULL
                  AND cvm."ConInserviceEndDate" IS NULL
                  AND cvm."ConPTOStartDate" IS NULL
                  AND cvm."ConPTOEndDate" IS NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM _tmp_seen_conflicts sc
                      WHERE sc.visit_id = cvm."VisitID"
                        AND sc.con_visit_id = cvm."ConVisitID"
                  )
            """)
            # Get count of stale records identified
            cursor.execute("SELECT COUNT(*) FROM _tmp_stale_ids")
            stale_count = cursor.fetchone()[0]
            phase1_duration = _time.time() - phase1_start
            logger.info(f"  Phase 1: Identified {stale_count:,} stale records ({phase1_duration:.1f}s)")
            
            # 4. Phase 2: Update stale records by PK with minimal SET, in batches
            phase2_start = _time.time()
            total_rows_updated = 0
            
            if stale_count > 0:
                # Add a row_number to _tmp_stale_ids for batching
                cursor.execute("ALTER TABLE _tmp_stale_ids ADD COLUMN batch_id SERIAL")
                
                num_batches = (stale_count + BATCH_SIZE - 1) // BATCH_SIZE
                logger.info(f"  Phase 2: Updating {stale_count:,} rows in {num_batches} batch(es) of {BATCH_SIZE:,}...")
                
                for batch_num in range(num_batches):
                    batch_start = batch_num * BATCH_SIZE + 1
                    batch_end = (batch_num + 1) * BATCH_SIZE
                    
                    cursor.execute(f"""
                        UPDATE {schema}.conflictvisitmaps cvm
                        SET "StatusFlag" = CASE
                                WHEN cvm."StatusFlag" NOT IN ('W', 'I') THEN 'R'
                                ELSE cvm."StatusFlag"
                            END,
                            "UpdatedDate" = CURRENT_TIMESTAMP
                        FROM _tmp_stale_ids si
                        WHERE cvm."VisitID" = si."VisitID"
                          AND cvm."ConVisitID" = si."ConVisitID"
                          AND si.batch_id BETWEEN {batch_start} AND {batch_end}
                    """)
                    batch_updated = cursor.rowcount
                    total_rows_updated += batch_updated
                    self.pg_connection.commit()
                    
                    batch_elapsed = _time.time() - phase2_start
                    logger.info(f"    Batch {batch_num + 1}/{num_batches}: {batch_updated:,} rows updated (cumulative: {total_rows_updated:,}, {batch_elapsed:.1f}s)")
            
            phase2_duration = _time.time() - phase2_start
            logger.info(f"  Phase 2 complete: {total_rows_updated:,} rows updated ({phase2_duration:.1f}s)")
            
            cursor.close()
            
            step4_total = _time.time() - step4_start
            self.stats['stale_conflicts_resolved'] = stale_count
            self.stats['stale_conflicts_reset'] = stale_count  # backward compat
            self.stats['records_marked_for_update'] = 0  # no upfront marking in seen-based approach
            
            if stale_count > 0:
                logger.info(f"  ✓ Resolved {stale_count:,} stale conflicts → StatusFlag='R'")
            else:
                logger.info(f"  ✓ No stale conflicts found")
            logger.info(f"  Total step 4: {step4_total:.1f}s (Phase 1: {phase1_duration:.1f}s, Phase 2: {phase2_duration:.1f}s)")
            
        except Exception as e:
            logger.error(f"Error resolving stale conflicts (seen-based): {e}", exc_info=True)
            if self.pg_connection:
                try:
                    self.pg_connection.rollback()
                except Exception:
                    pass
            # Don't raise - stale cleanup is important but shouldn't fail the whole run
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get processing statistics with enhanced metrics"""
        return {
            'rows_fetched': self.stats['rows_fetched'],
            'rows_processed': self.stats['rows_processed'],
            'rows_updated': self.stats['rows_updated'],
            'rows_inserted': self.stats['rows_inserted'],
            'rows_skipped_no_changes': self.stats['rows_skipped_no_changes'],
            'batches_processed': self.stats['batches_processed'],
            'errors': self.stats['errors'],
            'unique_visits': len(self.stats['unique_visits']),
            'matched_in_postgres': self.stats['matched_in_postgres'],
            'new_conflicts': self.stats['new_conflicts'],
            'insert_enabled': self.stats['insert_enabled'],
            'insert_batches': self.stats['insert_batches'],
            'stale_conflicts_reset': self.stats['stale_conflicts_reset'],
            'delta_ssns_count': self.stats['delta_ssns_count'],
            'delta_dates_count': self.stats.get('delta_dates_count', 0),
            'delta_pairs_count': self.stats.get('delta_pairs_count', 0),
            'delta_keys_count': self.stats.get('delta_pairs_count', 0),  # backward compat
            'modified_visit_ids_count': self.stats['delta_ssns_count'],  # backward compat
            'records_marked_for_update': self.stats['records_marked_for_update'],
            'stale_conflicts_resolved': self.stats['stale_conflicts_resolved'],
            'update_rate': (self.stats['rows_updated'] / max(self.stats['rows_fetched'], 1)) * 100,
            'match_rate': (self.stats['matched_in_postgres'] / max(self.stats['rows_fetched'], 1)) * 100,
            'efficiency_rate': (self.stats['rows_updated'] / max(self.stats['matched_in_postgres'], 1)) * 100
        }
    
    def _get_table_counts(self, skip_snowflake: bool = True) -> Dict[str, int]:
        """
        Fetch row counts from key tables.
        
        Args:
            skip_snowflake: If True, skip the Snowflake count query (~25s savings).
                          The SF count is informational only and not worth the latency.
        
        Returns:
            Dict with table names as keys and row counts as values
        """
        counts = {}
        
        try:
            # Postgres table counts (fast, <1 second each)
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
            
            # Snowflake FACTVISITCALLPERFORMANCE_CR count (expensive - ~25s)
            if not skip_snowflake:
                try:
                    sf_db = self.db_names['sf_database']
                    sf_schema = self.db_names['sf_schema']
                    query = f'SELECT COUNT(*) FROM {sf_db}.{sf_schema}.FACTVISITCALLPERFORMANCE_CR'
                    result = self.sf_manager.execute_query(query)
                    counts['factvisitcallperformance_cr'] = result[0][0] if result else 0
                except Exception as e:
                    logger.warning(f"Could not fetch FACTVISITCALLPERFORMANCE_CR count: {e}")
                    counts['factvisitcallperformance_cr'] = -1
            else:
                logger.info("  (Snowflake table count skipped for performance)")
                counts['factvisitcallperformance_cr'] = -2  # Skipped indicator
            
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
            logger.info(f"  Columns compared: 40 (7 flags + 33 business)")
        logger.info(f"  Asymmetric join: {'ENABLED' if self.enable_asymmetric_join else 'DISABLED'}")
        if self.enable_asymmetric_join:
            logger.info(f"  (Delta vs All visits - catches conflicts with unchanged records)")
        logger.info(f"  Stale cleanup: {'ENABLED' if self.enable_stale_cleanup else 'DISABLED'}")
        if self.enable_stale_cleanup:
            logger.info(f"  (Sets StatusFlag='R' and UpdatedDate for stale conflicts)")
        
        # Performance metrics
        logger.info("")
        logger.info("Performance Metrics:")
        logger.info(f"  Conflicts detected from Snowflake: {self.stats['rows_fetched']:,}")
        
        # Asymmetric join breakdown (if enabled)
        if self.enable_asymmetric_join:
            logger.info("")
            logger.info("Asymmetric Join Analysis:")
            unique_visit_count = self.stats['unique_visits'] if isinstance(self.stats['unique_visits'], int) else len(self.stats['unique_visits'])
            logger.info(f"  Delta visits (lookback window): {unique_visit_count:,}")
            
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
            unique_visit_count = self.stats['unique_visits'] if isinstance(self.stats['unique_visits'], int) else len(self.stats['unique_visits'])
            logger.info(f"  Unique visits in lookback window: {unique_visit_count:,}")
        
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
        
        # Stale cleanup results (pair-precise seen-based anti-join approach)
        if self.enable_stale_cleanup:
            logger.info("")
            logger.info("Stale Cleanup (Pair-Precise Seen-Based Anti-Join):")
            logger.info(f"  Delta pairs (scope): {self.stats.get('delta_pairs_count', 0):,}")
            logger.info(f"    Distinct SSNs: {self.stats['delta_ssns_count']:,}")
            logger.info(f"    Distinct dates: {self.stats.get('delta_dates_count', 0):,}")
            logger.info(f"  Seen conflict pairs (from Snowflake): {self.stats['rows_fetched']:,}")
            logger.info(f"  Stale conflicts resolved (→ 'R'): {self.stats['stale_conflicts_resolved']:,}")
        
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
        
        # Insert results
        logger.info("")
        logger.info("Insert Results:")
        if self.enable_insert:
            logger.info(f"  New conflicts inserted: {self.stats['rows_inserted']:,}")
            if new_conflicts > 0:
                insert_pct = (self.stats['rows_inserted'] / new_conflicts) * 100
                logger.info(f"  Insert rate (inserted/new): {insert_pct:.1f}%")
            logger.info(f"  Insert batches: {self.stats['insert_batches']:,}")
        else:
            logger.info(f"  INSERT DISABLED -- {new_conflicts:,} new conflicts skipped")
        
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
        sf_count = counts.get('factvisitcallperformance_cr', -1)
        if sf_count >= 0:
            logger.info(f"FactVisitCallPerformance_CR (Snowflake): {sf_count:,}")
        elif sf_count == -2:
            logger.info("FactVisitCallPerformance_CR (Snowflake): Skipped (performance optimization)")
        else:
            logger.info("FactVisitCallPerformance_CR (Snowflake): Error fetching count")
        
        logger.info("=" * 70)
