"""
TASK_02: Update ConflictVisitMaps with fresh Analytics data

Migrated from: TASK_02_UPDATE_DATA_CONFLICTVISITMAPS_0.sql

Purpose:
1. Mark CONFLICTVISITMAPS records for update (UpdateFlag = 1)
2. Fetch conflict visits from Postgres view
3. Fetch fresh Analytics data from Snowflake
4. Join in Python to create V1 and V2
5. Calculate conflicts using 7 detection rules
6. Bulk update Postgres with results

Author: Migration Team
Date: 2024
"""

from datetime import datetime, timedelta
from typing import Dict, Any, Tuple
import pandas as pd
from psycopg2 import sql
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.tasks.base_task import BaseTask
from src.connectors.snowflake_connector import SnowflakeConnector
from src.connectors.postgres_connector import PostgresConnector
from src.repositories.analytics_repository import AnalyticsRepository
from src.utils.conflict_calculator import ConflictCalculator
from config.settings import DATE_RANGE_YEARS_BACK, DATE_RANGE_DAYS_FORWARD, MAX_WORKERS


class Task02UpdateConflictVisitMaps(BaseTask):
    """
    TASK_02: Update CONFLICTVISITMAPS with fresh Analytics data.
    
    This task:
    1. Marks records for update based on date range
    2. Fetches filtered CONFLICTVISITMAPS from Postgres view
    3. Fetches fresh visit data from Snowflake Analytics
    4. Joins in Python to create V1 (with CONFLICTID) and V2 (all visits)
    5. Calculates conflicts using 7 rules
    6. Bulk updates Postgres using temp table pattern
    """
    
    def __init__(self,
                 snowflake_connector: SnowflakeConnector,
                 postgres_connector: PostgresConnector):
        """
        Initialize Task 02.
        
        Args:
            snowflake_connector: Connection to Analytics database (read-only)
            postgres_connector: Connection to ConflictReport database (read-write)
        """
        super().__init__('TASK_02')
        self.sf = snowflake_connector
        self.pg = postgres_connector
        self.analytics_repo = AnalyticsRepository(self.sf)
        self.conflict_calculator = ConflictCalculator()
        
        # Calculate date range
        today = datetime.now().date()
        self.date_from = today - timedelta(days=365 * DATE_RANGE_YEARS_BACK)
        self.date_to = today + timedelta(days=DATE_RANGE_DAYS_FORWARD)
    
    def execute(self) -> Dict[str, Any]:
        """
        Execute TASK_02 logic.
        
        Returns:
            Dictionary with task results
        """
        results = {}
        
        # Step 1: Mark records for update
        self.logger.info("Step 1: Marking records for update")
        self._set_update_flag()
        results['marked_for_update'] = True
        
        # Step 2: Get exclusion lists from Postgres
        self.logger.info("Step 2: Fetching exclusion lists")
        exclusions = self._get_exclusions()
        results['excluded_agencies'] = len(exclusions['agencies'])
        results['excluded_ssns'] = len(exclusions['ssns'])
        
        # Step 3: Process in batches by SSN prefix
        self.logger.info("Step 3: Processing batches")
        total_updated = self._process_batches(exclusions)
        results['total_updated'] = total_updated
        
        return results
    
    def _set_update_flag(self):
        """
        Mark CONFLICTVISITMAPS records for update.
        
        Sets UpdateFlag = 1 for records within date range that have CONFLICTID.
        Corresponds to SQL lines 8-9.
        """
        update_query = f"""
            UPDATE "{self.pg.schema}"."conflictvisitmaps"
            SET "UpdateFlag" = 1
            WHERE "CONFLICTID" IS NOT NULL
            AND "VisitDate" BETWEEN %(date_from)s AND %(date_to)s
        """
        
        self.pg.execute(update_query, {
            'date_from': self.date_from,
            'date_to': self.date_to
        })
        
        self.logger.info(f"Set UpdateFlag = 1 for date range {self.date_from} to {self.date_to}")
    
    def _get_exclusions(self) -> Dict[str, list]:
        """
        Fetch exclusion lists from Postgres.
        
        Returns:
            Dictionary with 'agencies' and 'ssns' lists
        """
        # Fetch excluded agencies
        excluded_agencies_df = self.pg.fetch_dataframe(
            f'SELECT "ProviderID" FROM "{self.pg.schema}"."excluded_agency"'
        )
        
        # Fetch excluded SSNs
        excluded_ssns_df = self.pg.fetch_dataframe(
            f'SELECT "SSN" FROM "{self.pg.schema}"."excluded_ssn"'
        )
        
        agencies = excluded_agencies_df['ProviderID'].tolist() if not excluded_agencies_df.empty else []
        ssns = excluded_ssns_df['SSN'].tolist() if not excluded_ssns_df.empty else []
        
        self.logger.info(f"Exclusions: {len(agencies)} agencies, {len(ssns)} SSNs")
        
        return {'agencies': agencies, 'ssns': ssns}
    
    def _process_batches(self, exclusions: Dict[str, list]) -> int:
        """
        Process visits in batches by SSN prefix with parallel execution.
        
        Uses ThreadPoolExecutor to process multiple batches concurrently,
        significantly reducing total runtime.
        
        Args:
            exclusions: Dictionary with exclusion lists
            
        Returns:
            Total number of records updated
        """
        # Get SSN prefixes for batching
        ssn_batches = self._get_ssn_batches()
        
        if not ssn_batches:
            self.logger.warning("No SSN batches found, nothing to process")
            return 0
        
        total_batches = len(ssn_batches)
        self.logger.info(f"Processing {total_batches} SSN batches with {MAX_WORKERS} parallel workers")
        
        total_updated = 0
        completed_count = 0
        failed_batches = []
        
        # Process batches in parallel using ThreadPoolExecutor
        try:
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                # Submit all batches to the executor
                future_to_batch = {
                    executor.submit(self._process_single_batch_safe, ssn_prefix, exclusions): (idx + 1, ssn_prefix)
                    for idx, ssn_prefix in enumerate(ssn_batches)
                }
                
                # Process completed batches as they finish
                for future in as_completed(future_to_batch):
                    batch_num, ssn_prefix = future_to_batch[future]
                    completed_count += 1
                    
                    try:
                        # Use timeout to make it interruptible
                        updated = future.result(timeout=600)  # 10 min timeout per batch
                        total_updated += updated
                        
                        self.logger.info(
                            f"[{completed_count}/{total_batches}] "
                            f"Batch {batch_num} (SSN {ssn_prefix}) complete: {updated} records updated"
                        )
                        
                    except Exception as e:
                        failed_batches.append((batch_num, ssn_prefix, str(e)))
                        self.logger.error(
                            f"[{completed_count}/{total_batches}] "
                            f"Batch {batch_num} (SSN prefix '{ssn_prefix}') FAILED: {str(e)}",
                            exc_info=True  # Include full traceback in log file
                        )
        
        except KeyboardInterrupt:
            self.logger.warning("Parallel processing interrupted by user! Shutting down gracefully...")
            executor.shutdown(wait=False, cancel_futures=True)
            raise
        
        # Summary
        self.logger.info(f"{'='*60}")
        if failed_batches:
            self.logger.warning(f"Completed with {len(failed_batches)} failed batches:")
            for batch_num, ssn_prefix, error in failed_batches:
                self.logger.warning(f"  - Batch {batch_num} (SSN {ssn_prefix}): {error}")
        
        self.logger.info(f"All batches complete: {total_updated} total records updated")
        self.logger.info(f"Success rate: {(total_batches - len(failed_batches))}/{total_batches} batches")
        self.logger.info(f"{'='*60}")
        
        return total_updated
    
    def _process_single_batch_safe(self, ssn_prefix: str, exclusions: Dict[str, list]) -> int:
        """
        Wrapper for _process_single_batch with error handling and logging.
        
        This wrapper ensures that each parallel batch execution:
        1. Has proper batch number logging
        2. Returns 0 on error instead of raising
        3. Logs at appropriate level
        
        Args:
            ssn_prefix: SSN prefix to process
            exclusions: Dictionary with exclusion lists
            
        Returns:
            Number of records updated (0 if error)
        """
        try:
            self.logger.info(f"{'='*60}")
            self.logger.info(f"Batch SSN prefix '{ssn_prefix}*' - Starting")
            self.logger.info(f"{'='*60}")
            
            updated = self._process_single_batch(ssn_prefix, exclusions)
            
            return updated
            
        except Exception as e:
            self.logger.error(f"Error in batch '{ssn_prefix}': {str(e)}", exc_info=True)
            raise  # Re-raise so as_completed can catch it
    
    def _process_single_batch(self, ssn_prefix: str, exclusions: Dict[str, list]) -> int:
        """
        Process a single SSN prefix batch.
        
        Args:
            ssn_prefix: 3-character SSN prefix
            exclusions: Exclusion lists
            
        Returns:
            Number of records updated
        """
        # Step 1: Fetch conflict visits from Postgres view
        conflict_visits_df = self._fetch_conflict_visits(ssn_prefix)
        
        if conflict_visits_df.empty:
            self.logger.info(f"  No conflict visits for prefix '{ssn_prefix}'")
            return 0
        
        self.logger.info(f"  Found {len(conflict_visits_df)} conflict visits in Postgres")
        
        # Extract SSNs for Analytics query
        ssns = conflict_visits_df['SSN'].unique().tolist()
        
        # Step 2: Fetch fresh Analytics data from Snowflake
        analytics_visits_df = self._fetch_analytics_visits(ssns, exclusions)
        
        if analytics_visits_df.empty:
            self.logger.info(f"  No analytics data found for these SSNs")
            return 0
        
        self.logger.info(f"  Fetched {len(analytics_visits_df)} visits from Analytics")
        
        # Step 3: Create V1 and V2 by joining in Python
        v1_df, v2_df = self._create_v1_v2(conflict_visits_df, analytics_visits_df)
        
        if v1_df.empty:
            self.logger.info(f"  No V1 data after join")
            return 0
        
        self.logger.info(f"  Created V1: {len(v1_df)} visits, V2: {len(v2_df)} visits")
        
        # Step 4: Calculate conflicts
        conflicts_df = self._calculate_conflicts(v1_df, v2_df)
        
        if conflicts_df.empty:
            self.logger.info(f"  No conflicts detected")
            return 0
        
        self.logger.info(f"  Detected {len(conflicts_df)} conflicts")
        
        # Step 5: Bulk update Postgres
        updated = self._bulk_update_conflicts(conflicts_df)
        
        return updated
    
    def _get_ssn_batches(self) -> list:
        """
        Get list of SSN prefixes for batching.
        
        Only returns prefixes that have data to process (UpdateFlag=1, CONFLICTID not null).
        This optimization skips empty batches automatically via the view.
        Excludes NULL or empty SSNs.
        
        Returns:
            List of 3-character SSN prefixes with data to process
        """
        query = f"""
            SELECT DISTINCT LEFT("SSN", 3) AS ssn_prefix
            FROM vw_conflictvisitmaps_base
            WHERE "VisitDate" BETWEEN %(date_from)s AND %(date_to)s
            AND "SSN" IS NOT NULL
            AND TRIM("SSN") != ''
            ORDER BY ssn_prefix
        """
        
        df = self.pg.fetch_dataframe(query, {
            'date_from': self.date_from,
            'date_to': self.date_to
        })
        
        active_batches = df['ssn_prefix'].tolist() if not df.empty else []
        
        # Log optimization impact
        max_possible_batches = 1000  # 000-999
        skipped = max_possible_batches - len(active_batches)
        if skipped > 0:
            self.logger.info(f"Optimization: Skipping {skipped} empty batches (processing {len(active_batches)}/{max_possible_batches})")
        
        return active_batches
    
    def _fetch_conflict_visits(self, ssn_prefix: str) -> pd.DataFrame:
        """
        Fetch conflict visits from Postgres view (filtered by SSN prefix).
        
        Uses vw_conflictvisitmaps_base view which encapsulates filtering logic.
        
        Args:
            ssn_prefix: 3-character SSN prefix
            
        Returns:
            DataFrame with conflict visits to update
        """
        query = f"""
            SELECT 
                "CONFLICTID",
                "VisitID",
                "AppVisitID",
                "SSN",
                "VisitDate",
                "ProviderID",
                "AppProviderID"
            FROM vw_conflictvisitmaps_base
            WHERE "SSN" LIKE %(ssn_prefix)s
            AND "VisitDate" BETWEEN %(date_from)s AND %(date_to)s
        """
        
        return self.pg.fetch_dataframe(query, {
            'ssn_prefix': f'{ssn_prefix}%',
            'date_from': self.date_from,
            'date_to': self.date_to
        })
    
    def _fetch_analytics_visits(self, ssns: list, exclusions: Dict[str, list]) -> pd.DataFrame:
        """
        Fetch fresh visit data from Snowflake Analytics.
        
        Args:
            ssns: List of SSNs to fetch
            exclusions: Dictionary with exclusion lists
            
        Returns:
            DataFrame with Analytics visit data
        """
        return self.analytics_repo.fetch_visit_data(
            date_from=self.date_from,
            date_to=self.date_to,
            ssns=ssns,
            excluded_agencies=exclusions['agencies'],
            excluded_ssns=exclusions['ssns']
        )
    
    def _create_v1_v2(self, 
                     conflict_visits_df: pd.DataFrame,
                     analytics_visits_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Create V1 and V2 by joining in Python.
        
        V1 = conflict_visits (Postgres) INNER JOIN analytics_visits (Snowflake)
             → Visits with CONFLICTID + fresh Analytics data
             
        V2 = analytics_visits (all visits for the SSNs)
             → For detecting conflicts against
        
        Args:
            conflict_visits_df: Visits from Postgres with CONFLICTID
            analytics_visits_df: Fresh visit data from Analytics
            
        Returns:
            Tuple of (v1_df, v2_df)
        """
        # V1: INNER JOIN on VisitID and AppVisitID
        # This merges Postgres CONFLICTID with Snowflake Analytics data
        v1_df = conflict_visits_df.merge(
            analytics_visits_df,
            on=['VisitID', 'AppVisitID'],
            how='inner',
            suffixes=('_pg', '_analytics')
        )
        
        # Handle column conflicts - prefer Analytics data, but keep Postgres CONFLICTID
        # For columns that exist in both, drop _pg and keep _analytics (rename to base name)
        for col in conflict_visits_df.columns:
            pg_col = f'{col}_pg'
            analytics_col = f'{col}_analytics'
            
            if pg_col in v1_df.columns and analytics_col in v1_df.columns:
                # Special case: keep CONFLICTID from Postgres
                if col == 'CONFLICTID':
                    v1_df[col] = v1_df[pg_col]
                    v1_df = v1_df.drop(columns=[pg_col, analytics_col])
                # For other columns, prefer Analytics data
                else:
                    v1_df[col] = v1_df[analytics_col]
                    v1_df = v1_df.drop(columns=[pg_col, analytics_col])
            elif pg_col in v1_df.columns:
                # Only _pg exists, rename it
                v1_df[col] = v1_df[pg_col]
                v1_df = v1_df.drop(columns=[pg_col])
            elif analytics_col in v1_df.columns:
                # Only _analytics exists, rename it
                v1_df[col] = v1_df[analytics_col]
                v1_df = v1_df.drop(columns=[analytics_col])
        
        # V2: All analytics visits (no join, no CONFLICTID)
        v2_df = analytics_visits_df.copy()
        if 'CONFLICTID' not in v2_df.columns:
            v2_df['CONFLICTID'] = None
        
        self.logger.info(f"Created V1 ({len(v1_df)} rows) and V2 ({len(v2_df)} rows)")
        
        return v1_df, v2_df
    
    def _calculate_conflicts(self, v1_df: pd.DataFrame, v2_df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate conflicts using ConflictCalculator.
        
        Args:
            v1_df: Visits to update (with CONFLICTID)
            v2_df: All visits (for conflict detection)
            
        Returns:
            DataFrame with detected conflicts
        """
        # Get settings and MPH lookup from Postgres
        settings = self._get_settings()
        mph_df = self._get_mph_lookup()
        
        # Calculate conflicts
        conflicts_df = self.conflict_calculator.calculate_conflicts(
            v1_df, v2_df, settings, mph_df
        )
        
        return conflicts_df
    
    def _get_settings(self) -> pd.Series:
        """Fetch settings from Postgres."""
        query = f'SELECT * FROM "{self.pg.schema}"."settings" LIMIT 1'
        df = self.pg.fetch_dataframe(query)
        return df.iloc[0] if not df.empty else None
    
    def _get_mph_lookup(self) -> pd.DataFrame:
        """Fetch MPH lookup table from Postgres."""
        query = f'SELECT "From", "To", "AverageMilesPerHour" FROM "{self.pg.schema}"."mph"'
        return self.pg.fetch_dataframe(query)
    
    def _get_column_types(self) -> dict:
        """
        Query the actual column types from Postgres information_schema.
        
        Returns:
            Dictionary mapping column names to their SQL data types
        """
        query = f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = '{self.pg.schema}'
            AND table_name = 'conflictvisitmaps'
        """
        result = self.pg.fetch_dataframe(query)
        return dict(zip(result['column_name'], result['data_type']))
    
    def _get_cast_expression(self, column_name: str, column_types: dict) -> str:
        """
        Get the appropriate SQL cast expression for a column from temp table (text) to target type.
        
        Args:
            column_name: Name of the column
            column_types: Dictionary of column names to their data types
            
        Returns:
            SQL expression with appropriate cast (e.g., 'U."ColName"::timestamp')
        """
        data_type = column_types.get(column_name, 'text')
        
        # Map Postgres data types to cast expressions
        if data_type in ('timestamp without time zone', 'timestamp with time zone', 'timestamp'):
            return f'U."{column_name}"::timestamp'
        elif data_type == 'date':
            return f'U."{column_name}"::date'
        elif data_type in ('numeric', 'decimal', 'double precision', 'real'):
            return f'U."{column_name}"::numeric'
        elif data_type in ('integer', 'bigint', 'smallint'):
            return f'U."{column_name}"::integer'
        elif data_type == 'boolean':
            return f'U."{column_name}"::boolean'
        elif data_type == 'uuid':
            return f'U."{column_name}"::uuid'
        else:
            # text, character varying, etc. - no cast needed
            return f'U."{column_name}"'
    
    def _bulk_update_conflicts(self, updates_df: pd.DataFrame) -> int:
        """
        Bulk update CONFLICTVISITMAPS using temp table pattern.
        
        Uses the same pattern as Task 01 for efficient bulk updates.
        
        Implementation:
        1. Query actual column types from information_schema (foolproof type casting)
        2. Create temp table with all columns as TEXT
        3. COPY data from DataFrame to temp table (fast bulk insert)
        4. UPDATE target table with proper type casts based on schema (no type mismatches)
        
        Args:
            updates_df: DataFrame with conflict data to update
            
        Returns:
            Number of records updated
        """
        if updates_df.empty:
            return 0
        
        # Prepare column list for UPDATE (all columns from SQL line 12-13)
        update_columns = [
            'SSN', 'ProviderID', 'AppProviderID', 'ProviderName',
            'ConProviderID', 'ConAppProviderID', 'ConProviderName',
            'ConVisitID', 'ConAppVisitID',
            'SchStartTime', 'SchEndTime', 'ConSchStartTime', 'ConSchEndTime',
            'VisitStartTime', 'VisitEndTime', 'ConVisitStartTime', 'ConVisitEndTime',
            'EVVStartTime', 'EVVEndTime', 'ConEVVStartTime', 'ConEVVEndTime',
            'CaregiverID', 'AppCaregiverID', 'AideCode', 'AideName', 'AideSSN',
            'ConCaregiverID', 'ConAppCaregiverID', 'ConAideCode', 'ConAideName', 'ConAideSSN',
            'OfficeID', 'AppOfficeID', 'Office',
            'ConOfficeID', 'ConAppOfficeID', 'ConOffice',
            'PatientID', 'AppPatientID', 'PAdmissionID', 'PName',
            'PAddressID', 'PAppAddressID', 'PAddressL1', 'PAddressL2', 'PCity',
            'PAddressState', 'PZipCode', 'PCounty', 'PLongitude', 'PLatitude',
            'ConPatientID', 'ConAppPatientID', 'ConPAdmissionID', 'ConPName',
            'ConPAddressID', 'ConPAppAddressID', 'ConPAddressL1', 'ConPAddressL2',
            'ConPCity', 'ConPAddressState', 'ConPZipCode', 'ConPCounty',
            'ConPLongitude', 'ConPLatitude',
            'PayerID', 'AppPayerID', 'Contract',
            'ConPayerID', 'ConAppPayerID', 'ConContract',
            'BilledDate', 'ConBilledDate', 'BilledHours', 'ConBilledHours',
            'Billed', 'ConBilled',
            'MinuteDiffBetweenSch', 'DistanceMilesFromLatLng',
            'AverageMilesPerHour', 'ETATravleMinutes',
            'ServiceCodeID', 'AppServiceCodeID', 'RateType', 'ServiceCode',
            'ConServiceCodeID', 'ConAppServiceCodeID', 'ConRateType', 'ConServiceCode',
            'AideFName', 'AideLName', 'ConAideFName', 'ConAideLName',
            'PFName', 'PLName', 'ConPFName', 'ConPLName',
            'PMedicaidNumber', 'ConPMedicaidNumber',
            'PayerState', 'ConPayerState',
            'LastUpdatedBy', 'ConLastUpdatedBy',
            'LastUpdatedDate', 'ConLastUpdatedDate',
            'BilledRate', 'TotalBilledAmount',
            'ConBilledRate', 'ConTotalBilledAmount',
            'IsMissed', 'MissedVisitReason', 'EVVType',
            'ConIsMissed', 'ConMissedVisitReason', 'ConEVVType',
            'PStatus', 'ConPStatus', 'AideStatus', 'ConAideStatus',
            'P_PatientID', 'P_AppPatientID', 'ConP_PatientID', 'ConP_AppPatientID',
            'PA_PatientID', 'PA_AppPatientID', 'ConPA_PatientID', 'ConPA_AppPatientID',
            'ContractType', 'ConContractType',
            'BillRateNonBilled', 'ConBillRateNonBilled',
            'BillRateBoth', 'ConBillRateBoth',
            'FederalTaxNumber', 'ConFederalTaxNumber',
            'SameSchTimeFlag', 'SameVisitTimeFlag', 'SchAndVisitTimeSameFlag',
            'SchOverAnotherSchTimeFlag', 'VisitTimeOverAnotherVisitTimeFlag',
            'SchTimeOverVisitTimeFlag', 'DistanceFlag'
        ]
        
        # Filter to only columns that exist in updates_df
        available_columns = [col for col in update_columns if col in updates_df.columns]
        
        # Ensure required columns
        if 'VisitID' not in updates_df.columns:
            self.logger.error("VisitID missing from updates_df")
            return 0
        
        # Get actual column types from database schema
        self.logger.debug("Fetching column types from schema")
        column_types = self._get_column_types()
        
        with self.pg.get_connection() as conn:
            with conn.cursor() as cur:
                # Prepare data
                updates_df = updates_df.copy()
                updates_df['UpdatedDate'] = datetime.now()
                
                # Select columns to update
                update_cols = ['VisitID'] + available_columns + ['UpdatedDate']
                update_df_subset = updates_df[update_cols]
                
                # Create temp table with all columns that will be inserted
                self.logger.debug("Creating temp table for bulk update")
                # Build column definitions - all as text for simplicity
                col_defs = ',\n                        '.join([f'"{col}" text' for col in update_cols])
                create_table_sql = f"""
                    CREATE TEMP TABLE conflict_updates (
                        {col_defs}
                    ) ON COMMIT DROP
                """
                cur.execute(create_table_sql)
                
                # Bulk COPY
                self.logger.debug(f"Copying {len(update_df_subset)} rows to temp table")
                csv_buffer = StringIO()
                update_df_subset.to_csv(csv_buffer, index=False, header=False, na_rep='\\N')
                csv_buffer.seek(0)
                
                # Build column list for COPY
                copy_columns = ','.join([f'"{col}"' for col in update_cols])
                
                cur.copy_expert(
                    sql=f'''COPY conflict_updates ({copy_columns}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')''',
                    file=csv_buffer
                )
                
                # Build SET clause for UPDATE with appropriate type casts
                set_clauses = []
                for col in available_columns:
                    # Get cast expression based on actual column type from schema
                    cast_expr = self._get_cast_expression(col, column_types)
                    set_clauses.append(f'"{col}" = {cast_expr}')
                
                # Add special columns with CASE logic
                set_clauses.extend([
                    '"UpdateFlag" = NULL',
                    '"UpdatedDate" = CURRENT_TIMESTAMP',
                    '''"StatusFlag" = CASE 
                        WHEN CVM."StatusFlag" NOT IN ('W', 'I') 
                        THEN 'U' 
                        ELSE CVM."StatusFlag" 
                    END''',
                    '"ResolveDate" = NULL'
                ])
                
                set_clause = ',\n                    '.join(set_clauses)
                
                # Execute UPDATE
                self.logger.debug("Executing bulk UPDATE")
                update_sql = f"""
                    UPDATE "{self.pg.schema}"."conflictvisitmaps" AS CVM
                    SET {set_clause}
                    FROM conflict_updates U
                    WHERE CVM."VisitID" = U."VisitID"
                    AND (
                        (CVM."ConVisitID" = U."ConVisitID")
                        OR (CVM."ConVisitID" IS NULL AND U."ConVisitID" IS NULL)
                    )
                """
                
                cur.execute(update_sql)
                updated_count = cur.rowcount
        
        self.logger.info(f"Updated {updated_count} records via bulk update")
        return updated_count

