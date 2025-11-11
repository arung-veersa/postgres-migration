"""
TASK_01: Copy Data from ConflictVisitMaps to Temp

Migrated from: TASK_01_COPY_DATA_FROM_CONFLICTVISITMAPS_TO_TEMP.sql

Purpose:
1. Manage PAYER_PROVIDER_REMINDERS (insert new, update existing)
2. Copy conflictvisitmaps data to conflictvisitmaps_temp with date filtering
3. Update SETTINGS.InProgressFlag

Author: Migration Team
Date: 2024
"""

from datetime import datetime, timedelta
from typing import Dict, Any
import pandas as pd
from psycopg2 import sql
from io import StringIO

from src.tasks.base_task import BaseTask
from src.connectors.snowflake_connector import SnowflakeConnector
from src.connectors.postgres_connector import PostgresConnector
from src.repositories.analytics_repository import AnalyticsRepository
from config.settings import DATE_RANGE_YEARS_BACK, DATE_RANGE_DAYS_FORWARD


class Task01CopyToTemp(BaseTask):
    """
    TASK_01: Copy conflict visit map data to temporary table.
    
    This task:
    1. Syncs payer-provider reminders from Analytics
    2. Truncates temp table
    3. Copies filtered conflict data to temp table
    4. Updates processing status
    """
    
    def __init__(self, 
                 snowflake_connector: SnowflakeConnector,
                 postgres_connector: PostgresConnector):
        """
        Initialize Task 01.
        
        Args:
            snowflake_connector: Connection to Analytics database (read-only)
            postgres_connector: Connection to ConflictReport database (read-write)
        """
        super().__init__('TASK_01')
        self.sf = snowflake_connector
        self.pg = postgres_connector
        # Minimal repository wrapper over Snowflake with basic caching
        self.analytics_repo = AnalyticsRepository(self.sf)
    
    def execute(self) -> Dict[str, Any]:
        """
        Execute TASK_01 logic.
        
        Returns:
            Dictionary with task results
        """
        results = {}
        
        # Step 1: Sync Payer-Provider Reminders
        self.logger.info("Step 1: Syncing payer-provider reminders")
        reminder_results = self._sync_payer_provider_reminders()
        results['payer_provider_reminders'] = reminder_results
        
        # Step 2: Truncate temp table
        self.logger.info("Step 2: Truncating conflictvisitmaps_temp")
        self._truncate_temp_table()
        results['temp_table_truncated'] = True
        
        # Step 3: Copy data to temp table
        self.logger.info("Step 3: Copying data to conflictvisitmaps_temp")
        copy_results = self._copy_to_temp_table()
        results['temp_table_rows'] = copy_results
        
        # Step 4: Update settings
        self.logger.info("Step 4: Updating SETTINGS.InProgressFlag")
        self._update_settings_flag()  # 1 = In Progress
        results['settings_updated'] = True
        
        return results
    
    def _sync_payer_provider_reminders(self) -> Dict[str, int]:
        """
        Sync payer-provider reminders from Analytics database.
        
        Process:
        1. Fetch payer-provider relationships from Analytics
        2. Insert new reminders that don't exist
        3. Update existing reminders with latest names
        
        Returns:
            Dictionary with insert and update counts
        """
        self.logger.info("Fetching payer-provider relationships from Analytics")
        relationships_df = self.analytics_repo.get_payer_provider_relationships()
        self.logger.info(f"Found {len(relationships_df)} relationships")
        
        if relationships_df.empty:
            return {'inserted': 0, 'updated': 0}
        
        # Get existing reminders
        existing_query = f"""
            SELECT "PayerID", "ProviderID"
            FROM "{self.pg.schema}"."payer_provider_reminders"
        """
        existing_df = self.pg.fetch_dataframe(existing_query)
        
        # Find new relationships (not in existing)
        if not existing_df.empty:
            merge_key = relationships_df[['PayerID', 'ProviderID']].apply(
                lambda x: f"{x['PayerID']}_{x['ProviderID']}", axis=1
            )
            existing_key = existing_df[['PayerID', 'ProviderID']].apply(
                lambda x: f"{x['PayerID']}_{x['ProviderID']}", axis=1
            )
            
            new_relationships = relationships_df[~merge_key.isin(existing_key)]
        else:
            new_relationships = relationships_df
        
        # Insert new reminders
        inserted = 0
        if not new_relationships.empty:
            insert_df = new_relationships.copy()
            insert_df['CreatedDateTime'] = datetime.now()
            insert_df['NumberOfDays'] = None
            
            inserted = self.pg.bulk_insert_dataframe(
                insert_df,
                'payer_provider_reminders'
            )
            self.logger.info(f"Inserted {inserted} new reminders")
        
        # Update existing reminders
        update_query = f"""
            UPDATE "{self.pg.schema}"."payer_provider_reminders" AS PPR
            SET 
                "Contract" = %(contract)s,
                "ProviderName" = %(provider_name)s
            WHERE 
                PPR."PayerID" = %(payer_id)s
                AND PPR."ProviderID" = %(provider_id)s
        """
        
        updated = 0
        if not existing_df.empty:
            # Prepare data for bulk update
            updates_df = relationships_df[['PayerID', 'ProviderID', 'Contract', 'ProviderName']].drop_duplicates().copy()
            # Ensure IDs are strings to support GUIDs and numeric values uniformly
            updates_df['PayerID'] = updates_df['PayerID'].astype(str)
            updates_df['ProviderID'] = updates_df['ProviderID'].astype(str)
            self.logger.info(f"Preparing {len(updates_df)} rows for bulk update via temp table")

            # Use a single connection for temp table lifecycle and update
            with self.pg.get_connection() as conn:
                with conn.cursor() as cur:
                    # 1) Create temp table (session-scoped)
                    self.logger.debug("Creating temporary table ppr_updates")
                    cur.execute("""
                        CREATE TEMP TABLE ppr_updates (
                            "PayerID" text,
                            "ProviderID" text,
                            "Contract" text,
                            "ProviderName" text
                        ) ON COMMIT DROP
                    """)

                    # 2) Bulk load data into temp table using COPY
                    self.logger.debug("COPYing update rows into temp table ppr_updates")
                    csv_buffer = StringIO()
                    updates_df.to_csv(csv_buffer, index=False, header=False, na_rep='\\N')
                    csv_buffer.seek(0)
                    cur.copy_expert(
                        sql='COPY ppr_updates ("PayerID","ProviderID","Contract","ProviderName") FROM STDIN WITH (FORMAT CSV, NULL \'\\N\')',
                        file=csv_buffer
                    )

                    # 3) Single set-based UPDATE with join; avoid no-op writes
                    self.logger.debug("Running set-based UPDATE join from temp table")
                    cur.execute(f"""
                        UPDATE "{self.pg.schema}"."payer_provider_reminders" AS PPR
                        SET 
                            "Contract" = U."Contract",
                            "ProviderName" = U."ProviderName"
                        FROM ppr_updates AS U
                        WHERE 
                            PPR."PayerID"::text = U."PayerID"
                            AND PPR."ProviderID"::text = U."ProviderID"
                            AND (
                                PPR."Contract" IS DISTINCT FROM U."Contract"
                                OR PPR."ProviderName" IS DISTINCT FROM U."ProviderName"
                            )
                    """)
                    updated = cur.rowcount

            self.logger.info(f"Updated {updated} existing reminders via temp table")
        
        return {'inserted': inserted, 'updated': updated}
    
    def _truncate_temp_table(self) -> None:
        """Truncate conflictvisitmaps_temp table."""
        self.pg.truncate_table('conflictvisitmaps_temp')
    
    def _copy_to_temp_table(self) -> int:
        """
        Copy data from conflictvisitmaps to conflictvisitmaps_temp.
        
        Filters:
        - VisitDate between (today - 2 years) and (today + 45 days)
        - Joins with CONFLICTS table to get StatusFlag and FlagForReview
        
        Returns:
            Number of rows copied
        """
        # Calculate date range
        today = datetime.now().date()
        date_from = today - timedelta(days=365 * DATE_RANGE_YEARS_BACK)
        date_to = today + timedelta(days=DATE_RANGE_DAYS_FORWARD)
        
        self.logger.info(f"Date range: {date_from} to {date_to}")
        
        # Perform a single set-based INSERT ... SELECT in-database
        self.logger.info("Inserting rows into conflictvisitmaps_temp via INSERT ... SELECT")
        
        insert_query = sql.SQL("""
            INSERT INTO {}."conflictvisitmaps_temp" (
                "ID", "CONFLICTID", "SSN",
                "ProviderID", "AppProviderID", "ProviderName",
                "FederalTaxNumber",
                "VisitID", "AppVisitID",
                "ConProviderID", "ConAppProviderID", "ConProviderName",
                "ConFederalTaxNumber",
                "ConVisitID", "ConAppVisitID",
                "VisitDate",
                "SchStartTime", "SchEndTime",
                "ConSchStartTime", "ConSchEndTime",
                "VisitStartTime", "VisitEndTime",
                "ConVisitStartTime", "ConVisitEndTime",
                "EVVStartTime", "EVVEndTime",
                "ConEVVStartTime", "ConEVVEndTime",
                "CaregiverID", "AppCaregiverID",
                "AideCode", "AideName", "AideSSN",
                "ConCaregiverID", "ConAppCaregiverID",
                "ConAideCode", "ConAideName", "ConAideSSN",
                "OfficeID", "AppOfficeID", "Office",
                "ConOfficeID", "ConAppOfficeID", "ConOffice",
                "PatientID", "AppPatientID", "PAdmissionID", "PName",
                "PAddressID", "PAppAddressID",
                "PAddressL1", "PAddressL2", "PCity",
                "PAddressState", "PZipCode", "PCounty",
                "PLongitude", "PLatitude",
                "ConPatientID", "ConAppPatientID", "ConPAdmissionID",
                "ConPName", "ConPAddressID", "ConPAppAddressID",
                "ConPAddressL1", "ConPAddressL2", "ConPCity",
                "ConPAddressState", "ConPZipCode", "ConPCounty",
                "ConPLongitude", "ConPLatitude",
                "PayerID", "AppPayerID", "Contract",
                "ConPayerID", "ConAppPayerID", "ConContract",
                "BilledDate", "ConBilledDate",
                "BilledHours", "ConBilledHours",
                "Billed", "ConBilled",
                "MinuteDiffBetweenSch",
                "DistanceMilesFromLatLng",
                "AverageMilesPerHour",
                "ETATravleMinutes",
                "InserviceStartDate", "InserviceEndDate",
                "PTOStartDate", "PTOEndDate",
                "ConInserviceStartDate", "ConInserviceEndDate",
                "ConPTOStartDate", "ConPTOEndDate",
                "ServiceCodeID", "AppServiceCodeID",
                "RateType", "ServiceCode",
                "ConServiceCodeID", "ConAppServiceCodeID",
                "ConRateType", "ConServiceCode",
                "SameSchTimeFlag", "SameVisitTimeFlag",
                "SchAndVisitTimeSameFlag",
                "SchOverAnotherSchTimeFlag",
                "VisitTimeOverAnotherVisitTimeFlag",
                "SchTimeOverVisitTimeFlag",
                "DistanceFlag", "InServiceFlag", "PTOFlag",
                "StatusFlag",
                "ConStatusFlag",
                "AideFName", "AideLName",
                "ConAideFName", "ConAideLName",
                "PFName", "PLName",
                "ConPFName", "ConPLName",
                "PMedicaidNumber", "ConPMedicaidNumber"
            )
            SELECT 
                CVM."ID", CVM."CONFLICTID", CVM."SSN", 
                CVM."ProviderID", CVM."AppProviderID", CVM."ProviderName",
                CVM."FederalTaxNumber",
                CVM."VisitID", CVM."AppVisitID",
                CVM."ConProviderID", CVM."ConAppProviderID", CVM."ConProviderName",
                CVM."ConFederalTaxNumber",
                CVM."ConVisitID", CVM."ConAppVisitID",
                CVM."VisitDate",
                CVM."SchStartTime", CVM."SchEndTime",
                CVM."ConSchStartTime", CVM."ConSchEndTime",
                CVM."VisitStartTime", CVM."VisitEndTime",
                CVM."ConVisitStartTime", CVM."ConVisitEndTime",
                CVM."EVVStartTime", CVM."EVVEndTime",
                CVM."ConEVVStartTime", CVM."ConEVVEndTime",
                CVM."CaregiverID", CVM."AppCaregiverID",
                CVM."AideCode", CVM."AideName", CVM."AideSSN",
                CVM."ConCaregiverID", CVM."ConAppCaregiverID",
                CVM."ConAideCode", CVM."ConAideName", CVM."ConAideSSN",
                CVM."OfficeID", CVM."AppOfficeID", CVM."Office",
                CVM."ConOfficeID", CVM."ConAppOfficeID", CVM."ConOffice",
                CVM."PatientID", CVM."AppPatientID", CVM."PAdmissionID", CVM."PName",
                CVM."PAddressID", CVM."PAppAddressID",
                CVM."PAddressL1", CVM."PAddressL2", CVM."PCity",
                CVM."PAddressState", CVM."PZipCode", CVM."PCounty",
                CVM."PLongitude", CVM."PLatitude",
                CVM."ConPatientID", CVM."ConAppPatientID", CVM."ConPAdmissionID",
                CVM."ConPName", CVM."ConPAddressID", CVM."ConPAppAddressID",
                CVM."ConPAddressL1", CVM."ConPAddressL2", CVM."ConPCity",
                CVM."ConPAddressState", CVM."ConPZipCode", CVM."ConPCounty",
                CVM."ConPLongitude", CVM."ConPLatitude",
                CVM."PayerID", CVM."AppPayerID", CVM."Contract",
                CVM."ConPayerID", CVM."ConAppPayerID", CVM."ConContract",
                CVM."BilledDate", CVM."ConBilledDate",
                CVM."BilledHours", CVM."ConBilledHours",
                CVM."Billed", CVM."ConBilled",
                CVM."MinuteDiffBetweenSch",
                CVM."DistanceMilesFromLatLng",
                CVM."AverageMilesPerHour",
                CVM."ETATravleMinutes",
                CVM."InserviceStartDate", CVM."InserviceEndDate",
                CVM."PTOStartDate", CVM."PTOEndDate",
                CVM."ConInserviceStartDate", CVM."ConInserviceEndDate",
                CVM."ConPTOStartDate", CVM."ConPTOEndDate",
                CVM."ServiceCodeID", CVM."AppServiceCodeID",
                CVM."RateType", CVM."ServiceCode",
                CVM."ConServiceCodeID", CVM."ConAppServiceCodeID",
                CVM."ConRateType", CVM."ConServiceCode",
                CVM."SameSchTimeFlag", CVM."SameVisitTimeFlag",
                CVM."SchAndVisitTimeSameFlag",
                CVM."SchOverAnotherSchTimeFlag",
                CVM."VisitTimeOverAnotherVisitTimeFlag",
                CVM."SchTimeOverVisitTimeFlag",
                CVM."DistanceFlag", CVM."InServiceFlag", CVM."PTOFlag",
                C."StatusFlag",
                CVM."StatusFlag" AS "ConStatusFlag",
                CVM."AideFName", CVM."AideLName",
                CVM."ConAideFName", CVM."ConAideLName",
                CVM."PFName", CVM."PLName",
                CVM."ConPFName", CVM."ConPLName",
                CVM."PMedicaidNumber", CVM."ConPMedicaidNumber"
            FROM {}."conflictvisitmaps" AS CVM
            INNER JOIN {}."conflicts" AS C 
                ON C."CONFLICTID" = CVM."CONFLICTID"
            WHERE CVM."VisitDate" BETWEEN %s AND %s
        """).format(
            sql.Identifier(self.pg.schema),
            sql.Identifier(self.pg.schema),
            sql.Identifier(self.pg.schema),
        )
        
        # Execute in a single transaction with improved throughput
        with self.pg.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SET LOCAL synchronous_commit = off")
                cur.execute(insert_query, (date_from, date_to))
                rows_inserted = cur.rowcount
        
        self.logger.info(f"Inserted {rows_inserted} rows into conflictvisitmaps_temp")
        return rows_inserted
    
    def _update_settings_flag(self) -> None:
        """
        Update SETTINGS.InProgressFlag.
        
        Args:
            flag_value: 1 = In Progress, 2 = Error
        """
        update_query = f"""
            UPDATE "{self.pg.schema}"."settings"
            SET "InProgressFlag" = %(flag)s
        """
        
        self.pg.execute(update_query, {'flag': 1}) # 1 = In Progress
        self.logger.info(f"Set InProgressFlag = 1")

