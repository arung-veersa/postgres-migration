import json
import os
import snowflake.connector
import psycopg2
from tqdm import tqdm
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import io
import concurrent.futures
import logging
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def load_config():
    """Loads the configuration from config.json and .env."""
    try:
        # Load environment variables from .env file
        script_dir = os.path.dirname(os.path.abspath(__file__))
        env_path = os.path.join(script_dir, '.env')
        load_dotenv(env_path)
        
        # Load config.json
        config_path = os.path.join(script_dir, 'config.json')
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        # Add Snowflake config from environment
        config['snowflake'] = {
            'user': os.getenv('SNOWFLAKE_USER'),
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA'),
            'rsa_key': os.getenv('SNOWFLAKE_RSA_KEY'),
            'authenticator': 'SNOWFLAKE_JWT'
        }
        
        # Add PostgreSQL config from environment
        config['postgres'] = {
            'host': os.getenv('POSTGRES_HOST'),
            'port': int(os.getenv('POSTGRES_PORT', 5432)),
            'database': os.getenv('POSTGRES_DATABASE'),
            'user': os.getenv('POSTGRES_USER'),
            'password': os.getenv('POSTGRES_PASSWORD'),
            'schema': os.getenv('POSTGRES_SCHEMA', 'public')
        }
        
        return config
    except FileNotFoundError as e:
        logging.error(f"Error: Required file not found: {e}")
        exit(1)
    except json.JSONDecodeError:
        logging.error("Error: Could not decode config.json. Please check for syntax errors.")
        exit(1)
    except Exception as e:
        logging.error(f"Error loading configuration: {e}")
        exit(1)


def get_private_key(rsa_key):
    """Convert base64 RSA key to private key bytes."""
    # Handle literal \n characters in the key (common copy-paste issue)
    rsa_key_cleaned = rsa_key.replace('\\n', '\n')
    
    private_key_prefix = '-----BEGIN PRIVATE KEY-----\n'
    private_key_suffix = '\n-----END PRIVATE KEY-----'
    
    full_rsa_key = private_key_prefix + rsa_key_cleaned + private_key_suffix
    p_key = serialization.load_pem_private_key(
        full_rsa_key.encode(),
        password=None,
        backend=default_backend()
    )
    return p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )


def get_snowflake_connection(config):
    """Establishes a connection to Snowflake using key pair authentication."""
    try:
        logging.info(f"Connecting to Snowflake: {config['user']}@{config['account']}")
        conn = snowflake.connector.connect(
            user=config['user'],
            account=config['account'],
            private_key=get_private_key(config.get('rsa_key')),
            warehouse=config['warehouse'],
            database=config['database'],
            schema=config['schema'],
            authenticator=config.get('authenticator'),
        )
        return conn
    except Exception as e:
        logging.error(f"Error connecting to Snowflake: {e}")
        return None


def get_postgres_connection(config):
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host=config['host'],
            port=config.get('port', 5432),
            dbname=config['database'],
            user=config['user'],
            password=config['password']
        )
        return conn
    except Exception as e:
        logging.error(f"Error connecting to PostgreSQL: {e}")
        return None


def get_target_columns(pg_cursor, schema, table_name):
    """Get column names from target PostgreSQL table."""
    pg_cursor.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """, (schema, table_name))
    return [row[0] for row in pg_cursor.fetchall()]


def get_primary_keys(pg_cursor, schema, table_name):
    """Get primary key columns from PostgreSQL table."""
    pg_cursor.execute("""
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid 
                           AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = (
            SELECT oid FROM pg_class 
            WHERE relname = %s 
            AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = %s)
        )
        AND i.indisprimary
        ORDER BY array_position(i.indkey, a.attnum)
    """, (table_name, schema))
    return [row[0] for row in pg_cursor.fetchall()]


def build_source_query(source_view, columns, table_config):
    """Build SELECT query for Snowflake source."""
    # Handle column mapping (source column names may differ from target)
    source_watermark = table_config.get('source_watermark_column')
    target_watermark = table_config.get('target_watermark_column') or table_config.get('watermark_column')
    load_type = table_config.get('load_type', 'full')
    
    # Build column list with optional aliasing for watermark column
    column_parts = []
    for col in columns:
        # If there's a column mapping and this is the target watermark column, use source name with alias
        if source_watermark and target_watermark and col == target_watermark:
            # Use source column name but alias it to target name
            column_parts.append(f'"{source_watermark}" AS "{target_watermark}"')
        # Handle "Updated Datatimestamp" specially (may not exist in source)
        elif col == 'Updated Datatimestamp':
            # Only include if it's the watermark column (and no mapping)
            if target_watermark == 'Updated Datatimestamp' and not source_watermark:
                column_parts.append(f'"{col}"')
            else:
                # Column doesn't exist in source - select NULL with proper type casting
                column_parts.append(f'NULL::TIMESTAMP_TZ AS "{col}"')
        else:
            column_parts.append(f'"{col}"')
    
    column_list = ', '.join(column_parts)
    
    # Build WHERE clause
    where_parts = []
    
    # Add custom WHERE clause from config
    if table_config.get('source_where_clause'):
        where_parts.append(f"({table_config['source_where_clause']})")
    
    # Add incremental filter if configured
    if table_config.get('load_type') == 'incremental':
        watermark_col = source_watermark or target_watermark
        if watermark_col:
            days_back = table_config.get('incremental_days_back', 30)
            where_parts.append(f'"{watermark_col}" >= DATEADD(day, -{days_back}, CURRENT_DATE())')
    
    where_clause = ' AND '.join(where_parts) if where_parts else ''
    
    query = f'SELECT {column_list} FROM "{source_view}"'
    if where_clause:
        query += f' WHERE {where_clause}'
    
    return query


def merge_data(pg_cursor, staging_table, target_table, columns, primary_keys, watermark_column, perform_deletes):
    """
    Perform MERGE operation: UPDATE existing rows, INSERT new rows, optionally DELETE missing rows.
    """
    schema_table = target_table
    column_list = ', '.join([f'"{col}"' for col in columns])
    pk_join = ' AND '.join([f't."{pk}" = s."{pk}"' for pk in primary_keys])
    
    # 1. UPDATE: Existing rows where source has newer data
    if watermark_column and watermark_column in columns:
        update_set = ', '.join([f'"{col}" = s."{col}"' for col in columns if col not in primary_keys])
        update_query = f"""
            UPDATE {schema_table} t
            SET {update_set}
            FROM {staging_table} s
            WHERE {pk_join}
              AND s."{watermark_column}" > t."{watermark_column}";
        """
        logging.info("Updating existing records with newer data...")
        pg_cursor.execute(update_query)
        updated_count = pg_cursor.rowcount
        logging.info(f"Updated {updated_count} rows")
    else:
        # No watermark comparison, update all matching rows
        update_set = ', '.join([f'"{col}" = s."{col}"' for col in columns if col not in primary_keys])
        update_query = f"""
            UPDATE {schema_table} t
            SET {update_set}
            FROM {staging_table} s
            WHERE {pk_join};
        """
        logging.info("Updating existing records...")
        pg_cursor.execute(update_query)
        updated_count = pg_cursor.rowcount
        logging.info(f"Updated {updated_count} rows")
    
    # 2. INSERT: New rows that don't exist in target
    insert_query = f"""
        INSERT INTO {schema_table} ({column_list})
        SELECT {column_list}
        FROM {staging_table} s
        WHERE NOT EXISTS (
            SELECT 1 FROM {schema_table} t WHERE {pk_join}
        );
    """
    logging.info("Inserting new records...")
    pg_cursor.execute(insert_query)
    inserted_count = pg_cursor.rowcount
    logging.info(f"Inserted {inserted_count} rows")
    
    # 3. DELETE: Remove rows from target that don't exist in source (only if perform_deletes is True)
    deleted_count = 0
    if perform_deletes:
        delete_query = f"""
            DELETE FROM {schema_table} t
            WHERE NOT EXISTS (
                SELECT 1 FROM {staging_table} s WHERE {pk_join}
            );
        """
        logging.info("Deleting records not in source...")
        pg_cursor.execute(delete_query)
        deleted_count = pg_cursor.rowcount
        logging.info(f"Deleted {deleted_count} rows")
    
    return updated_count, inserted_count, deleted_count


def migrate_table_with_merge(sf_cursor, pg_cursor, pg_schema, table_config):
    """
    Migrate a table from Snowflake to PostgreSQL using MERGE logic.
    """
    source_view = table_config['source_view']
    target_table = table_config['target_table']
    target_watermark = table_config.get('target_watermark_column') or table_config.get('watermark_column')
    source_watermark = table_config.get('source_watermark_column')
    perform_deletes = table_config.get('perform_deletes', False)
    batch_size = table_config.get('batch_size', 10000)
    
    logging.info(f"\n{'='*60}")
    logging.info(f"Starting migration: {source_view} -> {target_table}")
    logging.info(f"Load Type: {table_config.get('load_type', 'full')}")
    logging.info(f"{'='*60}")
    
    # Get target table columns and primary keys
    columns = get_target_columns(pg_cursor, pg_schema, target_table)
    primary_keys = get_primary_keys(pg_cursor, pg_schema, target_table)
    
    logging.info(f"Target columns: {len(columns)} columns")
    logging.info(f"Primary keys: {primary_keys}")
    if target_watermark:
        watermark_display = f"{target_watermark}"
        if source_watermark and source_watermark != target_watermark:
            watermark_display = f"{source_watermark} (source) -> {target_watermark} (target)"
        logging.info(f"Watermark column: {watermark_display}")
    
    # Build source query
    source_query = build_source_query(source_view, columns, table_config)
    logging.info(f"Source query: {source_query}")
    
    # Execute query and get row count
    count_query = f"SELECT COUNT(*) FROM ({source_query})"
    sf_cursor.execute(count_query)
    total_rows = sf_cursor.fetchone()[0]
    
    if total_rows == 0:
        logging.info("No rows to migrate from source.")
        return
    
    logging.info(f"Fetching {total_rows} rows from source...")
    
    # Create temporary staging table
    staging_table = f"staging_{target_table}"
    column_defs = ', '.join([f'"{col}" VARCHAR' for col in columns])  # Simplified for staging
    
    # Get actual column types from target table
    pg_cursor.execute(f"""
        SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """, (pg_schema, target_table))
    
    col_types = {}
    for row in pg_cursor.fetchall():
        col_name, data_type, char_len, num_prec, num_scale = row
        if data_type == 'character varying':
            col_types[col_name] = f'VARCHAR({char_len})' if char_len else 'VARCHAR'
        elif data_type == 'numeric':
            col_types[col_name] = f'NUMERIC({num_prec},{num_scale})' if num_prec and num_scale else 'NUMERIC'
        elif data_type == 'timestamp with time zone':
            col_types[col_name] = 'TIMESTAMPTZ'
        elif data_type == 'timestamp without time zone':
            col_types[col_name] = 'TIMESTAMP'
        elif data_type == 'date':
            col_types[col_name] = 'DATE'
        elif data_type == 'boolean':
            col_types[col_name] = 'BOOLEAN'
        else:
            col_types[col_name] = data_type.upper()
    
    column_defs = ', '.join([f'"{col}" {col_types[col]}' for col in columns])
    
    logging.info("Creating temporary staging table...")
    pg_cursor.execute(f"DROP TABLE IF EXISTS {staging_table};")
    pg_cursor.execute(f"CREATE TEMP TABLE {staging_table} ({column_defs});")
    
    # Fetch and load data in batches
    sf_cursor.execute(source_query)
    batches = sf_cursor.fetch_pandas_batches()
    
    rows_loaded = 0
    with tqdm(total=total_rows, unit="rows", desc=f"Loading to staging") as pbar:
        for df in batches:
            # Create an in-memory CSV file
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False, header=False, na_rep='\\N')
            csv_buffer.seek(0)
            
            # Use COPY to load into staging table
            pg_cursor.copy_expert(
                sql=f"COPY {staging_table} ({','.join([f'\"{c}\"' for c in columns])}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
                file=csv_buffer
            )
            rows_loaded += len(df)
            pbar.update(len(df))
    
    logging.info(f"Loaded {rows_loaded} rows into staging table")
    
    # Perform MERGE operation
    logging.info("Performing MERGE operation...")
    updated, inserted, deleted = merge_data(
        pg_cursor, 
        staging_table, 
        f'"{pg_schema}"."{target_table}"',
        columns, 
        primary_keys, 
        target_watermark,
        perform_deletes
    )
    
    # Drop staging table
    pg_cursor.execute(f"DROP TABLE IF EXISTS {staging_table};")
    
    logging.info(f"\n{'='*60}")
    logging.info(f"Migration Summary for {target_table}:")
    logging.info(f"  Updated: {updated}")
    logging.info(f"  Inserted: {inserted}")
    logging.info(f"  Deleted: {deleted}")
    logging.info(f"  Total: {updated + inserted + deleted}")
    logging.info(f"{'='*60}\n")


def migrate_single_table_wrapper(sf_config, pg_config, table_config):
    """Wrapper to migrate a single table, handling connections and transactions."""
    sf_conn = get_snowflake_connection(sf_config)
    pg_conn = get_postgres_connection(pg_config)

    if not sf_conn or not pg_conn:
        logging.error(f"Could not establish database connections. Skipping {table_config['target_table']}.")
        return

    sf_cursor = sf_conn.cursor()
    pg_cursor = pg_conn.cursor()

    try:
        migrate_table_with_merge(
            sf_cursor, 
            pg_cursor, 
            pg_config.get('schema', 'public'),
            table_config
        )
        pg_conn.commit()
        logging.info(f"Successfully migrated table {table_config['target_table']}.")

    except Exception as e:
        pg_conn.rollback()
        logging.error(f"An error occurred during migration for table {table_config['target_table']}: {e}")
        raise
    finally:
        sf_cursor.close()
        pg_cursor.close()
        sf_conn.close()
        pg_conn.close()


def main():
    """Main function to run the migration process."""
    config = load_config()
    sf_config = config['snowflake']
    pg_config = config['postgres']
    max_workers = config.get('max_parallel_jobs', 1)
    
    logging.info("Starting Analytics data migration process...")
    logging.info(f"Max parallel jobs: {max_workers}")
    logging.info(f"Tables to migrate: {len(config['tables'])}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(migrate_single_table_wrapper, sf_config, pg_config, table_config) 
            for table_config in config['tables']
        ]
        
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"A migration task failed: {e}")
    
    logging.info("Analytics data migration process finished.")


if __name__ == "__main__":
    main()

