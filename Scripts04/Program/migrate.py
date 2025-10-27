import json
import os
import snowflake.connector
import psycopg2
from tqdm import tqdm
from getpass import getpass
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import io
import concurrent.futures
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def load_config():
    """Loads the configuration from config.json."""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(script_dir, 'config.json')
        with open(config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print("Error: config.json not found. Please ensure it is in the same directory as the migrate.py script.")
        exit(1)
    except json.JSONDecodeError:
        print("Error: Could not decode config.json. Please check for syntax errors.")
        exit(1)


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
            # Support for different auth methods can be added here
        )
        return conn
    except Exception as e:
        logging.error(f"Error connecting to Snowflake: {e}")
        return None


def get_private_key(rsa_key):
    private_key_prefix = '-----BEGIN PRIVATE KEY-----\n'
    private_key_suffix = '\n-----END PRIVATE KEY-----'
    
    full_rsa_key =  private_key_prefix + rsa_key + private_key_suffix
    p_key = serialization.load_pem_private_key(
            full_rsa_key.encode(),
            password=None,
            backend=default_backend()
    )
    return  p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )


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

def get_pg_definitions(pg_cursor, pg_schema, table_name):
    """Gets the definitions for indexes and foreign key constraints for a table."""
    definitions = {'indexes': [], 'constraints': []}
    
    # Get index definitions (excluding primary key indexes)
    pg_cursor.execute("""
        SELECT indexname, indexdef FROM pg_indexes
        WHERE schemaname = %s AND tablename = %s AND indexname NOT IN (
            SELECT conname FROM pg_constraint WHERE contype = 'p' AND conrelid = (
                SELECT c.oid FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relname = %s AND n.nspname = %s
            )
        );
    """, (pg_schema, table_name, table_name, pg_schema))
    for row in pg_cursor.fetchall():
        definitions['indexes'].append({'name': row[0], 'definition': row[1]})

    # Get foreign key constraint definitions
    pg_cursor.execute("""
        SELECT conname, pg_get_constraintdef(oid) as condef
        FROM pg_constraint
        WHERE contype = 'f' AND conrelid = (
            SELECT c.oid FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname = %s AND n.nspname = %s
        );
    """, (table_name, pg_schema))
    for row in pg_cursor.fetchall():
        definitions['constraints'].append({'name': row[0], 'definition': row[1]})

    return definitions

def manage_pg_objects(pg_cursor, pg_schema, table_name, definitions, action='drop'):
    """Drops or recreates indexes and constraints."""
    # Manage indexes
    for index in definitions['indexes']:
        index_name = index['name']
        index_def = index['definition']
        try:
            if action == 'drop':
                logging.info(f"Dropping index {index_name} on {table_name}...")
                pg_cursor.execute(f'DROP INDEX IF EXISTS "{pg_schema}"."{index_name}";')
            elif action == 'recreate':
                logging.info(f"Recreating index {index_name} on {table_name}...")
                pg_cursor.execute(index_def)
        except Exception as e:
            logging.error(f"Error managing index {index_name} on {table_name}: {e}")

    # Manage constraints
    for constraint in definitions['constraints']:
        constraint_name = constraint['name']
        constraint_def = constraint['definition']
        try:
            if action == 'drop':
                logging.info(f"Dropping constraint {constraint_name} on {table_name}...")
                pg_cursor.execute(f'ALTER TABLE "{pg_schema}"."{table_name}" DROP CONSTRAINT IF EXISTS "{constraint_name}";')
            elif action == 'recreate':
                logging.info(f"Recreating constraint {constraint_name} on {table_name}...")
                pg_cursor.execute(f'ALTER TABLE "{pg_schema}"."{table_name}" ADD CONSTRAINT "{constraint_name}" {constraint_def};')
        except Exception as e:
            logging.error(f"Error managing constraint {constraint_name} on {table_name}: {e}")


def migrate_table_chunked(sf_cursor, pg_cursor, column_names, source_table, target_table, where_clause, chunk_size, order_by_column):
    """Migrates a table from Snowflake to PostgreSQL in chunks."""
    offset = 0
    total_rows_migrated = 0
    
    # Get total row count for progress bar
    count_query = f"SELECT COUNT(*) FROM {source_table}{where_clause};"
    sf_cursor.execute(count_query)
    total_rows = sf_cursor.fetchone()[0]

    if total_rows == 0:
        logging.info(f"Source table {source_table} with the given filter has no rows to migrate.")
        return

    with tqdm(total=total_rows, unit="rows", desc=f"Migrating {source_table}") as pbar:
        while True:
            query = f"SELECT * FROM {source_table}{where_clause} ORDER BY \"{order_by_column}\" LIMIT {chunk_size} OFFSET {offset};"
            sf_cursor.execute(query)
            
            df = sf_cursor.fetch_pandas_all()

            if df.empty:
                break

            # Create an in-memory CSV file
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False, header=False, na_rep='\\N')
            csv_buffer.seek(0)
            
            # Use COPY for bulk insert
            try:
                pg_cursor.copy_expert(
                    sql=f"COPY {target_table} ({','.join([f'\"{c}\"' for c in column_names])}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
                    file=csv_buffer
                )
                pbar.update(len(df))
                total_rows_migrated += len(df)
                offset += chunk_size
            except Exception as e:
                logging.error(f"Error copying data for chunk in {source_table}: {e}")
                raise

    logging.info(f"Successfully migrated {total_rows_migrated} rows to {target_table}.")


def migrate_table_full(sf_cursor, pg_cursor, column_names, source_table, target_table, where_clause):
    """Migrates a full table from Snowflake to PostgreSQL."""
    # Get total row count from Snowflake
    count_query = f"SELECT COUNT(*) FROM {source_table}{where_clause};"
    sf_cursor.execute(count_query)
    total_rows = sf_cursor.fetchone()[0]
    
    if total_rows == 0:
        logging.info(f"Source table {source_table} with the given filter has no rows to migrate.")
        return

    logging.info(f"Fetching {total_rows} rows from {source_table}...")
    
    # Fetch data from Snowflake
    query = f"SELECT * FROM {source_table}{where_clause};"
    sf_cursor.execute(query)
    
    # Fetch data in pandas DataFrame batches
    batches = sf_cursor.fetch_pandas_batches()
    
    logging.info(f"Inserting data into {target_table}...")
    with tqdm(total=total_rows, unit="rows", desc=f"Migrating {source_table}") as pbar:
        for df in batches:
            # Create an in-memory CSV file
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False, header=False, na_rep='\\N')
            csv_buffer.seek(0)
            
            # Use COPY for bulk insert
            pg_cursor.copy_expert(
                sql=f"COPY {target_table} ({','.join([f'\"{c}\"' for c in column_names])}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
                file=csv_buffer
            )
            pbar.update(len(df))

    logging.info(f"Successfully migrated {total_rows} rows to {target_table}.")
    
def migrate_single_table_wrapper(sf_config, pg_config, table_config):
    """Wrapper to migrate a single table, handling connections and transactions."""
    source_table = f'"{table_config["source_table"]}"'
    target_table = f'"{pg_config.get("schema", "public")}"."{table_config["target_table"]}"'
    filter_condition = table_config.get('filter_condition')
    chunk_size = table_config.get('chunk_size')
    order_by_column = table_config.get('order_by_column')
    disable_objects = table_config.get('disable_indexes_constraints', False)

    sf_conn = get_snowflake_connection(sf_config)
    pg_conn = get_postgres_connection(pg_config)

    if not sf_conn or not pg_conn:
        logging.error(f"Could not establish database connections for table {source_table}. Skipping.")
        return

    sf_cursor = sf_conn.cursor()
    pg_cursor = pg_conn.cursor()
    definitions = None

    try:
        logging.info(f"\nStarting migration for table: {source_table} -> {target_table}")

        if disable_objects:
            definitions = get_pg_definitions(pg_cursor, pg_config.get("schema", "public"), table_config["target_table"])
            manage_pg_objects(pg_cursor, pg_config.get("schema", "public"), table_config["target_table"], definitions, action='drop')

        logging.info(f"Truncating target table {target_table}...")
        pg_cursor.execute(f"TRUNCATE TABLE {target_table} RESTART IDENTITY;")
        logging.info("Truncate complete.")

        where_clause = f" WHERE {filter_condition}" if filter_condition else ""
        
        # Get column names from the snowflake cursor description
        sf_cursor.execute(f"SELECT * FROM {source_table} LIMIT 1;")
        column_names = [desc[0] for desc in sf_cursor.description]
        
        if chunk_size and order_by_column:
            migrate_table_chunked(sf_cursor, pg_cursor, column_names, source_table, target_table, where_clause, chunk_size, order_by_column)
        else:
            migrate_table_full(sf_cursor, pg_cursor, column_names, source_table, target_table, where_clause)

        pg_conn.commit()
        logging.info(f"Successfully migrated table {source_table}.")

    except Exception as e:
        pg_conn.rollback()
        logging.error(f"An error occurred during migration for table {source_table}: {e}")
    finally:
        if disable_objects and definitions:
            manage_pg_objects(pg_cursor, pg_config.get("schema", "public"), table_config["target_table"], definitions, action='recreate')
            pg_conn.commit()

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
    
    logging.info("Starting data migration process...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(migrate_single_table_wrapper, sf_config, pg_config, table_config) for table_config in config['tables']]
        
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"A migration task failed: {e}")
    
    logging.info("Data migration process finished.")


if __name__ == "__main__":
    main()

