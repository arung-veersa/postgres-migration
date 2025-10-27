import json
import os
import snowflake.connector
import psycopg2
from tqdm import tqdm
from getpass import getpass
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import io

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
        
        print(f"Connecting to Snowflake: {config['user']}@{config['account']}")
        conn = snowflake.connector.connect(
            user=config['user'],
            account=config['account'],
            private_key=get_private_key(config['rsa_key']),
            warehouse=config['warehouse'],
            database=config['database'],
            schema=config['schema'],
            authenticator=config['authenticator'],
            port=config.get('port', 443),
            auth_method=config['auth_method']
        )
        return conn
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")
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
            port=config['port'],
            dbname=config['database'],
            user=config['user'],
            password=config['password']
        )
        return conn
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None

def migrate_table(sf_conn, pg_conn, table_config, pg_schema):
    """Migrates a single table from Snowflake to PostgreSQL."""
    source_table = f'"{table_config["source_table"]}"'
    target_table = f'"{pg_schema}"."{table_config["target_table"]}"'
    filter_condition = table_config.get('filter_condition')

    print(f"\nStarting migration for table: {source_table} -> {target_table}")

    sf_cursor = sf_conn.cursor()
    pg_cursor = pg_conn.cursor()

    try:
        # Truncate the target table
        print(f"Truncating target table {target_table}...")
        pg_cursor.execute(f"TRUNCATE TABLE {target_table};")
        print("Truncate complete.")

        # Build queries, adding WHERE clause only if a filter condition is provided
        where_clause = f" WHERE {filter_condition}" if filter_condition else ""

        # Get total row count from Snowflake
        count_query = f"SELECT COUNT(*) FROM {source_table}{where_clause};"
        sf_cursor.execute(count_query)
        total_rows = sf_cursor.fetchone()[0]
        
        if total_rows == 0:
            print(f"Source table {source_table} with the given filter has no rows to migrate.")
            return

        print(f"Fetching {total_rows} rows from {source_table}...")
        
        # Fetch data from Snowflake
        query = f"SELECT * FROM {source_table}{where_clause};"
        sf_cursor.execute(query)
        
        # Fetch data in pandas DataFrame batches
        batches = sf_cursor.fetch_pandas_batches()
        
        # Get column names from the snowflake cursor description
        column_names = [desc[0] for desc in sf_cursor.description]
        
        # Insert data into PostgreSQL
        print(f"Inserting data into {target_table}...")
        with tqdm(total=total_rows, unit="rows", desc=f"Migrating {source_table}") as pbar:
            for df in batches:
                # Create an in-memory CSV file
                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, index=False, header=False)
                csv_buffer.seek(0)
                
                # Use COPY for bulk insert
                pg_cursor.copy_expert(
                    sql=f"COPY {target_table} ({','.join([f'\"{c}\"' for c in column_names])}) FROM STDIN WITH (FORMAT CSV)",
                    file=csv_buffer
                )
                pbar.update(len(df))

        pg_conn.commit()
        print(f"Successfully migrated {total_rows} rows to {target_table}.")

    except Exception as e:
        pg_conn.rollback()
        print(f"An error occurred during migration for table {source_table}: {e}")
    finally:
        sf_cursor.close()
        pg_cursor.close()


def main():
    """Main function to run the migration process."""
    config = load_config()
    sf_config = config['snowflake']
    pg_config = config['postgres']
    
    print("Establishing database connections...")
    
    sf_conn = get_snowflake_connection(sf_config)
    if not sf_conn:
        print("Could not connect to the source Snowflake database, exiting...")
        return
    print("Connected to Snowflake database successfully.")
    
    pg_conn = get_postgres_connection(pg_config)
    if not pg_conn:
        print("Could not connect to the target PostgreSQL database, make sure you are connected to HHA VPN, exiting...")
        return
    print("Connected to PostgreSQL database successfully.")

    try:
        for table_config in config['tables']:
            migrate_table(sf_conn, pg_conn, table_config, pg_config.get('schema', 'public'))
    finally:
        print("\nClosing connections.")
        if sf_conn:
            sf_conn.close()
        if pg_conn:
            pg_conn.close()
    
    print("Data migration process finished.")

if __name__ == "__main__":
    main()

