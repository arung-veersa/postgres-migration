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

def main():
    """Main function to run the migration process."""
    config = load_config()
    sf_config = config['snowflake']
    pg_config = config['postgres']

if __name__ == "__main__":
    main()

