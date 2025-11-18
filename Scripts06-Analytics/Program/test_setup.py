"""
Test script to validate the migration setup before running the full migration.
"""
import os
import sys
from dotenv import load_dotenv
import json

def test_env_file():
    """Test if .env file exists and can be loaded."""
    print("Testing .env file...")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    env_path = os.path.join(script_dir, '.env')
    
    if not os.path.exists(env_path):
        print("  ❌ .env file not found!")
        print("  → Create .env file based on .env.example")
        return False
    
    load_dotenv(env_path)
    
    required_vars = [
        'SNOWFLAKE_USER', 'SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_WAREHOUSE',
        'SNOWFLAKE_DATABASE', 'SNOWFLAKE_SCHEMA', 'SNOWFLAKE_RSA_KEY',
        'POSTGRES_HOST', 'POSTGRES_DATABASE', 'POSTGRES_USER', 'POSTGRES_PASSWORD'
    ]
    
    missing = [var for var in required_vars if not os.getenv(var)]
    
    if missing:
        print(f"  ❌ Missing environment variables: {', '.join(missing)}")
        return False
    
    print("  ✅ .env file loaded successfully")
    return True


def test_config_file():
    """Test if config.json exists and is valid."""
    print("\nTesting config.json...")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, 'config.json')
    
    if not os.path.exists(config_path):
        print("  ❌ config.json not found!")
        return False
    
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        if 'tables' not in config:
            print("  ❌ config.json missing 'tables' key")
            return False
        
        print(f"  ✅ config.json loaded successfully ({len(config['tables'])} table(s) configured)")
        
        for i, table in enumerate(config['tables']):
            print(f"\n  Table {i+1}: {table.get('source_view')} → {table.get('target_table')}")
            print(f"    Load type: {table.get('load_type')}")
            print(f"    Watermark: {table.get('watermark_column', 'N/A')}")
            if table.get('source_where_clause'):
                print(f"    WHERE: {table.get('source_where_clause')}")
        
        return True
    except json.JSONDecodeError as e:
        print(f"  ❌ Invalid JSON: {e}")
        return False


def test_dependencies():
    """Test if required Python packages are installed."""
    print("\nTesting Python dependencies...")
    required_packages = [
        'snowflake.connector',
        'psycopg2',
        'tqdm',
        'dotenv'
    ]
    
    missing = []
    for package in required_packages:
        try:
            __import__(package)
            print(f"  ✅ {package}")
        except ImportError:
            print(f"  ❌ {package} not installed")
            missing.append(package)
    
    if missing:
        print(f"\n  → Run: pip install -r requirements.txt")
        return False
    
    return True


def test_connections():
    """Test database connections."""
    print("\nTesting database connections...")
    
    # Import after dependency check
    try:
        import snowflake.connector
        import psycopg2
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import serialization
    except ImportError as e:
        print(f"  ❌ Cannot import required modules: {e}")
        return False
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    load_dotenv(os.path.join(script_dir, '.env'))
    
    # Test Snowflake
    print("\n  Testing Snowflake connection...")
    try:
        rsa_key = os.getenv('SNOWFLAKE_RSA_KEY')
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
        private_key_bytes = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        
        conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            private_key=private_key_bytes,
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA'),
            authenticator='SNOWFLAKE_JWT'
        )
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_USER(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
        user, db, schema = cursor.fetchone()
        print(f"    ✅ Connected as {user} to {db}.{schema}")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"    ❌ Snowflake connection failed: {e}")
        return False
    
    # Test PostgreSQL
    print("\n  Testing PostgreSQL connection...")
    try:
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST'),
            port=int(os.getenv('POSTGRES_PORT', 5432)),
            dbname=os.getenv('POSTGRES_DATABASE'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD')
        )
        cursor = conn.cursor()
        cursor.execute("SELECT current_user, current_database()")
        user, db = cursor.fetchone()
        print(f"    ✅ Connected as {user} to {db}")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"    ❌ PostgreSQL connection failed: {e}")
        return False
    
    return True


def main():
    """Run all tests."""
    print("="*60)
    print("Analytics Migration - Pre-flight Checks")
    print("="*60)
    
    tests = [
        ("Environment file", test_env_file),
        ("Configuration file", test_config_file),
        ("Python dependencies", test_dependencies),
        ("Database connections", test_connections),
    ]
    
    results = []
    for name, test_func in tests:
        try:
            result = test_func()
            results.append(result)
        except Exception as e:
            print(f"\n  ❌ {name} test failed with error: {e}")
            results.append(False)
    
    print("\n" + "="*60)
    if all(results):
        print("✅ All checks passed! Ready to run migration.")
        print("\nRun: python migrate.py")
    else:
        print("❌ Some checks failed. Please fix the issues above.")
        sys.exit(1)
    print("="*60)


if __name__ == "__main__":
    main()

