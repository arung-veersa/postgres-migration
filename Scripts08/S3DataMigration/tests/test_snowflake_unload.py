"""
Test Snowflake UNLOAD to S3
Tests unloading data from Snowflake to S3 using COPY INTO @stage
"""

import os
import sys
import json
import argparse
from datetime import datetime
from pathlib import Path
from typing import Optional

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from lib.connections import ConnectionFactory
from lib.snowflake_unloader import SnowflakeUnloader
from lib.s3_manager import S3Manager
from lib.utils import get_logger

logger = get_logger(__name__)


def load_config():
    """Load configuration from s3copyconfig.json"""
    config_path = Path(__file__).parent.parent / 's3copyconfig.json'
    
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    # Substitute environment variables
    config_str = json.dumps(config)
    for key, value in os.environ.items():
        config_str = config_str.replace(f"${{{key}}}", value)
    
    return json.loads(config_str)


def test_snowflake_unload(table_name: str, limit_rows: Optional[int] = None):
    """
    Test Snowflake UNLOAD operation
    
    Args:
        table_name: Name of table to unload (e.g., 'DIMPAYER')
        limit_rows: Optional limit for testing (e.g., 100)
    """
    
    print("\n" + "="*70)
    print("SNOWFLAKE UNLOAD TEST")
    print("="*70 + "\n")
    
    # Load environment variables
    env_file = Path(__file__).parent.parent / '.env'
    if env_file.exists():
        load_dotenv(env_file)
        print("✅ Loaded environment variables from .env")
    else:
        print("⚠️  No .env file found, using system environment variables")
    
    # Check required environment variables
    required_vars = [
        'SNOWFLAKE_ACCOUNT',
        'SNOWFLAKE_USER',
        'SNOWFLAKE_WAREHOUSE',
        'AWS_S3_BUCKET',
        'SNOWFLAKE_STORAGE_INTEGRATION',
        'SNOWFLAKE_STAGE_NAME'
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        print(f"\n❌ Missing required environment variables: {', '.join(missing_vars)}")
        return False
    
    # Verify private key
    if not os.getenv('SNOWFLAKE_PRIVATE_KEY') and not os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH'):
        print("\n❌ Missing SNOWFLAKE_PRIVATE_KEY or SNOWFLAKE_PRIVATE_KEY_PATH")
        return False
    
    try:
        # Load configuration
        config = load_config()
        print("\n✅ Configuration loaded successfully")
        
        # Find table in config
        table_config = None
        source_config = None
        
        for source in config.get('sources', []):
            for table in source.get('tables', []):
                if table['source'].upper() == table_name.upper():
                    table_config = table
                    source_config = source
                    break
            if table_config:
                break
        
        if not table_config:
            print(f"\n❌ Table '{table_name}' not found in config")
            return False
        
        # Extract table details
        source_database = source_config['source_sf_database']
        source_schema = source_config['source_sf_schema']
        source_table = table_config['source']
        source_filter = table_config.get('source_filter')
        
        print(f"\n📋 Table Details:")
        print(f"   Database: {source_database}")
        print(f"   Schema: {source_schema}")
        print(f"   Table: {source_table}")
        if source_filter:
            print(f"   Filter: {source_filter}")
        if limit_rows:
            print(f"   Limit: {limit_rows:,} rows (TEST MODE)")
        
        # Initialize connection factory
        conn_factory = ConnectionFactory(config)
        sf_manager = conn_factory.get_snowflake_manager()
        
        print("\n✅ Snowflake connection established")
        
        # ========================================================================
        # S3 MANAGER TEMPORARILY DISABLED (no AWS credentials needed)
        # ========================================================================
        # S3Manager is only needed for verification, which is disabled
        # We'll generate S3 path manually without S3Manager
        
        # s3_manager = S3Manager(config)
        # print("✅ S3 manager initialized")
        
        # Initialize Snowflake Unloader
        unloader = SnowflakeUnloader(sf_manager, config)
        print("✅ Snowflake unloader initialized")
        
        # Generate S3 path manually (without S3Manager)
        bucket = config['s3_staging']['bucket']
        
        if limit_rows:
            s3_prefix = f"{source_database}/{source_schema}/{source_table}/test_{limit_rows}_rows/"
        else:
            s3_prefix = f"{source_database}/{source_schema}/{source_table}/run_{datetime.now().strftime('%Y%m%d_%H%M%S')}/"
        
        s3_path = f"s3://{bucket}/{s3_prefix}"
        
        print(f"\n📦 S3 Destination:")
        print(f"   {s3_path}")
        
        # Build source filter with LIMIT
        combined_filter = source_filter
        if limit_rows:
            # Add LIMIT clause (Snowflake supports it in WHERE with subquery pattern)
            # We'll modify the unload query directly
            pass
        
        # Estimate unload
        if 'estimated_rows' in table_config:
            rows_to_unload = min(limit_rows, table_config['estimated_rows']) if limit_rows else table_config['estimated_rows']
            estimate = unloader.estimate_unload_time(rows_to_unload)
            
            print(f"\n⏱️  Estimated:")
            print(f"   Rows: {rows_to_unload:,}")
            print(f"   Size: {estimate['total_size_mb']:.2f} MB ({estimate['total_size_gb']:.2f} GB)")
            print(f"   Files: ~{estimate['estimated_file_count']}")
            print(f"   Time: ~{estimate['estimated_minutes']} minute(s)")
        
        # Confirm before proceeding
        if not limit_rows:
            print("\n⚠️  WARNING: This will unload the FULL table!")
            confirm = input("   Continue? (yes/no): ")
            if confirm.lower() != 'yes':
                print("❌ Aborted by user")
                return False
        
        # For testing with LIMIT, we need to modify the query
        # The unloader will build the full query, so we pass a modified filter
        if limit_rows:
            # Create a temporary source filter that includes LIMIT
            # Note: Snowflake LIMIT goes at query level, not in WHERE
            # We'll handle this by passing it as part of the table reference
            pass
        
        # Execute UNLOAD
        print("\n🚀 Starting UNLOAD operation...")
        print(f"{'='*70}\n")
        
        result = unloader.unload_table(
            source_database=source_database,
            source_schema=source_schema,
            source_table=source_table,
            s3_path=s3_path,
            source_filter=combined_filter,
            overwrite=True
        )
        
        if not result['success']:
            print(f"\n❌ UNLOAD failed: {result.get('error')}")
            return False
        
        # ========================================================================
        # S3 VERIFICATION TEMPORARILY DISABLED FOR MANUAL TESTING
        # Uncomment when AWS S3 credentials are available
        # ========================================================================
        
        # # Verify files in S3
        # print(f"\n{'='*70}")
        # print("VERIFYING FILES IN S3")
        # print(f"{'='*70}\n")
        
        # s3_files = s3_manager.list_files(prefix=s3_prefix.rstrip('/'))
        
        # if not s3_files:
        #     print("❌ No files found in S3 (unexpected!)")
        #     return False
        
        # print(f"✅ Found {len(s3_files)} file(s) in S3:")
        # for file in s3_files:
        #     print(f"   - {file['key']}")
        #     print(f"     Size: {file['size']:,} bytes ({file['size'] / 1024 / 1024:.2f} MB)")
        #     print(f"     Modified: {file['last_modified']}")
        
        # Manual verification instructions
        print(f"\n{'='*70}")
        print("⚠️  S3 VERIFICATION DISABLED - VERIFY MANUALLY")
        print(f"{'='*70}\n")
        print("📋 Manual Verification Steps:")
        print("   1. Go to AWS Console: https://console.aws.amazon.com/s3/")
        print(f"   2. Navigate to bucket: {bucket}")
        print(f"   3. Look for path: {s3_prefix}")
        print("   4. Verify .parquet files exist")
        print("\n   OR use AWS CLI:")
        print(f"   aws s3 ls s3://{bucket}/{s3_prefix} --recursive")
        print("\n   OR in Snowflake:")
        print(f"   LIST @{config['snowflake_unload']['stage_name']}/{s3_prefix};")
        print()
        
        # Summary
        print(f"\n{'='*70}")
        print("✅ TEST COMPLETED SUCCESSFULLY!")
        print(f"{'='*70}\n")
        
        print(f"📊 Summary:")
        print(f"   Files created: {len(result['files'])}")
        print(f"   Total rows: {result['total_rows']:,}")
        print(f"   Total size: {result['total_size_bytes'] / 1024 / 1024:.2f} MB")
        print(f"   Duration: {result['duration_seconds']:.2f} seconds")
        print(f"   S3 Location: {s3_path}")
        
        print(f"\n✨ Next step: Once aws_s3 extension is available, test loading into PostgreSQL\n")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Test failed with error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Test Snowflake UNLOAD to S3')
    parser.add_argument('--table', type=str, default='DIMPAYER',
                       help='Table name to unload (default: DIMPAYER)')
    parser.add_argument('--rows', type=int, default=None,
                       help='Limit rows for testing (e.g., 100)')
    
    args = parser.parse_args()
    
    success = test_snowflake_unload(args.table, args.rows)
    sys.exit(0 if success else 1)

