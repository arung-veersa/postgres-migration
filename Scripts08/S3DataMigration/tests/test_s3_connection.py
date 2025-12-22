"""
S3 Connection Test Script
Tests S3 connectivity and basic operations
"""

import os
import sys
import json
from datetime import datetime
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
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


def test_s3_connection():
    """Test S3 connection and basic operations"""
    
    print("\n" + "="*70)
    print("S3 CONNECTION TEST")
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
        'AWS_ACCESS_KEY_ID',
        'AWS_SECRET_ACCESS_KEY',
        'AWS_REGION',
        'AWS_S3_BUCKET'
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        print(f"\n❌ Missing required environment variables: {', '.join(missing_vars)}")
        print("\nPlease set these in your .env file or environment:")
        for var in missing_vars:
            print(f"  {var}=your_value")
        return False
    
    print(f"\n📋 Configuration:")
    print(f"   AWS Region: {os.getenv('AWS_REGION')}")
    print(f"   S3 Bucket: {os.getenv('AWS_S3_BUCKET')}")
    print(f"   Access Key: {os.getenv('AWS_ACCESS_KEY_ID')[:10]}...")
    
    try:
        # Load configuration
        config = load_config()
        print("\n✅ Configuration loaded successfully")
        
        # Initialize S3 Manager
        print("\n🔧 Initializing S3 Manager...")
        s3_manager = S3Manager(config)
        
        # Test 1: Verify bucket access
        print("\n" + "-"*70)
        print("TEST 1: Verify Bucket Access")
        print("-"*70)
        
        if not s3_manager.verify_bucket_access():
            print("❌ Cannot access S3 bucket")
            return False
        
        print("✅ Bucket access verified")
        
        # Test 2: Upload test file
        print("\n" + "-"*70)
        print("TEST 2: Upload Test File")
        print("-"*70)
        
        test_content = f"S3 Migration Test - {datetime.now().isoformat()}\n"
        test_file_path = Path(__file__).parent / 'test_upload.txt'
        
        with open(test_file_path, 'w') as f:
            f.write(test_content)
        
        test_s3_key = 'test/test_upload.txt'
        upload_result = s3_manager.upload_file(
            local_file_path=str(test_file_path),
            s3_key=test_s3_key,
            metadata={'test': 'true', 'timestamp': datetime.now().isoformat()}
        )
        
        if not upload_result['success']:
            print(f"❌ Upload failed: {upload_result.get('error')}")
            return False
        
        print(f"✅ Upload successful: {upload_result['s3_url']}")
        print(f"   File size: {upload_result['file_size']:,} bytes")
        
        # Test 3: List files
        print("\n" + "-"*70)
        print("TEST 3: List Files")
        print("-"*70)
        
        files = s3_manager.list_files(prefix='test/')
        
        if not files:
            print("❌ No files found (expected at least 1)")
            return False
        
        print(f"✅ Found {len(files)} file(s) in 'test/' prefix:")
        for file in files:
            print(f"   - {file['key']} ({file['size']:,} bytes)")
        
        # Test 4: Get file info
        print("\n" + "-"*70)
        print("TEST 4: Get File Info")
        print("-"*70)
        
        file_info = s3_manager.get_file_info(test_s3_key)
        
        if not file_info:
            print("❌ Failed to get file info")
            return False
        
        print(f"✅ File info retrieved:")
        print(f"   Key: {file_info['key']}")
        print(f"   Size: {file_info['size']:,} bytes")
        print(f"   Last Modified: {file_info['last_modified']}")
        print(f"   Content Type: {file_info.get('content_type', 'N/A')}")
        
        # Test 5: Download file
        print("\n" + "-"*70)
        print("TEST 5: Download File")
        print("-"*70)
        
        download_path = Path(__file__).parent / 'test_download.txt'
        download_result = s3_manager.download_file(
            s3_key=test_s3_key,
            local_file_path=str(download_path)
        )
        
        if not download_result['success']:
            print(f"❌ Download failed: {download_result.get('error')}")
            return False
        
        print(f"✅ Download successful: {download_result['local_path']}")
        
        # Verify content
        with open(download_path, 'r') as f:
            downloaded_content = f.read()
        
        if downloaded_content == test_content:
            print("✅ Downloaded content matches uploaded content")
        else:
            print("❌ Content mismatch!")
            return False
        
        # Test 6: Delete file
        print("\n" + "-"*70)
        print("TEST 6: Delete File")
        print("-"*70)
        
        if not s3_manager.delete_file(test_s3_key):
            print("❌ Delete failed")
            return False
        
        print(f"✅ File deleted: {test_s3_key}")
        
        # Verify deletion
        file_info_after = s3_manager.get_file_info(test_s3_key)
        if file_info_after is None:
            print("✅ Verified: File no longer exists in S3")
        else:
            print("⚠️  Warning: File still exists after deletion")
        
        # Cleanup local test files
        test_file_path.unlink(missing_ok=True)
        download_path.unlink(missing_ok=True)
        print("\n✅ Local test files cleaned up")
        
        # Test 7: Test prefix generation
        print("\n" + "-"*70)
        print("TEST 7: S3 Prefix Generation")
        print("-"*70)
        
        prefix1 = s3_manager.get_s3_prefix('ANALYTICS', 'BI', 'DIMPAYER')
        print(f"✅ Basic prefix: {prefix1}")
        
        prefix2 = s3_manager.get_s3_prefix('ANALYTICS', 'BI', 'DIMPAYER', run_id='test-123')
        print(f"✅ With run_id: {prefix2}")
        
        prefix3 = s3_manager.get_s3_prefix('ANALYTICS', 'BI', 'DIMPAYER', run_id='test-123', chunk_id=1)
        print(f"✅ With run_id + chunk_id: {prefix3}")
        
        # All tests passed
        print("\n" + "="*70)
        print("✅ ALL TESTS PASSED!")
        print("="*70 + "\n")
        
        print("✨ S3 connectivity verified. You can now proceed to Phase 3 (Snowflake UNLOAD).\n")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Test failed with error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == '__main__':
    success = test_s3_connection()
    sys.exit(0 if success else 1)


