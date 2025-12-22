"""
S3 Manager Module
Handles all S3 operations for migration staging
"""

import os
import logging
from typing import List, Dict, Any, Optional
import boto3
from botocore.exceptions import ClientError, BotoCoreError
from datetime import datetime

logger = logging.getLogger(__name__)


class S3Manager:
    """Manages S3 operations for data migration staging"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize S3 Manager
        
        Args:
            config: Configuration dictionary containing AWS and S3 settings
        """
        self.aws_config = config.get('aws', {})
        self.s3_config = config.get('s3_staging', {})
        
        # AWS credentials
        self.access_key_id = self.aws_config.get('access_key_id')
        self.secret_access_key = self.aws_config.get('secret_access_key')
        self.region = self.aws_config.get('region', 'us-east-1')
        
        # S3 settings
        self.bucket = self.s3_config.get('bucket')
        self.prefix_pattern = self.s3_config.get('prefix_pattern', '{source_database}/{source_schema}/{source_table}/')
        
        # Initialize S3 client
        self.s3_client = self._initialize_s3_client()
        
        logger.info(f"S3Manager initialized - Bucket: {self.bucket}, Region: {self.region}")
    
    def _initialize_s3_client(self):
        """Initialize boto3 S3 client with credentials"""
        try:
            session = boto3.Session(
                aws_access_key_id=self.access_key_id,
                aws_secret_access_key=self.secret_access_key,
                region_name=self.region
            )
            
            s3_client = session.client('s3')
            
            # Test connection by listing buckets
            s3_client.list_buckets()
            logger.info("✅ S3 client initialized successfully")
            
            return s3_client
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            error_msg = e.response.get('Error', {}).get('Message', str(e))
            logger.error(f"❌ Failed to initialize S3 client: {error_code} - {error_msg}")
            raise
        except Exception as e:
            logger.error(f"❌ Unexpected error initializing S3 client: {str(e)}")
            raise
    
    def get_s3_prefix(
        self,
        source_database: str,
        source_schema: str,
        source_table: str,
        run_id: Optional[str] = None,
        chunk_id: Optional[int] = None,
        custom_prefix: Optional[str] = None
    ) -> str:
        """
        Generate S3 prefix based on pattern
        
        Args:
            source_database: Source database name
            source_schema: Source schema name
            source_table: Source table name
            run_id: Migration run ID (optional)
            chunk_id: Chunk ID (optional)
            custom_prefix: Custom prefix override (optional)
        
        Returns:
            S3 prefix string (e.g., "ANALYTICS/BI/DIMPAYER/run_123/chunk_001/")
        """
        if custom_prefix:
            prefix = custom_prefix
        else:
            prefix = self.prefix_pattern.format(
                source_database=source_database,
                source_schema=source_schema,
                source_table=source_table
            )
        
        # Add run_id if provided
        if run_id:
            prefix = os.path.join(prefix, f"run_{run_id}")
        
        # Add chunk_id if provided
        if chunk_id:
            prefix = os.path.join(prefix, f"chunk_{chunk_id:03d}")
        
        # Ensure trailing slash and normalize path separators
        prefix = prefix.replace('\\', '/').rstrip('/') + '/'
        
        return prefix
    
    def upload_file(
        self,
        local_file_path: str,
        s3_key: str,
        metadata: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Upload a file to S3
        
        Args:
            local_file_path: Path to local file
            s3_key: S3 key (path within bucket)
            metadata: Optional metadata to attach to object
        
        Returns:
            Dictionary with upload details
        """
        try:
            file_size = os.path.getsize(local_file_path)
            
            logger.info(f"Uploading {local_file_path} to s3://{self.bucket}/{s3_key} ({file_size:,} bytes)")
            
            extra_args = {}
            if metadata:
                extra_args['Metadata'] = metadata
            
            self.s3_client.upload_file(
                Filename=local_file_path,
                Bucket=self.bucket,
                Key=s3_key,
                ExtraArgs=extra_args if extra_args else None
            )
            
            logger.info(f"✅ Upload successful: s3://{self.bucket}/{s3_key}")
            
            return {
                'success': True,
                'bucket': self.bucket,
                's3_key': s3_key,
                'file_size': file_size,
                's3_url': f"s3://{self.bucket}/{s3_key}"
            }
            
        except ClientError as e:
            error_msg = f"S3 upload failed: {str(e)}"
            logger.error(f"❌ {error_msg}")
            return {
                'success': False,
                'error': error_msg
            }
    
    def download_file(
        self,
        s3_key: str,
        local_file_path: str
    ) -> Dict[str, Any]:
        """
        Download a file from S3
        
        Args:
            s3_key: S3 key (path within bucket)
            local_file_path: Path to save file locally
        
        Returns:
            Dictionary with download details
        """
        try:
            # Get object metadata first to check size
            response = self.s3_client.head_object(Bucket=self.bucket, Key=s3_key)
            file_size = response['ContentLength']
            
            logger.info(f"Downloading s3://{self.bucket}/{s3_key} to {local_file_path} ({file_size:,} bytes)")
            
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
            
            self.s3_client.download_file(
                Bucket=self.bucket,
                Key=s3_key,
                Filename=local_file_path
            )
            
            logger.info(f"✅ Download successful: {local_file_path}")
            
            return {
                'success': True,
                'bucket': self.bucket,
                's3_key': s3_key,
                'local_path': local_file_path,
                'file_size': file_size
            }
            
        except ClientError as e:
            error_msg = f"S3 download failed: {str(e)}"
            logger.error(f"❌ {error_msg}")
            return {
                'success': False,
                'error': error_msg
            }
    
    def list_files(
        self,
        prefix: str,
        max_keys: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        List files in S3 with given prefix
        
        Args:
            prefix: S3 prefix to search
            max_keys: Maximum number of keys to return
        
        Returns:
            List of file dictionaries with keys: key, size, last_modified
        """
        try:
            logger.debug(f"Listing files in s3://{self.bucket}/{prefix}")
            
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=prefix,
                MaxKeys=max_keys
            )
            
            files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    files.append({
                        'key': obj['Key'],
                        'size': obj['Size'],
                        'last_modified': obj['LastModified'],
                        's3_url': f"s3://{self.bucket}/{obj['Key']}"
                    })
            
            logger.info(f"✅ Found {len(files)} file(s) in s3://{self.bucket}/{prefix}")
            
            return files
            
        except ClientError as e:
            error_msg = f"S3 list failed: {str(e)}"
            logger.error(f"❌ {error_msg}")
            return []
    
    def delete_file(self, s3_key: str) -> bool:
        """
        Delete a file from S3
        
        Args:
            s3_key: S3 key (path within bucket)
        
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Deleting s3://{self.bucket}/{s3_key}")
            
            self.s3_client.delete_object(
                Bucket=self.bucket,
                Key=s3_key
            )
            
            logger.info(f"✅ Delete successful: s3://{self.bucket}/{s3_key}")
            return True
            
        except ClientError as e:
            error_msg = f"S3 delete failed: {str(e)}"
            logger.error(f"❌ {error_msg}")
            return False
    
    def delete_files(self, s3_keys: List[str]) -> Dict[str, Any]:
        """
        Delete multiple files from S3 (batch operation)
        
        Args:
            s3_keys: List of S3 keys to delete
        
        Returns:
            Dictionary with deletion results
        """
        if not s3_keys:
            return {'deleted': 0, 'failed': 0}
        
        try:
            logger.info(f"Deleting {len(s3_keys)} file(s) from S3")
            
            # Batch delete (max 1000 objects per request)
            deleted = 0
            failed = 0
            
            for i in range(0, len(s3_keys), 1000):
                batch = s3_keys[i:i+1000]
                
                delete_objects = [{'Key': key} for key in batch]
                
                response = self.s3_client.delete_objects(
                    Bucket=self.bucket,
                    Delete={'Objects': delete_objects}
                )
                
                deleted += len(response.get('Deleted', []))
                failed += len(response.get('Errors', []))
            
            logger.info(f"✅ Batch delete completed: {deleted} deleted, {failed} failed")
            
            return {
                'deleted': deleted,
                'failed': failed,
                'success': failed == 0
            }
            
        except ClientError as e:
            error_msg = f"S3 batch delete failed: {str(e)}"
            logger.error(f"❌ {error_msg}")
            return {
                'deleted': 0,
                'failed': len(s3_keys),
                'success': False,
                'error': error_msg
            }
    
    def get_file_info(self, s3_key: str) -> Optional[Dict[str, Any]]:
        """
        Get metadata for a single S3 file
        
        Args:
            s3_key: S3 key (path within bucket)
        
        Returns:
            Dictionary with file info or None if not found
        """
        try:
            response = self.s3_client.head_object(Bucket=self.bucket, Key=s3_key)
            
            return {
                'key': s3_key,
                'size': response['ContentLength'],
                'last_modified': response['LastModified'],
                'content_type': response.get('ContentType'),
                'metadata': response.get('Metadata', {}),
                's3_url': f"s3://{self.bucket}/{s3_key}"
            }
            
        except ClientError as e:
            if e.response.get('Error', {}).get('Code') == '404':
                logger.warning(f"File not found: s3://{self.bucket}/{s3_key}")
            else:
                logger.error(f"Failed to get file info: {str(e)}")
            return None
    
    def verify_bucket_access(self) -> bool:
        """
        Verify that we have access to the configured S3 bucket
        
        Returns:
            True if access is verified, False otherwise
        """
        try:
            # Try to list objects (limited to 1)
            self.s3_client.list_objects_v2(
                Bucket=self.bucket,
                MaxKeys=1
            )
            
            logger.info(f"✅ Verified access to S3 bucket: {self.bucket}")
            return True
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            logger.error(f"❌ Cannot access S3 bucket '{self.bucket}': {error_code}")
            return False


