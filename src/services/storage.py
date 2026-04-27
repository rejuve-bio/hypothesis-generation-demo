"""
MinIO Storage Client for handling file uploads and downloads
"""
import os
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from loguru import logger
from typing import Optional, BinaryIO


class MinIOStorage:
    """MinIO object storage client wrapper"""
    
    def __init__(self, endpoint: str, access_key: str, secret_key: str, 
                 bucket: str, use_ssl: bool = False):
        """
        Initialize MinIO client
        """
        self.bucket = bucket
        self.endpoint = endpoint
        
        # Create boto3 client with path-style addressing for MinIO
        self.client = boto3.client(
            's3',
            endpoint_url=f"{'https' if use_ssl else 'http'}://{endpoint}",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(
                signature_version='s3v4',
                s3={'addressing_style': 'path'},  # Force path-style for MinIO compatibility
                connect_timeout=60,  # Increased timeout for remote MinIO
                read_timeout=300,  # 5 minutes for large file uploads
                retries={'max_attempts': 3}
            ),
        )
        
        logger.info(f"MinIO client initialized for bucket '{bucket}' at {endpoint}")
    
    def upload_file(self, local_path: str, object_key: str, bucket: Optional[str] = None) -> bool:

        target_bucket = bucket or self.bucket
        
        try:
            self.client.upload_file(local_path, target_bucket, object_key)
            file_size_mb = os.path.getsize(local_path) / (1024 * 1024)
            logger.info(f"[MinIO] Uploaded {file_size_mb:.2f}MB to s3://{target_bucket}/{object_key}")
            return True
        except ClientError as e:
            logger.error(f"[MinIO] Upload failed for {object_key}: {e}")
            return False
        except Exception as e:
            logger.error(f"[MinIO] Unexpected error uploading {object_key}: {e}")
            return False
    
    def upload_fileobj(self, file_obj: BinaryIO, object_key: str, bucket: Optional[str] = None) -> bool:
        target_bucket = bucket or self.bucket
        
        try:
            self.client.upload_fileobj(file_obj, target_bucket, object_key)
            logger.info(f"[MinIO] Uploaded file object to s3://{target_bucket}/{object_key}")
            return True
        except ClientError as e:
            logger.error(f"[MinIO] Upload failed for {object_key}: {e}")
            return False
        except Exception as e:
            logger.error(f"[MinIO] Unexpected error uploading {object_key}: {e}")
            return False
    
    def download_file(self, object_key: str, local_path: str, bucket: Optional[str] = None) -> bool:
        target_bucket = bucket or self.bucket
        
        try:
            # Create parent directory if it doesn't exist
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            self.client.download_file(target_bucket, object_key, local_path)
            file_size_mb = os.path.getsize(local_path) / (1024 * 1024)
            logger.info(f"[MinIO] Downloaded {file_size_mb:.2f}MB from s3://{target_bucket}/{object_key}")
            return True
        except ClientError as e:
            logger.error(f"[MinIO] Download failed for {object_key}: {e}")
            return False
        except Exception as e:
            logger.error(f"[MinIO] Unexpected error downloading {object_key}: {e}")
            return False
    
    def exists(self, object_key: str, bucket: Optional[str] = None) -> bool:
        target_bucket = bucket or self.bucket
        
        try:
            self.client.head_object(Bucket=target_bucket, Key=object_key)
            return True
        except ClientError:
            return False
    
    def delete_file(self, object_key: str, bucket: Optional[str] = None) -> bool:
        target_bucket = bucket or self.bucket
        
        try:
            self.client.delete_object(Bucket=target_bucket, Key=object_key)
            logger.info(f"[MinIO] Deleted s3://{target_bucket}/{object_key}")
            return True
        except ClientError as e:
            logger.error(f"[MinIO] Delete failed for {object_key}: {e}")
            return False
    
    def generate_presigned_url(self, object_key: str, expiration: int = 3600, 
                               bucket: Optional[str] = None) -> Optional[str]:
        target_bucket = bucket or self.bucket
        
        try:
            url = self.client.generate_presigned_url(
                'get_object',
                Params={'Bucket': target_bucket, 'Key': object_key},
                ExpiresIn=expiration
            )
            logger.info(f"[MinIO] Generated presigned URL for {object_key} (expires in {expiration}s)")
            return url
        except ClientError as e:
            logger.error(f"[MinIO] Failed to generate presigned URL for {object_key}: {e}")
            return None


def create_minio_client_from_env() -> Optional[MinIOStorage]:
    endpoint = os.getenv('MINIO_ENDPOINT')
    access_key = os.getenv('MINIO_ACCESS_KEY')
    secret_key = os.getenv('MINIO_SECRET_KEY')
    bucket = os.getenv('MINIO_BUCKET', 'hypothesis')
    use_ssl = os.getenv('MINIO_USE_SSL', 'false').lower() == 'true'
    
    if not all([endpoint, access_key, secret_key]):
        logger.warning("[MinIO] Missing MinIO configuration in environment variables")
        return None
    
    return MinIOStorage(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        bucket=bucket,
        use_ssl=use_ssl
    )

