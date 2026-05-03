import boto3
from botocore.config import Config
from typing import Optional
import os
from .config import settings

_client = None

def get_s3_client():
    """Return a cached S3-compatible client configured for MinIO.

    The client is created lazily on the first call and reused afterwards.
    Endpoint, credentials, SSL mode, and retry settings come from the storage
    client configuration.
    """
    global _client
    if _client is None:
        cfg = Config(signature_version='s3v4', retries={'max_attempts': 5})
        _client = boto3.client(
            "s3",
            endpoint_url=settings.MINIO_ENDPOINT,
            aws_access_key_id=settings.MINIO_ACCESS_KEY,
            aws_secret_access_key=settings.MINIO_SECRET_KEY,
            config=cfg,
            use_ssl=settings.MINIO_SECURE,
        )
    return _client

def upload_file(local_path: str, bucket: str, key: str) -> None:
    """Upload a local file to S3-compatible object storage.

    Args:
        local_path: Path to the file on the local filesystem.
        bucket: Target bucket name.
        key: Object key to create or overwrite in the bucket.
    """
    s3 = get_s3_client()
    os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
    s3.upload_file(local_path, bucket, key)

def upload_bytes(data: bytes, bucket: str, key: str, content_type: str = "application/octet-stream") -> None:
    """Upload an in-memory byte payload as a single S3 object.

    This is useful when the caller already has a bounded chunk of data in
    memory and does not need to stage it as a temporary local file.

    Args:
        data: Bytes to store as the object body.
        bucket: Target bucket name.
        key: Object key to create or overwrite in the bucket.
        content_type: MIME type stored as object metadata.
    """
    s3 = get_s3_client()
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=data,
        ContentLength=len(data),
        ContentType=content_type,
    )


def read_object_bytes(bucket: str, key: str) -> bytes:
    """Read an object from S3-compatible storage into memory."""
    s3 = get_s3_client()
    response = s3.get_object(Bucket=bucket, Key=key)
    return response["Body"].read()


def download_file(bucket: str, key: str, local_path: str) -> None:
    """Download an object from S3-compatible storage to a local file.

    Parent directories for the target path are created automatically.

    Args:
        bucket: Source bucket name.
        key: Object key to download.
        local_path: Local filesystem path where the object will be saved.
    """
    s3 = get_s3_client()
    os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
    s3.download_file(bucket, key, local_path)

def generate_presigned_url(bucket: str, key: str, expires_in: int = 3600) -> str:
    """Generate a temporary URL for downloading an object.

    Args:
        bucket: Source bucket name.
        key: Object key to expose.
        expires_in: URL lifetime in seconds.

    Returns:
        A presigned GET URL that can be used without direct S3 credentials.
    """
    s3 = get_s3_client()
    return s3.generate_presigned_url('get_object', Params={'Bucket': bucket, 'Key': key}, ExpiresIn=expires_in)

def list_objects(bucket: str, prefix: str = "") -> list[str]:
    """List object keys in a bucket under the given prefix.

    Args:
        bucket: Bucket name to search in.
        prefix: Optional key prefix, similar to a folder path.

    Returns:
        A list of object keys matching the prefix.
    """
    s3 = get_s3_client()
    paginator = s3.get_paginator("list_objects_v2")

    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])

    return keys
