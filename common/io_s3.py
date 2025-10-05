from __future__ import annotations

import io

import boto3
from botocore.client import Config

from .config import MINIO

_session = boto3.session.Session()
s3 = _session.client(
    "s3",
    endpoint_url=f"http://{MINIO.endpoint}",
    aws_access_key_id=MINIO.access_key,
    aws_secret_access_key=MINIO.secret_key,
    config=Config(s3={"addressing_style": "path"}),
)


def put_bytes(key: str, data: bytes, content_type: str = "application/octet-stream") -> None:
    s3.put_object(Bucket=MINIO.bucket, Key=key, Body=io.BytesIO(data), ContentType=content_type)


def put_file(key: str, file_path: str, content_type: str = "application/octet-stream") -> None:
    with open(file_path, "rb") as f:
        s3.upload_fileobj(f, MINIO.bucket, key, ExtraArgs={"ContentType": content_type})
