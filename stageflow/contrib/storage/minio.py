"""MinioStorage — boto3 S3 client (AWS S3 / MinIO / R2).

行为对齐 ai_writer backend/integration/sf_storage.py:
- get 返 None 当 key 不存在
- delete 容忍 NoSuchKey/404 (静默)
"""
from __future__ import annotations

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError as _e:
    raise ImportError(
        "MinioStorage 需要 boto3. 安装: pip install 'stageflow[minio]'"
    ) from _e

from stageflow.storage import StorageBackend

from ._base import _decode_payload, _encode_payload, _validate_key

__all__ = ["MinioStorage"]


class MinioStorage(StorageBackend):
    """S3-compatible object storage.

    Args:
        bucket: 必须预先存在
        prefix: 防止跨项目 key 冲突
        endpoint_url: None = AWS default; 设 'http://localhost:9000' = MinIO
        access_key / secret_key: 凭证 (None = env / IAM role)
        region: AWS region (default 'us-east-1')
    """

    def __init__(
        self,
        *,
        bucket: str,
        prefix: str = "stageflow:",
        endpoint_url: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
        region: str = "us-east-1",
    ):
        self.bucket = bucket
        self.prefix = prefix
        kwargs: dict = {"region_name": region}
        if endpoint_url:
            kwargs["endpoint_url"] = endpoint_url
        if access_key and secret_key:
            kwargs["aws_access_key_id"] = access_key
            kwargs["aws_secret_access_key"] = secret_key
        self._client = boto3.client("s3", **kwargs)

    def put(self, key: str, data: dict) -> None:
        _validate_key(key)
        self._client.put_object(
            Bucket=self.bucket, Key=self.prefix + key, Body=_encode_payload(data)
        )

    def get(self, key: str) -> dict | None:
        _validate_key(key)
        try:
            obj = self._client.get_object(Bucket=self.bucket, Key=self.prefix + key)
        except ClientError as e:
            if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
                return None
            raise
        return _decode_payload(obj["Body"].read())

    def list_keys(self, prefix: str) -> list[str]:
        full_prefix = self.prefix + prefix
        out: list[str] = []
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=full_prefix):
            for obj in page.get("Contents", []):
                raw_key = obj["Key"]
                if raw_key.startswith(self.prefix):
                    out.append(raw_key[len(self.prefix) :])
        return sorted(out)

    def delete(self, key: str) -> None:
        _validate_key(key)
        try:
            self._client.delete_object(Bucket=self.bucket, Key=self.prefix + key)
        except ClientError as e:
            if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
                return  # 静默 — 对齐 ai_writer sf_storage.py
            raise
