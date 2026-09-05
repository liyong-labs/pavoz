import pytest

from stageflow.contrib.storage.minio import MinioStorage


@pytest.fixture
def aws_credentials(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture
def moto_bucket(aws_credentials):
    moto = pytest.importorskip("moto")
    mock = moto.mock_aws()
    mock.start()
    import boto3

    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="test-kv")
    yield "test-kv"
    mock.stop()


@pytest.fixture
def store(moto_bucket) -> MinioStorage:
    return MinioStorage(
        bucket=moto_bucket, access_key="testing", secret_key="testing", prefix="sf:"
    )


def test_put_get_roundtrip(store):
    store.put("k", {"a": 1, "中文": "ok"})
    assert store.get("k") == {"a": 1, "中文": "ok"}


def test_get_missing_returns_none(store):
    assert store.get("nope") is None


def test_put_overwrites(store):
    store.put("k", {"v": 1})
    store.put("k", {"v": 2})
    assert store.get("k") == {"v": 2}


def test_delete_silent_on_missing(store):
    store.delete("never-existed")  # 不抛


def test_list_keys_prefix(store):
    store.put("a/1", {})
    store.put("a/2", {})
    store.put("b/1", {})
    assert store.list_keys("a/") == ["a/1", "a/2"]


def test_unicode_safe(store):
    data = {"emoji": "🎉", "中文": "ok"}
    store.put("u", data)
    assert store.get("u") == data


def test_delete_swallows_no_such_key(store, monkeypatch):
    """对齐 ai_writer sf_storage.py: NoSuchKey 抛 → 静默. 强制 mock 抛 ClientError."""
    import botocore.exceptions

    def _raise_no_such_key(Bucket, Key):
        raise botocore.exceptions.ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "not found"}}, "DeleteObject"
        )

    monkeypatch.setattr(store._client, "delete_object", _raise_no_such_key)
    store.delete("never-existed")  # 必须不抛 (swallow path)


def test_get_swallows_no_such_key(store, aws_credentials, monkeypatch):
    """get 也容忍 NoSuchKey → 返 None."""
    import boto3

    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="test-kv-3")
    s32 = MinioStorage(bucket="test-kv-3", access_key="testing", secret_key="testing")
    assert s32.get("never-was") is None
