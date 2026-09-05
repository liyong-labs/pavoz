"""contrib 公共 helpers 测试."""
import pytest

from stageflow.contrib.storage._base import _decode_payload, _encode_payload, _validate_key


def test_validate_key_accepts_safe_chars():
    _validate_key("abc-123/X_y.0")
    _validate_key("runs/task-001/checkpoint")


def test_validate_key_rejects_path_traversal():
    with pytest.raises(ValueError, match="key"):
        _validate_key("../etc/passwd")


def test_validate_key_rejects_empty():
    with pytest.raises(ValueError, match="empty"):
        _validate_key("")


def test_validate_key_rejects_unsafe_chars():
    with pytest.raises(ValueError, match="key"):
        _validate_key("with space")
    with pytest.raises(ValueError, match="key"):
        _validate_key("with?query")


def test_encode_decode_roundtrip():
    data = {"a": 1, "b": ["x", "y"], "c": "中文"}
    raw = _encode_payload(data)
    assert isinstance(raw, bytes)
    assert _decode_payload(raw) == data


def test_decode_returns_none_on_invalid_json():
    assert _decode_payload(b"\xff\xfe garbage") is None


def test_decode_returns_none_on_empty():
    assert _decode_payload(b"") is None
