from __future__ import annotations

import pytest

from app.load import payload_hash, validate_record
from app.models import AdRecord
from conftest import make_ad_payload


def test_valid_record_parses_zulu_timestamps():
    record, error = validate_record(make_ad_payload())
    assert error is None
    assert isinstance(record, AdRecord)
    assert record.ad_id == "ad_test_001"
    assert record.platform == "YOUTUBE"
    assert record.version == 1


@pytest.mark.parametrize(
    "overrides,fragment",
    [
        ({"impression_count": "not-a-number"}, "impression_count"),
        ({"version": -3}, "version"),
        ({"updated_at": "yesterday"}, "updated_at"),
        ({"platform": None}, "platform"),
        ({"platform": "TIKTOK"}, "platform"),
    ],
)
def test_malformed_records_are_rejected(overrides, fragment):
    payload = make_ad_payload(**overrides)
    if "advertiser_id" in overrides and overrides["advertiser_id"] is None:
        payload.pop("advertiser_id", None)
    record, error = validate_record(payload)
    assert record is None
    assert error is not None
    assert fragment in error


def test_missing_required_field_is_rejected():
    payload = make_ad_payload()
    del payload["advertiser_id"]
    record, error = validate_record(payload)
    assert record is None
    assert "advertiser_id" in (error or "")


def test_payload_hash_is_stable_and_order_independent():
    a = {"b": 1, "a": 2}
    b = {"a": 2, "b": 1}
    assert payload_hash(a) == payload_hash(b)
    assert payload_hash(a) != payload_hash({"a": 2, "b": 3})
