from __future__ import annotations

import hashlib
import random
from datetime import datetime, timedelta, timezone
from typing import Any

from .config import settings

PLATFORMS = ("YOUTUBE", "SEARCH", "DISPLAY")
ADVERTISERS = [
    ("adv_1001", "PeakForm Apparel"),
    ("adv_1002", "Northline Outdoor"),
    ("adv_1003", "Velvet Roast Coffee"),
    ("adv_1004", "Atlas Home Goods"),
    ("adv_1005", "Kinetic Wear"),
    ("adv_1006", "Harbor & Oak"),
    ("adv_1007", "Lumen Skincare"),
    ("adv_1008", "Forge Fitness Co"),
]


def _stable_rng(seed: int) -> random.Random:
    return random.Random(seed)


def _ad_id(index: int) -> str:
    digest = hashlib.sha1(f"ad-{settings.seed}-{index}".encode()).hexdigest()[:12]
    return f"ad_{digest}"


def _valid_record(rng: random.Random, index: int, version: int = 1) -> dict[str, Any]:
    advertiser_id, advertiser_name = ADVERTISERS[index % len(ADVERTISERS)]
    first_shown = datetime(2024, 1, 1, tzinfo=timezone.utc) + timedelta(days=rng.randint(0, 500))
    last_shown = first_shown + timedelta(days=rng.randint(1, 120))
    updated_at = last_shown + timedelta(hours=rng.randint(1, 72) * version)

    return {
        "ad_id": _ad_id(index),
        "advertiser_id": advertiser_id,
        "advertiser_name": advertiser_name,
        "platform": PLATFORMS[index % len(PLATFORMS)],
        "creative_url": f"https://cdn.example.com/creatives/{_ad_id(index)}.mp4",
        "impression_count": rng.randint(1_000, 5_000_000) * version,
        "first_shown_at": first_shown.isoformat().replace("+00:00", "Z"),
        "last_shown_at": last_shown.isoformat().replace("+00:00", "Z"),
        "version": version,
        "updated_at": updated_at.isoformat().replace("+00:00", "Z"),
    }


def _malformed_record(rng: random.Random, index: int) -> dict[str, Any]:
    kind = rng.randint(0, 4)
    base = _valid_record(rng, index)
    if kind == 0:
        del base["advertiser_id"]
    elif kind == 1:
        base["impression_count"] = "not-a-number"
    elif kind == 2:
        base["version"] = -3
    elif kind == 3:
        base["updated_at"] = "yesterday"
    else:
        base["platform"] = None
    return base


def build_feed() -> list[dict[str, Any]]:
    rng = _stable_rng(settings.seed)
    feed: list[dict[str, Any]] = []

    for index in range(settings.record_count):
        if rng.random() < settings.malformed_rate:
            feed.append(_malformed_record(rng, index))
        else:
            feed.append(_valid_record(rng, index, version=1))

        if rng.random() < settings.duplicate_rate:
            # At-least-once: emit an older snapshot after a newer one may already exist.
            older = _valid_record(rng, index, version=1)
            older["updated_at"] = (
                datetime.fromisoformat(older["updated_at"].replace("Z", "+00:00"))
                - timedelta(days=3)
            ).isoformat().replace("+00:00", "Z")
            older["impression_count"] = max(1, int(older["impression_count"] * 0.5))
            feed.append(older)

        if rng.random() < settings.duplicate_rate * 0.5:
            # Newer version of the same ad (version bump).
            feed.append(_valid_record(rng, index, version=2))

    # Shuffle so delivery order is not monotonic by index, while remaining deterministic.
    order_rng = _stable_rng(settings.seed + 7)
    order_rng.shuffle(feed)
    return feed


FEED: list[dict[str, Any]] = build_feed()
