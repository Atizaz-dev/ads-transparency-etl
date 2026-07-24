from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator


ALLOWED_PLATFORMS = {"YOUTUBE", "SEARCH", "DISPLAY"}


class AdRecord(BaseModel):
    ad_id: str = Field(min_length=1)
    advertiser_id: str = Field(min_length=1)
    advertiser_name: str = Field(min_length=1)
    platform: str
    creative_url: str = Field(min_length=1)
    impression_count: int = Field(ge=0)
    first_shown_at: datetime
    last_shown_at: datetime
    version: int = Field(gt=0)
    updated_at: datetime

    @field_validator("platform")
    @classmethod
    def validate_platform(cls, value: str) -> str:
        if value not in ALLOWED_PLATFORMS:
            raise ValueError(f"unsupported platform: {value}")
        return value

    @field_validator("first_shown_at", "last_shown_at", "updated_at", mode="before")
    @classmethod
    def parse_timestamps(cls, value: Any) -> Any:
        if isinstance(value, str) and value.endswith("Z"):
            return value.replace("Z", "+00:00")
        return value
