"""Feature encoding: maps raw table values → integer codes expected by the model."""

from __future__ import annotations

import math
from datetime import UTC, datetime

GENDER_MAP: dict[str, int] = {"male": 1, "female": 2}
SUBSCRIPTION_MAP: dict[str, int] = {"FREE": 0, "STANDARD": 1, "PREMIUM": 2}
INCOME_MAP: dict[str, int] = {"LOW": 0, "MED": 1, "HIGH": 2}
CHANNEL_MAP: dict[str, int] = {"ONLINE": 0, "IN-STORE": 1}

_NAN = math.nan


def encode_gender(raw: str | None) -> float:
    """0 for unknown/other, 1 for male, 2 for female, nan if missing."""
    if raw is None:
        return _NAN
    return GENDER_MAP.get(raw.strip().lower(), 0)


def encode_subscription(raw: str | None) -> float:
    if raw is None:
        return _NAN
    return SUBSCRIPTION_MAP[raw.strip().upper()]


def encode_income(raw: str | None) -> float:
    if raw is None:
        return _NAN
    return INCOME_MAP[raw.strip().upper()]


def encode_channel(raw: str | None) -> float:
    if raw is None:
        return _NAN
    return CHANNEL_MAP[raw.strip().upper()]


def encode_event_created_at(raw: datetime | float | None) -> float:
    """Convert event_created_at (DATETIME from user_activity) to a Unix timestamp float.

    Accepts a Python datetime (naive treated as UTC) or a numeric value already
    in Unix seconds. Returns nan if missing.
    """
    if raw is None:
        return _NAN
    if isinstance(raw, datetime):
        if raw.tzinfo is None:
            raw = raw.replace(tzinfo=UTC)
        return raw.timestamp()
    return float(raw)


def encode_location(raw: str | None) -> float:
    # TODO: load the LabelEncoder fitted at training time (stored alongside fraud_model.pkl in S3)
    #       and use it for the string → integer mapping. Until then, non-numeric values return nan.
    if raw is None:
        return _NAN
    try:
        return int(raw)
    except (ValueError, TypeError):
        return _NAN
