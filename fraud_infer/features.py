"""Feature encoding: maps raw table values → integer codes expected by the model."""

from __future__ import annotations

GENDER_MAP: dict[str, int] = {"male": 1, "female": 2}
SUBSCRIPTION_MAP: dict[str, int] = {"FREE": 0, "STANDARD": 1, "PREMIUM": 2}
INCOME_MAP: dict[str, int] = {"LOW": 0, "MED": 1, "HIGH": 2}
CHANNEL_MAP: dict[str, int] = {"ONLINE": 0, "IN-STORE": 1}


def encode_gender(raw: str) -> int:
    """0 for unknown/other, 1 for male, 2 for female."""
    return GENDER_MAP.get(raw.strip().lower(), 0)


def encode_subscription(raw: str) -> int:
    return SUBSCRIPTION_MAP[raw.strip().upper()]


def encode_income(raw: str) -> int:
    return INCOME_MAP[raw.strip().upper()]


def encode_channel(raw: str) -> int:
    return CHANNEL_MAP[raw.strip().upper()]


def encode_location(raw: str) -> int:
    """
    In production, load a LabelEncoder fitted at training time from the model
    artifact store (e.g. S3 alongside fraud_model.pkl). Here we use a
    deterministic hash as a safe fallback so unknown locations don't crash.
    """
    try:
        return int(raw)
    except (ValueError, TypeError):
        return abs(hash(raw)) % 100_000
