"""Generate synthetic transaction data matching inference.json v1 schema."""

from __future__ import annotations

import numpy as np
import pandas as pd

FEATURE_ORDER = [
    "age",
    "gender_code",
    "location",
    "subscription_type_code",
    "tenure_months",
    "income_bracket_code",
    "event_created_at_ts",
    "transaction_value",
    "channel_code",
]

# Columns XGBoost should treat as categorical (not ordinal numeric).
CATEGORICAL_COLS = [
    "gender_code",
    "location",
    "subscription_type_code",
    "income_bracket_code",
    "channel_code",
]


def generate(n: int = 20_000, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)

    df = pd.DataFrame(
        {
            "age": rng.integers(18, 81, n),
            "gender_code": rng.integers(0, 3, n),  # 0=other,1=male,2=female
            "location": rng.integers(0, 200, n),
            "subscription_type_code": rng.integers(0, 3, n),  # 0=FREE,1=STD,2=PREM
            "tenure_months": rng.integers(0, 121, n),
            "income_bracket_code": rng.integers(0, 3, n),  # 0=LOW,1=MED,2=HIGH
            "event_created_at_ts": rng.uniform(1_700_000_000.0, 1_720_000_000.0, n),
            "transaction_value": np.abs(rng.exponential(scale=150.0, size=n)),
            "channel_code": rng.integers(0, 2, n),  # 0=ONLINE,1=IN-STORE
        }
    )

    # Fraud heuristic: high-value tx + short tenure + premium tier = higher risk.
    risk = (
        (df["transaction_value"] > 300).astype(float) * 0.35
        + (df["tenure_months"] < 6).astype(float) * 0.25
        + (df["subscription_type_code"] == 2).astype(float) * 0.15
        + rng.random(n) * 0.25
    )
    df["fraud_flag"] = (risk > 0.55).astype(int)

    for col in CATEGORICAL_COLS:
        df[col] = df[col].astype("category")

    return df


if __name__ == "__main__":
    df = generate()
    print(df.shape)
    print(df["fraud_flag"].value_counts(normalize=True).round(3))
    print(df.dtypes)
