from __future__ import annotations

import pickle
from pathlib import Path

import numpy as np

from fraud_infer.features import (
    encode_channel,
    encode_gender,
    encode_income,
    encode_location,
    encode_subscription,
)
from fraud_infer.schemas import FraudResponse, TransactionRequest

# Column order the model was trained with (inference.json v1).
FEATURE_ORDER: list[str] = [
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


class FraudModel:
    def __init__(self, model_path: str | Path = "fraud_model.pkl") -> None:
        with open(model_path, "rb") as fh:
            self._model = pickle.load(fh)

    def predict(self, request: TransactionRequest) -> FraudResponse:
        encoded = [
            float(request.age) if request.age is not None else np.nan,
            encode_gender(request.gender),
            encode_location(request.location),
            encode_subscription(request.subscription_type),
            float(request.tenure_months) if request.tenure_months is not None else np.nan,
            encode_income(request.income_bracket),
            float(request.event_created_at_ts)
            if request.event_created_at_ts is not None
            else np.nan,
            float(request.transaction_value) if request.transaction_value is not None else np.nan,
            encode_channel(request.channel_type),
        ]
        features = np.array([encoded], dtype=np.float32)
        proba = self._get_positive_proba(features)
        return FraudResponse(fraud_flag=proba >= 0.5, fraud_probability=proba)

    def _get_positive_proba(self, features: np.ndarray) -> float:
        """Handle both sklearn (XGBClassifier) and native XGBoost Booster APIs."""
        if hasattr(self._model, "predict_proba"):
            return float(self._model.predict_proba(features)[0][1])

        import xgboost as xgb

        dm = xgb.DMatrix(features, feature_names=FEATURE_ORDER)
        return float(self._model.predict(dm)[0])
