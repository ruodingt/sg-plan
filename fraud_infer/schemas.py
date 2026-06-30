from typing import Literal

from pydantic import BaseModel, Field


class TransactionRequest(BaseModel):
    """Raw enriched transaction as produced by Flink after join and aggregation.
    Categorical encoding is performed inside this service (features.py) before
    the feature vector is passed to the model.
    """

    customer_id: str  # passthrough: not a model feature, used for logging only
    age: int = Field(ge=0, le=120)
    gender: str
    location: str
    subscription_type: Literal["FREE", "STANDARD", "PREMIUM"]
    tenure_months: int = Field(ge=0)
    income_bracket: Literal["LOW", "MED", "HIGH"]
    event_created_at_ts: float
    transaction_value: float = Field(ge=0)
    channel_type: Literal["ONLINE", "IN-STORE"]


class FraudResponse(BaseModel):
    fraud_flag: bool
    fraud_probability: float = Field(ge=0.0, le=1.0)
