"""Smoke tests: inference must not crash when fields are None (missing values)."""

from __future__ import annotations

import math
from pathlib import Path

import pytest

from fraud_infer.features import (
    encode_channel,
    encode_gender,
    encode_income,
    encode_location,
    encode_subscription,
)
from fraud_infer.model import FraudModel
from fraud_infer.schemas import TransactionRequest

MODEL_PATH = Path(__file__).parent.parent / "mock_model_train" / "fraud_model_mock.pkl"


# ---------------------------------------------------------------------------
# Encode functions return nan for None
# ---------------------------------------------------------------------------


def test_encode_gender_none_returns_nan():
    assert math.isnan(encode_gender(None))


def test_encode_subscription_none_returns_nan():
    assert math.isnan(encode_subscription(None))


def test_encode_income_none_returns_nan():
    assert math.isnan(encode_income(None))


def test_encode_channel_none_returns_nan():
    assert math.isnan(encode_channel(None))


def test_encode_location_none_returns_nan():
    assert math.isnan(encode_location(None))


# ---------------------------------------------------------------------------
# End-to-end: model.predict must not raise with missing fields
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def model() -> FraudModel:
    return FraudModel(MODEL_PATH)


def _all_none_request() -> TransactionRequest:
    return TransactionRequest(customer_id="test-customer")


def _partial_request() -> TransactionRequest:
    return TransactionRequest(
        customer_id="test-customer",
        age=35,
        transaction_value=500.0,
    )


def test_all_fields_missing_does_not_crash(model):
    result = model.predict(_all_none_request())
    assert isinstance(result.fraud_probability, float)
    assert isinstance(result.fraud_flag, bool)


def test_partial_fields_missing_does_not_crash(model):
    result = model.predict(_partial_request())
    assert isinstance(result.fraud_probability, float)
    assert isinstance(result.fraud_flag, bool)


def test_fraud_probability_in_valid_range_when_missing(model):
    result = model.predict(_all_none_request())
    assert 0.0 <= result.fraud_probability <= 1.0
