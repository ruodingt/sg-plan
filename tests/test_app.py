from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from fraud_infer.app import app
from fraud_infer.schemas import FraudResponse

VALID_PAYLOAD = {
    "customer_id": "550e8400-e29b-41d4-a716-446655440000",
    "age": 35,
    "gender": "male",
    "location": "Melbourne",
    "subscription_type": "STANDARD",
    "tenure_months": 24,
    "income_bracket": "MED",
    "event_created_at_ts": 1_720_000_000.0,
    "transaction_value": 250.0,
    "channel_type": "ONLINE",
}


@pytest.fixture
def client():
    with patch("fraud_infer.app.FraudModel") as MockModel:
        instance = MockModel.return_value
        instance.predict.return_value = FraudResponse(
            fraud_flag=False,
            fraud_probability=0.12,
        )
        with TestClient(app) as c:
            yield c, instance


def test_health_ok(client):
    c, _ = client
    resp = c.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"
    assert resp.json()["model_loaded"] is True


def test_predict_valid_payload(client):
    c, mock_instance = client
    resp = c.post("/predict", json=VALID_PAYLOAD)
    assert resp.status_code == 200
    data = resp.json()
    assert "fraud_flag" in data
    assert "fraud_probability" in data
    assert 0.0 <= data["fraud_probability"] <= 1.0
    mock_instance.predict.assert_called_once()


def test_predict_invalid_subscription_type(client):
    c, _ = client
    payload = {**VALID_PAYLOAD, "subscription_type": "GOLD"}
    resp = c.post("/predict", json=payload)
    assert resp.status_code == 422


def test_predict_invalid_channel_type(client):
    c, _ = client
    payload = {**VALID_PAYLOAD, "channel_type": "DRIVE-THRU"}
    resp = c.post("/predict", json=payload)
    assert resp.status_code == 422


def test_predict_negative_transaction_value(client):
    c, _ = client
    payload = {**VALID_PAYLOAD, "transaction_value": -10.0}
    resp = c.post("/predict", json=payload)
    assert resp.status_code == 422


def test_predict_missing_required_field(client):
    c, _ = client
    payload = {k: v for k, v in VALID_PAYLOAD.items() if k != "age"}
    resp = c.post("/predict", json=payload)
    assert resp.status_code == 422


def test_predict_high_fraud_score(client):
    c, mock_instance = client
    mock_instance.predict.return_value = FraudResponse(
        fraud_flag=True,
        fraud_probability=0.95,
    )
    resp = c.post("/predict", json=VALID_PAYLOAD)
    assert resp.status_code == 200
    data = resp.json()
    assert data["fraud_flag"] is True
    assert data["fraud_probability"] == pytest.approx(0.95)
