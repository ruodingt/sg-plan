"""Integration tests — run against the containerised service via docker compose.

task integration-test
"""

import httpx

BASE_URL = "http://localhost:8080"

VALID_PAYLOAD = {
    "customer_id": "cust-001",
    "age": 35,
    "gender": "male",
    "location": "42",
    "subscription_type": "PREMIUM",
    "tenure_months": 24,
    "income_bracket": "HIGH",
    "event_created_at_ts": 1_700_000_000.0,
    "transaction_value": 250.0,
    "channel_type": "ONLINE",
}


def test_ping():
    r = httpx.get(f"{BASE_URL}/ping")
    assert r.status_code == 200


def test_invocations_shape():
    r = httpx.post(f"{BASE_URL}/invocations", json=VALID_PAYLOAD)
    assert r.status_code == 200
    body = r.json()
    assert "fraud_flag" in body
    assert "fraud_probability" in body
    assert 0.0 <= body["fraud_probability"] <= 1.0
    assert isinstance(body["fraud_flag"], bool)


def test_invalid_subscription_type_returns_422():
    r = httpx.post(f"{BASE_URL}/invocations", json={**VALID_PAYLOAD, "subscription_type": "GOLD"})
    assert r.status_code == 422


def test_negative_transaction_value_returns_422():
    r = httpx.post(f"{BASE_URL}/invocations", json={**VALID_PAYLOAD, "transaction_value": -1.0})
    assert r.status_code == 422


def test_in_store_channel():
    r = httpx.post(f"{BASE_URL}/invocations", json={**VALID_PAYLOAD, "channel_type": "IN-STORE"})
    assert r.status_code == 200
