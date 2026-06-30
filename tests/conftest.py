import pytest

from fraud_infer.schemas import TransactionRequest


@pytest.fixture
def sample_transaction() -> TransactionRequest:
    return TransactionRequest(
        customer_id="550e8400-e29b-41d4-a716-446655440000",
        age=35,
        gender="male",
        location="Melbourne",
        subscription_type="STANDARD",
        tenure_months=24,
        income_bracket="MED",
        event_created_at_ts=1_720_000_000.0,
        transaction_value=250.0,
        channel_type="ONLINE",
    )
