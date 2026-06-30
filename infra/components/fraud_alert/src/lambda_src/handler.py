"""
Lambda: reads fraud-alert messages from SQS and posts Slack notifications.
Flink is responsible for threshold filtering — every message received here is posted.
Slack webhook URL is retrieved from Secrets Manager and cached with a TTL so that
secret rotation is picked up within _WEBHOOK_URL_TTL_S seconds without needing a cold start.
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.request

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

SLACK_SECRET_ARN = os.environ["SLACK_SECRET_ARN"]
ALERT_THRESHOLD = float(os.environ.get("ALERT_THRESHOLD", "0.7"))

_WEBHOOK_URL_TTL_S = 300  # re-fetch from Secrets Manager every 5 minutes

# Fields shown in the Slack alert, in order.
# (slack_title, payload_key, formatter)
_ALERT_FIELDS: list[tuple[str, str, object]] = [
    ("Fraud Probability", "fraud_probability", lambda v: f"{v:.1%}"),
    ("Transaction Value", "transaction_value", lambda v: f"${v:.2f}"),
    ("Customer ID", "customer_id", str),
    ("Age", "age", str),
    ("Gender", "gender", str),
    ("Location", "location", str),
    ("Channel", "channel_type", str),
    ("Subscription", "subscription_type", str),
    ("Income Bracket", "income_bracket", str),
    ("Model Version", "model_version", str),
]

_cached_webhook_url: str | None = None
_cached_at: float = 0.0


def _get_webhook_url() -> str:
    global _cached_webhook_url, _cached_at
    if _cached_webhook_url is None or time.monotonic() - _cached_at > _WEBHOOK_URL_TTL_S:
        client = boto3.client("secretsmanager")
        secret = client.get_secret_value(SecretId=SLACK_SECRET_ARN)
        _cached_webhook_url = json.loads(secret["SecretString"])["webhook_url"]
        _cached_at = time.monotonic()
    return _cached_webhook_url


def lambda_handler(event: dict, context) -> dict:
    batch_item_failures = []

    for record in event.get("Records", []):
        message_id = record.get("messageId", "unknown")
        try:
            body = json.loads(record["body"])
            if body.get("fraud_probability", 0.0) >= ALERT_THRESHOLD:
                _post_to_slack(_build_message(body))
        except Exception:
            logger.exception("Failed to post Slack alert for messageId=%s", message_id)
            batch_item_failures.append({"itemIdentifier": message_id})

    return {"batchItemFailures": batch_item_failures}


def _build_message(body: dict) -> dict:
    prob = body.get("fraud_probability", 0.0)
    fields = []
    for title, key, fmt in _ALERT_FIELDS:
        value = body.get(key)
        if value is not None:
            fields.append({"title": title, "value": fmt(value), "short": True})

    return {
        "text": f":rotating_light: *Fraud Alert* — {prob:.1%} probability",
        "attachments": [{"color": "danger", "fields": fields}],
    }


def _post_to_slack(message: dict) -> None:
    data = json.dumps(message).encode()
    req = urllib.request.Request(
        _get_webhook_url(),
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        if resp.status != 200:
            raise RuntimeError(f"Slack webhook returned HTTP {resp.status}")
