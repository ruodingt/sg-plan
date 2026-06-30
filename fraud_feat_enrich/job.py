# ruff: noqa
# TODO: This is a design-level pseudocode sketch of the PyFlink job.
# Real implementation requires:
#   - pyflink dependency setup (pyflink / apache-flink package)
#   - Kinesis connector JAR on the classpath
#   - ActivityAggregates dataclass with add() / snapshot() logic
#   - SQSSink implementation
#   - Config wired from env vars / application properties


import json

import boto3

AWS_REGION = "ap-southeast-2"
SAGEMAKER_ENDPOINT = "eg-fraud-endpoint"
FRAUD_ALERTS_QUEUE = "https://sqs.ap-southeast-2.amazonaws.com/.../fraud-alerts"

# ── Environment ───────────────────────────────────────────────────────────────
env = StreamExecutionEnvironment.get_execution_environment()
env.enable_checkpointing(60_000)  # checkpoint every 60 s → S3

# ── Sources ───────────────────────────────────────────────────────────────────
transaction_stream = (
    env.add_source(FlinkKinesisConsumer("transactions-stream", kinesis_props)).key_by(
        lambda e: e["customer_id"]
    )  # all events for same user → same task
)

demographic_stream = env.add_source(
    FlinkKinesisConsumer("demographics-stream", kinesis_props)  # DMS CDC
)

# ── Broadcast state descriptor (demographics snapshot) ────────────────────────
demographic_desc = MapStateDescriptor(
    "demographic_state",
    key_type=Types.STRING(),  # customer_id
    value_type=Types.MAP(Types.STRING(), Types.STRING()),
)

broadcast_demographics = demographic_stream.broadcast(demographic_desc)

# ── Step 1: Rolling activity aggregates (keyed state, RocksDB) ────────────────
# Each transaction updates the per-customer 24h/7d/30d buckets and emits
# the transaction + current aggregate snapshot downstream.
aggregated_stream = transaction_stream.process(ActivityAggregator())  # already keyed by customer_id

# ── Step 2: Broadcast join + SageMaker scoring ────────────────────────────────
scored_stream = aggregated_stream.connect(broadcast_demographics).process(
    EnrichAndScoreFunction(demographic_desc)
)

# ── Sink: flagged fraud → SQS (Lambda applies the Slack alert threshold) ─────
(scored_stream.filter(lambda r: r["fraud_flag"]).add_sink(SQSSink(FRAUD_ALERTS_QUEUE)))


# ── ActivityAggregator ────────────────────────────────────────────────────────
class ActivityAggregator(KeyedProcessFunction):
    def open(self, ctx):
        self.activity = ctx.get_keyed_state(ValueStateDescriptor("activity", ActivityAggregates))

    def process_element(self, tx, ctx, out):
        agg = self.activity.value() or ActivityAggregates()
        agg.add(tx)  # updates 24h / 7d / 30d buckets, evicts stale events
        self.activity.update(agg)

        out.collect({**tx, "activity": agg.snapshot()})
        # snapshot() → {tx_count_24h, tx_sum_24h, tx_count_7d, tx_sum_7d, ...}


# ── EnrichAndScoreFunction ────────────────────────────────────────────────────
class EnrichAndScoreFunction(KeyedBroadcastProcessFunction):
    def open(self, ctx):
        # boto3 client created once per task; TCP connection pool reused
        self.sm = boto3.client("sagemaker-runtime", region_name=AWS_REGION)

    def process_element(self, event, ctx, out):
        tx, agg = event, event["activity"]
        demo = ctx.get_broadcast_state(demographic_desc).get(tx["customer_id"]) or {}

        # Build raw payload — encoding is FastAPI's responsibility
        payload = {
            "customer_id": tx["customer_id"],
            "age": demo.get("age"),
            "gender": demo.get("gender"),
            "location": demo.get("location"),
            "subscription_type": demo.get("subscription_type"),
            "tenure_months": demo.get("tenure_months"),
            "income_bracket": demo.get("income_bracket"),
            "event_created_at_ts": tx["event_created_at_ts"],
            "transaction_value": tx["transaction_value"],
            "channel_type": tx["channel_type"],
        }

        resp = self.sm.invoke_endpoint(
            EndpointName=SAGEMAKER_ENDPOINT,
            ContentType="application/json",
            Body=json.dumps(payload),
        )
        result = json.loads(resp["Body"].read())  # {fraud_flag, fraud_probability}
        out.collect({**payload, **result, **agg})

    def process_broadcast_element(self, demo_update, ctx, out):
        ctx.get_broadcast_state(demographic_desc).put(demo_update["customer_id"], demo_update)
