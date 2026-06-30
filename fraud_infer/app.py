from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import Response

from fraud_infer.model import FraudModel
from fraud_infer.schemas import FraudResponse, TransactionRequest

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

_model: FraudModel | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _model
    model_path = os.getenv("MODEL_PATH", "fraud_model.pkl")
    model_version = os.getenv("MODEL_VERSION", "unknown")
    fraud_threshold = float(os.getenv("FRAUD_THRESHOLD", "0.5"))
    logger.info(
        "Loading model from %s (version=%s, threshold=%.2f)",
        model_path,
        model_version,
        fraud_threshold,
    )
    _model = FraudModel(model_path, model_version, fraud_threshold)
    logger.info("Model loaded successfully")
    yield
    _model = None


app = FastAPI(
    title="Fraud Inference Service",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "model_loaded": _model is not None}


# SageMaker custom container protocol
@app.get("/ping")
def ping():
    if _model is None:
        raise HTTPException(status_code=503, detail="Model not loaded")
    return Response(status_code=200)


@app.post("/invocations", response_model=FraudResponse)
def invocations(payload: TransactionRequest) -> FraudResponse:
    return predict(payload)


@app.post("/predict", response_model=FraudResponse)
def predict(payload: TransactionRequest) -> FraudResponse:
    if _model is None:
        raise HTTPException(status_code=503, detail="Model not loaded")
    result = _model.predict(payload)
    logger.info(
        "customer_id=%s fraud_probability=%.4f fraud_flag=%s",
        payload.customer_id,
        result.fraud_probability,
        result.fraud_flag,
    )
    return result
