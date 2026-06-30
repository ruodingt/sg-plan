FROM python:3.11-slim AS base

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY fraud_infer/ fraud_infer/

FROM base AS runtime

RUN useradd -m -u 1000 appuser && chown -R appuser /app
USER appuser

ENV MODEL_PATH=/opt/ml/model/fraud_model.pkl
ENV PORT=8080

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/ping')"

CMD ["uvicorn", "fraud_infer.app:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "2"]
