"""Train a mock XGBoost fraud model and save it as fraud_model.pkl."""

from __future__ import annotations

import pickle
from pathlib import Path

import xgboost as xgb
from generate import FEATURE_ORDER, generate
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split

OUTPUT = Path(__file__).parent / "fraud_model_mock.pkl"


def main() -> None:
    df = generate(n=20_000, seed=42)

    X = df[FEATURE_ORDER]
    y = df["fraud_flag"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    model = xgb.XGBClassifier(
        enable_categorical=True,
        tree_method="hist",
        n_estimators=200,
        max_depth=4,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        scale_pos_weight=(y_train == 0).sum() / (y_train == 1).sum(),
        random_state=42,
        eval_metric="auc",
    )
    model.fit(
        X_train,
        y_train,
        eval_set=[(X_test, y_test)],
        verbose=False,
    )

    auc = roc_auc_score(y_test, model.predict_proba(X_test)[:, 1])
    print(f"Test AUC: {auc:.4f}")

    with open(OUTPUT, "wb") as fh:
        pickle.dump(model, fh)
    print(f"Saved → {OUTPUT}")


if __name__ == "__main__":
    main()
