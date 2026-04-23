from __future__ import annotations

import os
from dataclasses import dataclass

import pandas as pd

from app.models.energy_point import EnergyPoint


@dataclass(frozen=True)
class PredictionResult:
    timestamp_referencia_utc: str
    pred_flag_defice_t_plus_1: int
    prob_defice_t_plus_1: float
    model_uri: str


class DeficePredictionService:
    """Serviço de inferência para previsão de défice em t+1."""

    def __init__(self):
        self._model = None

    @staticmethod
    def _model_uri() -> str:
        return os.getenv(
            "MLFLOW_MODEL_URI",
            "models:/producao_consumo_defice_classifier/latest",
        )

    @staticmethod
    def _tracking_uri() -> str:
        return os.getenv("MLFLOW_TRACKING_URI", "http://localhost:15000")

    def _load_model(self):
        if self._model is not None:
            return self._model

        import mlflow
        import mlflow.sklearn

        mlflow.set_tracking_uri(self._tracking_uri())
        self._model = mlflow.sklearn.load_model(self._model_uri())
        return self._model

    @staticmethod
    def _to_features(hourly_points: list[EnergyPoint]) -> tuple[pd.DataFrame, str]:
        if len(hourly_points) < 25:
            raise ValueError("São necessárias pelo menos 25 horas para calcular lags até 24.")

        rows = [
            {
                "timestamp_utc": point.timestamp,
                "consumo_total_kwh": point.consumo_total_kwh,
                "producao_total_kwh": point.producao_total_kwh,
                "saldo_kwh": point.saldo_kwh,
                "ratio_producao_consumo": point.ratio_producao_consumo,
            }
            for point in hourly_points
        ]

        data = pd.DataFrame(rows).sort_values("timestamp_utc").reset_index(drop=True)
        data["timestamp_utc"] = pd.to_datetime(data["timestamp_utc"], utc=True)

        for col in ["consumo_total_kwh", "producao_total_kwh", "saldo_kwh", "ratio_producao_consumo"]:
            data[col] = pd.to_numeric(data[col], errors="coerce")

        data["hour"] = data["timestamp_utc"].dt.hour
        data["day_of_week"] = data["timestamp_utc"].dt.dayofweek
        data["month"] = data["timestamp_utc"].dt.month

        feature_cols = ["hour", "day_of_week", "month"]
        for base_col in [
            "consumo_total_kwh",
            "producao_total_kwh",
            "saldo_kwh",
            "ratio_producao_consumo",
        ]:
            for lag in (1, 2, 3, 6, 12, 24):
                lag_col = f"{base_col}_lag_{lag}"
                data[lag_col] = data[base_col].shift(lag)
                feature_cols.append(lag_col)

        latest = data.tail(1).copy()
        if latest[feature_cols].isna().any(axis=1).iloc[0]:
            raise ValueError(
                "Última linha sem dados suficientes para inferência. Verifique missing values nas últimas 24h."
            )

        reference_ts = latest["timestamp_utc"].iloc[0].isoformat()
        return latest[feature_cols], reference_ts

    def predict_next_hour(self, hourly_points: list[EnergyPoint]) -> PredictionResult:
        model = self._load_model()
        features, reference_ts = self._to_features(hourly_points)

        prediction = int(model.predict(features)[0])
        probability = float(model.predict_proba(features)[0][1])

        return PredictionResult(
            timestamp_referencia_utc=reference_ts,
            pred_flag_defice_t_plus_1=prediction,
            prob_defice_t_plus_1=probability,
            model_uri=self._model_uri(),
        )
