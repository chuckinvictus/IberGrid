# Architecture

## Runtime split

IberGrid is organized around four runtime responsibilities:

- `packages/ml`: ingestion, feature engineering, training, promotion, publication, and persistence logic
- `apps/api`: public FastAPI read layer for persisted forecasts, diagnostics, and lineage
- `apps/web`: Next.js read-only product surface
- `apps/worker`: scheduled execution layer for ingestion, retraining, and OMIE reconciliation

## Storage model

The platform uses two storage shapes on purpose:

- Parquet lakehouse for reproducible offline datasets
- PostgreSQL for application-serving metadata and published outputs

### Lakehouse layers

- `bronze`
  - `redata_market_prices`
  - `redata_demand`
  - `redata_generation_daily`
  - `open_meteo_weather`
- `silver`
  - `pvpc_hourly`
  - `spot_hourly`
  - `demand_actual`
  - `demand_forecast`
  - `generation_mix_daily`
  - `weather_hourly`
- `gold`
  - `training_dataset`
  - `serving_snapshot`
  - `published_forecast_snapshot`
  - `forecast_explanations`
  - `backtest_summary`
  - `source_health_snapshot`
  - `spot_reconciliation`

### Relational tables

- `ingestion_run`
- `training_run`
- `model_version`
- `forecast_run`
- `forecast_point`
- `forecast_explanation`
- `backtest_result`
- `source_health_snapshot`

## Serving contract

The API never recomputes forecasts on request. It only serves the latest persisted publication:

1. The worker ingests and refreshes source health.
2. The worker trains benchmarks and a TFT candidate.
3. Promotion rules decide whether the TFT becomes champion.
4. The worker publishes a persisted forecast run.
5. The API reads published rows and metadata from PostgreSQL.

## Model policy

Training includes:

- `D-1`
- `D-7`
- ridge regression
- LightGBM quantile
- TFT production candidate

Promotion is strict:

- `>= 8%` day-ahead MAE improvement versus both `D-1` and `D-7`
- `>= 5%` sMAPE improvement versus both `D-1` and `D-7`
- `P10-P90` coverage inside `0.75-0.85`

If the TFT does not satisfy the bar, the previously promoted model stays live. If there is no promoted model yet, the system publishes in explicit `heuristic-fallback` mode.

## Source governance

- REData failure blocks publication.
- Open-Meteo failure degrades confidence and freshness but does not block publication.
- OMIE failure degrades reconciliation only.
- Every run materializes source freshness, null-rate, and reconciliation evidence.

## Time handling

All timestamps are normalized to `Europe/Madrid`. The pipeline is designed to tolerate `23`, `24`, and `25` hour local days across DST transitions.
