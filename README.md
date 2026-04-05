# IberGrid PVPC Intelligence Platform

IberGrid is an English-only, production-shaped monorepo for forecasting PVPC electricity prices in peninsular Spain. It combines official REData market data, OMIE reconciliation files, and no-key Open-Meteo covariates to publish persisted forecasts, market context, model diagnostics, and source-lineage signals.

## What the platform does

- Publishes a persisted next-day hourly PVPC forecast with `P10 / P50 / P90`.
- Extends the same forecast into a weekly outlook with daily bands, cheapest windows, and peak-risk signals.
- Tracks source freshness, null rates, and OMIE-vs-REData spot reconciliation.
- Trains benchmark challengers and a TFT production candidate under explicit promotion rules.
- Serves only persisted forecast runs from the database instead of recomputing inside API requests.

## Monorepo layout

```text
apps/
  api/      FastAPI public API
  web/      Next.js analyst dashboard
  worker/   Scheduled worker and cron-friendly jobs
packages/
  ml/       Data ingestion, feature engineering, training, publication
docs/
  architecture.md
  deployment.md
```

## Data sources

- REData: primary source for PVPC, spot, demand, and generation structure.
- OMIE `marginalpdbc`: reference source for spot reconciliation.
- Open-Meteo: no-key archive and forecast weather covariates.

## Runtime architecture

- `apps/api`: public read-only API serving persisted outputs from PostgreSQL.
- `apps/web`: read-only dashboard for forecast, utility, market context, and lineage.
- `apps/worker`: scheduled jobs for daily ingestion/publication, weekly retraining, and OMIE reconciliation.
- `mlflow`: internal experiment tracking and model lineage service.

## Local setup

### Python

The project targets Python `3.12`.

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -U pip
python -m pip install -e '.[dev]'
python -m pip install --index-url https://pypi.org/simple --extra-index-url https://download.pytorch.org/whl/cpu \
  torch==2.7.1+cpu lightning==2.5.6 pytorch-forecasting==1.5.0
```

### Web

```bash
npm install
npm --workspace apps/web run dev
```

### Full stack with Docker Compose

```bash
docker compose up --build
```

This starts:

- `postgres` on `5432`
- `mlflow` on `5000`
- `api` on `8000`
- `web` on `3000`
- `worker` as the scheduled runtime

## Core commands

```bash
make refresh     # ingest recent market and weather data
make backfill    # ingest a wider historical window
make train       # run benchmarks, TFT candidate training, and promotion logic
make publish     # publish the next persisted forecast run
make reconcile   # refresh OMIE spot reconciliation
make api         # run FastAPI locally
make web         # run Next.js locally
make worker      # run the local scheduled worker
make test        # run Python tests
```

Or via CLI:

```bash
python -m ibergrid_ml.cli refresh --days 120
python -m ibergrid_ml.cli train
python -m ibergrid_ml.cli publish
python -m ibergrid_ml.cli status
python -m ibergrid_worker.main daily-job
python -m ibergrid_worker.main weekly-job
python -m ibergrid_worker.main reconciliation-job
```

## API surface

- `GET /health`
- `GET /ready`
- `GET /api/v1/forecast/day-ahead?date=YYYY-MM-DD`
- `GET /api/v1/forecast/week-ahead?from=YYYY-MM-DD`
- `GET /api/v1/context/market?from=...&to=...`
- `GET /api/v1/model/performance/latest`
- `GET /api/v1/status/latest`

## Model policy

IberGrid keeps four challenger families in every training cycle:

- `D-1`
- `D-7`
- ridge regression
- LightGBM quantile

The production candidate is a TFT. It is promoted only when it:

- beats both `D-1` and `D-7` by at least `8%` on day-ahead MAE
- beats both `D-1` and `D-7` by at least `5%` on sMAPE
- keeps empirical `P10-P90` coverage between `75%` and `85%`

If no promoted TFT exists, publication stays explicit about `heuristic-fallback` mode.

## Data layers

- `bronze`: raw REData, OMIE, and Open-Meteo inputs
- `silver`: canonical hourly and daily market tables
- `gold`: training dataset, serving snapshot, published forecast snapshot, forecast explanations, source health, and backtest summary

## Deployment

The deployment blueprint is documented in [deployment.md](/home/alonso/Documents/proyectos/energy-prediction/docs/deployment.md). The intended public topology is:

- Vercel for the Next.js frontend
- Railway for API, worker cron jobs, Postgres, and MLflow

## Scope

- Peninsular Spain only
- Read-only public product
- No user accounts
- No external alert channels in this release
