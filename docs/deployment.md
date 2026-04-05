# Deployment

## Recommended path

The easiest production deployment for this repository is a single Linux server using:

- `docker-compose.prod.yml`
- `Dockerfile.api`
- `Dockerfile.worker`
- `Dockerfile.web`
- `deploy/Caddyfile`

This path is the simplest because the application still depends on shared filesystem state for:

- parquet lakehouse data under `data/`
- trained artifacts under `artifacts/`
- MLflow files under `mlruns/`

Keeping `api`, `worker`, and `mlflow` on the same Docker host avoids cross-service volume problems.

## What gets deployed

- `caddy`: HTTPS termination and reverse proxy
- `web`: Next.js production server
- `api`: FastAPI read API
- `worker`: in-process scheduler for ingestion, training, publication, and reconciliation
- `postgres`: application database
- `mlflow`: experiment tracking UI and metadata store

## Required files

- Copy `.env.prod.example` to `.env.prod`
- Set at least:
  - `DOMAIN`
  - `POSTGRES_PASSWORD`

You can keep `NEXT_PUBLIC_API_BASE_URL=` empty to use same-origin `/api/*` behind Caddy.

## Public routes

With the default Caddy config:

- `https://<your-domain>/` -> web app
- `https://<your-domain>/api/v1/...` -> FastAPI
- `https://<your-domain>/health` -> API health
- `https://<your-domain>/ready` -> API readiness

## Release flow

1. Provision one Ubuntu server.
2. Point your domain DNS to the server IP.
3. Copy the repo to the server.
4. Create `.env.prod`.
5. Start the stack with:
   - `docker compose --env-file .env.prod -f docker-compose.prod.yml up -d --build`
6. Run the initial bootstrap:
   - `docker compose --env-file .env.prod -f docker-compose.prod.yml exec worker python -m ibergrid_ml.cli backfill --years 2`
   - `docker compose --env-file .env.prod -f docker-compose.prod.yml exec worker python -m ibergrid_ml.cli train`
   - `docker compose --env-file .env.prod -f docker-compose.prod.yml exec worker python -m ibergrid_ml.cli publish`
7. Verify:
   - `https://<your-domain>/health`
   - `https://<your-domain>/ready`
   - `https://<your-domain>/api/v1/status/latest`
   - homepage loads live data

## Important environment names

The Python services read:

- `IBERGRID_DATABASE_URL`
- `IBERGRID_DATA_ROOT`
- `IBERGRID_ARTIFACTS_ROOT`
- `IBERGRID_MLFLOW_TRACKING_URI`

Do not use bare `DATABASE_URL` unless you also map it yourself.

## Alternative topology

`Vercel + Railway` is possible, but it is not the easiest path for this repository as currently written because `api` and `worker` both rely on shared on-disk datasets and artifacts. If you want that topology later, first move the remaining API reads off local parquet storage.
