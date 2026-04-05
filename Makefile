PYTHON ?= python3.12

.PHONY: api web worker test refresh backfill train publish reconcile status

api:
	$(PYTHON) -m uvicorn ibergrid_api.main:app --reload --app-dir apps/api/src

web:
	npm --workspace apps/web run dev

worker:
	$(PYTHON) -m ibergrid_worker.main serve

test:
	$(PYTHON) -m pytest

refresh:
	$(PYTHON) -m ibergrid_ml.cli refresh --days 120

backfill:
	$(PYTHON) -m ibergrid_ml.cli backfill --years 2

train:
	$(PYTHON) -m ibergrid_ml.cli train

publish:
	$(PYTHON) -m ibergrid_ml.cli publish

reconcile:
	$(PYTHON) -m ibergrid_worker.main reconciliation-job

status:
	$(PYTHON) -m ibergrid_ml.cli status
