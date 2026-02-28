# Makefile for Pack Data Challenge
# macOS: `make` is available by default.
# Windows: requires Git Bash + `choco install make`, or use scripts/dbt.ps1 instead.

DBT_DIR    := dbt
TARGET     ?= prod
PYTHON     := python

.PHONY: help install test pipeline pipeline-ingest pipeline-transform pipeline-analysis \
        dbt-deps dbt-snapshot dbt-run dbt-test dbt-build dbt-docs clean

# ── Help ──────────────────────────────────────────────────────────────────────
help:
	@echo ""
	@echo "  Pack Data Challenge — available targets"
	@echo ""
	@echo "  Setup"
	@echo "    make install            Install all dependencies"
	@echo ""
	@echo "  Pipeline"
	@echo "    make pipeline           Full pipeline (ingest + transform + analysis)"
	@echo "    make pipeline-ingest    Ingest only"
	@echo "    make pipeline-transform Transform only (dbt + tests)"
	@echo "    make pipeline-analysis  Analysis only (CEO query on existing warehouse)"
	@echo ""
	@echo "  Tests"
	@echo "    make test               Run all 42 pytest tests"
	@echo ""
	@echo "  dbt (manual)"
	@echo "    make dbt-deps           Install dbt packages"
	@echo "    make dbt-snapshot       Run SCD2 snapshot"
	@echo "    make dbt-run            Build staging + mart models"
	@echo "    make dbt-test           Run schema + relationship tests"
	@echo "    make dbt-build          snapshot + run + test"
	@echo "    make dbt-docs           Generate + serve lineage docs"
	@echo ""
	@echo "  Utilities"
	@echo "    make clean              Clear the warehouse (calls src/utils.py)"
	@echo ""

# ── Setup ─────────────────────────────────────────────────────────────────────
install:
	$(PYTHON) -m pip install -r requirements.txt
	$(PYTHON) -m pip install -r requirements-dev.txt

# ── Tests ─────────────────────────────────────────────────────────────────────
test:
	$(PYTHON) -m pytest tests/ -v --tb=short

# ── Pipeline ──────────────────────────────────────────────────────────────────
pipeline:
	$(PYTHON) run_pipeline.py

pipeline-ingest:
	$(PYTHON) run_pipeline.py --mode ingest-only

pipeline-transform:
	$(PYTHON) run_pipeline.py --mode transform-only

pipeline-analysis:
	$(PYTHON) run_pipeline.py --mode analysis-only

# ── dbt (manual invocation) ───────────────────────────────────────────────────
dbt-deps:
	cd $(DBT_DIR) && dbt deps --profiles-dir .

dbt-snapshot:
	cd $(DBT_DIR) && dbt snapshot --target $(TARGET) --profiles-dir .

dbt-run:
	cd $(DBT_DIR) && dbt run --target $(TARGET) --profiles-dir .

dbt-test:
	cd $(DBT_DIR) && dbt test --target $(TARGET) --profiles-dir .

dbt-build: dbt-snapshot dbt-run dbt-test

dbt-docs:
	cd $(DBT_DIR) && dbt docs generate --target $(TARGET) --profiles-dir . \
	  && dbt docs serve --profiles-dir .

# ── Utilities ─────────────────────────────────────────────────────────────────
clean:
	$(PYTHON) src/utils.py
