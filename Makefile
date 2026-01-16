# ==============================================================================
# Olist Data Engineering Pipeline - Makefile
# ==============================================================================
# Usage:
#   make setup    - Create folders and install dependencies
#   make spark    - Run PySpark transformation (CSV -> Parquet)
#   make upload   - Upload silver data to GCS
#   make ingest   - Load data from GCS to BigQuery
#   make dbt-run  - Run dbt models (Gold Layer)
#   make dbt-test - Run dbt tests
#   make all      - Run full pipeline
#   make clean    - Remove intermediate files
# ==============================================================================

.PHONY: setup spark upload ingest dbt-run dbt-test dbt-docs dbt-all all clean help test

# Default Python command (adjust for your environment)
PYTHON := python
DBT_DIR := dbt_olist

# ==============================================================================
# SETUP
# ==============================================================================

## Create project folders and install dependencies
setup:
	@echo "=================================================="
	@echo "Setting up Olist Data Engineering Pipeline..."
	@echo "=================================================="
	$(PYTHON) -m pip install -r requirements.txt
	@if not exist "data\raw" mkdir "data\raw"
	@if not exist "data\silver" mkdir "data\silver"
	@if not exist "logs" mkdir "logs"
	@if not exist ".google" mkdir ".google"
	@if not exist "dbt_olist" mkdir "dbt_olist"
	@echo ""
	@echo "[OK] Setup complete!"
	@echo "[!] Remember to add your Service Account key to .google/"
	@echo ""

# ==============================================================================
# SILVER LAYER PIPELINE
# ==============================================================================

## Run PySpark transformation (CSV -> Parquet)
spark:
	@echo "=================================================="
	@echo "Running Spark transformation..."
	@echo "=================================================="
	$(PYTHON) spark/build_silver.py

## Upload silver layer to GCS
upload:
	@echo "=================================================="
	@echo "Uploading silver data to GCS..."
	@echo "=================================================="
	$(PYTHON) -c "from utils.gcp_utils import GCPUtils; gcp = GCPUtils(); gcp.upload_folder('data/silver', 'silver')"

## Load data from GCS to BigQuery
ingest:
	@echo "=================================================="
	@echo "Loading data to BigQuery..."
	@echo "=================================================="
	$(PYTHON) ingest/bq_loader.py

# ==============================================================================
# GOLD LAYER (dbt)
# ==============================================================================

## Install dbt packages
dbt-deps:
	@echo "=================================================="
	@echo "Installing dbt packages..."
	@echo "=================================================="
	cd $(DBT_DIR) && dbt deps

## Run dbt models (Gold Layer)
dbt-run:
	@echo "=================================================="
	@echo "Running dbt models..."
	@echo "=================================================="
	cd $(DBT_DIR) && dbt run

## Run dbt tests
dbt-test:
	@echo "=================================================="
	@echo "Running dbt tests..."
	@echo "=================================================="
	cd $(DBT_DIR) && dbt test

## Generate dbt documentation
dbt-docs:
	@echo "=================================================="
	@echo "Generating dbt documentation..."
	@echo "=================================================="
	cd $(DBT_DIR) && dbt docs generate

## Serve dbt documentation
dbt-serve:
	cd $(DBT_DIR) && dbt docs serve

## Run full dbt pipeline (deps -> run -> test)
dbt-all: dbt-deps dbt-run dbt-test
	@echo "=================================================="
	@echo "dbt pipeline completed!"
	@echo "=================================================="

# ==============================================================================
# FULL PIPELINE
# ==============================================================================

## Run silver pipeline: spark -> upload -> ingest
silver: spark upload ingest
	@echo "=================================================="
	@echo "Silver layer pipeline completed!"
	@echo "=================================================="

## Run full pipeline: silver + gold
all: setup silver dbt-all
	@echo "=================================================="
	@echo "Full pipeline completed successfully!"
	@echo "=================================================="

# ==============================================================================
# UTILITIES
# ==============================================================================

## Clean intermediate files
clean:
	@echo "Cleaning intermediate files..."
	@if exist "data\silver" rmdir /s /q "data\silver"
	@if not exist "data\silver" mkdir "data\silver"
	@if exist "logs\*.log" del /q "logs\*.log"
	@if exist "$(DBT_DIR)\target" rmdir /s /q "$(DBT_DIR)\target"
	@echo "[OK] Cleaned!"

## Run tests
test:
	@echo "Running tests..."
	$(PYTHON) -m pytest tests/ -v

## Show help
help:
	@echo ""
	@echo "Olist Data Engineering Pipeline"
	@echo "================================"
	@echo ""
	@echo "Silver Layer (Data Processing):"
	@echo "  setup     Create folders and install dependencies"
	@echo "  spark     Run PySpark transformation (CSV -> Parquet)"
	@echo "  upload    Upload silver data to GCS"
	@echo "  ingest    Load data from GCS to BigQuery"
	@echo "  silver    Run silver pipeline (spark -> upload -> ingest)"
	@echo ""
	@echo "Gold Layer (dbt Modeling):"
	@echo "  dbt-deps  Install dbt packages"
	@echo "  dbt-run   Run dbt models"
	@echo "  dbt-test  Run dbt tests"
	@echo "  dbt-docs  Generate documentation"
	@echo "  dbt-serve Serve documentation locally"
	@echo "  dbt-all   Run full dbt pipeline"
	@echo ""
	@echo "Full Pipeline:"
	@echo "  all       Run complete pipeline (silver + gold)"
	@echo "  clean     Remove intermediate files"
	@echo "  help      Show this help message"
	@echo ""
	@echo "Example: make all"
	@echo ""

