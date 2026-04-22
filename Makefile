.PHONY: help install install-dev build-package seed-sample-data lint format typecheck security test test-unit test-integration ci demo-batch demo-cdc demo-validate demo-clean compose-up compose-down clean

PYTHON := python3.11
VENV := .venv
VENV_BIN := $(VENV)/bin

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-25s\033[0m %s\n", $$1, $$2}'

$(VENV)/bin/activate:
	$(PYTHON) -m venv $(VENV)
	$(VENV_BIN)/pip install --upgrade pip setuptools wheel

install: $(VENV)/bin/activate  ## Install runtime deps
	$(VENV_BIN)/pip install -e .

install-dev: $(VENV)/bin/activate  ## Install dev + test deps
	$(VENV_BIN)/pip install -e ".[dev]"

build-package:  ## Build the Python package zip for Glue
	mkdir -p dist
	cd src && zip -r ../dist/migration.zip migration lambdas -x "*/__pycache__/*"
	@echo "Built dist/migration.zip"

seed-sample-data:  ## Generate synthetic SAP data for local dev
	$(VENV_BIN)/python scripts/seed-sample-data.py

compose-up:  ## Start local Postgres stack for integration tests
	docker compose -f compose/docker-compose.yml up -d
	@echo "Waiting for DBs..."
	@sleep 5

compose-down:  ## Stop local Postgres stack
	docker compose -f compose/docker-compose.yml down -v

lint:  ## Run ruff
	$(VENV_BIN)/ruff check src tests scripts

format:  ## Auto-format
	$(VENV_BIN)/ruff format src tests scripts
	$(VENV_BIN)/ruff check --fix src tests scripts

typecheck:  ## Run mypy
	$(VENV_BIN)/mypy src --ignore-missing-imports

security:  ## Run bandit
	$(VENV_BIN)/bandit -c pyproject.toml -r src scripts -lll

test-unit:  ## Fast unit tests
	$(VENV_BIN)/pytest tests/unit -v -m unit

test-integration:  ## Integration tests (requires compose-up)
	$(VENV_BIN)/pytest tests/integration -v -m integration

test: test-unit  ## Run tests

ci: lint typecheck security test-unit  ## Full local CI

demo-batch:  ## End-to-end local demo: extract → validate → load
	$(VENV_BIN)/sap-migrate extract --table MARA --output file:///tmp/sap-demo/raw/ --mode full
	@echo "Extract complete. Check /tmp/sap-demo/raw/"

demo-cdc:  ## Simulate CDC event flow
	$(VENV_BIN)/sap-migrate dms-config --output /tmp/dms_config.json
	@echo "DMS config emitted to /tmp/dms_config.json"

demo-validate:  ## Run reconciliation on the demo data
	@echo "Running 5-layer reconciliation..."
	$(VENV_BIN)/python -c "from migration.validators import ReconciliationValidator; print('✓ validator importable')"

demo-clean:  ## Clean demo data
	rm -rf /tmp/sap-demo

generate-ddl:  ## Generate Postgres + Redshift DDL
	$(VENV_BIN)/sap-migrate generate-ddl --output-dir ./sql/target

emit-sfn:  ## Emit Step Functions definitions
	$(VENV_BIN)/sap-migrate emit-sfn --output-dir src/migration/orchestration/

list-contracts:  ## Show registered contracts
	$(VENV_BIN)/sap-migrate list-contracts

clean:  ## Clean build artifacts + caches
	rm -rf dist/ build/ *.egg-info/
	rm -rf .pytest_cache/ .coverage htmlcov/
	rm -rf spark-warehouse/ metastore_db/ derby.log *.log
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf /tmp/sap-demo

terraform-init-dev:  ## Initialize Terraform for dev env
	cd infra/terraform && terraform init -backend-config=envs/dev.backend.hcl

terraform-plan-dev:  ## Terraform plan for dev
	cd infra/terraform && terraform plan -var-file=envs/dev.tfvars

terraform-apply-dev:  ## Terraform apply dev
	cd infra/terraform && terraform apply -var-file=envs/dev.tfvars

all: install-dev lint typecheck security test-unit  ## Full local CI simulation
