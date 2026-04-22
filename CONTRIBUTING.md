# Contributing

## Adding a new SAP table

This is the most common contribution. Walkthrough:

### 1. Define the contract

Edit `src/migration/schemas/mdm_contracts.py`:

```python
BKPF_CONTRACT = TableContract(
    sap_system="HANA",
    sap_schema="SAPSR3",
    sap_table="BKPF",
    target_postgres_schema="fi",
    target_postgres_table="accounting_document_header",
    target_redshift_schema="fi",
    target_redshift_table="accounting_document_header",
    partition_column="BELNR",
    partition_num_chunks=16,
    primary_keys=["document_number", "company_code", "fiscal_year"],
    load_strategy="incremental_merge",
    incremental_column="CPUDT",
    enable_cdc=True,
    columns=[
        ColumnMapping(
            source_name="BELNR", target_name="document_number",
            sap_type="CHAR", sap_length=10,
            postgres_type="VARCHAR(10)", redshift_type="VARCHAR(10)",
            nullable=False, is_pk=True,
        ),
        # ... remaining columns
    ],
)

TABLE_CONTRACTS["BKPF"] = BKPF_CONTRACT
```

### 2. Add a unit test

In `tests/unit/test_contracts.py`:

```python
def test_bkpf_has_expected_columns():
    assert any(m.target_name == "document_number" for m in BKPF_CONTRACT.columns)
```

### 3. Regenerate DDL

```bash
sap-migrate generate-ddl --output-dir sql/target
```

Review the generated SQL — does the DISTKEY/SORTKEY make sense for how this table will be queried? If not, extend `_choose_distkey()` / `_choose_sortkeys()` in `DdlGenerator`.

### 4. Regenerate DMS config

```bash
sap-migrate dms-config --output config/dms_task_config.json
```

### 5. Test locally

```bash
make test-unit
make demo-batch  # end-to-end against sample data
```

### 6. Submit PR

Required CI checks:
- ruff lint
- mypy typecheck (continues on error)
- bandit security
- terraform validate

## Coding style

- Use `ruff format` to auto-format
- Full type hints on public functions
- Docstrings on every class + public method
- Prefer named imports over `import *`
- No `print()` in library code — use structlog

## Working on Terraform

Module naming: `modules/<resource_type>/` — each module is self-contained with `main.tf`, `variables.tf`, `outputs.tf`.

Resource naming: `${local.name_prefix}-<resource-specific-name>`.

Test before submitting:
```bash
cd infra/terraform
terraform init -backend=false
terraform validate
terraform fmt -check -recursive
```

## Working on Databricks notebooks

Notebooks live in `notebooks/` as `.py` files with the `# Databricks notebook source` header. This format:

- Renders as notebooks when imported into a Databricks workspace
- Is git-diffable (unlike `.ipynb`)
- Can be linted by ruff as regular Python

To test a notebook locally, you can't — Databricks-specific imports (`dbutils`, `spark` as built-in) don't exist locally. Test the underlying library code instead (`src/migration/`), which has full test coverage.

## Pull request checklist

- [ ] Tests added for new functionality
- [ ] `make ci` green locally
- [ ] No secrets, API keys, or real customer data in diff
- [ ] CHANGELOG entry (if notable change)
- [ ] Documentation updated (if API change)
- [ ] Terraform `terraform plan` reviewed if infra changed

## Questions?

Open a [discussion](https://github.com/sushmakl95/sap-mdm-cloud-migration-platform/discussions) or ping me on [LinkedIn](https://www.linkedin.com/in/sushmakl1995/).
