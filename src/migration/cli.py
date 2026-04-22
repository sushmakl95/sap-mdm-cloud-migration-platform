"""Unified CLI: sap-migrate [extract|load|validate|run-full|generate-ddl]."""

from __future__ import annotations

import sys
import uuid

import click


@click.group()
@click.version_option(version="1.0.0")
def cli() -> None:
    """SAP MDM → AWS + Databricks migration CLI."""


@cli.command()
@click.option("--table", required=True, help="SAP table name (e.g., MARA)")
@click.option("--output", required=True, help="Output path (s3:// or local)")
@click.option("--mode", type=click.Choice(["full", "incremental"]), default="full")
@click.option("--batch-id", default=None)
def extract(table: str, output: str, mode: str, batch_id: str | None) -> None:
    """Extract one SAP table to S3 raw zone."""
    from migration.orchestration.glue_extract_job import main

    bid = batch_id or uuid.uuid4().hex[:12]
    args = {
        "JOB_NAME": "sap-extract-cli",
        "contract_fqn": table,
        "batch_id": bid,
        "mode": mode,
        "output_base": output,
    }
    sys.exit(main(args))


@cli.command()
@click.option("--table", required=True)
@click.option("--extract-path", required=True)
@click.option("--redshift-staging", required=True)
@click.option("--redshift-iam-role", required=True)
@click.option("--batch-id", default=None)
def load(
    table: str,
    extract_path: str,
    redshift_staging: str,
    redshift_iam_role: str,
    batch_id: str | None,
) -> None:
    """Load extracted data to Postgres + Redshift."""
    from migration.orchestration.glue_load_job import main

    bid = batch_id or uuid.uuid4().hex[:12]
    args = {
        "JOB_NAME": "sap-load-cli",
        "contract_fqn": table,
        "batch_id": bid,
        "extract_path": extract_path,
        "redshift_staging_path": redshift_staging,
        "redshift_iam_role": redshift_iam_role,
    }
    sys.exit(main(args))


@cli.command(name="generate-ddl")
@click.option("--output-dir", default="./sql/target", help="Where to write DDL files")
def generate_ddl(output_dir: str) -> None:
    """Generate Postgres + Redshift DDL from all contracts."""
    from pathlib import Path

    from migration.schemas import TABLE_CONTRACTS
    from migration.transformers import DdlGenerator

    gen = DdlGenerator()
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    for name, contract in TABLE_CONTRACTS.items():
        ddl = gen.generate(contract)
        pg_path = out / f"postgres_{name.lower()}.sql"
        rs_path = out / f"redshift_{name.lower()}.sql"

        pg_content = ddl.postgres_ddl + "\n-- Indexes\n" + "\n".join(ddl.postgres_indexes)
        pg_path.write_text(pg_content)
        rs_path.write_text(ddl.redshift_ddl)

        click.echo(f"Generated {pg_path} and {rs_path}")


@cli.command(name="emit-sfn")
@click.option("--output-dir", default="./stepfunctions")
def emit_state_machines(output_dir: str) -> None:
    """Emit all Step Functions state machine definitions."""
    from migration.orchestration.state_machines import emit_all_state_machines

    emit_all_state_machines(output_dir)
    click.echo(f"State machines emitted to {output_dir}/")


@cli.command(name="dms-config")
@click.option("--output", default="./config/dms_task_config.json")
def dms_config(output: str) -> None:
    """Generate DMS replication task configuration."""
    import json
    from pathlib import Path

    from migration.cdc import DmsConfigGenerator

    gen = DmsConfigGenerator()
    config = {
        "table_mappings": gen.generate_table_mappings(),
        "task_settings": gen.generate_task_settings(),
    }

    Path(output).parent.mkdir(parents=True, exist_ok=True)
    Path(output).write_text(json.dumps(config, indent=2))
    click.echo(f"DMS config written to {output}")


@cli.command(name="list-contracts")
def list_contracts() -> None:
    """Show all registered contracts."""
    from migration.schemas import TABLE_CONTRACTS

    click.echo(f"{'SAP Table':20s}  {'Postgres Target':30s}  {'Redshift Target':30s}  Strategy")
    click.echo("-" * 110)
    for name, c in TABLE_CONTRACTS.items():
        click.echo(
            f"{name:20s}  {c.target_fqn_postgres:30s}  "
            f"{c.target_fqn_redshift:30s}  {c.load_strategy}"
        )


if __name__ == "__main__":
    cli()
