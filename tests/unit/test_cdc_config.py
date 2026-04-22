"""Tests for DMS config generator."""

from __future__ import annotations

import json

import pytest

from migration.cdc import DmsConfigGenerator

pytestmark = pytest.mark.unit


def test_table_mappings_has_selection_rules():
    gen = DmsConfigGenerator()
    config = gen.generate_table_mappings()

    selection_rules = [r for r in config["rules"] if r["rule-type"] == "selection"]
    # MARA, KNA1, LFA1 + any others that have enable_cdc=True
    assert len(selection_rules) >= 3


def test_table_mappings_renames_schema_and_table():
    gen = DmsConfigGenerator()
    config = gen.generate_table_mappings()

    rename_rules = [r for r in config["rules"] if r["rule-type"] == "transformation"]
    # Must include schema renames + table renames + column renames
    assert len(rename_rules) > 0
    assert any(r["rule-target"] == "schema" for r in rename_rules)
    assert any(r["rule-target"] == "table" for r in rename_rules)
    assert any(r["rule-target"] == "column" for r in rename_rules)


def test_task_settings_enables_validation():
    gen = DmsConfigGenerator()
    settings = gen.generate_task_settings()

    assert settings["ValidationSettings"]["EnableValidation"] is True
    assert settings["ValidationSettings"]["ValidationMode"] == "ROW_LEVEL"


def test_task_settings_enables_batch_apply():
    """BatchApply is critical for CDC throughput."""
    gen = DmsConfigGenerator()
    settings = gen.generate_task_settings()

    assert settings["TargetMetadata"]["BatchApplyEnabled"] is True


def test_build_config_produces_parseable_json():
    gen = DmsConfigGenerator()
    config = gen.build_config(
        task_identifier="test-task",
        source_endpoint_arn="arn:aws:dms:us-east-1:123:endpoint:source",
        target_endpoint_arn="arn:aws:dms:us-east-1:123:endpoint:target",
        replication_instance_arn="arn:aws:dms:us-east-1:123:rep:instance",
    )

    # Should be parseable JSON
    mappings = json.loads(config.table_mappings)
    settings = json.loads(config.replication_task_settings)
    assert "rules" in mappings
    assert "ValidationSettings" in settings
