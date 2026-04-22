"""Pipeline orchestration — state machines + Glue job entrypoints.

NOTE: Glue job modules run in the AWS Glue runtime which injects SparkContext,
GlueContext, and SparkSession. The `glue_jobs/*` path is excluded from ruff
per pyproject.toml.
"""
