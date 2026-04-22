"""CloudWatch custom metrics emitter.

Emits per-(table, phase, batch) metrics that feed the CloudWatch dashboard
and alarms. We batch metric puts to stay within the 20-metrics-per-call limit.
"""

from __future__ import annotations

from typing import Any

import boto3

from migration.utils.logging_config import get_logger

log = get_logger(__name__, component="metrics")


class MetricsEmitter:
    """Wrapper around CloudWatch PutMetricData."""

    def __init__(self, namespace: str = "SAP/Migration", region: str = "us-east-1"):
        self.namespace = namespace
        self.region = region
        self.client = boto3.client("cloudwatch", region_name=region)

    def emit(
        self,
        metric_name: str,
        value: float,
        unit: str = "Count",
        dimensions: dict[str, str] | None = None,
    ) -> None:
        """Emit a single metric."""
        dim_list = (
            [{"Name": k, "Value": v} for k, v in dimensions.items()]
            if dimensions else []
        )
        try:
            self.client.put_metric_data(
                Namespace=self.namespace,
                MetricData=[{
                    "MetricName": metric_name,
                    "Value": value,
                    "Unit": unit,
                    "Dimensions": dim_list,
                }],
            )
        except Exception as exc:
            # Never fail a pipeline just because metrics couldn't send
            log.warning("metric_emit_failed", metric=metric_name, error=str(exc))

    def emit_batch(self, data: list[dict[str, Any]]) -> None:
        """Emit up to 1000 metrics in chunked 20-metric batches."""
        try:
            for chunk_start in range(0, len(data), 20):
                chunk = data[chunk_start : chunk_start + 20]
                self.client.put_metric_data(Namespace=self.namespace, MetricData=chunk)
        except Exception as exc:
            log.warning("metric_batch_emit_failed", error=str(exc))
