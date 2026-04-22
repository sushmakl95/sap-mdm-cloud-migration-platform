"""CDC pipeline components — DMS config + Databricks Auto Loader consumer."""

from migration.cdc.auto_loader import CdcConsumer, CdcConsumerConfig
from migration.cdc.dms_config import DmsConfigGenerator, DmsTaskConfig

__all__ = ["CdcConsumer", "CdcConsumerConfig", "DmsConfigGenerator", "DmsTaskConfig"]
