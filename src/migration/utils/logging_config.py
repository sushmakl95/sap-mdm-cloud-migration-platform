"""Structured JSON logging for production + console for local dev."""

from __future__ import annotations

import logging
import os
import sys
from typing import Any

import structlog


def configure_logging(level: str = "INFO", fmt: str = "json") -> None:
    """Configure structlog once, globally.

    :param level: log level (DEBUG/INFO/WARNING/ERROR)
    :param fmt: 'json' for CloudWatch, 'console' for local dev
    """
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(message)s",
        stream=sys.stdout,
    )

    processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
    ]

    if fmt == "json":
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer(colors=True))

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper())
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
    )


# Auto-configure based on env var (ETL_LOG_FORMAT=json or console)
configure_logging(
    level=os.environ.get("ETL_LOG_LEVEL", "INFO"),
    fmt=os.environ.get("ETL_LOG_FORMAT", "console"),
)


def get_logger(name: str, **bindings):
    """Get a structured logger with optional default bindings.

    Usage:
        log = get_logger(__name__, component="extractor", table="MARA")
        log.info("started", rows=1000)
    """
    return structlog.get_logger(name).bind(**bindings)
