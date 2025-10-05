from __future__ import annotations

import logging

import structlog

_DEF_LEVEL = logging.INFO

logging.basicConfig(level=_DEF_LEVEL, format="%(message)s")
structlog.configure(
    processors=[
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ]
)

get_logger = structlog.get_logger
