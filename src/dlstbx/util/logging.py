from __future__ import annotations

import logging


class DowngradeErrorsFilter(logging.Filter):
    """Reduce all ERROR messages to WARNING level."""

    def __repr__(self) -> str:
        return "<DowngradeErrorsFilter>"

    def filter(self, record: logging.LogRecord):
        if record.levelno <= logging.WARNING:
            return True
        if record.levelno >= logging.CRITICAL:
            return True
        record.levelno = logging.WARNING
        record.levelname = "WARNING"
        return True
