from __future__ import annotations

import logging

from dlstbx.util import DowngradeErrorsFilter


def test_downgrading_filter_reduces_severity_of_error_messages(caplog):
    logname = "dlstbx.tests.logging"
    logger = logging.getLogger(logname)
    logger.addFilter(DowngradeErrorsFilter())
    caplog.set_level(logging.DEBUG, logger=logname)

    for level in (logging.DEBUG, logging.INFO, logging.WARNING, logging.CRITICAL):
        logger.log(level, "test message")
        assert len(caplog.records) == 1
        assert caplog.records[0].levelno == level
        caplog.clear()

    logger.log(logging.ERROR, "test message")
    assert len(caplog.records) == 1
    assert caplog.records[0].levelno == logging.WARNING
