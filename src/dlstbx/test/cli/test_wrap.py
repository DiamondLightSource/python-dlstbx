from __future__ import annotations

import procrunner


def test_dummy_wrapper(caplog):
    result = procrunner.run(
        ["dlstbx.wrap", "--wrap", "dummy", "-t", "OfflineTransport"], timeout=65
    )
    assert b"successfully finished processing" in result.stderr
