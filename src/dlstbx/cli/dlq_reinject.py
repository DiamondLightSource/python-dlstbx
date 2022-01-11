from __future__ import annotations

import time

import zocalo.cli.dlq_reinject


def run() -> None:
    print("\ndlstbx.dlq_reinject is deprecated. Use 'zocalo.dlq_reinject' instead\n")
    time.sleep(1)
    zocalo.cli.dlq_reinject.run()
