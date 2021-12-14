import time

import zocalo.cli.dlq_purge


def run() -> None:
    print("\ndlstbx.dlq_purge is deprecated. Use 'zocalo.dlq_purge' instead\n")
    time.sleep(1)
    zocalo.cli.dlq_purge.run()
