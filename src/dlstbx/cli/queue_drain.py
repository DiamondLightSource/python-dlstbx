import zocalo.cli.queue_drain


def run():
    print("\ndlstbx.queue_drain is deprecated. Use 'zocalo.queue_drain' instead\n")
    zocalo.cli.queue_drain.run()
