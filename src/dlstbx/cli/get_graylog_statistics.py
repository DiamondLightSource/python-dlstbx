from __future__ import annotations

import logging
import time

from dlstbx.util.colorstreamhandler import ColorStreamHandler
from dlstbx.util.graylog import GraylogAPI
from dlstbx.util.rrdtool import RRDTool

loglevels = {
    2: "critical",
    3: "error",
    4: "warning",
    5: "notice",
    6: "info",
    7: "debug",
}


class GraylogRRD:
    def __init__(self, path=".", api=None):
        self.rrd = RRDTool(path)
        self.setup_rrd()
        self.api_graylog = api
        self.log = logging.getLogger("dlstbx.command_line.graylog_stats")

    def setup_rrd(self):
        daydata = ["RRA:%s:0.5:1:1440" % cls for cls in ("AVERAGE", "MAX")]
        weekdata = ["RRA:%s:0.5:3:3360" % cls for cls in ("AVERAGE", "MAX")]
        monthdata = ["RRA:%s:0.5:6:7440" % cls for cls in ("AVERAGE", "MAX")]
        self.rrd_graylog = self.rrd.create(
            "graylog-message-summary.rrd",
            ["--step", "60"]
            + ["DS:%s:GAUGE:180:0:U" % loglevels[k] for k in sorted(loglevels)]
            + daydata
            + weekdata
            + monthdata,
        )

    def update(self):
        update_time = int(time.time())
        self.log.info("Last known data point:    %d", self.rrd_graylog.last_update)
        self.log.info("Current time:             %d", update_time)
        if update_time - (update_time % 60) <= self.rrd_graylog.last_update + 60:
            self.log.info("No update required.")
            return
        if not self.api_graylog:
            self.log.warn("Graylog API not available.")
            return
        # Process at most one month worth of log history
        update_from = max(
            self.rrd_graylog.last_update + 60, update_time - 30 * 24 * 3600
        )
        update_from -= update_from % 60  # Capture first minute in full
        self.log.info("Update log starting from: %d", update_from)

        data = self.api_graylog.gather_log_levels_histogram_since(update_from)

        updates = []
        for datapoint in sorted(data):
            if datapoint > update_time - 60:
                # last minute was not captured in full
                continue
            update_record = [datapoint]
            update_record.extend(
                data[datapoint].get(level, 0) for level in sorted(loglevels)
            )
            updates.append(update_record)

        if not updates:
            self.log.warn("No updates available")
            return

        while updates:
            self.rrd_graylog.update(updates[0:30])
            updates = updates[30:]
        self.log.info("Updated to:               %d", self.rrd_graylog.last_update)


def setup_logging(level=logging.INFO):
    console = ColorStreamHandler()
    console.setLevel(level)
    logger = logging.getLogger()
    logger.setLevel(logging.WARN)
    logger.addHandler(console)
    logging.getLogger("dlstbx").setLevel(level)


def run():
    setup_logging(logging.INFO)
    g = GraylogAPI("/dls_sw/apps/zocalo/secrets/credentials-log.cfg")
    GraylogRRD(api=g).update()
