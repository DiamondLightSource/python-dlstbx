import queue
import time

from workflows.services.common_service import CommonService


class DLSDropfilePickup(CommonService):
    """A service to collect dispatch message from alternative sources,
    specifically from a directory where messages can be stored if ActiveMQ
    is unavailable on submission."""

    # Human readable service name
    _service_name = "DLS Dropfile Pickup"

    # Logger name
    _logger_name = "dlstbx.services.dropfile"

    def initializing(self):
        """Subscribe to a synchronization queue to ensure only
        one instance of this service is active at a time."""
        self.log.info("Dropfile Pickup service starting")
        if self._environment.get("live"):
            #     self.location = '/dls_sw/apps/zocalo/dropfiles'
            self.location = "/dls/tmp/wra62962/directories/dropfiles"
        else:
            self.location = "."
        self.dropfiles = {}

        self.unlocked_from = 0
        self.unlocked_to = 0
        self._register_idle(60, self.send_activation_message)
        self._transport.subscribe(
            "dropfile", self.cluster_statistic, acknowledgement=True, exclusive=True
        )

    def cluster_statistic(self, header, message):
        """Receive an interesting statistic about the cluster."""
        if not isinstance(message, dict) or "statistic-timestamp" not in message:
            # Invalid message
            self._transport.nack(header)
            return
        self.queue.put(
            (
                message["statistic-timestamp"],
                "cluster-" + message["statistic-cluster"] + "-" + message["statistic"],
                header,
                message,
            )
        )
        if self.unlocked_write_from <= time.time() <= self.unlocked_write_to:
            self.unlocked_write_to = time.time() + 90
            self.process_statistics()
        elif self.unlocked_write_from < time.time():
            self.log.debug("Unlocking statistics writing one minute from now")
            self.unlocked_write_from = time.time() + 60
            self.unlocked_write_to = time.time() + 150

    def send_activation_message(self):
        """Self-activate the instance."""
        self._transport.send("dropfile", {}, expiration=60, persistent=False)

        if self.unlocked_write_from > time.time():
            return
        if time.time() > self.unlocked_write_to:
            self.log.debug("Locking statistics writing, processing all remaining data")
            while not self.queue.empty():
                self.write_out_records()
            self.rrd_file = {}

    def write_out_records(self):
        """Gather a limited number of statistics and write them to the database."""

        # Collect a limited number of records from the queue.
        records = {}
        try:
            while True:
                record = self.queue.get_nowait()
                if record[1] not in records:
                    records[record[1]] = []
                records[record[1]].append((record[2], record[3]))
                if len(records[record[1]]) >= 30:
                    break
        except queue.Empty:
            pass

        if not records:
            self.log.warn("Statistics service in invalid state")
            return

        # Process and acknowledge messages
        # Acknowledge outside TXN for now, https://issues.apache.org/jira/browse/AMQ-6796
        #    txn = self._transport.transaction_begin()

        ignore = lambda x: None

        dispatch = {
            "cluster-live-utilization": self.stats_live_cluster_utilization,
            "cluster-live-node-status": ignore,
            "cluster-live-waiting-jobs-per-queue": self.stats_live_cluster_jobs_waiting,
            "cluster-test-utilization": self.stats_test_cluster_utilization,
            "cluster-test-node-status": ignore,
            "cluster-test-waiting-jobs-per-queue": self.stats_test_cluster_jobs_waiting,
        }
        for key in records:
            headers, messages = list(zip(*records[key]))
            if key in dispatch:
                messages = self.order_and_deduplicate(messages)
                dispatch[key](messages)
            else:
                self.log.warning(
                    "Discarding %d statistics records of unknown type %s",
                    len(records[key]),
                    key,
                )
            for header in headers:
                self._transport.ack(header)  # , transaction=txn)
        #    self._transport.transaction_commit(txn)

        self.log.debug("Processed %d records", sum(len(r) for r in records.values()))

    @staticmethod
    def order_and_deduplicate(stats):
        """Eliminate duplicate records for identical timestamps."""
        dedup = {}
        for stat in stats:
            stat["statistic-timestamp"] = int(stat["statistic-timestamp"])
            dedup[stat["statistic-timestamp"]] = stat
        return [dedup[k] for k in sorted(dedup)]

    def ignore_and_dide_status(self, stats):
        pass

    def stats_live_cluster_utilization(self, stats):
        self.rrd_file["cluster"].update(
            list(
                map(
                    lambda r: [
                        r["statistic-timestamp"],
                        r["total"],
                        r["broken"],
                        r["used-high"],
                        r["used-medium"],
                        r["used-low"],
                    ],
                    stats,
                )
            )
        )
        if "admin" in stats[0]:
            self.rrd_file["clustergroups"].update(
                map(
                    lambda r: [
                        r["statistic-timestamp"],
                        r["cpu"]["total"],
                        r["cpu"]["broken"],
                        r["cpu"]["used-high"],
                        r["cpu"]["used-medium"],
                        r["cpu"]["used-low"],
                        r["gpu"]["total"],
                        r["gpu"]["broken"],
                        r["gpu"]["used-high"],
                        r["gpu"]["used-medium"],
                        r["gpu"]["used-low"],
                        r["admin"]["total"],
                        r["admin"]["broken"],
                        r["admin"]["used"],
                    ],
                    stats,
                )
            )

    def stats_test_cluster_utilization(self, stats):
        self.rrd_file["testcluster"].update(
            map(
                lambda r: [
                    r["statistic-timestamp"],
                    r["total"],
                    r["broken"],
                    r["used-high"],
                    r["used-medium"],
                    r["used-low"],
                ],
                stats,
            )
        )
        self.rrd_file["testclustergroups"].update(
            map(
                lambda r: [
                    r["statistic-timestamp"],
                    r["cpu"]["total"],
                    r["cpu"]["broken"],
                    r["cpu"]["used-high"],
                    r["cpu"]["used-medium"],
                    r["cpu"]["used-low"],
                    r["gpu"]["total"],
                    r["gpu"]["broken"],
                    r["gpu"]["used-high"],
                    r["gpu"]["used-medium"],
                    r["gpu"]["used-low"],
                    r["admin"]["total"],
                    r["admin"]["broken"],
                    r["admin"]["used"],
                ],
                stats,
            )
        )

    def stats_live_cluster_jobs_waiting(self, stats):
        self.rrd_file["clusterbacklog"].update(
            list(
                map(
                    lambda r: [
                        r["statistic-timestamp"],
                        r["admin.q"],
                        r["bottom.q"],
                        r["low.q"],
                        r["medium.q"],
                        r["high.q"],
                    ],
                    stats,
                )
            )
        )

    def stats_test_cluster_jobs_waiting(self, stats):
        self.rrd_file["testclusterbacklog"].update(
            list(
                map(
                    lambda r: [
                        r["statistic-timestamp"],
                        r["test-admin.q"],
                        r["test-bottom.q"],
                        r["test-low.q"],
                        r["test-medium.q"],
                        r["test-high.q"],
                    ],
                    stats,
                )
            )
        )

    def open_all_recordfiles(self):
        self.log.debug("opening record files")
        daydata = ["RRA:%s:0.5:1:1440" % cls for cls in ("AVERAGE", "MAX", "MIN")]
        weekdata = ["RRA:%s:0.5:3:3360" % cls for cls in ("AVERAGE", "MAX", "MIN")]
        monthdata = ["RRA:%s:0.5:6:7440" % cls for cls in ("AVERAGE", "MAX", "MIN")]
        self.rrd_file = {
            "cluster": self.rrd.create(
                "cluster-utilization-live-general.rrd",
                ["--step", "60"]
                + [
                    "DS:%s:GAUGE:180:0:U" % name
                    for name in (
                        "slot-total",
                        "slot-broken",
                        "slot-used-h",
                        "slot-used-m",
                        "slot-used-l",
                    )
                ]
                + daydata
                + weekdata
                + monthdata,
            ),
            "clusterbacklog": self.rrd.create(
                "cluster-jobswaiting-live.rrd",
                ["--step", "60"]
                + [
                    "DS:%s:GAUGE:180:0:U" % name
                    for name in ("admin", "bottom", "low", "medium", "high")
                ]
                + daydata
                + weekdata
                + monthdata,
            ),
            "clustergroups": self.rrd.create(
                "cluster-utilization-live-groups.rrd",
                ["--step", "60"]
                + [
                    "DS:%s:GAUGE:180:0:U" % name
                    for name in (
                        "cpu-slot-total",
                        "cpu-slot-broken",
                        "cpu-slot-used-h",
                        "cpu-slot-used-m",
                        "cpu-slot-used-l",
                    )
                ]
                + [
                    "DS:%s:GAUGE:180:0:U" % name
                    for name in (
                        "gpu-slot-total",
                        "gpu-slot-broken",
                        "gpu-slot-used-h",
                        "gpu-slot-used-m",
                        "gpu-slot-used-l",
                    )
                ]
                + [
                    "DS:%s:GAUGE:180:0:U" % name
                    for name in ("admin-total", "admin-broken", "admin-used")
                ]
                + daydata
                + weekdata
                + monthdata,
            ),
            "testcluster": self.rrd.create(
                "cluster-utilization-test-general.rrd",
                ["--step", "60"]
                + [
                    "DS:%s:GAUGE:180:0:U" % name
                    for name in (
                        "slot-total",
                        "slot-broken",
                        "slot-used-h",
                        "slot-used-m",
                        "slot-used-l",
                    )
                ]
                + daydata
                + weekdata
                + monthdata,
            ),
            "testclusterbacklog": self.rrd.create(
                "cluster-jobswaiting-test.rrd",
                ["--step", "60"]
                + [
                    "DS:%s:GAUGE:180:0:U" % name
                    for name in ("admin", "bottom", "low", "medium", "high")
                ]
                + daydata
                + weekdata
                + monthdata,
            ),
            "testclustergroups": self.rrd.create(
                "cluster-utilization-test-groups.rrd",
                ["--step", "60"]
                + [
                    "DS:%s:GAUGE:180:0:U" % name
                    for name in (
                        "cpu-slot-total",
                        "cpu-slot-broken",
                        "cpu-slot-used-h",
                        "cpu-slot-used-m",
                        "cpu-slot-used-l",
                    )
                ]
                + [
                    "DS:%s:GAUGE:180:0:U" % name
                    for name in (
                        "gpu-slot-total",
                        "gpu-slot-broken",
                        "gpu-slot-used-h",
                        "gpu-slot-used-m",
                        "gpu-slot-used-l",
                    )
                ]
                + [
                    "DS:%s:GAUGE:180:0:U" % name
                    for name in ("admin-total", "admin-broken", "admin-used")
                ]
                + daydata
                + weekdata
                + monthdata,
            ),
        }

        for k in self.rrd_file:
            if not self.rrd_file[k]:
                raise OSError("Failed to open record file for %s" % k)
