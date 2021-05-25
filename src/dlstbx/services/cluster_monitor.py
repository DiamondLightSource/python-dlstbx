import collections
import enum
import logging
import time

from prometheus_client import start_http_server, Gauge

import dlstbx.util.cluster
from workflows.services.common_service import CommonService


class NodeStatus(enum.Enum):
    RUNNING = 1
    SUSPENDED = 2
    BROKEN = 3


class DLSClusterMonitor(CommonService):
    """A service to interface zocalo with functions to gather cluster statistics."""

    # Human readable service name
    _service_name = "DLS Cluster monitor"

    # Logger name
    _logger_name = "dlstbx.services.cluster_monitor"

    def __new__(cls, *args, **kwargs):
        """
        Start DRMAA cluster control processes as children of the main process,
        and transparently inject references to those system-wide processes into
        all instantiated objects.
        """
        if not hasattr(DLSClusterMonitor, "__drmaa_cluster"):
            setattr(
                DLSClusterMonitor,
                "__drmaa_cluster",
                dlstbx.util.cluster.Cluster("dlscluster"),
            )
        if not hasattr(DLSClusterMonitor, "__drmaa_testcluster"):
            setattr(
                DLSClusterMonitor,
                "__drmaa_testcluster",
                dlstbx.util.cluster.Cluster("dlstestcluster"),
            )
        if not hasattr(DLSClusterMonitor, "__drmaa_hamilton"):
            setattr(
                DLSClusterMonitor,
                "__drmaa_hamilton",
                dlstbx.util.cluster.Cluster("hamilton"),
            )
        instance = super().__new__(DLSClusterMonitor)
        instance.__drmaa_cluster = getattr(DLSClusterMonitor, "__drmaa_cluster")
        instance.__drmaa_testcluster = getattr(DLSClusterMonitor, "__drmaa_testcluster")
        instance.__drmaa_hamilton = getattr(DLSClusterMonitor, "__drmaa_hamilton")
        return instance

    def initializing(self):
        """Set up monitoring timer. Do not subscribe to anything."""
        self.log.info("Cluster monitor starting")

        start_http_server(8000)

        # Prometheus instruments
        self._cluster_utilization = Gauge(
            name="zocalo_cluster_utilization",
            documentation="Zocalo cluster utilization",
            labelnames=("cluster", "node_type", "metric"),
        )
        self._cluster_jobs_waiting = Gauge(
            name="zocalo_cluster_jobs_waiting",
            documentation="Number of waiting cluster jobs",
            labelnames=("cluster", "queue"),
        )
        self._node_status = Gauge(
            "zocalo_cluster_node_status",
            "Current status of a given queue on a given cluster node. The status is represented as a numerical value where `running=1', `broken=2' and `suspended=3'.",
            labelnames=("node", "queue"),
        )
        self._node_slots_all = Gauge(
            "zocalo_cluster_node_slots_all",
            "Total number of available cluster slots for a given queue on a given node.",
            labelnames=("node", "queue"),
        )
        self._node_slots_used = Gauge(
            "zocalo_cluster_node_slots_used",
            "Total number of used cluster slots for a given queue on a given node.",
            labelnames=("node", "queue"),
        )
        self._node_slots_reserved = Gauge(
            "zocalo_cluster_node_slots_reserved",
            "Total number of reserved cluster slots for a given queue on a given node.",
            labelnames=("node", "queue"),
        )

        # Generate cluster statistics up to every 30 seconds.
        # Statistics go with debug level to a separate logger so they can be
        # filtered by log monitors.
        self.cluster_statistics = dlstbx.util.cluster.ClusterStatistics()
        self.stats_log = logging.getLogger(self._logger_name + ".stats")
        self.stats_log.setLevel(logging.DEBUG)
        self._register_idle(30, self.update_cluster_statistics)

    def update_cluster_statistics(self):
        """Gather some cluster statistics."""

        for cluster_name, cluster_reference, downstream_name in [
            ("science cluster", self.__drmaa_cluster, "live"),
            ("test cluster", self.__drmaa_testcluster, "test"),
            ("hamilton", self.__drmaa_hamilton, "hamilton"),
        ]:
            if not cluster_reference.is_accessible:
                self.log.warning(
                    f"Statistics for {cluster_name} unavailable due to previous error"
                )
                continue
            self.log.debug("Gathering %s statistics...", cluster_name)
            timestamp = time.time()
            try:
                joblist, queuelist = self.cluster_statistics.run_on(
                    cluster_reference, arguments=["-f", "-r", "-u", "gda2"]
                )
            except (AssertionError, RuntimeError):
                self.log.error(
                    f"Could not gather {cluster_name} statistics", exc_info=True
                )
            else:
                self.calculate_cluster_statistics(
                    joblist, queuelist, downstream_name, timestamp
                )

    def calculate_cluster_statistics(self, joblist, queuelist, cluster, timestamp):
        self.log.debug("Processing %s cluster statistics", cluster)
        hamilton = cluster == "hamilton"

        pending_jobs = collections.Counter(
            j["queue"].split("@@")[0]
            for j in joblist
            if j["state"] == "pending"
            and "h" not in j["statecode"]
            and "E" not in j["statecode"]
        )
        waiting_jobs_per_queue = {
            queue: pending_jobs[queue]
            for queue in {q["class"] for q in queuelist} | set(pending_jobs)
        }
        self.report_statistic(
            waiting_jobs_per_queue,
            description="waiting-jobs-per-queue",
            cluster=cluster,
            timestamp=timestamp,
        )
        for queue, v in waiting_jobs_per_queue.items():
            if queue.endswith(".q"):
                self._cluster_jobs_waiting.labels(
                    cluster,
                    queue,
                ).set(v)

        cluster_nodes = self.cluster_statistics.get_nodelist_from_queuelist(queuelist)
        node_summary = {
            node: self.cluster_statistics.summarize_node_status(status)
            for node, status in cluster_nodes.items()
        }
        self.report_statistic(
            node_summary,
            description="node-status",
            cluster=cluster,
            timestamp=timestamp,
        )
        for node, stats in node_summary.items():
            for queue, v in stats.items():
                if queue.endswith(".q"):
                    self._node_status.labels(node, queue).set(
                        NodeStatus[stats["status"].upper()].value
                    )
                    self._node_slots_all.labels(node, queue).set(v["slots"])
                    self._node_slots_used.labels(node, queue).set(v["used"])
                    self._node_slots_reserved.labels(node, queue).set(v["reserved"])

        corestats = {}
        corestats["cpu"] = {"total": 0, "broken": 0}
        if hamilton:
            corestats["cpu"]["free"] = 0
        else:
            corestats["cpu"].update(
                {"free_for_low": 0, "free_for_medium": 0, "free_for_high": 0}
            )
        corestats["gpu"] = corestats["cpu"].copy()

        for nodename, node in cluster_nodes.items():
            node = {q["class"]: q for q in node}
            for queuename in list(node):
                if queuename.startswith("test"):
                    if queuename.startswith("test-") and queuename[5:] not in node:
                        node[queuename[5:]] = node[queuename]
                    del node[queuename]

            if "admin.q" in node:
                if "admin" not in corestats:
                    corestats["admin"] = {"total": 0, "broken": 0, "free": 0}
                adminq_slots = node["admin.q"]["slots_total"]
                corestats["admin"]["total"] += adminq_slots
                if (
                    node["admin.q"]["enabled"]
                    and not node["admin.q"]["suspended"]
                    and not node["admin.q"]["error"]
                ):
                    corestats["admin"]["free"] += node["admin.q"]["slots_free"]
                else:
                    corestats["admin"]["broken"] += adminq_slots
                del node["admin.q"]

            if not node:
                continue

            nodename = (nodename.split("-")[2:3] or [None])[0]
            if nodename and (nodename == "com14" or nodename.startswith("gpu")):
                nodetype = "gpu"
            else:
                nodetype = "cpu"
            cores = max(q["slots_total"] for q in node.values())
            corestats[nodetype]["total"] += cores
            node = {
                n: q
                for n, q in node.items()
                if q["enabled"] and not q["suspended"] and not q["error"]
            }
            if not node:
                corestats[nodetype]["broken"] += cores
                continue
            if hamilton:
                corestats[nodetype]["free"] += node.get("all.q", {}).get(
                    "slots_free", 0
                )
            else:
                freelow, freemedium, freehigh = (
                    node.get(q, {}).get("slots_free", 0)
                    for q in ("low.q", "medium.q", "high.q")
                )
                corestats[nodetype]["free_for_low"] += freelow
                corestats[nodetype]["free_for_medium"] += max(freelow, freemedium)
                corestats[nodetype]["free_for_high"] += max(
                    freelow, freemedium, freehigh
                )

        for nodetype in ("cpu", "gpu"):
            if hamilton:
                corestats[nodetype]["used"] = (
                    corestats[nodetype]["total"]
                    - corestats[nodetype]["broken"]
                    - corestats[nodetype]["free"]
                )
            else:
                corestats[nodetype]["used-high"] = (
                    corestats[nodetype]["total"]
                    - corestats[nodetype]["broken"]
                    - corestats[nodetype]["free_for_high"]
                )
                corestats[nodetype]["used-medium"] = (
                    corestats[nodetype]["free_for_high"]
                    - corestats[nodetype]["free_for_medium"]
                )
                corestats[nodetype]["used-low"] = (
                    corestats[nodetype]["free_for_medium"]
                    - corestats[nodetype]["free_for_low"]
                )
            for k, v in corestats[nodetype].items():
                corestats[k] = corestats.get(k, 0) + v

        if "admin" in corestats:
            corestats["admin"]["used"] = (
                corestats["admin"]["total"]
                - corestats["admin"]["free"]
                - corestats["admin"]["broken"]
            )

        self.report_statistic(
            corestats, description="utilization", cluster=cluster, timestamp=timestamp
        )
        for nodetype in ("cpu", "gpu"):
            for metric, value in corestats[nodetype].items():
                self._cluster_utilization.labels(
                    cluster,
                    nodetype,
                    metric,
                ).set(value)

    def report_statistic(
        self, data: dict, *, description: str, cluster: str, timestamp: float
    ) -> None:
        data_pack = {
            "statistic-group": "cluster",
            "statistic": description,
            "statistic-cluster": cluster,
            "statistic-timestamp": timestamp,
        }
        data_pack.update(data)
        self._transport.broadcast("transient.statistics.cluster", data_pack)
        self._transport.send("statistics.cluster", data_pack, persistent=False)
