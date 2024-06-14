from __future__ import annotations

import collections
import itertools
import logging
import time

import requests
from workflows.services.common_service import CommonService
from zocalo.util import slurm

import dlstbx.util.cluster


class DLSClusterMonitor(CommonService):
    """A service to interface zocalo with functions to gather cluster statistics."""

    # Human readable service name
    _service_name = "DLS Cluster monitor"

    # Logger name
    _logger_name = "dlstbx.services.cluster_monitor"

    def initializing(self):
        """Set up monitoring timer. Do not subscribe to anything."""
        self.log.info("Cluster monitor starting")

        # Generate cluster statistics up to every 30 seconds.
        # Statistics go with debug level to a separate logger so they can be
        # filtered by log monitors.
        self.cluster_statistics = dlstbx.util.cluster.ClusterStatistics()

        self.slurm_api: slurm.SlurmRestApi = (
            slurm.SlurmRestApi.from_zocalo_configuration(self.config)
        )
        self.iris_api: slurm.SlurmRestApi = (
            slurm.SlurmRestApi.from_zocalo_configuration(self.config, cluster="iris")
        )

        self.stats_log = logging.getLogger(self._logger_name + ".stats")
        self.stats_log.setLevel(logging.DEBUG)
        self._register_idle(30, self.update_cluster_statistics)

    def update_cluster_statistics(self):
        """Gather some cluster statistics."""

        for scheduler, cluster_api in [
            ("slurm", self.slurm_api),
            ("iris", self.iris_api),
        ]:
            try:
                self.log.debug(f"Gathering {scheduler} job statistics")
                timestamp = time.time()
                job_info_resp: slurm.models.OpenapiJobInfoResp = cluster_api.get_jobs()
            except requests.HTTPError as e:
                self.log.error(f"Failed Slurm API call: {e}\n" f"{e.response.text}")
            else:
                self.calculate_slurm_statistics(scheduler, job_info_resp, timestamp)

    def calculate_slurm_statistics(
        self, scheduler, response: slurm.models.OpenapiJobInfoResp, timestamp
    ):
        self.log.debug(f"Processing {scheduler} job states")
        jobs_states = itertools.chain(
            *[
                job.job_state
                for job in dict(response.jobs).get("__root__", [])
                if job.user_name == "gda2"
            ]
        )
        data: dict[str, int] = dict(
            collections.Counter([js.name for js in jobs_states])
        )
        self.report_statistic(
            data, cluster=scheduler, description="job-states", timestamp=timestamp
        )

    def report_statistic(self, data: dict[str, int], **kwargs):
        data_pack = {
            "statistic-group": "cluster",
            "statistic": kwargs["description"],
            "statistic-cluster": kwargs["cluster"],
            "statistic-timestamp": kwargs["timestamp"],
        }
        data_pack.update(data)
        self._transport.broadcast("transient.statistics.cluster", data_pack)
        self._transport.send("statistics.cluster", data_pack, persistent=False)
