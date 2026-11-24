from __future__ import annotations

import collections
import itertools
import logging
import time

import requests
from workflows.services.common_service import CommonService
from zocalo.util import slurm


class DLSClusterMonitor(CommonService):
    """A service to interface zocalo with functions to gather statistics for the slurm cluster."""

    # Human readable service name
    _service_name = "DLS Cluster monitor"

    # Logger name
    _logger_name = "dlstbx.services.cluster_monitor"

    # Name of the cluster to monitor, as named by its zocalo slurm plugin
    _scheduler = "slurm"

    def initializing(self):
        """Set up monitoring timer. Do not subscribe to anything."""
        self.log.info("Cluster monitor starting")

        # Generate cluster statistics up to every 30 seconds.
        # Statistics go with debug level to a separate logger so they can be
        # filtered by log monitors.

        # The scheduler storage setting is deprecated, use DLSIrisClusterMonitor instead
        self.scheduler = self.config.storage.get("scheduler", self._scheduler)
        self.cluster_api: slurm.SlurmRestApi = (
            slurm.SlurmRestApi.from_zocalo_configuration(
                self.config, cluster=self.scheduler
            )
        )
        self.stats_log = logging.getLogger(self._logger_name + ".stats")
        self.stats_log.setLevel(logging.DEBUG)
        self._register_idle(30, self.update_cluster_statistics)

    def update_cluster_statistics(self):
        """Gather some cluster statistics."""

        try:
            self.log.debug(f"Gathering {self.scheduler} job statistics")
            timestamp = time.time()
            job_info_resp: slurm.models.OpenapiJobInfoResp = self.cluster_api.get_jobs()
        except requests.HTTPError as e:
            self.log.error(f"Failed Slurm API call: {e}\n{e.response.text}")
            raise e
        else:
            self.calculate_slurm_statistics(self.scheduler, job_info_resp, timestamp)
        # Add timeout between making SLURM REST API calls.
        # Trying to address https://support.schedmd.com/show_bug.cgi?id=21123
        time.sleep(5)

    def calculate_slurm_statistics(
        self, scheduler, response: slurm.models.OpenapiJobInfoResp, timestamp
    ):
        self.log.debug(f"Processing {scheduler} job states")
        jobs_states = itertools.chain(
            *[
                job.job_state
                for job in getattr(response.jobs, "root", [])
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


class DLSIrisMonitor(DLSClusterMonitor):
    """A service to interface zocalo with functions to gather statistics for the IRIS cluster."""

    _service_name = "DLS IRIS cluster monitor"
    _scheduler = "iris"
