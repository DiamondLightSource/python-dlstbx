from __future__ import annotations

import time
from pprint import pformat

import minio
from workflows.services.common_service import CommonService

from dlstbx.util.iris import get_minio_client


class CloudStats(CommonService):
    """
    A service that collects Cloud and S3 Echo object store utilization
    statistics.
    """

    # Human readable service name
    _service_name = "Cloudstats"

    # Logger name
    _logger_name = "dlstbx.services.cloudstats"

    # STFC S3 Echo credentials
    _s3echo_credentials = "/dls_sw/apps/zocalo/secrets/credentials-echo-mx.cfg"

    def initializing(self):
        """
        Register callback function to collect Cloud and S3 Echo stats.
        """
        self.log.info("Cloudstats starting")

        self.minio_client: minio.Minio = get_minio_client(
            CloudStats._s3echo_credentials
        )

        self._register_idle(30, self.update_slurm_statistics)

    def update_slurm_statistics(self):
        """Gather storage usage statistics from STFC/IRIS and S3 Echo."""

        # Query S3 Echo object store usage
        self.log.debug("Gathering S3Echo statistics...")
        data_pack = {
            "statistic": "used-storage",
            "statistic-cluster": "s3echo",
            "statistic-group": "dls-mx",
            "statistic-timestamp": time.time(),
        }
        data_pack["total"] = 0
        for bucket in self.minio_client.list_buckets():
            data_pack[bucket.name] = 0
            store_objects = [
                obj.object_name for obj in self.minio_client.list_objects(bucket.name)
            ]
            for filename in store_objects:
                try:
                    result = self.minio_client.stat_object(bucket.name, filename)
                    data_pack[bucket.name] += result.size / 2**40
                    data_pack["total"] += result.size / 2**40
                except minio.error.S3Error:
                    self.log.debug(
                        f"Exception raised trying to read {filename} object in {bucket.name}"
                    )

        self.log.debug(f"{pformat(data_pack)}")
        self._transport.broadcast("transient.statistics.cluster", data_pack)
