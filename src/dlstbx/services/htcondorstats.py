from __future__ import annotations

import time
from pathlib import Path
from pprint import pformat

import htcondor
import minio
from workflows.services.common_service import CommonService

from dlstbx.util.iris import get_minio_client


class HTCondorStats(CommonService):
    """
    A service that collects HTCondor and S3 Echo object store utilization
    statistics.
    """

    # Human readable service name
    _service_name = "HTCondorstats"

    # Logger name
    _logger_name = "dlstbx.services.htcondorstats"

    # STFC S3 Echo credentials
    _s3echo_credentials = "/dls_sw/apps/zocalo/secrets/credentials-echo-mx.cfg"

    def initializing(self):
        """
        Register callback function to collect HTCondor and S3 Echo stats.
        """
        self.log.info("HTCondorstats starting")

        collector = htcondor.Collector(htcondor.param["COLLECTOR_HOST"])
        schedd_ad = collector.locate(htcondor.DaemonTypes.Schedd)
        self.schedd = htcondor.Schedd(schedd_ad)

        self.minio_client: minio.Minio = get_minio_client(
            HTCondorStats._s3echo_credentials
        )

        self._register_idle(30, self.update_htcondor_statistics)

    def update_htcondor_statistics(self):
        """Gather job status statistics from STFC/IRIS and S3 Echo object store."""

        # Query number of jobs on STRF/IRIS
        self.log.debug("Gathering STFC/IRIS statistics...")
        data_pack = {
            "statistic": "job-status",
            "statistic-cluster": "iris",
            "statistic-group": "cluster",
            "statistic-timestamp": time.time(),
        }
        res = self.schedd.query(
            constraint='Owner=="gda2"',
            projection=["Owner", "ClusterId", "ProcId", "JobStatus", "Out"],
        )
        job_status_list = [job["JobStatus"] for job in res]
        for label, code in (("waiting", 1), ("running", 2), ("hold", 5)):
            data_pack[label] = job_status_list.count(code)

        self.log.debug(f"{pformat(data_pack)}")
        self._transport.broadcast("transient.statistics.cluster", data_pack)
        self._transport.send("statistics.cluster", data_pack, persistent=False)

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
        self._transport.send("statistics.cluster", data_pack, persistent=False)

        # Query /iris mount status
        self.log.debug("Gathering /iris mount status...")
        for beamline in ("i03", "i04", "i04-1", "i24"):
            start = time.time()
            data_pack = {
                "statistic": "storage-status",
                "statistic-cluster": "datasyncer",
                "statistic-group": "iris",
                "statistic-timestamp": start,
            }
            location = f"/iris/{beamline}/data/2024/"
            try:
                mtime_location = Path(location).stat().st_mtime
                self.log.info(f"/iris/{beamline} mount st_mtime: {mtime_location}")
            finally:
                runtime = time.time() - start
                if runtime > 5:
                    # Anything higher than 5 seconds should be explicitly logged
                    self.log.warning(
                        f"Excessive stat-time for accessing {location} on argus",
                        extra={
                            "stat-time": runtime,
                        },
                    )
                data_pack["path"] = location
                data_pack["stat-time"] = runtime

            if mtime_location:
                self.log.debug(f"{pformat(data_pack)}")
                self._transport.broadcast("transient.statistics.cluster", data_pack)
                self._transport.send("statistics.cluster", data_pack, persistent=False)
            else:
                self.log.error(f"Cannot access path {location} from argus")
