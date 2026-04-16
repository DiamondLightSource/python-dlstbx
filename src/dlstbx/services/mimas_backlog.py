from __future__ import annotations

import logging
import pathlib
import time
from datetime import datetime, timedelta

import ispyb.sqlalchemy
import sqlalchemy
import sqlalchemy.orm
import workflows.recipe
from ispyb.sqlalchemy import (
    AutoProcIntegration,
    AutoProcProgram,
    AutoProcProgramAttachment,
    DataCollection,
    ProcessingJob,
)
from sqlalchemy import or_
from workflows.services.common_service import CommonService


class DLSMimasBacklog(CommonService):
    """
    A service to monitor the mimas.held backlog queue and drip-feed them into
    the live queue as long as there isn't a cluster backlog.

    Also handles the mimas.multiplex queue, where xia2.multiplex invocations
    are held until all related xia2-dials jobs have completed.
    """

    _service_name = "DLS Mimas Backlog"
    _logger_name = "dlstbx.services.mimas_backlog"

    def initializing(self):
        """Subscribe to mimas.held, mimas.multiplex, and transient.statistics.cluster"""
        self.log.info("MimasBacklog service starting up")

        self._message_delay = 30
        self._jobs_waiting = {"slurm": 60, "iris": 3000}
        self._last_cluster_update = {"slurm": time.time(), "iris": time.time()}

        self._backoff_delay = 8
        self._backoff_multiplier = 2
        self._backoff_max_try = 10

        self._ispyb_sessionmaker = sqlalchemy.orm.sessionmaker(
            bind=sqlalchemy.create_engine(
                ispyb.sqlalchemy.url(), connect_args={"use_pure": True}
            )
        )

        # Subscribe to the mimas.held queue, which contains the held mimas
        # recipes we would like to drip-feed to the dispatcher
        workflows.recipe.wrap_subscribe(
            self._transport,
            "mimas.held",
            self.on_mimas_held,
            acknowledgement=True,
            exclusive=True,
            log_extender=self.extend_log,
        )

        # Subscribe to the mimas.multiplex queue, which holds xia2.multiplex
        # invocations that are waiting for related xia2-dials jobs to complete
        workflows.recipe.wrap_subscribe(
            self._transport,
            "mimas.multiplex",
            self.on_mimas_multiplex,
            acknowledgement=True,
            log_extender=self.extend_log,
        )

        # Subscribe to the transient.statistics.cluster topic, which we will
        # examine to determine the number of waiting jobs
        self._transport.subscribe_broadcast(
            "transient.statistics.cluster",
            self.on_statistics_cluster,
        )

    def on_statistics_cluster(self, header, message):
        """
        Examine the message to determine number of waiting jobs.

        We are interested in the "slurm" and "iris" clusters for now. We are
        only concerned about the number of pending gda2 jobs.
        """
        for statistic_cluster in ("slurm", "iris"):
            if (
                message["statistic-cluster"] == statistic_cluster
                and message["statistic"] == "job-states"
            ):
                self._last_cluster_update[statistic_cluster] = time.time()
                self._jobs_waiting[statistic_cluster] = message.get("PENDING", 0)
                self.log.log(
                    logging.INFO
                    if self._jobs_waiting[statistic_cluster]
                    else logging.DEBUG,
                    f"Jobs waiting on {statistic_cluster} cluster: {self._jobs_waiting[statistic_cluster]}\n",
                )

    def on_mimas_held(self, rw, header, message):
        """
        Forward message to trigger if number of waiting jobs doesn't exceed
        the predefined threshold.
        """
        # Conditionally acknowledge receipt of the message
        txn = rw.transport.transaction_begin(subscription_id=header["subscription"])
        rw.transport.ack(header, transaction=txn)

        statistic_cluster = message["parameters"].get("statistic-cluster", "slurm")
        try:
            max_jobs_waiting = self.config.storage.get(
                "max_jobs_waiting", {"slurm": 60, "iris": 3000}
            )
            timeout = self.config.storage.get("timeout", 300)
        except AttributeError:
            max_jobs_waiting = {"slurm": 60, "iris": 3000}
            timeout = 300
        self.log.debug(
            f"Jobs waiting on {statistic_cluster} cluster: {self._jobs_waiting[statistic_cluster]}\n"
        )

        if self._jobs_waiting[statistic_cluster] < max_jobs_waiting[statistic_cluster]:
            if self._last_cluster_update[statistic_cluster] > time.time() - timeout:
                rw.send(message, transaction=txn)
                self._jobs_waiting[statistic_cluster] += 1
                self.log.info(f"Sent message to trigger: {message}")
            else:
                self.log.warning(
                    f"Not heard from {statistic_cluster} cluster for over 5 minutes. Holding jobs."
                )
                rw.checkpoint(
                    message,
                    delay=self._message_delay,
                    transaction=txn,
                )
        else:
            rw.checkpoint(
                message,
                delay=self._message_delay,
                transaction=txn,
            )

        # Commit transaction
        rw.transport.transaction_commit(txn)

    def on_mimas_multiplex(self, rw, header, message):
        """
        Wait for all related xia2-dials jobs to complete before forwarding the
        xia2.multiplex invocation to the ISPyB service.

        Uses exponential backoff (delay = backoff_delay * backoff_multiplier ** ntry)
        up to backoff_max_try retries. After that, forwards regardless so the
        xia2.multiplex recipe can proceed with whatever results are available.
        """
        txn = rw.transport.transaction_begin(subscription_id=header["subscription"])
        rw.transport.ack(header, transaction=txn)

        # Abort if a newer xia2-dials job has been triggered on a subsequent DCID
        # in any sample group, meaning a fresher multiplex will be triggered later.
        if not message.get("trigger_every_collection"):
            dcid = message.get("DCID")
            related_dcids = message.get("related_dcids", [])
            if (
                dcid
                and related_dcids
                and all(
                    max(el.get("dcids", [0]), default=0) > dcid for el in related_dcids
                )
            ):
                added_dcids = [
                    d for el in related_dcids for d in el.get("dcids", []) if d > dcid
                ]
                if added_dcids:
                    min_start_time = datetime.now() - timedelta(hours=12)
                    with self._ispyb_sessionmaker() as session:
                        triggered_job = (
                            session.query(
                                AutoProcProgram, ProcessingJob.dataCollectionId
                            )
                            .join(
                                ProcessingJob,
                                ProcessingJob.processingJobId
                                == AutoProcProgram.processingJobId,
                            )
                            .filter(ProcessingJob.dataCollectionId.in_(added_dcids))
                            .filter(ProcessingJob.automatic == True)  # noqa: E712
                            .filter(AutoProcProgram.processingPrograms == "xia2 dials")
                            .filter(AutoProcProgram.recordTimeStamp > min_start_time)
                            .first()
                        )
                    if triggered_job:
                        self.log.info(
                            f"Aborting multiplex trigger for dcid={dcid} as xia2-dials "
                            f"has been triggered for dcid={triggered_job.dataCollectionId}"
                        )
                        rw.transport.transaction_commit(txn)
                        return

        dcids = [sweep["DCID"] for sweep in message.get("sweeps", [])]

        status = message.get("trigger-status", {"ntry": 0})
        message_delay = int(
            self._backoff_delay * self._backoff_multiplier ** status["ntry"]
        )
        status["ntry"] += 1

        if dcids and status["ntry"] <= self._backoff_max_try:
            min_start_time = datetime.now() - timedelta(hours=24)
            with self._ispyb_sessionmaker() as session:
                waiting_jobs = (
                    session.query(AutoProcProgram)
                    .join(
                        ProcessingJob,
                        ProcessingJob.processingJobId
                        == AutoProcProgram.processingJobId,
                    )
                    .filter(ProcessingJob.dataCollectionId.in_(dcids))
                    .filter(ProcessingJob.automatic == True)  # noqa: E712
                    .filter(AutoProcProgram.processingPrograms == "xia2 dials")
                    .filter(ProcessingJob.recordTimestamp > min_start_time)
                    .filter(
                        or_(
                            AutoProcProgram.processingStatus == None,  # noqa: E711
                            AutoProcProgram.processingStartTime == None,  # noqa: E711
                        )
                    )
                    .all()
                )

            if waiting_jobs:
                self.log.info(
                    f"Waiting on {len(waiting_jobs)} xia2 dials job(s) for dcids={dcids}"
                )
                message["trigger-status"] = status
                rw.checkpoint(message, delay=message_delay, transaction=txn)
                rw.transport.transaction_commit(txn)
                return
        else:
            if status["ntry"] > self._backoff_max_try:
                self.log.info(
                    f"Max retries exceeded for dcids={dcids}, forwarding to ISPyB"
                )

        # Look up .expt/.refl attachment files for each sweep from xia2-dials
        # AutoProcProgramAttachment records and add as data parameters, following
        # the same pattern as trigger_multiplex in services/trigger.py.
        if dcids:
            with self._ispyb_sessionmaker() as session:
                rows = (
                    session.query(
                        DataCollection.dataCollectionId,
                        AutoProcProgramAttachment.filePath,
                        AutoProcProgramAttachment.fileName,
                    )
                    .select_from(DataCollection)
                    .join(
                        AutoProcIntegration,
                        AutoProcIntegration.dataCollectionId
                        == DataCollection.dataCollectionId,
                    )
                    .join(
                        AutoProcProgram,
                        AutoProcProgram.autoProcProgramId
                        == AutoProcIntegration.autoProcProgramId,
                    )
                    .join(
                        ProcessingJob,
                        ProcessingJob.processingJobId
                        == AutoProcProgram.processingJobId,
                    )
                    .join(AutoProcProgram.AutoProcProgramAttachments)
                    .filter(DataCollection.dataCollectionId.in_(dcids))
                    .filter(ProcessingJob.automatic == True)  # noqa: E712
                    .filter(AutoProcProgram.processingPrograms == "xia2 dials")
                    .filter(AutoProcProgram.processingStatus == 1)
                    .filter(
                        (
                            AutoProcProgramAttachment.fileName.endswith(".expt")
                            | AutoProcProgramAttachment.fileName.endswith(".refl")
                        )
                        & ~AutoProcProgramAttachment.fileName.contains("_scaled.")
                    )
                    .all()
                )

            sweep_attachments: dict[int, list[str]] = {}
            for sweep_dcid, file_path, file_name in rows:
                sweep_attachments.setdefault(sweep_dcid, []).append(
                    str(pathlib.Path(file_path) / file_name)
                )

            for sweep_dcid in dcids:
                files = sweep_attachments.get(sweep_dcid, [])
                if len(files) == 2:
                    message["parameters"].append(
                        {"key": "data", "value": ";".join(files)}
                    )
                elif files:
                    self.log.warning(
                        f"Expected 2 attachment files for DCID {sweep_dcid}, "
                        f"found {len(files)}: {files}"
                    )

        # All jobs done (or max retries exceeded) — forward to ISPyB
        message.pop("trigger-status", None)
        rw.send_to("ispyb", message, transaction=txn)
        rw.transport.transaction_commit(txn)
