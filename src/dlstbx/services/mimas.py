from __future__ import annotations

import time
from pprint import pformat
from typing import Any

import workflows.recipe
from workflows.services.common_service import CommonService

from dlstbx import mimas
from dlstbx.mimas import specification


class DLSMimas(CommonService):
    """
    Business logic component. Given a data collection ID and some description
    of event circumstances (beamline, visit, experiment description, start or end of
    scan) this service decides what recipes should be run with what settings.
    """

    # Human readable service name
    _service_name = "DLS Mimas"

    # Logger name
    _logger_name = "dlstbx.services.mimas"

    def initializing(self):
        """Subscribe to the mimas queue. Received messages must be acknowledged."""
        self.log.info("Mimas starting")

        self.cluster_stats = {
            "live": {
                "jobs_waiting": 60,
                "last_cluster_update": time.time(),
            },
            "iris": {
                "jobs_waiting": 60,
                "last_cluster_update": time.time(),
            },
            "s3echo": {"total": 0.0, "last_cluster_update": time.time()},
        }

        workflows.recipe.wrap_subscribe(
            self._transport,
            "mimas",
            self.process,
            acknowledgement=True,
            log_extender=self.extend_log,
        )

        # Subscribe to the transient.statistics.cluster topic, which we will
        # examine to determine the number of waiting jobs
        self._transport.subscribe_broadcast(
            "transient.statistics.cluster",
            self.on_statistics_cluster,
        )

    def _extract_scenario(self, step):
        dcid = step.get("dcid")
        if not dcid or not dcid.isnumeric():
            return f"Invalid Mimas request rejected (DCID = {dcid!r})"

        event = step.get("event")
        if not isinstance(event, str):
            event = repr(event)
        try:
            event = mimas.MimasEvent[event.upper()]
        except KeyError:
            return f"Invalid Mimas request rejected (Event = {event})"

        # TODO: push the default recipe determination logic into mimas.core,
        #       and pass dc_class instead.
        dc_class = step.get("dc_class")
        if isinstance(dc_class, dict):
            # legacy format
            if dc_class["serial_fixed"]:
                dc_class_mimas = mimas.MimasDCClass.SERIAL_FIXED
            elif dc_class["serial_jet"]:
                dc_class_mimas = mimas.MimasDCClass.SERIAL_JET
            elif dc_class["grid"]:
                dc_class_mimas = mimas.MimasDCClass.GRIDSCAN
            elif dc_class["screen"]:
                dc_class_mimas = mimas.MimasDCClass.SCREENING
            elif dc_class["diamond_anvil_cell"]:
                dc_class_mimas = mimas.MimasDCClass.DIAMOND_ANVIL_CELL
            elif dc_class["rotation"]:
                dc_class_mimas = mimas.MimasDCClass.ROTATION
            else:
                dc_class_mimas = mimas.MimasDCClass.UNDEFINED
        else:
            try:
                dc_class_mimas = mimas.MimasDCClass[dc_class.upper()]
            except KeyError:
                self.log.warning(
                    f"Invalid Mimas request (Data collection class = {dc_class!r})"
                )
                dc_class_mimas = mimas.MimasDCClass.UNDEFINED

        sweep_list = tuple(
            mimas.MimasISPyBSweep(*info) for info in (step.get("sweep_list") or [])
        )

        cell = step.get("unit_cell")
        if cell:
            try:
                cell = mimas.MimasISPyBUnitCell(*cell)
                mimas.validate(cell)
            except Exception:
                self.log.warning(
                    f"Invalid unit cell for dcid {dcid}: {cell}", exc_info=True
                )
                cell = None
        else:
            cell = None

        spacegroup = step.get("space_group")
        if spacegroup:
            spacegroup = mimas.MimasISPyBSpaceGroup(spacegroup)
            self.log.info(spacegroup)
            try:
                mimas.validate(spacegroup)
            except ValueError:
                self.log.warning(
                    f"Invalid spacegroup for dcid {dcid}: {spacegroup}", exc_info=True
                )
                spacegroup = None
        else:
            spacegroup = None

        anomalous_scatterer = None
        diffraction_plan_info = step.get("diffraction_plan_info")
        if diffraction_plan_info:
            anomalous_scatterer = diffraction_plan_info.get("anomalousScatterer")
            if anomalous_scatterer:
                anomalous_scatterer = mimas.MimasISPyBAnomalousScatterer(
                    anomalous_scatterer
                )
                self.log.info(f"anomalous_scatterer: {anomalous_scatterer}")
                try:
                    mimas.validate(anomalous_scatterer)
                except ValueError:
                    self.log.warning(
                        f"Invalid anomalous scatterer for dcid {dcid}: {anomalous_scatterer}",
                        exc_info=True,
                    )
                    anomalous_scatterer = None

        detectorclass = {
            "eiger": mimas.MimasDetectorClass.EIGER,
            "pilatus": mimas.MimasDetectorClass.PILATUS,
        }.get(step.get("detectorclass", "").lower())

        return mimas.MimasScenario(
            DCID=int(dcid),
            dcclass=dc_class_mimas,
            event=event,
            beamline=step.get("beamline"),
            visit=step.get("visit"),
            runstatus=step.get("run_status"),
            spacegroup=spacegroup,
            unitcell=cell,
            getsweepslistfromsamedcg=sweep_list,
            preferred_processing=step.get("preferred_processing"),
            detectorclass=detectorclass,
            anomalous_scatterer=anomalous_scatterer,
            cloudbursting=self.get_cloudbursting_spec(),
        )

    def on_statistics_cluster(self, header, message):
        """
        Examine the message to determine number of waiting jobs on
        DLS Science "live" cluster in high.q and medium.q, on STFC/IRIS cluster and
        storage utilisation for dls-mx user on S3 Echo object store.
        """
        try:
            sc = message["statistic-cluster"]
        except KeyError:
            return
        if sc in ("live", "iris", "s3echo"):
            self.log.debug(f"Received cluster stat message: {pformat(message)}")
            self.cluster_stats[sc]["last_cluster_update"] = time.time()
            if message["statistic"] == "waiting-jobs-per-queue":
                self.cluster_stats[sc]["jobs_waiting"] = (
                    message["high.q"] + message["medium.q"]
                )
                self.log.debug(
                    f"Jobs waiting on {sc} cluster: {self.cluster_stats[sc]['jobs_waiting']}\n",
                )
            elif message["statistic"] == "job-status":
                self.cluster_stats[sc]["jobs_waiting"] = message["waiting"]
                self.log.debug(
                    f"Jobs waiting on {sc} cluster: {self.cluster_stats[sc]['jobs_waiting']}\n",
                )
            elif message["statistic"] == "used-storage":
                self.cluster_stats[sc]["total"] = message["total"]
                self.log.debug(
                    f"Total used storage on {sc}: {self.cluster_stats[sc]['total']}\n",
                )

    def get_cloudbursting_spec(
        self,
    ) -> list[dict[str, Any]]:
        """
        Activate cloudbursting if number of waiting jobs on DLS cluster exceeded
        the predefined threshold or DLS cluster stats update timed out. Check
        that queue of jobs on STFC/IRIS and S3 Echo storage utilisation are
        below threshold and statistics updates haven't timed out.
        """
        cloud_spec_list: list[dict[str, Any]] = []
        try:
            max_jobs_waiting = self.config.storage.get(
                "max_jobs_waiting", {"live": 60, "iris": 500}
            )
            timeout = self.config.storage.get("timeout", 300)
            s3echo_quota = 0.95 * self.config.storage.get("s3echo_quota", 100)
            timeout_threshold = time.time() - timeout
            self.log.debug(f"Live cluster stats: {self.cluster_stats['live']}")
            self.log.debug(f"IRIS cluster stats: {self.cluster_stats['iris']}")
            self.log.debug(f"S3Echo stats: {self.cluster_stats['s3echo']}")
            self.log.debug(
                "Cloudbursting threshold values\n"
                f"  max_jobs_waiting: {max_jobs_waiting}\n"
                f"  s3echo_quota: {s3echo_quota}"
                f"  timeout_threshold: {timeout_threshold}"
            )
            # Check if global cloudbursting flag is False or if S3 Echo is full
            if (not self.config.storage.get("cloudbursting", False)) or (
                self.cluster_stats["s3echo"]["total"] > s3echo_quota
            ):
                self.log.debug(
                    "Cloudbursting disabled:\n"
                    f"  is_cludbursting {self.config.storage.get('cloudbursting', False)}\n"
                    f"  s3echo_storage: {self.cluster_stats['s3echo']['total']}"
                )
                return cloud_spec_list
            # Create cloud specification entry for each element in zocalo.mimas.cloud
            # Add specification to the list if science cluster if oversubscribed
            # and cluster statistics are up-to-date
            for group in self.config.storage.get("zocalo.mimas.cloud", []):
                if not group.get("cloudbursting", True):
                    continue
                cloud_spec = specification.VisitSpecification(
                    set(group.get("visit_pattern", []))
                ) & specification.BeamlineSpecification(
                    beamlines=set(group.get("beamlines", []))
                )
                group_max_jobs_waiting = group.get("max_jobs_waiting", max_jobs_waiting)
                if (
                    (
                        (
                            self.cluster_stats["live"]["jobs_waiting"]
                            > group_max_jobs_waiting["live"]
                        )
                        or (
                            self.cluster_stats["live"]["last_cluster_update"]
                            < timeout_threshold
                        )
                    )
                    and (
                        self.cluster_stats["iris"]["jobs_waiting"]
                        < group_max_jobs_waiting["iris"]
                    )
                    and (
                        self.cluster_stats["iris"]["last_cluster_update"]
                        > timeout_threshold
                    )
                    and (
                        self.cluster_stats["s3echo"]["last_cluster_update"]
                        > timeout_threshold
                    )
                ):
                    cloud_spec_list.append(
                        {
                            "cloud_spec": cloud_spec,
                            "recipes": group.get("recipes", ["autoprocessing"]),
                        }
                    )
        except AttributeError:
            self.log.exception("Error reading cluster statistics")
        return cloud_spec_list

    def process(self, rw, header, message):
        """Process an incoming event."""

        # Pass incoming event information into Mimas scenario object
        scenario = self._extract_scenario(rw.recipe_step["parameters"])
        if isinstance(scenario, str):
            self.log.error(scenario)
            rw.transport.nack(header)
            return

        # Validate scenario
        try:
            mimas.validate(scenario, expectedtype=mimas.MimasScenario)
        except ValueError:
            self.log.error("Invalid Mimas request rejected", exc_info=True)
            rw.transport.nack(header)
            return

        txn = rw.transport.transaction_begin(subscription_id=header["subscription"])
        rw.set_default_channel("dispatcher")

        self.log.debug("Evaluating %r", scenario)
        things_to_do = mimas.handle_scenario(scenario, self.config)

        for ttd in things_to_do:
            try:
                mimas.validate(
                    ttd,
                    expectedtype=(
                        mimas.MimasRecipeInvocation,
                        mimas.MimasISPyBJobInvocation,
                    ),
                )
            except ValueError:
                self.log.error("Invalid Mimas response detected", exc_info=True)
                rw.transport.nack(header)
                rw.transport.transaction_abort(txn)
                return

            self.log.info("Running: %r", ttd)
            try:
                ttd_zocalo = mimas.zocalo_message(ttd)
            except ValueError:
                self.log.error(f"Error zocalizing Mimas object {ttd!r}", exc_info=True)
                rw.transport.nack(header)
                rw.transport.transaction_abort(txn)
                return

            if isinstance(ttd, mimas.MimasRecipeInvocation):
                # Pass through specific parameters from the mimas invocation
                # This is somewhat a fudge for I03 GPU until we work out a better way
                passthrough_params = {
                    "filename",
                    "message_index",
                    "number_of_frames",
                    "start_frame_index",
                }
                for key in passthrough_params:
                    if key in rw.recipe_step["parameters"]:
                        ttd_zocalo["parameters"][key] = rw.recipe_step["parameters"][
                            key
                        ]

                rw.send(ttd_zocalo, transaction=txn)
            else:
                rw.send_to("ispyb", ttd_zocalo, transaction=txn)

        rw.transport.ack(header, transaction=txn)
        rw.transport.transaction_commit(txn)
