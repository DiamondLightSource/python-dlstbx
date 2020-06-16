import dlstbx.mimas.core
import workflows.recipe
from workflows.services.common_service import CommonService


class DLSMimas(CommonService):
    """
    Business logic component. Given a data collection ID and some description
    of event circumstances (beamline, experiment description, start or end of
    scan) this service decides what recipes should be run with what settings.
    """

    # Human readable service name
    _service_name = "DLS Mimas"

    # Logger name
    _logger_name = "dlstbx.services.mimas"

    def initializing(self):
        """Subscribe to the mimas queue. Received messages must be acknowledged."""
        self.log.info("Mimas starting")

        workflows.recipe.wrap_subscribe(
            self._transport,
            "mimas",
            self.process,
            acknowledgement=True,
            log_extender=self.extend_log,
        )

    def _extract_scenario(self, step):
        dcid = step.get("dcid")
        if not dcid or not dcid.isnumeric():
            return f"Invalid Mimas request rejected (DCID = {dcid!r})"

        event = step.get("event")
        if not isinstance(event, str):
            event = repr(event)
        try:
            event = dlstbx.mimas.MimasEvent[event.upper()]
        except KeyError:
            return f"Invalid Mimas request rejected (Event = {event})"

        # TODO: push the default recipe determination logic into mimas.core,
        #       and pass dc_class instead.
        dc_class = step.get("dc_class")
        if isinstance(dc_class, dict):
            # legacy format
            if dc_class["grid"]:
                dc_class_mimas = dlstbx.mimas.MimasDCClass.GRIDSCAN
            elif dc_class["screen"]:
                dc_class_mimas = dlstbx.mimas.MimasDCClass.SCREENING
            elif dc_class["rotation"]:
                dc_class_mimas = dlstbx.mimas.MimasDCClass.ROTATION
            else:
                dc_class_mimas = dlstbx.mimas.MimasDCClass.UNDEFINED
        else:
            try:
                dc_class_mimas = dlstbx.mimas.MimasDCClass[dc_class.upper()]
            except KeyError:
                self.log.warning(
                    f"Invalid Mimas request (Data collection class = {dc_class!r})"
                )
                dc_class_mimas = dlstbx.mimas.MimasDCClass.UNDEFINED

        gridscan = step.get("gridscan")
        if isinstance(gridscan, bool):
            pass
        elif isinstance(gridscan, str):
            if gridscan.lower() == "true":
                gridscan = True
            elif gridscan.lower() == "false":
                gridscan = False
            else:
                return f"Invalid Mimas request rejected (gridscan = {gridscan})"
        else:
            return f"Invalid Mimas request rejected (gridscan = {gridscan!r})"
        sweep_list = tuple(
            dlstbx.mimas.MimasISPyBSweep(*info)
            for info in (step.get("sweep_list") or [])
        )

        cell = step.get("unit_cell")
        if cell:
            cell = dlstbx.mimas.MimasISPyBUnitCell(*cell)
        else:
            cell = None

        spacegroup = step.get("space_group")
        if spacegroup:
            spacegroup = dlstbx.mimas.MimasISPyBSpaceGroup(spacegroup)
        else:
            spacegroup = None

        detectorclass = {
            "eiger": dlstbx.mimas.MimasDetectorClass.EIGER,
            "pilatus": dlstbx.mimas.MimasDetectorClass.PILATUS,
        }.get(step.get("detectorclass", "").lower())

        return dlstbx.mimas.MimasScenario(
            DCID=int(dcid),
            dcclass=dc_class_mimas,
            event=event,
            beamline=step.get("beamline"),
            runstatus=step.get("run_status"),
            spacegroup=spacegroup,
            unitcell=cell,
            isitagridscan=gridscan,
            getsweepslistfromsamedcg=sweep_list,
            preferred_processing=step.get("preferred_processing"),
            detectorclass=detectorclass,
        )

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
            dlstbx.mimas.validate(scenario, expectedtype=dlstbx.mimas.MimasScenario)
        except ValueError:
            self.log.error("Invalid Mimas request rejected", exc_info=True)
            rw.transport.nack(header)
            return

        txn = rw.transport.transaction_begin()
        rw.set_default_channel("dispatcher")

        self.log.debug("Evaluating %r", scenario)
        things_to_do = dlstbx.mimas.core.run(scenario)

        for ttd in things_to_do:
            try:
                dlstbx.mimas.validate(
                    ttd,
                    expectedtype=(
                        dlstbx.mimas.MimasRecipeInvocation,
                        dlstbx.mimas.MimasISPyBJobInvocation,
                    ),
                )
            except ValueError:
                self.log.error("Invalid Mimas response detected", exc_info=True)
                rw.transport.nack(header)
                rw.transport.transaction_abort(txn)
                return

            self.log.info("Running: %r", ttd)
            try:
                ttd_zocalo = dlstbx.mimas.zocalo_message(ttd)
            except ValueError:
                self.log.error(f"Error zocalizing Mimas object {ttd!r}", exc_info=True)
                rw.transport.nack(header)
                rw.transport.transaction_abort(txn)
                return

            if isinstance(ttd, dlstbx.mimas.MimasRecipeInvocation):
                rw.send(ttd_zocalo, transaction=txn)
            else:
                rw.send_to("ispyb", ttd_zocalo, transaction=txn)

        rw.transport.ack(header, transaction=txn)
        rw.transport.transaction_commit(txn)
