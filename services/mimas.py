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

    @staticmethod
    def _extract_scenario(step):
        dcid = step.get("dcid")
        if not dcid or not dcid.isnumeric():
            return f"Invalid Mimas request rejected (DCID = {dcid!r})"

        event = step.get("event")
        if not isinstance(event, str):
            event = repr(event)
        if event.lower() == "start":
            event = dlstbx.mimas.MimasEvent.START
        elif event.lower() == "end":
            event = dlstbx.mimas.MimasEvent.END
        else:
            return f"Invalid Mimas request rejected (Event = {event})"

        dc_class = step.get("dc_class")
        if not dc_class or not isinstance(dc_class, dict):
            return f"Invalid Mimas request rejected (dc_class = {dc_class!r})"

        if dc_class["grid"]:
            if step.get("beamline") == "i02-2":
                default_recipes = ["archive-nexus", "vmxi-spot-counts-per-image"]
            else:
                default_recipes = ["per-image-analysis-gridscan"]
        elif dc_class["screen"]:
            default_recipes = [
                "per-image-analysis-rotation",
                "strategy-edna",
                "strategy-mosflm",
            ]
        elif not dc_class["rotation"]:
            # possibly EM dataset
            default_recipes = []
        else:
            if step.get("beamline") == "i02-2":
                default_recipes = [
                    "archive-nexus",
                    "processing-autoproc",
                    "processing-fast-dp",
                    "processing-xia2-3dii",
                    "processing-xia2-dials",
                    "vmxi-per-image-analysis",
                ]
            else:
                default_recipes = [
                    "per-image-analysis-rotation",
                    "processing-autoproc",
                    "processing-fast-dp",
                    "processing-rlv",
                    "processing-xia2-3dii",
                    "processing-xia2-dials",
                ]

            if step.get("ispyb_images"):
                default_recipes.append("processing-multi-xia2-dials")
                default_recipes.append("processing-multi-xia2-3dii")

        return dlstbx.mimas.MimasScenario(
            DCID=int(dcid),
            event=event,
            beamline=step.get("beamline"),
            runstatus=step.get("run_status"),
            spacegroup=step.get("space_group"),
            unitcell=step.get("unit_cell"),
            default_recipes=default_recipes,
            isitagridscan=step.get("isitagridscan"),
            getsweepslistfromsamedcg=step.get("getsweepslistfromsamedcg"),
            # step.get(dc_class)
            # step.get(preferred_processing)
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
        rw.set_default_channel("output")  # ? tbc

        self.log.debug("Evaluating %r", scenario)
        things_to_do = dlstbx.mimas.core.run(scenario)
        for ttd in things_to_do:
            self.log.info("Would run: %r", ttd)

        rw.transport.ack(header, transaction=txn)
        rw.transport.transaction_commit(txn)
