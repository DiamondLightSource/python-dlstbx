import dlstbx.mimas
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

    def process(self, rw, header, message):
        """Process an incoming event."""

        # Pass incoming event information into Mimas scenario object
        step = rw.recipe_step["parameters"]
        scenario = dlstbx.mimas.MimasScenario(
            DCID=step.get("DCID"),
            event=step.get("event"),
            beamline=step.get("beamline"),
            runstatus=step.get("runstatus"),
            spacegroup=step.get("spacegroup"),
            unitcell=step.get("unitcell"),
            default_recipes=step.get("default_recipes"),
            isitagridscan=step.get("isitagridscan"),
            getsweepslistfromsamedcg=step.get("getsweepslistfromsamedcg"),
        )

        # Validate scenario
        try:
            dlstbx.mimas.validate(scenario)
        except ValueError:
            self.log.error("Invalid Mimas request rejected", exc_info=True)
            rw.transport.nack(header)
            return

        txn = rw.transport.transaction_begin()
        rw.set_default_channel("output")  # ? tbc

        self.log.debug("Doing this... doing that...")

        rw.transport.ack(header, transaction=txn)
        rw.transport.transaction_commit(txn)
