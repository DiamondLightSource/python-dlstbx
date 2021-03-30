from dlstbx.services.ispybsvc import DLSISPyB
import ispyb
import workflows.recipe


class DLSISPyBPIA(DLSISPyB):
    """
    A temporary service that receives PIA results to be written to ISPyB.
    cf. https://jira.diamond.ac.uk/browse/SCI-7846
    """

    # Human readable service name
    _service_name = "DLS ISPyB PIA inserter"

    # Logger name
    _logger_name = "dlstbx.services.ispybpia"

    def initializing(self):
        """
        Subscribe to the temporary ISPyB PIA queue. Received messages must be
        acknowledged. Prepare ISPyB database connection.
        """

        self.log.info("ISPyB PIA inserter using ispyb v%s", ispyb.__version__)
        self.ispyb = ispyb.open("/dls_sw/apps/zocalo/secrets/credentials-ispyb-sp.cfg")
        self.log.debug("ISPyB PIA inserter starting")
        workflows.recipe.wrap_subscribe(
            self._transport,
            "ispyb_pia",
            self.receive_msg,
            acknowledgement=True,
            log_extender=self.extend_log,
            allow_non_recipe_messages=True,
        )
