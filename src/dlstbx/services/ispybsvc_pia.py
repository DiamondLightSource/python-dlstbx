import ispyb
import sqlalchemy
import workflows.recipe

from dlstbx.services.ispybsvc import DLSISPyB


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
        This function is copied from ispybsvc.py with a changed queue name.
        """
        self.log.info("ISPyB PIA inserter using ispyb v%s", ispyb.__version__)
        self.ispyb = ispyb.open()
        self._ispyb_sessionmaker = sqlalchemy.orm.sessionmaker(
            bind=sqlalchemy.create_engine(
                ispyb.sqlalchemy.url(), connect_args={"use_pure": True}
            )
        )
        self.log.debug("ISPyB PIA inserter starting")
        workflows.recipe.wrap_subscribe(
            self._transport,
            "ispyb_pia",
            self.receive_msg,
            acknowledgement=True,
            log_extender=self.extend_log,
            allow_non_recipe_messages=True,
        )
