#
# dlstbx.service
#   Starts a workflow service
#

# Note that the call semantics of dlstbx.service differs from other dlstbx
# commands. dlstbx.service defaults to running in the testing ActiveMQ
# namespace (zocdev), rather than the live namespace (zocalo). This is to
# stop servers started by developers on their machines accidentally interfering
# with live data processing.
# To run a live server you must specify '--live'


import logging
import os
import sys
import time

import workflows
import workflows.contrib.start_service
import workflows.logging

import dlstbx.util
from dlstbx import enable_graylog
from dlstbx.util.colorstreamhandler import ColorStreamHandler
from dlstbx.util.version import dlstbx_version


class DLSTBXServiceStarter(workflows.contrib.start_service.ServiceStarter):
    __frontendref = None
    use_live_infrastructure = False

    def setup_logging(self):
        """Initialize common logging framework.
        Everything is logged to the central graylog server.
        Depending on setting messages of DEBUG or INFO and higher go to console.
        """
        logger = logging.getLogger()
        logger.setLevel(logging.WARN)

        # Enable logging to console
        self.console = ColorStreamHandler()
        self.console.setLevel(logging.INFO)
        logger.addHandler(self.console)

        logging.getLogger("dials").setLevel(logging.INFO)
        logging.getLogger("dlstbx").setLevel(logging.DEBUG)
        logging.getLogger("dxtbx").setLevel(logging.INFO)
        logging.getLogger("ispyb").setLevel(logging.DEBUG)
        logging.getLogger("relion").setLevel(logging.INFO)
        logging.getLogger("workflows").setLevel(logging.INFO)
        logging.getLogger("xia2").setLevel(logging.INFO)
        logging.getLogger("zocalo").setLevel(logging.DEBUG)
        logging.getLogger("sqlalchemy.engine").setLevel(logging.DEBUG)

        self.log = logging.getLogger("dlstbx.service")
        self.log.setLevel(logging.DEBUG)

        # Enable logging to graylog
        enable_graylog(live=self.use_live_infrastructure)

    def __init__(self):
        # change settings when in live mode
        default_configuration = "/dls_sw/apps/zocalo/secrets/credentials-testing.cfg"
        if "--live" in sys.argv:
            self.use_live_infrastructure = True
            default_configuration = "/dls_sw/apps/zocalo/secrets/credentials-live.cfg"

        # initialize logging
        self.setup_logging()

        self.log.debug("Loading dlstbx credentials")

        # override default stomp host
        from workflows.transport.stomp_transport import StompTransport

        try:
            StompTransport.load_configuration_file(default_configuration)
        except workflows.Error as e:
            self.log.warning(e)

    def on_parser_preparation(self, parser):
        parser.add_option(
            "-v",
            "--verbose",
            dest="verbose",
            action="store_true",
            default=False,
            help="Show debug output",
        )
        parser.add_option(
            "--tag",
            dest="tag",
            metavar="TAG",
            default=None,
            help="Individual tag related to this service instance",
        )
        parser.add_option(
            "-d",
            "--debug",
            dest="debug",
            action="store_true",
            default=False,
            help="Set debug log level for workflows",
        )
        parser.add_option(
            "-r",
            "--restart",
            dest="service_restart",
            action="store_true",
            default=False,
            help="Restart service on failure",
        )
        parser.add_option(
            "--test",
            action="store_true",
            dest="test",
            help="Run in ActiveMQ testing namespace (zocdev, default)",
        )
        parser.add_option(
            "--live",
            action="store_true",
            dest="test",
            help="Run in ActiveMQ live namespace (zocalo)",
        )
        self.log.debug("Launching " + str(sys.argv))

    def on_parsing(self, options, args):
        if options.verbose:
            self.console.setLevel(logging.DEBUG)
            logging.getLogger("dials").setLevel(logging.DEBUG)
            logging.getLogger("dlstbx").setLevel(logging.DEBUG)
            logging.getLogger("xia2").setLevel(logging.DEBUG)
        if options.debug:
            logging.getLogger("workflows").setLevel(logging.DEBUG)
        self.options = options

    def before_frontend_construction(self, kwargs):
        kwargs["verbose_service"] = True
        kwargs["environment"] = kwargs.get("environment", {})
        kwargs["environment"]["live"] = self.use_live_infrastructure
        return kwargs

    def on_frontend_preparation(self, frontend):
        self.log.info("Attaching ActiveMQ logging to transport")

        def logging_call(record):
            if frontend._transport.is_connected():
                try:
                    record = record.__dict__["records"]
                except Exception:
                    record = record.__dict__
                frontend._transport.broadcast("transient.log", record)

        amq_handler = workflows.logging.CallbackHandler(logging_call)
        if not self.options.verbose:
            amq_handler.setLevel(logging.WARNING)
        logging.getLogger().addHandler(amq_handler)

        if self.options.service_restart:
            frontend.restart_service = True

        extended_status = {}
        if self.options.tag:
            extended_status["tag"] = self.options.tag
        for env in ("SGE_CELL", "JOB_ID"):
            if env in os.environ:
                extended_status["cluster_" + env] = os.environ[env]
        if os.getenv("KUBERNETES") == "1":
            split_name = os.environ["HOSTNAME"].split("-")
            container_image = ":".join(split_name[:2])
            extended_status["container_image"] = container_image

        extended_status["dlstbx"] = dlstbx_version()

        original_status_function = frontend.get_status

        def extend_status_wrapper():
            if getattr(extend_status_wrapper, "_uss_time", 0) < time.time() - 10:
                # Cache USS value for up to 10 seconds
                (extend_status_wrapper._uss_time, extend_status_wrapper._uss) = (
                    time.time(),
                    dlstbx.util.get_process_uss(),
                )
            status = original_status_function()
            status.update(extended_status)
            status["mem-uss"] = extend_status_wrapper._uss
            return status

        frontend.get_status = extend_status_wrapper


def run():
    DLSTBXServiceStarter().run(
        program_name="dlstbx.service",
        version=dlstbx_version(),
        transport_command_channel="command",
    )
