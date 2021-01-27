import logging
import sys
import time
from optparse import SUPPRESS_HELP, OptionParser

from dlstbx.util.colorstreamhandler import ColorStreamHandler
from workflows.transport.stomp_transport import StompTransport


class DLSLog:
    """Listens on ActiveMQ for log messages."""

    last_info = None
    last_info_messages = 0

    def __init__(self, transport):
        """Create a log viewer."""
        self._transport = transport

    def initializing(self):
        """Disable all irrelevant logging for this service.
        Then subscribe to log messages."""
        self._transport.subscribe_broadcast("transient.log", self.read_log_message)

        if hasattr(ColorStreamHandler, "_get_color"):

            def setbold():
                sys.stdout.write(ColorStreamHandler.BOLD)

            def setcolor(level):
                sys.stdout.write(getattr(ColorStreamHandler, "_get_color")(level))

            def resetcolor():
                sys.stdout.write(ColorStreamHandler.DEFAULT)

            self.setbold = setbold
            self.setcolor = setcolor
            self.resetcolor = resetcolor
        else:
            self.setbold = lambda: None
            self.setcolor = lambda x: None
            self.resetcolor = lambda: None

    def read_log_message(self, header, message):
        """Process a log message"""
        if not isinstance(message, dict) or "message" not in message:
            self.setcolor(logging.ERROR)
            print("=" * 80)
            print("Unknown message:")
            print(message)
            print("=" * 80)
            self.resetcolor()
        else:
            if message["name"] == "dlstbx.services.cluster.stats":
                return  # Filter cluster statistics messages
            message["service_description"] = message.get("workflows_service", "")
            if "workflows_statustext" in message:
                message[
                    "service_description"
                ] = " ({workflows_service}:{workflows_statustext})".format(**message)
            message["workflows_host"] = message.get("workflows_host", "???")
            if (
                self.last_info
                != [
                    message.get(x)
                    for x in ("workflows_host", "workflows_service", "workflows_status")
                ]
                or self.last_info_messages > 20
            ):
                self.last_info = [
                    message.get(x)
                    for x in ("workflows_host", "workflows_service", "workflows_status")
                ]
                self.last_info_messages = 0
                self.setbold()
                print(
                    "====== {workflows_host}{service_description} ======".format(
                        **message
                    )
                )
                self.resetcolor()
            self.last_info_messages += 1
            self.setcolor(message.get("levelno", 0))
            if message.get("exc_text"):
                print("{name}: {msg}{service_description}".format(**message))
                print(message.get("exc_text"))
            else:
                if message["levelno"] >= logging.WARN:
                    print("{pathname}:{lineno}{service_description}".format(**message))
                print("{name}: {msg}".format(**message))
            self.resetcolor()


if __name__ == "__main__":
    parser = OptionParser(usage="dlstbx.log [options]")
    parser.add_option("-?", action="help", help=SUPPRESS_HELP)

    # override default stomp host
    parser.add_option(
        "--test",
        action="store_true",
        dest="test",
        help="Run in ActiveMQ testing (zocdev) namespace",
    )
    default_configuration = "/dls_sw/apps/zocalo/secrets/credentials-live.cfg"
    if "--test" in sys.argv:
        default_configuration = "/dls_sw/apps/zocalo/secrets/credentials-testing.cfg"
    StompTransport.load_configuration_file(default_configuration)
    StompTransport.add_command_line_options(parser)
    (options, args) = parser.parse_args(sys.argv[1:])

    stomp = StompTransport()
    stomp.connect()
    logviewer = DLSLog(stomp)
    logviewer.initializing()

    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        logviewer.resetcolor()
        print()
