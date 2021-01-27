#
# dlstbx.gda_write
#   Write status messages to GDA
#


import os
import random
import sys
import time
from optparse import SUPPRESS_HELP, OptionParser

from workflows.transport.stomp_transport import StompTransport

if __name__ == "__main__":
    parser = OptionParser(usage="dlstbx.gda_write [options]")

    parser.add_option("-?", action="help", help=SUPPRESS_HELP)
    parser.add_option(
        "-b", "--beamlime", dest="beamline", default=None, help="Name of beamline"
    )
    parser.add_option("-v", "--visit", dest="visit", default=None, help="Name of visit")
    parser.add_option(
        "-a",
        "--application",
        dest="application",
        default=None,
        help="Name of application",
    )
    parser.add_option("-u", "--url", dest="URL", default=None, help="URL field")
    parser.add_option(
        "-s", "--summary", dest="summary", default=None, help="Summary field"
    )
    parser.add_option(
        "-c",
        "--count",
        dest="count",
        type="int",
        default=None,
        help="Number of messages",
    )
    parser.add_option(
        "-i",
        "--interval",
        dest="interval",
        default=0.2,
        help="Number seconds between messages",
    )

    # override default stomp host
    StompTransport.load_configuration_file(
        "/dls_sw/apps/zocalo/secrets/credentials-testing.cfg"
    )

    StompTransport.add_command_line_options(parser)
    (options, args) = parser.parse_args(sys.argv[1:])

    if not options.count:
        options.count = int((24 * 60 * 60) / options.interval)  # run for 1 day

    if options.summary:
        summaries = [options.summary]
    else:
        summaries = []
        for a in (
            "Grid scan",
            "Screening",
            "Fast_EP",
            "Fast_DP",
            "Dimple",
            "xia2",
            "multi-xia2",
            "xia2 3d",
            "xia2 3dii",
        ):
            summaries.append("%s processing completed successfully" % a)
            summaries.append("%s processing failed" % a)

    if options.URL:
        urls = [options.URL]
    else:
        urls = [
            None,
            "https://google.com",
            "http://stackoverflow.com",
            "https://dials.github.io",
        ]

    if options.beamline:
        beamline = lambda: options.beamline
    else:
        beamline = lambda: random.choice(
            ["i03", "i03-1", "i04", "i19-1", "i19-2", "i23", "i24"]
        )

    if options.visit:
        visit = lambda: options.visit
    else:

        def visit():
            return "%s%05d-%d" % (
                random.choice(["cm", "mt", "sw", "sp", "mx", "in", "nr", "nt"]),
                random.choice([10000, 12345, 13631, 19973]),
                int(random.expovariate(1.0 / 7) + 1),
            )

    if options.application:
        application = lambda: options.application
    else:
        application = lambda: random.choice(["fast_dp", "fast_ep", "xia2", "screen19"])

    message_text = None
    if not os.isatty(sys.stdin.fileno()):
        message_text = sys.stdin.read().strip()
        if not message_text:
            message_text = None

    def message_gen():
        if message_text:
            return message_text
        if random.random() < 0.7:
            return None
        return """                       \\/
                    __.---;_
                  .'  './'0)',\\
                  |o)  |     | ';
                  :'--; \\.__/'   ;
                   ;.' (         |
              __.-'   _.)        |
        ---=="=----'''           |
                 ;^;         .  ^+^^;
               ;^  :         :       ^;
                \\  {          :_     /
                 ^'-;          :'--'^
                    ",,____,,-'

                 __   _______   ______
       ============(((=======(((============dlK
"""

    stomp = StompTransport()
    stomp.connect()
    for msgno in range(1, options.count + 1):
        if msgno > 1:
            time.sleep(options.interval)

        message = {
            "summary": random.choice(summaries),
            "URL": random.choice(urls),
            "text": message_gen(),
        }
        destination = "transient.report.%s.%s.status.%s" % (
            beamline(),
            visit(),
            application(),
        )

        print("Sending message %d: %s" % (msgno, message))
        print("to %s\n" % destination)

        stomp.broadcast(destination, message)

    print("\nDone.")
