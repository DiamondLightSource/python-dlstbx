import json
import os
import sys
import time
from optparse import OptionParser, SUPPRESS_HELP

from workflows.transport.stomp_transport import StompTransport


def run():
    dropdir = "/dls_sw/apps/zocalo/dropfiles"
    parser = OptionParser(
        usage="dlstbx.pickup [options]", description="Processes dlstbx.go backlog"
    )

    parser.add_option("-?", action="help", help=SUPPRESS_HELP)
    parser.add_option(
        "-d",
        "--delay",
        dest="delay",
        action="store",
        type="int",
        default=2,
        help="Number of seconds to wait between message dispatches",
    )
    parser.add_option(
        "-w",
        "--wait",
        dest="wait",
        action="store",
        type="int",
        default=60,
        help="Number of seconds to wait initially",
    )
    parser.add_option(
        "-v",
        "--verbose",
        dest="verbose",
        action="store_true",
        default=False,
        help="Show raw message before sending",
    )

    default_configuration = "/dls_sw/apps/zocalo/secrets/credentials-live.cfg"
    StompTransport.load_configuration_file(default_configuration)
    StompTransport.add_command_line_options(parser)
    (options, args) = parser.parse_args(sys.argv[1:])

    try:
        files = os.listdir(dropdir)
    except OSError:
        sys.exit("This program is only available to privileged users")

    print("Found %d files" % len(files))
    if not files:
        sys.exit()

    if options.wait:
        print("Waiting %d seconds" % options.wait)
        time.sleep(options.wait)

    print("Connecting to stomp...")
    stomp = StompTransport()
    stomp.connect()

    file_info = {f: {"filename": os.path.join(dropdir, f)} for f in files}
    hosts = {}

    recipe_priorities = {
        "per-image-analysis-dozor": -30,
        "xray-centering": -10,
        "per-image-analysis-gridscan": -10,
        "per-image-analysis-rotation": -5,
        "archive-cbf": 3,
        "most-recent-for-host": 30,
    }

    for f in file_info.values():
        with open(f["filename"]) as fh:
            data = json.load(fh)
            f["message"] = data["message"]
            f["headers"] = data["headers"]
        f["originating-host"] = f["headers"].get("dlstbx.go.host")
        f["recipes"] = ",".join(f["message"].get("recipes", []))
        f["priority"] = sum(
            recipe_priorities.get(r, 0) for r in f["message"].get("recipes", [])
        )
        f["last-touch"] = os.path.getmtime(f["filename"])
        if hosts.get(f["originating-host"], {}).get("last-touch", 0) < f["last-touch"]:
            hosts[f["originating-host"]] = f

    for f in hosts.values():
        f["priority"] += recipe_priorities["most-recent-for-host"]

    count = 0
    file_count = len(file_info)
    for f in sorted(file_info, key=lambda f: file_info[f]["priority"], reverse=True):
        print(
            "Sending {f} from host {finfo[originating-host]} with recipes {finfo[recipes]}".format(
                f=f, finfo=file_info[f]
            )
        )
        assert os.path.exists(file_info[f]["filename"])
        stomp.send(
            "processing_recipe",
            file_info[f]["message"],
            headers=file_info[f]["headers"],
        )
        os.remove(file_info[f]["filename"])
        count = count + 1
        print(f"Done ({count} of {file_count})")
        try:
            time.sleep(options.delay)
        except KeyboardInterrupt:
            print("CTRL+C - stopping")
            time.sleep(0.5)
            sys.exit(1)
