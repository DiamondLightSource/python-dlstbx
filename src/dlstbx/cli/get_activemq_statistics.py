from __future__ import annotations

import getpass
import logging
import sys
import time
from optparse import SUPPRESS_HELP, OptionParser

import dlstbx.util.activemqapi
import dlstbx.util.jmxstats
from dlstbx.util.colorstreamhandler import ColorStreamHandler
from dlstbx.util.rrdtool import RRDTool


class ActiveMQRRD:
    def __init__(self, path=".", api=None):
        self.rrd = RRDTool(path)
        self.setup_rrd()
        self.api_activemq = api
        self.log = logging.getLogger("dlstbx.command_line.activemq_stats")

    def setup_rrd(self):
        daydata = ["RRA:%s:0.5:1:1440" % cls for cls in ("AVERAGE", "MAX")]
        weekdata = ["RRA:%s:0.5:3:3360" % cls for cls in ("AVERAGE", "MAX")]
        monthdata = ["RRA:%s:0.5:6:7440" % cls for cls in ("AVERAGE", "MAX")]
        yeardata = ["RRA:%s:0.5:15:35040" % cls for cls in ("AVERAGE", "MAX")]
        self.rrd_activemq = self.rrd.create(
            "activemq-statistics.rrd",
            ["--step", "60"]
            + [
                "DS:storageused:GAUGE:180:0:U",
                "DS:tempused:GAUGE:180:0:U",
                "DS:memoryused:GAUGE:180:0:U",
                "DS:connections:GAUGE:180:0:U",
                "DS:mimasheld:GAUGE:180:0:U",
            ]
            + daydata
            + weekdata
            + monthdata
            + yeardata,
        )
        self.rrd_amqmemory = self.rrd.create(
            "activemq-memory.rrd",
            ["--step", "60"]
            + [
                "DS:heapinitial:GAUGE:180:0:U",
                "DS:heapused:GAUGE:180:0:U",
                "DS:heapcommitted:GAUGE:180:0:U",
                "DS:heapmaximum:GAUGE:180:0:U",
                "DS:nonheapinitial:GAUGE:180:0:U",
                "DS:nonheapused:GAUGE:180:0:U",
                "DS:nonheapcommitted:GAUGE:180:0:U",
            ]
            + daydata
            + weekdata
            + monthdata
            + yeardata,
        )

    def update(self):
        update_time = int(time.time())
        self.log.info("Last known data point:    %d", self.rrd_activemq.last_update)
        self.log.info("Current time:             %d", update_time)
        if update_time - self.rrd_activemq.last_update <= 30:
            self.log.info("No update required.")
            return
        if not self.api_activemq:
            self.log.warn("ActiveMQ API not available.")
            return

        self.rrd_activemq.update(
            [
                [
                    update_time,
                    self.api_activemq.getStorePercentUsage(),
                    self.api_activemq.getTempPercentUsage(),
                    self.api_activemq.getMemoryPercentUsage(),
                    self.api_activemq.getConnectionsCount(),
                    self.api_activemq.getMimasHeldQueueSize(),
                ]
            ]
        )
        self.rrd_amqmemory.update(
            [
                [
                    update_time,
                    self.api_activemq.getHeapMemoryInitial(),
                    self.api_activemq.getHeapMemoryUsed(),
                    self.api_activemq.getHeapMemoryCommitted(),
                    self.api_activemq.getHeapMemoryMaximum(),
                    self.api_activemq.getNonHeapMemoryInitial(),
                    self.api_activemq.getNonHeapMemoryUsed(),
                    self.api_activemq.getNonHeapMemoryCommitted(),
                ]
            ]
        )
        self.log.info("Updated to:               %d", self.rrd_activemq.last_update)


def _setup_logging(level):
    console = ColorStreamHandler()
    console.setLevel(level)
    logger = logging.getLogger()
    logger.setLevel(logging.WARN)
    logger.addHandler(console)
    logging.getLogger("dlstbx").setLevel(level)


def run():
    parser = OptionParser(
        usage="dlstbx.get_activemq_statistics [options]",
        description="Collects statistics from an ActiveMQ server",
    )

    parser.add_option("-?", action="help", help=SUPPRESS_HELP)
    parser.add_option(
        "--rrd",
        action="store_true",
        default=False,
        help="Collect all information and store it in an RRD file for aggregation",
    )
    parser.add_option(
        "--test",
        action="store_true",
        dest="test",
        help="Connect to personal development ActiveMQ server",
    )
    parser.add_option(
        "-r",
        "--read",
        dest="keys",
        action="append",
        default=[],
        metavar="KEY",
        help="Read named value from ActiveMQ server",
    )
    (options, args) = parser.parse_args(sys.argv[1:])

    _setup_logging(logging.INFO)
    amq = dlstbx.util.activemqapi.ActiveMQAPI()

    if options.test:
        amq.connect(
            "/dls/tmp/%s/zocdev-activemq/latest-credentials" % getpass.getuser()
        )
    else:
        amq.connect()

    if options.rrd:
        ActiveMQRRD(api=amq).update()

    if options.keys:
        available_keys = {k[3:].lower(): k for k in dir(amq) if k.startswith("get")}
        for name in options.keys:
            fn = available_keys.get(name.lower())
            if fn:
                value = getattr(amq, fn)()
                print(f"{name}:{value}")

    if not options.rrd and not options.keys:

        def readable_memory(value):
            return f"{value / 1024 / 1024:.1f} MB"

        connections = amq.getConnectionsCount()
        queues = amq.getQueueCount()
        memory = amq.getMemoryPercentUsage()
        store = amq.getStorePercentUsage()
        temp = amq.getTempPercentUsage()
        heapused = amq.getHeapMemoryUsed()
        heapmax = amq.getHeapMemoryMaximum()
        nonheapused = amq.getNonHeapMemoryUsed()
        mimasheldqueuesize = amq.getMimasHeldQueueSize()

        def colour(value, warnlevel, errlevel):
            if not sys.stdout.isatty():
                return ""
            if value < warnlevel:
                return ColorStreamHandler.GREEN
            elif value < errlevel:
                return ColorStreamHandler.YELLOW + ColorStreamHandler.BOLD
            else:
                return ColorStreamHandler.RED + ColorStreamHandler.BOLD

        def colourreset():
            if not sys.stdout.isatty():
                return ""
            return ColorStreamHandler.DEFAULT

        print(
            """
ActiveMQ connections: {colourconn}{connections}{reset}   queues: {colourqueue}{queues}{reset}

Storage statistics:
   persistent:{colourstore}{store:>3} %{reset}
   temporary :{colourtemp}{temp:>3} %{reset}
   memory    :{colourmemory}{memory:>3} %{reset}

Virtual machine memory statistics:
   heap: using {colourheap}{heapused}{reset} of {heapmax}
   used memory outside of heap: {colournonheap}{nonheapused}{reset}

Mimas held queue size: {colourmimasheld}{mimasheldqueuesize}{reset}
""".format(
                connections=connections,
                queues=queues,
                store=store,
                temp=temp,
                memory=memory,
                heapused=readable_memory(heapused),
                heapmax=readable_memory(heapmax),
                nonheapused=readable_memory(nonheapused),
                reset=colourreset(),
                mimasheldqueuesize=mimasheldqueuesize,
                colourconn=colour(connections, 400, 600),
                colourqueue=colour(queues, 200, 400),
                colourstore=colour(store, 10, 30),
                colourtemp=colour(temp, 10, 30),
                colourmemory=colour(memory, 10, 30),
                colourheap=colour(heapused / heapmax, 0.85, 0.95),
                colournonheap=colour(nonheapused, 125 * 1024 * 1024, 200 * 1024 * 1024),
                colourmimasheld=colour(mimasheldqueuesize, 50, 250),
            )
        )
