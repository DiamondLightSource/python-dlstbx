from __future__ import annotations

import getpass
import logging
import sys
import time
from optparse import SUPPRESS_HELP, OptionParser

import dlstbx.util.jmxstats
from dlstbx.util.colorstreamhandler import ColorStreamHandler
from dlstbx.util.rrdtool import RRDTool


class ActiveMQAPI:
    def connect(self, *args, **kwargs):
        self.jmx = dlstbx.util.jmxstats.JMXAPI(*args, **kwargs)

        # List of supported variables:
        # curl -XGET --user rrdapi:**password** http://cs04r-sc-vserv-69:80/api/jolokia/list | python -m json.tool

    def _query(self, **kwargs):
        result = self.jmx.org.apache.activemq(**kwargs)
        if result["status"] == 404:
            return 0
        if result["status"] != 200:
            raise RuntimeError(f"ActiveMQ responded with {result}")
        return result["value"]

    def getStorePercentUsage(self):
        return self._query(
            type="Broker", brokerName="localhost", attribute="StorePercentUsage"
        )

    def getTempPercentUsage(self):
        return self._query(
            type="Broker", brokerName="localhost", attribute="TempPercentUsage"
        )

    def getMemoryPercentUsage(self):
        return self._query(
            type="Broker", brokerName="localhost", attribute="MemoryPercentUsage"
        )

    def getConnectionsCount(self):
        return self._query(
            type="Broker", brokerName="localhost", attribute="CurrentConnectionsCount"
        )

    @property
    def _VMMemoryInfo(self):
        if not hasattr(self, "_vmmeminfo"):
            result = self.jmx.java.lang(
                type="Memory", attribute="HeapMemoryUsage,NonHeapMemoryUsage"
            )
            assert result["status"] == 200
            self._vmmeminfo = result["value"]
        return self._vmmeminfo

    def getHeapMemoryCommitted(self):
        return self._VMMemoryInfo["HeapMemoryUsage"]["committed"]

    def getHeapMemoryInitial(self):
        return self._VMMemoryInfo["HeapMemoryUsage"]["init"]

    def getHeapMemoryMaximum(self):
        return self._VMMemoryInfo["HeapMemoryUsage"]["max"]

    def getHeapMemoryUsed(self):
        return self._VMMemoryInfo["HeapMemoryUsage"]["used"]

    def getNonHeapMemoryCommitted(self):
        return self._VMMemoryInfo["NonHeapMemoryUsage"]["committed"]

    def getNonHeapMemoryInitial(self):
        return self._VMMemoryInfo["NonHeapMemoryUsage"]["init"]

    def getNonHeapMemoryMaximum(self):
        return self._VMMemoryInfo["NonHeapMemoryUsage"]["max"]

    def getNonHeapMemoryUsed(self):
        return self._VMMemoryInfo["NonHeapMemoryUsage"]["used"]

    def getMimasHeldQueueSize(self):
        return self._query(
            type="Broker",
            brokerName="localhost",
            destinationType="Queue",
            destinationName="zocalo.mimas.held",
            attribute="QueueSize",
        )

    def getQueueCount(self):
        return len(
            self._query(
                type="Broker",
                brokerName="localhost",
                destinationType="Queue",
                destinationName="*",
                attribute="ConsumerCount",
            )
        )


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


def setup_logging(level=logging.INFO):
    console = ColorStreamHandler()
    console.setLevel(level)
    logger = logging.getLogger()
    logger.setLevel(logging.WARN)
    logger.addHandler(console)
    logging.getLogger("dlstbx").setLevel(level)


setup_logging(logging.INFO)
amq = ActiveMQAPI()


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
