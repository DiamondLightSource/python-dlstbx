import argparse
import json
import logging
import sys
import urllib
import urllib.error
from typing import Any, Dict, List, Tuple

import workflows.transport
import zocalo.configuration
from zocalo.util.rabbitmq import http_api_request

from dlstbx.util.colorstreamhandler import ColorStreamHandler

workflows.transport.default_transport = "PikaTransport"

from workflows.transport import pika_transport


class RabbitMQAPI:
    def __init__(self, zc: zocalo.configuration.Configuration):
        self._zc = zc

    def health_checks(self) -> Tuple[Dict[str, Any], Dict[str, str]]:
        # https://rawcdn.githack.com/rabbitmq/rabbitmq-server/v3.9.7/deps/rabbitmq_management/priv/www/api/index.html
        HEALTH_CHECKS = {
            "/health/checks/alarms",
            "/health/checks/local-alarms",
            "/health/checks/certificate-expiration/1/months",
            f"/health/checks/port-listener/{pika_transport.PikaTransport.defaults['--rabbit-port']}",
            # f"/health/checks/port-listener/1234",
            "/health/checks/protocol-listener/amqp",
            "/health/checks/virtual-hosts",
            "/health/checks/node-is-mirror-sync-critical",
            "/health/checks/node-is-quorum-critical",
        }

        success = {}
        failure = {}
        for health_check in HEALTH_CHECKS:
            try:
                with urllib.request.urlopen(
                    http_api_request(self._zc, health_check)
                ) as response:
                    success[health_check] = json.loads(response.read())
            except urllib.error.urllib.error.HTTPError as e:
                failure[health_check] = str(e)
        return success, failure

    @property
    def connections(self) -> List[Dict[str, Any]]:
        with urllib.request.urlopen(
            http_api_request(self._zc, "/connections")
        ) as response:
            return json.loads(response.read())

    @property
    def nodes(self) -> List[Dict[str, Any]]:
        # https://www.rabbitmq.com/monitoring.html#node-metrics
        with urllib.request.urlopen(http_api_request(self._zc, "/nodes")) as response:
            nodes = json.loads(response.read())
        useful_keys = {
            "name",
            "mem_used",
            "mem_limit",
            "mem_alarm",
            "disk_free",
            "disk_free_limit",
            "disk_free_alarm",
            "fd_total",
            "fd_used",
            "io_file_handle_open_attempt_count",
            "sockets_total",
            "sockets_used",
            "message_stats.disk_reads",
            "message_stats.disk_writes",
            "gc_num",
            "gc_bytes_reclaimed",
            "proc_total",
            "proc_used",
            "run_queue",
        }
        filtered = [
            {k: v for k, v in node.items() if k in useful_keys} for node in nodes
        ]
        return filtered

    @property
    def queues(self) -> List[Dict[str, Any]]:
        # https://www.rabbitmq.com/monitoring.html#queue-metrics
        with urllib.request.urlopen(http_api_request(self._zc, "/queues")) as response:
            nodes = json.loads(response.read())
        useful_keys = {
            "consumers",
            "name",
            "memory",
            "messages",
            "messages_ready",
            "messages_unacknowledged",
            "message_stats.publish",
            "message_stats.publish_details.rate",
            "message_stats.deliver_get",
            "message_stats.deliver_get_details.rate",
        }
        filtered = [
            {k: v for k, v in node.items() if k in useful_keys} for node in nodes
        ]
        return filtered


def setup_logging(level=logging.INFO):
    console = ColorStreamHandler()
    console.setLevel(level)
    logger = logging.getLogger()
    logger.setLevel(logging.WARN)
    logger.addHandler(console)
    logging.getLogger("dlstbx").setLevel(level)


setup_logging(logging.INFO)


def run():
    parser = argparse.ArgumentParser(
        usage="dlstbx.get_activemq_statistics [options]",
        description="Collects statistics from an ActiveMQ server",
    )

    parser.add_argument("-?", action="help", help=argparse.SUPPRESS)

    # Load configuration
    zc = zocalo.configuration.from_file()
    zc.activate()
    zc.add_command_line_options(parser)

    parser.parse_args()

    rmq = RabbitMQAPI(zc)
    _, hc_failures = rmq.health_checks()

    def readable_memory(value):
        return "{:.1f} MB".format(value / 1024 / 1024)

    connections_count = len(rmq.connections)
    nodes = rmq.nodes

    memory = max([node["mem_used"] / node["mem_limit"] for node in nodes]) * 100
    disk_free = min([node["disk_free"] for node in nodes])
    disk_free_limit = [
        node["disk_free_limit"] for node in nodes if node["disk_free"] == disk_free
    ][0]
    fd_used = max([node["fd_used"] for node in nodes])
    fd_total = [node["fd_total"] for node in nodes if node["fd_used"] == fd_used][0]

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

    if hc_failures:
        print("RabbitMQ health check failures:")
        for check, msg in hc_failures.items():
            print(
                f"{ColorStreamHandler.RED + ColorStreamHandler.BOLD}    /api{check}: {msg}{colourreset()}"
            )

    print(
        f"""
RabbitMQ connections: {colour(connections_count, 400, 600)}{connections_count}{colourreset()}

Storage statistics:
   memory    :{colour(memory, 10, 30)}{memory:>3} %{colourreset()}
   fd_used   :{colour(fd_used, 0.5 * fd_total, 0.9 * fd_total)}{fd_used:>3} {colourreset()}
   disk_free :{colour(-disk_free, -10 * disk_free_limit, disk_free_limit)}{readable_memory(disk_free):>3} {colourreset()}

"""
    )
