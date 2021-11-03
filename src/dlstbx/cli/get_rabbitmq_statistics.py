import argparse
import logging
import sys

import workflows.transport
import zocalo.configuration
from zocalo.util.rabbitmq import RabbitMQAPI

from dlstbx.util.colorstreamhandler import ColorStreamHandler

workflows.transport.default_transport = "PikaTransport"


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

    rmq = RabbitMQAPI.from_zocalo_configuration(zc)
    _, hc_failures = rmq.health_checks

    def readable_memory(value):
        return "{:.0f} MB".format(value / 1024 / 1024)

    connections_count = len(rmq.connections())
    nodes = rmq.nodes()

    memory = max([node.mem_used / node.mem_limit for node in nodes]) * 100
    disk_free = min([node.disk_free for node in nodes])
    disk_free_limit = [
        node.disk_free_limit for node in nodes if node.disk_free == disk_free
    ][0]
    fd_used = max([node.fd_used for node in nodes])
    fd_total = [node.fd_total for node in nodes if node.fd_used == fd_used][0]

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
   memory    :{colour(memory, 10, 30)}{memory:>7.2f} %{colourreset()}
   fd_used   :{colour(fd_used, 0.5 * fd_total, 0.9 * fd_total)}{fd_used:>7} {colourreset()}
   disk_free :{colour(-disk_free, -10 * disk_free_limit, disk_free_limit)}{readable_memory(disk_free):>10} {colourreset()}
"""
    )
