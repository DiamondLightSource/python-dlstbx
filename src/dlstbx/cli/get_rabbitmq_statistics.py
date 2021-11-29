from __future__ import annotations

import argparse
import logging
import sys
import threading
from typing import Any, Dict, Optional

import requests
import workflows.transport.pika_transport
import zocalo.configuration

JSONDict = Dict[str, Any]

from dlstbx.util.certificate import problems_with_certificate
from dlstbx.util.colorstreamhandler import ColorStreamHandler

workflows.transport.default_transport = "PikaTransport"


def setup_logging(level=logging.INFO):
    console = ColorStreamHandler()
    console.setLevel(level)
    logger = logging.getLogger()
    logger.setLevel(logging.WARN)
    logger.addHandler(console)
    logging.getLogger("dlstbx").setLevel(level)


def readable_byte_size(value):
    value = value / (1024 * 1024)
    if value < 1100:
        return f"{value:.0f} MB"
    value = value / 1024
    return f"{value:.1f} GB"


def readable_time(seconds: float) -> str:
    if seconds > 86400 * 3:
        return f"{seconds/86400:.0f} days"
    elif seconds > 86400:
        return f"{seconds/86400:.1f} days"
    elif seconds > 36000:
        return f"{seconds/3600:.0f} hours"
    elif seconds > 3600:
        return f"{seconds/3600:.1f} hours"
    elif seconds > 120:
        return f"{seconds/60:.0f} minutes"
    else:
        return f"{seconds:.0f} seconds"


setup_logging(logging.INFO)

colour_limits = {
    "channels": (600, 800),
    "connections": (400, 600),
    "consumers": (400, 600),
    "messages": (10000, 15000),
    "messages_unacknowledged": (2000, 15000),
}


class _MicroAPI:
    def __init__(self, zc: zocalo.configuration.Configuration, base_url: str):
        self._base_url = base_url
        self._auth = (zc.rabbitmqapi["username"], zc.rabbitmqapi["password"])

    def endpoint(self, endpoint: str) -> JSONDict:
        try:
            result = requests.get(f"{self._base_url}/{endpoint}", auth=self._auth)
        except requests.exceptions.ConnectionError as e:
            raise ConnectionError(
                f"{self._base_url} raised connection error {e!r}"
            ) from None
        if result.status_code != 200:
            raise ConnectionError(
                f"{self._base_url} returned status code {result.status_code}: {result.reason}"
            )
        return result.json()

    def test(self, endpoint: str) -> bool:
        try:
            result = requests.get(
                f"{self._base_url}/{endpoint.lstrip('/')}", auth=self._auth
            )
        except requests.exceptions.ConnectionError as e:
            raise ConnectionError(
                f"{self._base_url} raised connection error {e!r}"
            ) from None
        return result.status_code == 200


def colourreset():
    if not sys.stdout.isatty():
        return ""
    return ColorStreamHandler.DEFAULT


def run():
    parser = argparse.ArgumentParser(
        usage="dlstbx.get_rabbitmq_statistics [options]",
        description="Collects statistics from an RabbitMQ server",
    )

    parser.add_argument("-?", action="help", help=argparse.SUPPRESS)

    # Load configuration
    zc = zocalo.configuration.from_file()
    zc.activate()
    zc.add_command_line_options(parser)

    parser.parse_args()

    error_encountered = threading.Event()

    def conditional_colour(c: str) -> str:
        return c if sys.stdout.isatty() else ""

    red = conditional_colour(ColorStreamHandler.RED + ColorStreamHandler.BOLD)
    yellow = conditional_colour(ColorStreamHandler.YELLOW + ColorStreamHandler.BOLD)
    green = conditional_colour(ColorStreamHandler.GREEN)
    reset = conditional_colour(ColorStreamHandler.DEFAULT)

    def colour(
        value, name: Optional[str] = None, *, warnlevel=None, errorlevel=None, fmt="{}"
    ):
        if name:
            warnlevel, errorlevel = colour_limits[name]
        if value > errorlevel:
            error_encountered.set()
        if value < warnlevel:
            return f"{green}{fmt}{reset}".format(value)
        elif value < errorlevel:
            return f"{yellow}{fmt}{reset}".format(value)
        else:
            return f"{red}{fmt}{reset}".format(value)

    rabbit = {
        host: _MicroAPI(zc, base_url=f"https://{host}")
        for host in workflows.transport.pika_transport.PikaTransport.defaults[
            "--rabbit-host"
        ].split(",")
    }

    status: Dict[str, JSONDict] = {}
    nodes: Dict[str, JSONDict] = {}
    cluster_name: Optional[str] = None
    print("RabbitMQ hosts:")
    for host in rabbit:
        try:
            status[host] = rabbit[host].endpoint("api/overview")
            cluster_name = cluster_name or status[host].get("cluster_name")
        except ConnectionError:
            error_encountered.set()
            print(f"  {host:23s}: {red}Unreachable{reset}")
            continue
        try:
            nodes[host] = rabbit[host].endpoint(f"api/nodes/{status[host]['node']}")
        except ConnectionError:
            error_encountered.set()
            print(f"  {host:23s}: {red}Partially unresponsive{reset}")
        certificate_issue = problems_with_certificate(host)
        if certificate_issue:
            error_encountered.set()
            print(f"  {host:23s}: {red}{certificate_issue}{reset}")

    if not status:
        error_encountered.set()
        print(f"\n{red}RabbitMQ cluster unavailable{reset}")
        exit(1)

    cluster_links = 0
    required_node_protocols = {"http", "clustering", "http/prometheus", "amqp"}
    for host in status:
        print(
            f"  {host:23s}: RabbitMQ {status[host]['rabbitmq_version']}, Erlang {status[host]['erlang_version']}",
            end="",
        )
        if host in nodes and nodes[host].get("uptime"):
            print(f", up {readable_time(nodes[host]['uptime']/1000)}")
        else:
            print()
        ports_open = {
            listener["protocol"]
            for listener in status[host]["listeners"]
            if listener["node"] == status[host]["node"]
        }
        missing_protocols = required_node_protocols - ports_open
        if missing_protocols:
            error_encountered.set()
            print(
                f"  {'':23s}  {red}Node not listening on port(s) {', '.join(missing_protocols)}{reset}"
            )
        if not status[host].get("cluster_name"):
            error_encountered.set()
            print(f"  {'':23s}  {yellow}Node not tied into cluster{reset}")
        elif status[host]["cluster_name"] != cluster_name:
            error_encountered.set()
            print(f"  {'':23s}  {red}Node is member of a different cluster{reset}")
        if not rabbit[host].test("/api/health/checks/local-alarms"):
            error_encountered.set()
            print(
                f"  {'':23s}  {red}Node is running outside of specified limits{reset}"
            )
        if host in nodes:
            if (
                any(
                    key not in nodes[host]
                    for key in {
                        "running",
                        "cluster_links",
                        "disk_free",
                        "disk_free_limit",
                        "mem_used",
                        "mem_limit",
                        "sockets_used",
                        "sockets_total",
                    }
                )
                or nodes[host]["running"] is not True
            ):
                error_encountered.set()
                print(f"{'':26s} {red}Node disabled{reset}")
                continue
            cluster_links += max(len(nodes[host]["cluster_links"]), len(rabbit))
            if nodes[host]["disk_free"] <= nodes[host]["disk_free_limit"]:
                error_encountered.set()
                disk_colour = red
            elif nodes[host]["disk_free"] <= nodes[host]["disk_free_limit"] * 10:
                disk_colour = yellow
            else:
                disk_colour = green
            if nodes[host]["mem_used"] >= nodes[host]["mem_limit"]:
                error_encountered.set()
                memory = red
            elif nodes[host]["mem_used"] >= nodes[host]["mem_limit"] / 10:
                memory = yellow
            else:
                memory = green
            sockets = nodes[host]["sockets_used"] / nodes[host]["sockets_total"]
            if sockets >= 0.75:
                error_encountered.set()
                socket_colour = red
            elif sockets > 0.5:
                socket_colour = yellow
            else:
                socket_colour = green
            print(
                f"{'':26s} "
                f"{memory}{readable_byte_size(nodes[host]['mem_used'])}{reset} memory used, "
                f"{disk_colour}{readable_byte_size(nodes[host]['disk_free'])}{reset} disk space, "
                f"{socket_colour}{sockets:.0f}%{reset} sockets used"
            )

    if not cluster_name:
        exit(1 if error_encountered.is_set() else 0)

    cluster_status = [
        h for h in status.values() if h.get("cluster_name") == cluster_name
    ][0]

    if cluster_links < len(rabbit) ** 2:
        error_encountered.set()
        print(
            f"\nCluster {red}degraded{reset} ({cluster_links} out of {len(rabbit) ** 2} links in operation)"
        )
    else:
        print(f"\nCluster {cluster_status['cluster_name']}:")
    for thing in ("connections", "consumers", "channels"):
        print(
            f"  {thing.capitalize():11s}: {colour(cluster_status['object_totals'][thing], name=thing, fmt='{:3d}')}"
        )
    for thing, name in (
        ("messages", "Messages"),
        ("messages_unacknowledged", "in flight"),
    ):
        print(
            f"  {name:11s}: {colour(cluster_status['queue_totals'][thing], name=thing, fmt='{:3d}')}"
        )
    exit(1 if error_encountered.is_set() else 0)
