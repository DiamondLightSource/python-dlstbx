from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Union
from urllib.parse import urlparse

import requests
import workflows.transport.pika_transport
import zocalo.configuration

from dlstbx.util.certificate import problems_with_certificate
from dlstbx.util.colorstreamhandler import ColorStreamHandler

workflows.transport.default_transport = "PikaTransport"
JSONDict = Dict[str, Any]


def _setup_logging(level=logging.INFO):
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


@dataclass(frozen=True)
class StatusValue:
    value: float
    level: int = 0

    @classmethod
    def from_value(
        cls, value: float, warning_threshold: float, error_threshold: float
    ) -> StatusValue:
        if value > error_threshold:
            return StatusValue(value=value, level=2)
        elif value > warning_threshold:
            return StatusValue(value=value, level=1)
        return StatusValue(value=value, level=0)


@dataclass(frozen=True)
class StatusText:
    text: str
    level: int = 0


colour_limits = {
    "connections": (600, 800),
    "consumers": (400, 600),
    "channels": (600, 800),
    "messages": (10000, 15000),
    "messages_unacknowledged": (8000, 20000),
}


class _MicroAPI:
    def __init__(self, zc: zocalo.configuration.Configuration, base_url: str):
        self._url = urlparse(base_url, allow_fragments=False)
        self._auth = (zc.rabbitmqapi["username"], zc.rabbitmqapi["password"])

    def endpoint(self, endpoint: str) -> JSONDict:
        url = self._url._replace(path=endpoint).geturl()
        try:
            result = requests.get(url, auth=self._auth, timeout=2)
        except requests.exceptions.ConnectionError as e:
            raise ConnectionError(f"{url} raised connection error {e!r}") from None
        if result.status_code != 200:
            raise ConnectionError(
                f"{url} returned status code {result.status_code}: {result.reason}"
            )
        return result.json()

    def test(self, endpoint: str) -> bool:
        url = self._url._replace(path=endpoint).geturl()
        try:
            result = requests.get(url, auth=self._auth, timeout=2)
        except requests.exceptions.ConnectionError as e:
            raise ConnectionError(f"{url} raised connection error {e!r}") from None
        return result.status_code == 200

    def check_metrics(self) -> bool:
        prometheus_port = (self._url.port or 15672) + 20
        url = self._url._replace(
            scheme="http",
            path="/metrics",
            netloc=f"{self._url.hostname}:{prometheus_port}",
        ).geturl()
        try:
            result = requests.get(url, timeout=2)
        except requests.exceptions.ConnectionError as e:
            raise ConnectionError(f"{url} raised connection error {e!r}") from None
        if result.status_code != 200:
            raise ConnectionError(
                f"{url} returned status code {result.status_code}: {result.reason}"
            )
        return "rabbitmq_identity_info" in result.text


def colourreset():
    if not sys.stdout.isatty():
        return ""
    return ColorStreamHandler.DEFAULT


def rabbit_checks(zc, hosts: List[str]):
    rabbit = {host: _MicroAPI(zc, base_url=f"https://{host}") for host in hosts}
    result: Dict[str, JSONDict] = {"hosts": {}, "cluster": {}}

    status: Dict[str, JSONDict] = {}
    nodes: Dict[str, JSONDict] = {}
    cluster_name: Optional[str] = None
    for host in rabbit:
        certificate_issue = problems_with_certificate(host)
        result["hosts"][host] = {}
        try:
            status[host] = rabbit[host].endpoint("api/overview")
            cluster_name = cluster_name or status[host].get("cluster_name")
        except ConnectionError:
            if certificate_issue and "Socket" not in certificate_issue:
                result["hosts"][host]["connection"] = StatusText(
                    level=2, text=f"Unreachable, {certificate_issue}"
                )
            else:
                result["hosts"][host]["connection"] = StatusText(
                    level=2, text="Unreachable"
                )
            continue
        try:
            nodes[host] = rabbit[host].endpoint(f"api/nodes/{status[host]['node']}")
        except ConnectionError:
            result["hosts"][host]["connection"] = StatusText(
                level=2, text="Partially unresponsive"
            )
        if certificate_issue:
            if "will expire" in certificate_issue and "days" in certificate_issue:
                result["hosts"][host]["certificate"] = StatusText(
                    level=1, text=certificate_issue
                )
            else:
                result["hosts"][host]["certificate"] = StatusText(
                    level=2, text=certificate_issue
                )
        try:
            if not rabbit[host].check_metrics():
                result["hosts"][host]["connection"] = StatusText(
                    level=2, text="Prometheus metrics not available"
                )
        except ConnectionError:
            result["hosts"][host]["connection"] = StatusText(
                level=2, text="Prometheus metrics not available"
            )

    if not status:
        result["cluster"]["status"] = StatusText(
            level=2, text="RabbitMQ cluster unavailable"
        )
        return result

    cluster_links = 0
    required_node_protocols = {"http", "clustering", "http/prometheus", "amqp"}
    for host in status:
        result["hosts"][host]["version_rabbitmq"] = StatusText(
            text=status[host]["rabbitmq_version"]
        )
        result["hosts"][host]["version_erlang"] = StatusText(
            text=status[host]["erlang_version"]
        )
        if host in nodes and nodes[host].get("uptime"):
            result["hosts"][host]["uptime"] = StatusValue(
                value=nodes[host]["uptime"] / 1000
            )
        ports_open = {
            listener["protocol"]
            for listener in status[host]["listeners"]
            if listener["node"] == status[host]["node"]
        }
        missing_protocols = required_node_protocols - ports_open
        if missing_protocols:
            result["hosts"][host]["ports"] = StatusText(
                level=2,
                text=(f"Node not listening on port(s) {', '.join(missing_protocols)}"),
            )
        if not status[host].get("cluster_name"):
            result["hosts"][host]["cluster"] = StatusText(
                level=1, text="Node not tied into cluster"
            )
        elif status[host]["cluster_name"] != cluster_name:
            result["hosts"][host]["cluster"] = StatusText(
                level=2, text="Node is member of a different cluster"
            )
        if not rabbit[host].test("/api/health/checks/local-alarms"):
            result["hosts"][host]["local_alarms"] = StatusText(
                level=2, text="Node is running outside of specified limits"
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
                result["hosts"][host]["local_alarms"] = StatusText(
                    level=2, text="Node disabled"
                )
                continue
            cluster_links += max(len(nodes[host]["cluster_links"]), len(rabbit))
            if nodes[host]["disk_free"] <= nodes[host]["disk_free_limit"]:
                disk_space_level = 2
            elif nodes[host]["disk_free"] <= nodes[host]["disk_free_limit"] * 10:
                disk_space_level = 1
            else:
                disk_space_level = 0
            result["hosts"][host]["disk"] = StatusValue(
                level=disk_space_level, value=nodes[host]["disk_free"]
            )
            result["hosts"][host]["memory"] = StatusValue.from_value(
                value=nodes[host]["mem_used"],
                warning_threshold=nodes[host]["mem_limit"] / 10,
                error_threshold=nodes[host]["mem_limit"],
            )
            result["hosts"][host]["sockets"] = StatusValue.from_value(
                value=nodes[host]["sockets_used"] / nodes[host]["sockets_total"],
                warning_threshold=0.3,
                error_threshold=0.5,
            )

    if cluster_name:
        cluster_status = [
            h for h in status.values() if h.get("cluster_name") == cluster_name
        ][0]
        result["cluster"]["name"] = StatusText(text=cluster_name)
        if cluster_links < len(rabbit) ** 2:
            result["cluster"]["links"] = StatusText(
                level=2,
                text=f"Cluster degraded ({cluster_links} out of {len(rabbit) ** 2} links in operation)",
            )

        for thing, threshold in colour_limits.items():
            value = cluster_status["object_totals"].get(
                thing, cluster_status["queue_totals"].get(thing)
            )
            result["cluster"][thing] = StatusValue.from_value(
                value=value,
                warning_threshold=threshold[0],
                error_threshold=threshold[1],
            )

    return result


def run():
    parser = argparse.ArgumentParser(
        usage="dlstbx.get_rabbitmq_statistics [options]",
        description="Collects statistics from an RabbitMQ server",
    )

    parser.add_argument("-?", action="help", help=argparse.SUPPRESS)
    _setup_logging(logging.INFO)

    # Load configuration
    zc = zocalo.configuration.from_file()
    zc.activate()
    zc.add_command_line_options(parser)

    parser.parse_args()

    error_encountered = False

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
        if value < warnlevel:
            return f"{green}{fmt}{reset}".format(value)
        elif value < errorlevel:
            return f"{yellow}{fmt}{reset}".format(value)
        else:
            return f"{red}{fmt}{reset}".format(value)

    system_status = rabbit_checks(
        zc,
        hosts=workflows.transport.pika_transport.PikaTransport.defaults[
            "--rabbit-host"
        ].split(","),
    )

    def fmt(
        s: Union[StatusText, StatusValue], formatter: Optional[Callable] = None
    ) -> str:
        if isinstance(s, StatusText):
            value = s.text
        elif formatter:
            value = formatter(s.value)
        else:
            value = str(s)
        if s.level == 0:
            return f"{green}{value}{reset}"
        elif s.level == 1:
            return f"{yellow}{value}{reset}"
        else:
            return f"{red}{value}{reset}"

    print("RabbitMQ hosts:")
    for host, hs in system_status.get("hosts", {}).items():
        node_status = max(s.level for s in hs.values() if hasattr(s, "level"))
        if node_status >= 2:
            node_colour = red
            error_encountered = True
        elif node_status:
            node_colour = yellow
        else:
            node_colour = ""

        host_line = []
        if "version_rabbitmq" in hs:
            host_line.append(f"RabbitMQ {fmt(hs['version_rabbitmq'])}")
        if "version_erlang" in hs:
            host_line.append(f"Erlang {fmt(hs['version_erlang'])}")
        if "uptime" in hs:
            host_line.append(f"up {fmt(hs['uptime'], formatter=readable_time)}")
        print(f"  {node_colour}{host:23s}:{reset} {', '.join(host_line)}")
        if {"memory", "disk", "sockets"}.issubset(hs):
            print(
                f"{'':26s} "
                f"{fmt(hs['memory'], formatter=readable_byte_size)} memory used, "
                f"{fmt(hs['disk'], formatter=readable_byte_size)} disk space, "
                f"{fmt(hs['sockets'], formatter='{:.0f}%'.format)} sockets used"
            )
        for extra_field in hs.keys() - {
            "memory",
            "disk",
            "sockets",
            "version_rabbitmq",
            "version_erlang",
            "uptime",
        }:
            print(f"{'':26s} {fmt(hs[extra_field])}")

    if "name" not in system_status.get("cluster", {}):
        print(f"\n{red}RabbitMQ cluster unavailable{reset}")
        for item in system_status.get("cluster", {}).values():
            print(fmt(item))
        exit(1)

    cluster_status = max(
        s.level for s in system_status["cluster"].values() if hasattr(s, "level")
    )
    if cluster_status >= 2:
        cluster_colour = red
        error_encountered = True
    elif cluster_status:
        cluster_colour = yellow
    else:
        cluster_colour = ""

    print(f"\n{cluster_colour}Cluster {system_status['cluster']['name'].text}:{reset}")

    def _readable(name):
        if name == "messages_unacknowledged":
            return "in flight"
        return name.capitalize()

    for name in colour_limits:
        if name not in system_status["cluster"]:
            continue
        value = system_status["cluster"][name]
        print(f"  {_readable(name):11s}:", fmt(value, formatter="{:3d}".format))

    for name in system_status["cluster"].keys() - colour_limits.keys() - {"name"}:
        value = system_status["cluster"][name]
        if isinstance(value, StatusValue):
            print(f"  {_readable(name):11s}:", fmt(value, formatter="{:3d}".format))
        else:
            print(f"  {fmt(value)}")

    exit(1 if error_encountered else 0)
