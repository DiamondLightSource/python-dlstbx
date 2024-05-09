from __future__ import annotations

import subprocess
import xml.dom.minidom
from typing import List

from dlstbx.health_checks import REPORT, CheckFunctionInterface, Status


def _find_server_issues(hosts: List[str], group_name: str, check_name: str) -> Status:
    try:
        result = subprocess.run(
            ["nmap", "-sT", "-oX", "-", "-p", "3306,4306"] + hosts,
            capture_output=True,
            timeout=10,
        )
    except subprocess.TimeoutExpired:
        return Status(
            Source=check_name,
            Level=REPORT.ERROR,
            Message=f"{group_name} degraded",
            MessageBody="Encountered timeout checking nodes",
        )
    if result.returncode or result.stderr:
        return Status(
            Source=check_name,
            Level=REPORT.ERROR,
            Message=f"{group_name} degraded",
            MessageBody=f"Encountered error checking nodes:\n{result.stderr.decode('latin1')}",
        )
    rxml = xml.dom.minidom.parseString(result.stdout)
    rhosts = rxml.getElementsByTagName("host")
    host_result = {
        hn.getAttribute("name"): h
        for h in rhosts
        for hn in h.getElementsByTagName("hostname")
    }
    return_level = REPORT.PASS
    return_message = []
    for h in hosts:
        if h in host_result:
            port_status = {
                int(p.getAttribute("portid")): any(
                    ps.getAttribute("state") == "open"
                    for ps in p.getElementsByTagName("state")
                )
                for p in host_result[h].getElementsByTagName("port")
            }
            if any(port_status.values()):
                return_message.append(
                    f"DB server {h} is up on port {', '.join(str(p) for p in port_status if port_status[p])}"
                )
            else:
                return_message.append(
                    f"DB server {h} is not responding on ports {', '.join(str(p) for p in port_status)}"
                )
                return_level = REPORT.ERROR
        else:
            return_message.append(f"DB server {h} appears to be down")
            return_level = REPORT.ERROR
    return Status(
        Source=check_name,
        Level=return_level,
        Message=(
            f"{group_name} online"
            if return_level == REPORT.PASS
            else f"{group_name} degraded"
        ),
        MessageBody="\n".join(return_message),
    )


def check_ispyb_servers(cfc: CheckFunctionInterface):
    production = [
        "ispybdbproxy.diamond.ac.uk",
        "cs04r-sc-serv-97.diamond.ac.uk",
        "cs04r-sc-serv-98.diamond.ac.uk",
        "cs04r-sc-serv-99.diamond.ac.uk",
    ]
    development = [
        "cs04r-sc-vserv-163.diamond.ac.uk",
        "cs04r-sc-vserv-86.diamond.ac.uk",
        "cs04r-sc-vserv-87.diamond.ac.uk",
        "cs04r-sc-vserv-88.diamond.ac.uk",
    ]
    return [
        _find_server_issues(
            production,
            group_name="ISPyB production servers",
            check_name=f"{cfc.name}.production",
        ),
        _find_server_issues(
            development,
            group_name="ISPyB development servers",
            check_name=f"{cfc.name}.development",
        ),
    ]
