import re
import socket
import subprocess
import urllib.request

import procrunner

from dlstbx.health_checks import REPORT, CheckFunctionInterface, Status, limit_level


def _check_service(URL, checkname, servicename):
    try:
        with urllib.request.urlopen(URL, timeout=3) as response:
            data = response.read(250)
            status = response.status
    except socket.timeout:
        return Status(
            Source=checkname,
            Level=REPORT.ERROR,
            Message=f"Connection to {servicename} timed out",
            URL=URL,
        )
    except urllib.error.URLError as e:
        return Status(
            Source=checkname,
            Level=REPORT.ERROR,
            Message=f"Connection to {servicename} failed with {e}",
            MessageBody=repr(e),
            URL=URL,
        )

    if status == 200:
        return Status(
            Source=checkname,
            Level=REPORT.PASS,
            Message=f"{servicename} online",
            URL=URL,
        )
    return Status(
        Source=checkname,
        Level=REPORT.ERROR,
        Message=f"{servicename} returned status {status}",
        MessageBody="Response: " + data.decode("latin-1"),
        URL=URL,
    )


def check_agamemnon(cfc: CheckFunctionInterface):
    URL = "http://agamemnon.diamond.ac.uk:8080/status"
    servicename = "Agamemnon"
    try:
        with urllib.request.urlopen(URL, timeout=3) as response:
            data = response.read(250)
            status = response.status
    except socket.timeout:
        return Status(
            Source=cfc.name,
            Level=REPORT.ERROR,
            Message=f"Connection to {servicename} timed out",
            MessageBody="",
            URL=URL,
        )
    except urllib.error.URLError as e:
        return Status(
            Source=cfc.name,
            Level=REPORT.ERROR,
            Message=f"Connecting to {servicename} failed with {e}",
            MessageBody=repr(e),
            URL=URL,
        )

    if status == 200 and b"Status: OK" in data:
        version = re.search(b"Version: ([0-9.]+)", data)
        if version:
            version = " v" + version.group(1).decode("latin-1")
        else:
            version = ""
        return Status(
            Source=cfc.name,
            Level=REPORT.PASS,
            Message=f"{servicename}{version} is alive and responsive",
            MessageBody="",
            URL=URL,
        )
    return Status(
        Source=cfc.name,
        Level=REPORT.ERROR,
        Message=f"{servicename} returned status {status}",
        MessageBody="Response: " + data.decode("latin-1"),
        URL=URL,
    )


def check_cas(cfc: CheckFunctionInterface):
    URL = "https://auth.diamond.ac.uk/cas"
    servicename = "Authentication services"
    return _check_service(URL, cfc.name, servicename)


def check_dbserver(cfc: CheckFunctionInterface):
    URL = "http://sci-serv3:2611/server-status"
    servicename = "dbserver"
    return _check_service(URL, cfc.name, servicename)


def check_gitlab(cfc: CheckFunctionInterface):
    URL = "https://gitlab.diamond.ac.uk"
    servicename = "Gitlab"
    return _check_service(URL, cfc.name, servicename)


def check_jira(cfc: CheckFunctionInterface):
    URL = "https://jira.diamond.ac.uk"
    servicename = "JIRA"
    return _check_service(URL, cfc.name, servicename)


def check_synchweb(cfc: CheckFunctionInterface):
    URL = "http://ispyb.diamond.ac.uk"
    servicename = "Synchweb"
    return _check_service(URL, cfc.name, servicename)


def check_uas(cfc: CheckFunctionInterface):
    URL = "https://uas.diamond.ac.uk/"
    servicename = "UAS"
    return _check_service(URL, cfc.name, servicename)


@limit_level(max_increase=7)
def check_internet(cfc: CheckFunctionInterface):
    command = ("ping", "-c", "1", "-W", "1", "8.8.8.8")
    try:
        result = procrunner.run(
            command,
            print_stdout=False,
            print_stderr=False,
            timeout=5,
            raise_timeout_exception=True,
        )
    except subprocess.TimeoutExpired:
        return Status(
            Source=cfc.name,
            Level=REPORT.ERROR,
            Message="Internet connection is down",
            MessageBody="Timeout running 'ping' command",
        )

    if result.returncode:
        output = (result.stderr or result.stdout).decode("latin-1").rstrip()
        return Status(
            Source=cfc.name,
            Level=REPORT.ERROR,
            Message="Internet connection is down",
            MessageBody=output,
        )
    latency = re.search(b" time=([0-9.]+) ?ms", result.stdout)
    if latency:
        latency = ", " + latency.group(1).decode("latin-1") + " ms latency"
    else:
        latency = ""
    return Status(
        Source=cfc.name,
        Level=REPORT.PASS,
        Message=f"Internet connection is up{latency}",
    )
