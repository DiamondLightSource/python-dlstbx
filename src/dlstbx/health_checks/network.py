import re
import socket
import urllib.request

from dlstbx.health_checks import REPORT, CheckFunctionInterface, Status


def check_agamemnon(cfc: CheckFunctionInterface):
    URL = "http://agamemnon.diamond.ac.uk:8080/status"
    try:
        with urllib.request.urlopen(URL, timeout=3) as response:
            data = response.read(250)
            status = response.status
    except socket.timeout:
        return Status(
            Source=cfc.name,
            Level=REPORT.ERROR,
            Message=f"Connection to Agamemnon timed out",
            URL=URL,
        )
    except urllib.error.URLError as e:
        return Status(
            Source=cfc.name,
            Level=REPORT.ERROR,
            Message=f"Connecting to Agamemnon failed with {e}",
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
            Message=f"Agamemnon{version} is alive and responsive",
            URL=URL,
        )
    return Status(
        Source=cfc.name,
        Level=REPORT.ERROR,
        Message=f"Agamemnon returned status {status}",
        MessageBody="Response: " + data.decode("latin-1"),
        URL=URL,
    )
