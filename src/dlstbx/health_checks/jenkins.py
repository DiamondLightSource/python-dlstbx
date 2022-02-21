from __future__ import annotations

from dlstbx.health_checks import REPORT, CheckFunctionInterface, Status
from dlstbx.util.certificate import problems_with_certificate


def check_jenkins_certificate(cfc: CheckFunctionInterface):
    jenkins_host = "jenkins.diamond.ac.uk"
    URL = f"https://{jenkins_host}/"
    certificate_problems = problems_with_certificate(jenkins_host)
    if certificate_problems:
        return Status(
            Source=cfc.name,
            Level=REPORT.WARNING,
            Message="Jenkins certificate warning",
            MessageBody=certificate_problems,
            URL=URL,
        )
    return Status(
        Source=cfc.name,
        Level=REPORT.PASS,
        Message="Jenkins certificate valid",
        MessageBody="",
        URL=URL,
    )
