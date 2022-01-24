from __future__ import annotations

import dlstbx.util.jmxstats
from dlstbx.health_checks import REPORT, CheckFunctionInterface, Status


def check_PIA_bridge_runs(cfc: CheckFunctionInterface) -> Status:
    jmx = dlstbx.util.jmxstats.JMXAPI()
    try:
        pia_consumers = jmx.org.apache.activemq(
            type="Broker",
            brokerName="localhost",
            destinationType="Queue",
            destinationName="zocalo.per_image_analysis",
            attribute="ConsumerCount",
        )["value"]
    except Exception as e:
        return Status(
            Source=cfc.name,
            Level=REPORT.WARNING,
            Message="Could not obtain PIA listener count from ActiveMQ",
            MessageBody=repr(e),
        )
    if pia_consumers:
        return Status(
            Source=cfc.name,
            Level=REPORT.PASS,
            Message=f"{pia_consumers} listener(s) on PIA queue",
        )
    else:
        return Status(
            Source=cfc.name, Level=REPORT.ERROR, Message="No listeners on PIA queue"
        )
