# Experimental monitoring of beamline events

from __future__ import absolute_import, division, print_function

import base64
import datetime
import dlstbx.dejava
import functools
import itertools
import json
import queue
import pprint
import stomp
import struct
import threading
import time
import StringIO

beamlines = {
    "i02-1",
    "i02-2",
    "i03",
    "i04",
    "i04-1",
    "i08",
    "i11",
    "i13",
    "i14",
    "i15-1",
    "i19-1",
    "i19-2",
    "i23",
    "i24",
    "p45",
}
beamlines = {"i02-1", "i02-2", "i03", "i04", "i04-1", "i19-1", "i19-2", "i23", "i24"}

print_queue = queue.Queue()


def dumb_to_value(dumb):
    if len(dumb) != 1:
        return None
    dumb = dumb[0]
    if dumb["_cls"]["_name"] in ("ScannableStatus", "EnumPositionerStatus"):
        return dumb["_name"]
    if dumb["_cls"]["_name"] == "Double":
        value = dumb["value"]
        return struct.unpack(">d", value.replace(" ", "").decode("hex"))[0]
    if dumb["_cls"]["_name"] == "ScannablePositionChangeEvent":
        value = dumb["newPosition"]["value"]
        return struct.unpack(">d", value.replace(" ", "").decode("hex"))[0]
    if dumb["_cls"]["_name"] == "BatonLeaseRenewRequest":
        date_time = datetime.datetime.fromtimestamp(dumb["timestamp"] / 1000)
        return "BatonLeaseRenewRequest: %d (%s)" % (
            dumb["timestamp"],
            date_time.strftime("%m/%d/%Y, %H:%M:%S"),
        )
    return None


def common_listener(beamline, header, message):
    destination = header["destination"]
    if destination in (
        "/topic/gda.event.flux",
        "/topic/gda.event.timeToRefill",
        "/topic/gda.event.idGap",
    ):
        return

    try:
        message = base64.decodestring(json.loads(message)["byte-array"])
    except Exception as e:
        print_queue.put(
            "%s: Error decoding in %s\n%r" % (beamline, header["destination"], e)
        )
        return
    try:
        jobject = dlstbx.dejava.parse(StringIO.StringIO(message))
    except Exception as e:
        print_queue.put(
            "%s: Error decoding in %s\n%s\n%r"
            % (beamline, header["destination"], message, e)
        )
        return
    message = pprint.pformat(jobject)
    try:
        value = dumb_to_value(jobject)
        if value is None:
            print_queue.put("%s: %s\n%s" % (beamline, header["destination"], message))
        else:
            print_queue.put("%s %s %s" % (beamline, header["destination"], value))
    except Exception:
        print(message)
        raise


def line_printer():
    while True:
        line = print_queue.get()
        print(line)
        print_queue.task_done()


qt = threading.Thread(target=line_printer)
qt.setDaemon(True)
qt.start()

bl_settings = {
    bl: {"host": "%s-control.diamond.ac.uk" % bl, "port": 61613} for bl in beamlines
}

for bl, settings in bl_settings.items():
    settings["listener"] = stomp.listener.ConnectionListener()
    #  settings["listener"] = stomp.listener.PrintingListener()
    settings["listener"].on_before_message = lambda x, y: (x, y)
    settings["listener"].on_message = functools.partial(common_listener, bl)

    settings["connection"] = stomp.Connection(
        [(settings["host"], int(settings["port"]))]
    )
    settings["connection"].set_listener("", settings["listener"])
    print("Connecting to", bl)
    try:
        settings["connection"].connect(wait=False)
    except stomp.exception.ConnectFailedException:
        print("Could not connect to", bl)
        settings["connection"] = None
        continue
    timeout = time.time() + 10
    while time.time() < timeout and not settings["connection"].is_connected():
        time.sleep(0.02)
    if not settings["connection"].is_connected():
        print("Connection timeout to", bl)
        settings["connection"] = None
        continue
    settings["subscription_ids"] = itertools.count(1)
    settings["connection"].subscribe(
        "/topic/gda.>",
        next(settings["subscription_ids"]),
        headers={"transformation": "jms-object-json"},
    )
    #  settings["connection"].subscribe("/topic/gda.event.timeToRefill", next(settings["subscription_ids"]), headers={"transformation":"jms-object-json"})
    #  settings["connection"].subscribe("/topic/uk.>", next(settings["subscription_ids"]))
    print("Connected to", bl)

time.sleep(3600)
