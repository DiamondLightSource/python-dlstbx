# Experimental monitoring of beamline events


import base64
import datetime
import dlstbx.gda_interface.dejava
import functools
import itertools
import json
import math
import queue
import pprint
import stomp
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

last_shown = {}


def dumb_to_value(dumb):
    if len(dumb) != 1:
        return None
    dumb = dumb[0]
    if not isinstance(dumb, dict):
        # Sometimes we get plain strings here
        return dumb
    if dumb["_cls"]["_name"] in {"ScannableStatus", "EnumPositionerStatus"}:
        return dumb["_name"]
    if dumb["_cls"]["_name"] == "Processor$STATE":
        return "Processor state: " + dumb["_name"]
    if dumb["_cls"]["_name"] == "Command$STATE":
        return "Command state: " + dumb["_name"]
    if dumb["_cls"]["_name"] in {"Double"}:
        return dumb["value"]
    if dumb["_cls"]["_name"] == "TerminalOutput":
        return "Terminal: " + dumb["output"].strip()
    if dumb["_cls"]["_name"] == "ScannablePositionChangeEvent":
        if not isinstance(dumb["newPosition"], dict):
            return dumb["newPosition"]
        if dumb["newPosition"]["_cls"]["_name"] == "SampleChangerStatus":
            return "Sample Change: %d" % dumb["newPosition"]["status"]
        return dumb["newPosition"]["value"]
    if dumb["_cls"]["_name"] == "BatonLeaseRenewRequest":
        date_time = datetime.datetime.fromtimestamp(dumb["timestamp"] / 1000)
        return "BatonLeaseRenewRequest: %d (%s)" % (
            dumb["timestamp"],
            date_time.strftime("%Y-%m-%d %H:%M:%S"),
        )
    if dumb["_cls"]["_name"] == "SimpleCommandProgress":
        return "%s: %.1f%%" % (dumb["msg"], dumb["percentDone"])
    if dumb["_cls"]["_name"] == "Progress":
        progress = int(
            math.floor(60 * dumb["currentImage"] / dumb["totalNumberOfImages"])
        )
        if dumb["currentImage"] % 10 == 0 and dumb["startDate"]:
            start = datetime.datetime.fromtimestamp(
                dumb["startDate"]["data"] / 1000
            ).strftime("\nStart: %Y-%m-%d %H:%M:%S")
        else:
            start = ""
        if dumb["currentImage"] % 10 == 0 and dumb["expectedEndDate"]:
            end = datetime.datetime.fromtimestamp(
                dumb["expectedEndDate"]["data"] / 1000
            ).strftime("  End: %Y-%m-%d %H:%M:%S")
        else:
            end = ""
        return (
            "|"
            + ("=" * progress)
            + (" " * (60 - progress))
            + "| %d / %d images" % (dumb["currentImage"], dumb["totalNumberOfImages"])
            + start
            + end
        )
    if dumb["_cls"]["_name"] == "HighestExistingFileMonitorData":
        return "Most recent file seen has index %d (%s/%s)" % (
            dumb["foundIndex"]["value"],
            dumb["highestExistingFileMonitorSettings"]["fileTemplatePrefix"],
            dumb["highestExistingFileMonitorSettings"]["fileTemplate"],
        )
    if dumb["_cls"]["_name"] == "IQIResult":
        return "Grid scan result received for DCID %d image %d" % (
            dumb["iqi"]["_dataCollectionId"],
            dumb["imageNumber"],
        )
    if dumb["_cls"]["_name"] == "CameraStatus":
        return (
            "Camera status: Beam @ %.2f x %.2f mm, position %.2f, distance %.2f (valid=%s)\n     states: expo=%s ra=%s shutter=%s"
            % (
                dumb["beamXinMM"],
                dumb["beamYinMM"],
                dumb["currentPosition"],
                dumb["detectorDistanceValue"],
                dumb["distanceValid"],
                dumb["expoState"],
                dumb["raState"],
                dumb["shutterState"],
            )
        )
    return None


def common_listener(beamline, header, message):
    destination = header["destination"]
    if (beamline, destination) == ("i23", "/topic/gda.event.flux"):
        return
    try:
        message = base64.decodestring(json.loads(message)["byte-array"])
    except Exception as e:
        print_queue.put("%s: Error b64 decoding in %s\n%r" % (beamline, destination, e))
        return
    try:
        jobject = dlstbx.gda_interface.dejava.parse(StringIO.StringIO(message))
    except NotImplementedError:
        with open("nie.txt", "wb") as fh:
            fh.write(message)
        print("NotImplementedError encountered")
    except Exception as e:
        print_queue.put(
            "%s: Error decoding in %s\n%s\n%r"
            % (beamline, header["destination"], message, e)
        )
        raise
        return
    message = pprint.pformat(jobject)
    try:
        value = dumb_to_value(jobject)
        if value is None:
            print_queue.put("%s: %s\n%s" % (beamline, destination, message))
            return
        if destination == "/topic/gda.event.timeToRefill":
            value = int(value)
        if destination == "/topic/gda.event.idGap":
            value = round(value, 3)

    except Exception:
        print(message)
        raise

    if last_shown.get((beamline, destination)) == value:
        return
    last_shown[(beamline, destination)] = value
    print_queue.put("%s %s %s" % (beamline, destination, value))


def line_printer():
    while True:
        line = print_queue.get()
        print("".join(x for x in line if ord(x) >= 32 or ord(x) in (10, 13)))
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
