from __future__ import annotations

import datetime
import http.client
import re
import sys
import xml.etree.cElementTree as ET

DBSERVER_HOST = "sci-serv3"
DBSERVER_PORT = "2611"


def flatten_xml(xml, tag):
    return "".join("\n".join(i for t in xml.iter(tag) for i in t.itertext()))


def _f(_v):
    if _v is None:
        return float("nan")
    else:
        return float(_v)


def _i(_v):
    if _v is None:
        return -1
    else:
        return int(_v)


def _s(_v):
    if _v is None:
        return "null"
    else:
        return str(_v)


def _clean_nan_null_minusone(s):
    return re.sub(r"\<[^<>]*\>(null|nan|-1)\</[^<>]*\>", "", s)


def populate_blsample_xml_template(_row):
    temp = _blsample_xml % (
        _s(_row["name"]),
        _s(_row["code"]),
        _s(_row["location"]),
        _f(_row["holderlength"]),
        _f(_row["looplength"]),
        _s(_row["looptype"]),
        _f(_row["wirewidth"]),
        _s(_row["comments"]),
        _s(_row["blsamplestatus"]),
        _s(_row["lastknowncenteringposition"]),
    )

    # remove lines with null, nan and -1 values:
    temp = _clean_nan_null_minusone(temp)
    return temp


def populate_dcg_xml_template(datacollection, sessionid, blsample_id):
    nowstr = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if blsample_id is None:
        blsample_id_elem = ""
    else:
        blsample_id_elem = f"<blSampleId>{blsample_id}</blSampleId>\n"

    temp = _dcg_temp_xml_format.format(
        sessionid=sessionid,
        blsample_xml=blsample_id_elem,
        comments="Simulated datacollection.",
        experimenttype=_s(datacollection.DataCollectionGroup.experimentType),
        starttime=nowstr,
        crystalclass=_s(datacollection.DataCollectionGroup.crystalClass),
        detectormode=_s(datacollection.DataCollectionGroup.detectorMode),
    )

    # remove lines with null, nan and -1 values:
    temp = _clean_nan_null_minusone(temp)
    return temp


def populate_grid_info_xml_template(_row, _dcgid):
    temp = _grid_info_temp_xml % (
        _dcgid,
        _f(_row["dx_mm"]),
        _f(_row["dy_mm"]),
        _i(_row["steps_x"]),
        _i(_row["steps_y"]),
        _f(_row["pixelspermicronx"]),
        _f(_row["pixelspermicrony"]),
        _f(_row["snapshot_offsetxpixel"]),
        _f(_row["snapshot_offsetypixel"]),
        _s(_row["orientation"]),
    )

    # remove lines with null, nan and -1 values:
    temp = _clean_nan_null_minusone(temp)
    return temp


def populate_dc_xml_template(
    _row,
    _sessionid,
    _dcg_id,
    _no_images,
    _dir,
    _prefix,
    _run_number,
    _xtal_snapshot_path,
    _blsample_id,
    scenario_name=None,
):
    nowstr = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    suffix = _row["imagesuffix"]
    if suffix == "h5":
        file_template = "%s_%d_master.%s" % (_prefix, _run_number, suffix)
    else:
        file_template = "%s_%d_####.%s" % (_prefix, _run_number, suffix)
    if _blsample_id is None:
        blsample_id_elem = ""
    else:
        blsample_id_elem = "<blSampleId>%d</blSampleId>\n" % _blsample_id

    temp = _dc_temp_xml % (
        _sessionid,
        _dcg_id,
        blsample_id_elem,
        _run_number,
        nowstr,
        _s(_row["runstatus"]),
        _f(_row["axisstart"]),
        _f(_row["axisend"]),
        _f(_row["axisrange"]),
        _f(_row["overlap"]),
        _no_images,
        _i(_row["startimagenumber"]),
        _i(_row["numberofpasses"]),
        _f(_row["exposuretime"]),
        _dir,
        _prefix,
        suffix,
        file_template,
        _f(_row["wavelength"]),
        _f(_row["resolution"]),
        _f(_row["detectordistance"]),
        _f(_row["xbeam"]),
        _f(_row["ybeam"]),
        _i(_row["printableforreport"]),
        _f(_row["slitgapvertical"]),
        _f(_row["slitgaphorizontal"]),
        _f(_row["transmission"]),
        _s(_row["synchrotronmode"]),
        _xtal_snapshot_path[0],
        _xtal_snapshot_path[1],
        _xtal_snapshot_path[2],
        _xtal_snapshot_path[3],
        _s(_row["rotationaxis"] or None),
        _f(_row["phistart"]),
        _f(_row["chistart"]),
        _f(_row["kappastart"]),
        _f(_row["omegastart"]),
        _f(_row["undulatorgap1"]),
        _f(_row["beamsizeatsamplex"]),
        _f(_row["beamsizeatsampley"]),
        _f(_row["flux"]),
        _i(_row["focalspotsizeatsamplex"]),
        _i(_row["focalspotsizeatsampley"]),
    )
    temp = temp.format(
        comments=f"Simulated datacollection ({scenario_name})."
        if scenario_name
        else "Simulated datacollection."
    )

    # remove lines with null, nan and -1 values:
    temp = _clean_nan_null_minusone(temp)
    return temp


_blsample_xml = (
    '<?xml version="1.0" encoding="ISO-8859-1"?>'
    "<BLSample>"
    "<name>%s</name>"
    "<code>%s</code>"
    "<location>%s</location>"
    "<holderLength>%.6f</holderLength>"
    "<loopLength>%.6f</loopLength>"
    "<loopType>%s</loopType>"
    "<wireWidth>%.6f</wireWidth>"
    "<comments>%s</comments>"
    "<blSampleStatus>%s</blSampleStatus>"
    "<isInSampleChanger>False</isInSampleChanger>"
    "<lastKnownCenteringPosition>%s</lastKnownCenteringPosition>"
    "</BLSample>"
)

_dcg_temp_xml_format = (
    '<?xml version="1.0" encoding="ISO-8859-1"?>'
    "<DataCollectionGroup>"
    "<sessionId>{sessionid}</sessionId>"
    "{blsample_xml}"
    "<experimentType>{experimenttype}</experimentType>"
    "<startTime>{starttime}</startTime>"
    "<crystalClass>{crystalclass}</crystalClass>"
    "<detectorMode>{detectormode}</detectorMode>"
    "<comments>{comments}</comments>"
    "</DataCollectionGroup>"
)

_grid_info_temp_xml = (
    '<?xml version="1.0" encoding="ISO-8859-1"?>'
    "<GridInfo>"
    "<dataCollectionGroupId>%d</dataCollectionGroupId>"
    "<dx_mm>%.2f</dx_mm>"
    "<dy_mm>%.2f</dy_mm>"
    "<steps_x>%d</steps_x>"
    "<steps_y>%d</steps_y>"
    "<pixelsPerMicronX>%.4f</pixelsPerMicronX>"
    "<pixelsPerMicronY>%.4f</pixelsPerMicronY>"
    "<snapshot_offsetXPixel>%.4f</snapshot_offsetXPixel>"
    "<snapshot_offsetYPixel>%.4f</snapshot_offsetYPixel>"
    "<orientation>%s</orientation>"
    "</GridInfo>"
)

_dc_temp_xml = (
    '<?xml version="1.0" encoding="ISO-8859-1"?>'
    "<DataCollection>"
    "<sessionId>%d</sessionId>"
    "<dataCollectionGroupId>%d</dataCollectionGroupId>"
    "%s<dataCollectionNumber>%d</dataCollectionNumber>"
    "<startTime>%s</startTime>"
    "<runStatus>%s</runStatus>"
    "<axisStart>%.2f</axisStart>"
    "<axisEnd>%.2f</axisEnd>"
    "<axisRange>%.2f</axisRange>"
    "<overlap>%.2f</overlap>"
    "<numberOfImages>%d</numberOfImages>"
    "<startImageNumber>%d</startImageNumber>"
    "<numberOfPasses>%d</numberOfPasses>"
    "<exposureTime>%.3f</exposureTime>"
    "<imageDirectory>%s</imageDirectory>"
    "<imagePrefix>%s</imagePrefix>"
    "<imageSuffix>%s</imageSuffix>"
    "<fileTemplate>%s</fileTemplate>"
    "<wavelength>%.6f</wavelength>"
    "<resolution>%.2f</resolution>"
    "<detectorDistance>%.6f</detectorDistance>"
    "<xBeam>%.6f</xBeam>"
    "<yBeam>%.6f</yBeam>"
    "<comments>{comments}</comments>"
    "<printableForReport>%d</printableForReport>"
    "<slitGapVertical>%.6f</slitGapVertical>"
    "<slitGapHorizontal>%.6f</slitGapHorizontal>"
    "<transmission>%.6f</transmission>"
    "<synchrotronMode>%s</synchrotronMode>"
    "<xtalSnapshotFullPath1>%s</xtalSnapshotFullPath1>"
    "<xtalSnapshotFullPath2>%s</xtalSnapshotFullPath2>"
    "<xtalSnapshotFullPath3>%s</xtalSnapshotFullPath3>"
    "<xtalSnapshotFullPath4>%s</xtalSnapshotFullPath4>"
    "<rotationAxis>%s</rotationAxis>"
    "<phiStart>%.1f</phiStart>"
    "<chiStart>%.1f</chiStart>"
    "<kappaStart>%.1f</kappaStart>"
    "<omegaStart>%.1f</omegaStart>"
    "<undulatorGap1>%.6f</undulatorGap1>"
    "<beamSizeAtSampleX>%.2f</beamSizeAtSampleX>"
    "<beamSizeAtSampleY>%.2f</beamSizeAtSampleY>"
    "<flux>%.6f</flux>"
    "<focalSpotSizeAtSampleX>%d</focalSpotSizeAtSampleX>"
    "<focalSpotSizeAtSampleY>%d</focalSpotSizeAtSampleY>"
    "</DataCollection>"
)

dc_endtime_temp_xml = (
    '<?xml version="1.0" encoding="ISO-8859-1"?>'
    "<DataCollection>"
    "<dataCollectionId>%d</dataCollectionId>"
    "<endTime>%s</endTime>"
    "</DataCollection>"
)

dcg_endtime_temp_xml = (
    '<?xml version="1.0" encoding="ISO-8859-1"?>'
    "<DataCollectionGroup>"
    "<dataCollectionGroupId>%d</dataCollectionGroupId>"
    "<endTime>%s</endTime>"
    "</DataCollectionGroup>"
)


class DbserverClient:
    def __init__(self, _host=DBSERVER_HOST, _port=DBSERVER_PORT):
        self.DB_host = _host
        self.DB_port = _port
        print(f"Connection parameters are: {self.DB_host}:{self.DB_port}")

    def _send(self, _path, _xml):
        conn = http.client.HTTPConnection(f"{self.DB_host}:{self.DB_port}")
        _xml = _xml.encode("latin-1")
        try:
            conn.putrequest("POST", str(_path))
            conn.putheader("Host", self.DB_host)
            conn.putheader("Content-type", "text/xml")
            conn.putheader("Content-length", len(_xml))
            conn.putheader("Accept", "text/xml")
            conn.endheaders()
            conn.send(_xml)
        except OSError:
            conn.close()
            sys.exit("socket.error - is the server available?")
        try:
            response = conn.getresponse()
        except http.client.BadStatusLine:
            conn.close()
            sys.exit("http.client.BadStatusLine: Unknown status code.")
        except OSError:
            conn.close()
            sys.exit(
                "socket.error - is the client authorised to connect to the server?"
            )

        xml = None
        if response.status < 400 or response.status >= 600:
            lengthstr = response.getheader("Content-length")
            if lengthstr:
                length = int(lengthstr)
                xml = response.read(length)  # get the raw XML
                xml = xml.decode("latin-1")
                print(xml)
            else:
                conn.close()
                sys.exit("No Content-length in received header.")
        else:
            conn.close()
            sys.exit(str(response.status) + ": " + response.reason)

        conn.close()
        return xml

    def storeBLSample(self, xml):
        xml_dbstatus_returned = self._send("/store_blsample_request", xml)

        e = ET.fromstring(xml_dbstatus_returned)
        flatcode = flatten_xml(e, "code")
        if flatcode != "ok":
            sys.exit(flatten_xml(e, "message"))

        blsampleid = flatten_xml(e, "blSampleId")
        if not blsampleid.isdigit():
            print(xml_dbstatus_returned)
            sys.exit("No BLSampleID found in output")
        return int(blsampleid)

    def storeDataCollectionGroup(self, xml):
        xml_dbstatus_returned = self._send("/store_object_request", xml)

        e = ET.fromstring(xml_dbstatus_returned)
        flatcode = flatten_xml(e, "code")
        if flatcode != "ok":
            sys.exit(flatten_xml(e, "message"))

        dcgid = flatten_xml(e, "dataCollectionGroupId")
        if not dcgid.isdigit():
            print(xml_dbstatus_returned)
            sys.exit("No DCGID found in output")
        return int(dcgid)

    def storeGridInfo(self, xml):
        xml_dbstatus_returned = self._send("/store_object_request", xml)

        e = ET.fromstring(xml_dbstatus_returned)
        flatcode = flatten_xml(e, "code")
        if flatcode != "ok":
            sys.exit(flatten_xml(e, "message"))

        gridid = flatten_xml(e, "gridInfoId")
        if not gridid.isdigit():
            print(xml_dbstatus_returned)
            sys.exit("No GridInfoID found in output")
        return int(gridid)

    def storeDataCollection(self, xml):
        xml_dbstatus_returned = self._send("/store_object_request", xml)

        e = ET.fromstring(xml_dbstatus_returned)
        flatcode = flatten_xml(e, "code")
        if flatcode != "ok":
            sys.exit(flatten_xml(e, "message"))

        dcid = flatten_xml(e, "dataCollectionId")
        if not dcid.isdigit():
            print(xml_dbstatus_returned)
            sys.exit("No DCID found in output")
        return int(dcid)

    def updateDbObject(self, xml):
        xml_dbstatus_returned = self._send("/update_object_request", xml)

        e = ET.fromstring(xml_dbstatus_returned)
        flatcode = flatten_xml(e, "code")
        if flatcode != "ok":
            print(xml_dbstatus_returned)
            sys.exit(flatten_xml(e, "message"))
