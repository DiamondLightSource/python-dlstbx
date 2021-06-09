import sys
import xml.etree.cElementTree as ET

import http.client


def flatten_xml(xml, tag):
    return "".join("\n".join(i for t in xml.iter(tag) for i in t.itertext()))


class DbserverClient:
    def __init__(self, _host, _port):
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
