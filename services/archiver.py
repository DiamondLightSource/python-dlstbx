from __future__ import absolute_import, division, print_function

import errno
import itertools
import os
import os.path
import xml.etree.cElementTree as ET
from datetime import datetime

import workflows.recipe
from workflows.services.common_service import CommonService


class Dropfile(object):
    """A class encapsulating the XML dropfile tree as it is built up."""

    def __init__(self, visit, beamline, datasetname):
        """Create the basic XML structure from given information."""
        self._closed = False

        self._xml = ET.Element("icat")
        self._xml.set("version", "1.0 RC6")
        self._xml.set("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
        self._xml.set("xsi:noNamespaceSchemaLocation", "icatXSD.xsd")

        visit = visit.upper()

        study = ET.SubElement(self._xml, "study")
        investigation = ET.SubElement(study, "investigation")
        ET.SubElement(investigation, "inv_number").text = (
            visit.split("-")[0] if "-" in visit else visit
        )
        ET.SubElement(investigation, "visit_id").text = visit
        ET.SubElement(investigation, "instrument").text = beamline
        ET.SubElement(investigation, "title").text = "dont need it"
        ET.SubElement(investigation, "inv_type").text = "experiment"

        self._dataset = ET.SubElement(investigation, "dataset")
        ET.SubElement(self._dataset, "name").text = datasetname
        ET.SubElement(self._dataset, "dataset_type").text = "EXPERIMENT_RAW"
        ET.SubElement(self._dataset, "description").text = "unknown"

    def add(self, filename):
        """Add a file to the dropfile.
       Will throw an exception if the file does not exist."""
        assert not self._closed
        stat = os.stat(filename)
        if stat.st_size > 3 * 1024 * 1024 * 1024 * 1024:
            self.log.error(
                "Cannot archive file %s. Files larger than 3 TB are not supported by the archiving infrastructure (%s bytes).",
                filename,
                str(stat.st_size),
            )
            raise OSError("file too large for archiving")
        df = ET.SubElement(self._dataset, "datafile")
        ET.SubElement(df, "name").text = os.path.basename(filename)
        ET.SubElement(df, "location").text = filename
        ET.SubElement(df, "description").text = "unknown"
        ET.SubElement(df, "datafile_version").text = "1.0"
        ET.SubElement(df, "datafile_create_time").text = datetime.fromtimestamp(
            stat.st_mtime
        ).strftime("%Y-%m-%dT%H:%M:%S")
        ET.SubElement(df, "datafile_modify_time").text = datetime.fromtimestamp(
            stat.st_mtime
        ).strftime("%Y-%m-%dT%H:%M:%S")
        # both are set to time of last modification
        ET.SubElement(df, "file_size").text = str(stat.st_size)

    def close(self):
        """Do not accept any more entries for this dropfile."""
        self._closed = True

        def indent(elem, level=0):
            i = "\n" + level * "  "
            if len(elem):
                if not elem.text or not elem.text.strip():
                    elem.text = i + "  "
                if not elem.tail or not elem.tail.strip():
                    elem.tail = i
                for elem in elem:
                    indent(elem, level + 1)
                if not elem.tail or not elem.tail.strip():
                    elem.tail = i
            else:
                if level and (not elem.tail or not elem.tail.strip()):
                    elem.tail = i

        indent(self._xml)

    def to_string(self):
        """Return the dropfile as formatted XML bytestring."""
        if not self._closed:
            self.close()
        return b'<?xml version="1.0" ?>\n' + ET.tostring(self._xml)


class DLSArchiver(CommonService):
    """A service that generates dropfiles for data collections."""

    # Human readable service name
    _service_name = "DLS Archiver"

    # Logger name
    _logger_name = "dlstbx.services.archiver"

    def initializing(self):
        """Subscribe to the archiver queue. Received messages must be
       acknowledged."""
        self.log.info("Archiver starting")
        workflows.recipe.wrap_subscribe(
            self._transport,
            "archive.pattern",
            self.archive_dcid,
            acknowledgement=True,
            log_extender=self.extend_log,
        )
        workflows.recipe.wrap_subscribe(
            self._transport,
            "archive.filelist",
            self.archive_filelist,
            acknowledgement=True,
            log_extender=self.extend_log,
        )

    @staticmethod
    def rangifier(numbers):
        """Convert lists into lists of ranges."""
        numbers = sorted(set(numbers))
        for _, group in itertools.groupby(enumerate(numbers), lambda t: t[1] - t[0]):
            group = list(group)
            yield group[0][1], group[-1][1]

    def archive_dcid(self, rw, header, message):
        """Archive collected datafiles connected to a data collection."""

        # Conditionally acknowledge receipt of the message
        txn = self._transport.transaction_begin()
        self._transport.ack(header, transaction=txn)

        # Extract parameters from the recipe
        params = rw.recipe_step["parameters"]
        self.log.info("Attempting to archive %s", params["pattern"])

        settings = params.copy()
        if isinstance(message, dict):
            for field in ("multipart", "pattern-start"):
                if "archive-" + field in message:
                    settings[field] = message["archive-" + field]

        file_range_limit = int(settings.get("limit-files", 0))

        filepaths = params["pattern"].split("/")
        _, _, beamline, _, _, visit_id = filepaths[0:6]

        df = Dropfile(visit_id.upper(), beamline, "/".join(filepaths[6:-1]) or "topdir")

        message_out = {"success": 0, "failed": 0}
        files_not_found = []
        files_found_past_missing_file = False
        for x in range(
            int(settings["pattern-start"]), int(settings["pattern-end"]) + 1
        ):
            if file_range_limit and message_out["success"] >= file_range_limit:
                # Test for limit at beginning, not end, so >= 1 file remains
                self.log.info(
                    "Reached dropfile limit of %d entries, splitting job.",
                    file_range_limit,
                )
                # limit reached - bail out
                if not settings.get("multipart"):
                    settings["multipart"] = 1
                rw.checkpoint(
                    {
                        "archive-multipart": settings["multipart"] + 1,
                        "archive-pattern-start": x,
                    },
                    transaction=txn,
                )
                break

            filename = params["pattern"] % x

            try:
                df.add(filename)
                files_found_past_missing_file = bool(files_not_found)
            except OSError as e:
                if e.errno == errno.ENOENT:
                    files_not_found.append(filename)
                else:
                    # Report all missing files as warnings unless recipe says otherwise
                    if params.get("log-file-warnings-as-info"):
                        self.log.info("Could not archive %s", filename, exc_info=True)
                    else:
                        self.log.warning(
                            "Could not archive %s", filename, exc_info=True
                        )
                message_out["failed"] += 1
                continue
            message_out["success"] += 1
        if files_not_found:
            if files_found_past_missing_file:
                self.log.error(
                    "The following files were not found. Files are missing from within the pattern!\n%s",
                    "\n".join(files_not_found),
                )
                rw.send_to("missing_files_within", files_not_found, transaction=txn)
            else:
                self.log.info(
                    "The following files were not found:\n%s",
                    "\n".join(files_not_found),
                )
            rw.send_to("missing_files", files_not_found, transaction=txn)
        if message_out["failed"]:
            if params.get("log-summary-warning-as-info"):
                self.log.info("Failed to archive %d files", message_out["failed"])
            else:
                self.log.warning("Failed to archive %d files", message_out["failed"])

        xml_string = df.to_string()
        dropfile = params.get("dropfile")
        if dropfile == "{dropfile_override}":
            dropfile = None
        if (
            not dropfile
            and params.get("dropfile-dir")
            and params.get("dropfile-filename")
        ):
            dropfile = os.path.join(params["dropfile-dir"], params["dropfile-filename"])
        if dropfile:
            timestamp = datetime.strftime(datetime.now(), "%Y%m%d-%H%M%S")
            multipart_label = (
                "-" + str(settings["multipart"]) if settings.get("multipart") else ""
            )
            dropfile = dropfile.format(
                visit_id=visit_id,
                beamline=beamline,
                timestamp=timestamp,
                multipart=multipart_label,
            )
            if message_out["success"]:
                with open(dropfile, "w") as fh:
                    fh.write(xml_string)
                self.log.info("Written dropfile XML to %s", dropfile)
            else:
                self.log.info("Skipped writing empty dropfile XML to %s", dropfile)
        message_out["xml"] = xml_string

        dropqueue = params.get("dropfile-queue")
        if dropqueue:
            self._transport.raw_send(dropqueue, xml_string, ignore_namespace=True)

        rw.set_default_channel("dropfile")
        rw.send_to("dropfile", message_out, transaction=txn)

        self._transport.transaction_commit(txn)
        self.log.info("%d files archived", message_out["success"])

    def archive_filelist(self, rw, header, message):
        """Archive an arbitrary list of files."""

        # Extract parameters
        params = rw.recipe_step["parameters"]
        if isinstance(message, dict):
            multipart = message.get("archive-multipart", 1)
            filelist = message.get("filelist", params.get("filelist", []))
        else:
            multipart = None
            filelist = params.get("filelist", [])
        if not isinstance(filelist, list):
            self.log.error(
                "Expected list of files to archive. Received %s.", str(type(filelist))
            )
            self._transport.nack(header)
            return
        if not filelist:
            self.log.warn("Attempted to archive an empty list of files.")
            self._transport.nack(header)
            return
        self.log.info(
            "Attempting to archive list of %d files, starting with %s",
            len(filelist),
            filelist[0],
        )
        file_range_limit = int(params.get("limit-files", 0))

        filepaths = filelist[0].split("/")
        beamline = "unknown"
        visit_id = "unknown"
        try:
            if filepaths[1:3] == ["dls", "mx"]:
                beamline = "i02-2"  # VMXi currently only beamline with new visit path structure
            else:
                beamline = filepaths[2]
            visit_id = filepaths[5]
        except IndexError:
            pass
        visit_id = params.get("visit", visit_id)
        beamline = params.get("beamline", beamline)

        # Conditionally acknowledge receipt of the message
        txn = self._transport.transaction_begin()
        self._transport.ack(header, transaction=txn)

        # Archive files
        df = Dropfile(visit_id.upper(), beamline, "/".join(filepaths[6:-1]) or "topdir")

        message_out = {"success": 0, "failed": 0}
        files_not_found = []
        for n, filename in enumerate(filelist):
            if file_range_limit and message_out["success"] >= file_range_limit:
                # Test for limit at beginning, not end, so >= 1 file remains
                self.log.info(
                    "Reached dropfile limit of %d entries, splitting job.",
                    file_range_limit,
                )
                # limit reached - bail out
                if not multipart:
                    multipart = 1
                rw.checkpoint(
                    {"archive-multipart": multipart + 1, "filelist": filelist[n:]},
                    transaction=txn,
                )
                break

            try:
                df.add(filename)
            except OSError as e:
                if e.errno == errno.ENOENT:
                    files_not_found.append(filename)
                else:
                    # Report all missing files as warnings unless recipe says otherwise
                    if params.get("log-file-warnings-as-info"):
                        self.log.info("Could not archive %s", filename, exc_info=True)
                    else:
                        self.log.warning(
                            "Could not archive %s", filename, exc_info=True
                        )
                message_out["failed"] += 1
                continue
            self.log.debug("Archived %s", filename)
            message_out["success"] += 1
        if files_not_found:
            self.log.info(
                "The following files were not found:\n%s", "\n".join(files_not_found)
            )
        self.log.info("%d files archived", message_out["success"])
        if message_out["failed"]:
            if params.get("log-summary-warning-as-info"):
                self.log.info("Failed to archive %d files", message_out["failed"])
            else:
                self.log.warning("Failed to archive %d files", message_out["failed"])

        xml_string = df.to_string()
        dropfile = params.get("dropfile")
        if dropfile == "{dropfile_override}":
            dropfile = None
        if not dropfile and all(
            k in params for k in ("dropfile-dir", "dropfile-filename")
        ):
            dropfile = os.path.join(params["dropfile-dir"], params["dropfile-filename"])
        if dropfile:
            timestamp = datetime.strftime(datetime.now(), "%Y%m%d-%H%M%S")
            multipart_label = "-" + str(multipart) if multipart else ""
            dropfile = dropfile.format(
                visit_id=visit_id,
                beamline=beamline,
                timestamp=timestamp,
                multipart=multipart_label,
            )
            if message_out["success"]:
                with open(dropfile, "wb") as fh:
                    fh.write(xml_string)
                self.log.info("Written dropfile XML to %s", dropfile)
            else:
                self.log.info("Skipped writing empty dropfile XML to %s", dropfile)
        message_out["xml"] = xml_string

        rw.set_default_channel("dropfile")
        rw.send_to("dropfile", message_out, transaction=txn)

        self._transport.transaction_commit(txn)
        self.log.info("Done.")
