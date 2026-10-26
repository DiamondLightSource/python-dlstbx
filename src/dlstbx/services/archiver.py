from __future__ import annotations

import errno
import itertools
import os
import os.path
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any, Optional

import pydantic
import workflows.recipe
from workflows.recipe.typed import typed_handler
from workflows.services.common_service import CommonService

from dlstbx.util import INDUSTRIAL_CODES


def _hyphenate(name: str) -> str:
    return name.replace("_", "-")


class _ArchiverModel(pydantic.BaseModel):
    """Recipe parameters and message fields are named with hyphens"""

    model_config = pydantic.ConfigDict(alias_generator=_hyphenate)


class ArchiverParameters(_ArchiverModel):
    visit: str
    beamline: str
    limit_files: int = 0
    log_file_warnings_as_info: bool = False
    log_summary_warning_as_info: bool = False
    dropfile: Optional[str] = None
    dropfile_dir: Optional[str] = None
    dropfile_filename: Optional[str] = None
    dropfile_queue: Optional[str] = None


class PatternParameters(ArchiverParameters):
    pattern: str
    pattern_start: int
    pattern_end: int
    multipart: Optional[int] = None


class FilelistParameters(ArchiverParameters):
    filelist: list[str] = []


def _none_unless_dict(value: Any) -> Any:
    """Messages that are not dictionaries carry no checkpoint state"""
    return value if isinstance(value, dict) else None


class PatternCheckpoint(_ArchiverModel):
    """State carried over from a previous part of a split archiving job"""

    archive_multipart: Optional[int] = None
    archive_pattern_start: int = 0


class FilelistCheckpoint(_ArchiverModel):
    """State carried over from a previous part of a split archiving job"""

    archive_multipart: int = 1
    filelist: list[str] = []


PatternMessage = Annotated[
    Optional[PatternCheckpoint], pydantic.BeforeValidator(_none_unless_dict)
]
FilelistMessage = Annotated[
    Optional[FilelistCheckpoint], pydantic.BeforeValidator(_none_unless_dict)
]


class Dropfile:
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
        investigation.append(ET.Comment("Producer: Zocalo dlstbx.services.archiver"))

        ET.SubElement(investigation, "inv_number").text = (
            visit.split("-")[0] if "-" in visit else visit
        )
        ET.SubElement(investigation, "visit_id").text = visit
        ET.SubElement(investigation, "instrument").text = beamline
        ET.SubElement(investigation, "title").text = "dont need it"
        ET.SubElement(investigation, "inv_type").text = "experiment"

        self._dataset = ET.SubElement(investigation, "dataset")
        ET.SubElement(self._dataset, "name").text = datasetname
        ET.SubElement(self._dataset, "dataset_type").text = "raw"
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
        self.subscribe("archive.pattern", self.archive_dcid)
        self.subscribe("archive.filelist", self.archive_filelist)

    @staticmethod
    def rangifier(numbers):
        """Convert lists into lists of ranges."""
        numbers = sorted(set(numbers))
        for _, group in itertools.groupby(enumerate(numbers), lambda t: t[1] - t[0]):
            group = list(group)
            yield group[0][1], group[-1][1]

    def visit_is_archivable(
        self, visit: str, allowed_industrial_visit_codes: tuple[str, ...]
    ) -> bool:
        """Return false if visit has an industrial code that is not in the list of allowed industrial visit codes."""
        if visit.startswith(tuple(INDUSTRIAL_CODES)) and not visit.startswith(
            allowed_industrial_visit_codes
        ):
            return False
        return True

    @typed_handler
    def archive_dcid(
        self,
        rw: workflows.recipe.RecipeWrapper,
        header: dict,
        message: PatternMessage,
        parameters: PatternParameters,
    ):
        """Archive collected datafiles connected to a data collection."""

        # Conditionally acknowledge receipt of the message
        txn = self._transport.transaction_begin(subscription_id=header["subscription"])
        self._transport.ack(header, transaction=txn)

        allowed_industrial_visit_codes = tuple(
            self.config.storage.get(
                "zocalo.archiver.allowed_industrial_visit_codes", []
            )
        )
        if not self.visit_is_archivable(
            parameters.visit, allowed_industrial_visit_codes
        ):
            self.log.info(
                f"Skipping archiving of {parameters.pattern} because it is from a forbidden visit"
            )
            self._transport.transaction_commit(txn)
            return

        self.log.info("Attempting to archive %s", parameters.pattern)

        multipart = parameters.multipart
        pattern_start = parameters.pattern_start
        if message is not None:
            if "archive_multipart" in message.model_fields_set:
                multipart = message.archive_multipart
            if "archive_pattern_start" in message.model_fields_set:
                pattern_start = message.archive_pattern_start

        file_range_limit = parameters.limit_files

        filepath = Path(parameters.pattern)
        dataset_name = Path(*filepath.parts[6:-1]).as_posix() or "topdir"
        beamline = parameters.beamline
        visit_id = parameters.visit

        df = Dropfile(visit_id.upper(), beamline, dataset_name)

        message_out = {"success": 0, "failed": 0}
        files_not_found: list[str] = []
        files_found_past_missing_file = False
        for x in range(pattern_start, parameters.pattern_end + 1):
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
                    {
                        "archive-multipart": multipart + 1,
                        "archive-pattern-start": x,
                    },
                    transaction=txn,
                )
                break

            filename = parameters.pattern % x

            try:
                df.add(filename)
                files_found_past_missing_file = bool(files_not_found)
            except OSError as e:
                if e.errno == errno.ENOENT:
                    files_not_found.append(filename)
                else:
                    # Report all missing files as warnings unless recipe says otherwise
                    if parameters.log_file_warnings_as_info:
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
            if not message_out["success"]:
                rw.send_to("all_files_missing", files_not_found, transaction=txn)
        if message_out["failed"]:
            if parameters.log_summary_warning_as_info:
                self.log.info("Failed to archive %d files", message_out["failed"])
            else:
                self.log.warning("Failed to archive %d files", message_out["failed"])

        xml_string = df.to_string()
        dropfile = parameters.dropfile
        if dropfile == "{dropfile_override}":
            dropfile = None
        if not dropfile and parameters.dropfile_dir and parameters.dropfile_filename:
            dropfile = os.path.join(
                parameters.dropfile_dir, parameters.dropfile_filename
            )
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
        message_out["xml"] = xml_string.decode("latin-1")

        dropqueue = parameters.dropfile_queue
        if dropqueue:
            self._transport.raw_send(dropqueue, xml_string, ignore_namespace=True)

        rw.set_default_channel("dropfile")
        rw.send_to("dropfile", message_out, transaction=txn)

        self._transport.transaction_commit(txn)
        self.log.info("%d files archived", message_out["success"])

    @typed_handler
    def archive_filelist(
        self,
        rw: workflows.recipe.RecipeWrapper,
        header: dict,
        message: FilelistMessage,
        parameters: FilelistParameters,
    ):
        """Archive an arbitrary list of files."""

        multipart: Optional[int] = None
        filelist = parameters.filelist
        if message is not None:
            multipart = message.archive_multipart
            if "filelist" in message.model_fields_set:
                filelist = message.filelist
        if not filelist:
            self.log.warning("Attempted to archive an empty list of files.")
            self._transport.nack(header)
            return
        self.log.info(
            "Attempting to archive list of %d files, starting with %s",
            len(filelist),
            filelist[0],
        )
        file_range_limit = parameters.limit_files

        # Conditionally acknowledge receipt of the message
        txn = self._transport.transaction_begin(subscription_id=header["subscription"])
        self._transport.ack(header, transaction=txn)

        visit_id = parameters.visit
        beamline = parameters.beamline
        allowed_industrial_visit_codes = tuple(
            self.config.storage.get(
                "zocalo.archiver.allowed_industrial_visit_codes", []
            )
        )
        if not self.visit_is_archivable(visit_id, allowed_industrial_visit_codes):
            self.log.info(
                f"Skipping archiving of {filelist} because it is from a forbidden visit"
            )
            self._transport.transaction_commit(txn)
            return

        filepath = Path(filelist[0])
        dataset_name = Path(*filepath.parts[6:-1]).as_posix() or "topdir"
        # Archive files
        df = Dropfile(visit_id.upper(), beamline, dataset_name)

        message_out = {"success": 0, "failed": 0}
        files_not_found: list[str] = []
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
                    if parameters.log_file_warnings_as_info:
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
            if parameters.log_summary_warning_as_info:
                self.log.info("Failed to archive %d files", message_out["failed"])
            else:
                self.log.warning("Failed to archive %d files", message_out["failed"])

        xml_string = df.to_string()
        dropfile = parameters.dropfile
        if dropfile == "{dropfile_override}":
            dropfile = None
        if (
            not dropfile
            and parameters.dropfile_dir is not None
            and parameters.dropfile_filename is not None
        ):
            dropfile = os.path.join(
                parameters.dropfile_dir, parameters.dropfile_filename
            )
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
        message_out["xml"] = xml_string.decode("latin-1")

        dropqueue = parameters.dropfile_queue
        if dropqueue:
            self._transport.raw_send(dropqueue, xml_string, ignore_namespace=True)

        rw.set_default_channel("dropfile")
        rw.send_to("dropfile", message_out, transaction=txn)

        self._transport.transaction_commit(txn)
        self.log.info("Done.")
