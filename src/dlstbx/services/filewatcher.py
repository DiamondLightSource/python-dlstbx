from __future__ import annotations

import abc
import contextlib
import logging
import os
import time
from collections.abc import Iterable
from typing import Any

import h5py
import pkg_resources
import workflows.recipe
from workflows.services.common_service import CommonService

from dlstbx.swmr import h5check


def is_file_selected(file_number, selection, total_files):
    """
    Checks if item number 'file_number' is in a list of 'selection'
    evenly spread out items out of a list of 'total_files' items,
    without constructing the full list of selected items.

    :param: file_number: positive number between 1 and total_files
    :param: selection: number of files to be selected out of total_files
    :param: total_files: number of total files
    :return: True if file_number would be selected, False otherwise.
    """
    return total_files <= selection or file_number in (
        total_files,
        1
        + round(file_number * (selection - 1) // total_files)
        * total_files
        // (selection - 1),
    )


class _Profiler:
    """
    A helper class that can record summary statistics on time spent in
    code blocks. Example usage:

    profiler = _Profiler()
    with profiler.record():
        ...
    print(profiler.mean)
    print(profiler.max)
    """

    def __init__(self):
        self._timing_max = None
        self._timing_sum = None
        self._timing_count = None

    @contextlib.contextmanager
    def record(self):
        start = time.time()
        try:
            yield
        finally:
            runtime = time.time() - start
            if self._timing_count:
                self._timing_count += 1
                self._timing_sum += runtime
                if runtime > self._timing_max:
                    self._timing_max = runtime
            else:
                self._timing_count = 1
                self._timing_sum = runtime
                self._timing_max = runtime

    @property
    def max(self):
        return self._timing_max

    @property
    def mean(self):
        if self._timing_count:
            return self._timing_sum / self._timing_count
        else:
            return None


class Watcher(abc.ABC):
    @abc.abstractmethod
    def __init__(self, parameters: dict, logger: logging.Logger):
        pass

    @abc.abstractmethod
    def watch(self, start: int) -> Iterable[dict[str, Any]]:
        pass


class FileListWatcher(Watcher):
    """
    Watch for a given list of files.
    """

    def __init__(self, parameters: dict, **kwargs):
        self.filelist = parameters["list"]
        self.filecount = len(self.filelist)
        self.burst_limit = int(parameters.get("burst-limit", 100))

    def watch(self, start: int) -> Iterable[dict[str, Any]]:
        for i in range(start, min(start + self.burst_limit, self.filecount)):
            filename = self.filelist[i]
            file_seen_at = (
                time.time() if filename and os.path.isfile(filename) else None
            )

            yield {
                "file": filename,
                # "file-number": i + 1,
                "file-index": i + 1,
                "file-seen-at": file_seen_at,
            }
            if not file_seen_at:
                # exit generator early
                return


class FilePatternWatcher(Watcher):
    """
    Watch for files where the names follow a linear numeric pattern.

    E.g. "template%05d.cbf" with indices 0 to 1800.
    """

    def __init__(self, parameters: dict, **kwargs):

        # List files to wait for
        self.pattern = parameters["pattern"]
        self.pattern_start = int(parameters["pattern-start"])
        self.filecount = int(parameters["pattern-end"]) - self.pattern_start + 1
        self.burst_limit = int(parameters.get("burst-limit", 100))

        # Sanity check received message
        try:
            self.pattern % 0
        except TypeError as e:
            raise RuntimeError(
                f"Rejecting message with non-conforming pattern string: {self.pattern}"
            ) from e

    def watch(self, start: int) -> Iterable[dict[str, Any]]:
        for i in range(start, min(start + self.burst_limit, self.filecount)):
            filename = self.pattern % (self.pattern_start + i)
            file_seen_at = (
                time.time() if filename and os.path.isfile(filename) else None
            )

            yield {
                "file": filename,
                "file-number": i + 1,
                "file-index": self.pattern_start + i,
                "file-seen-at": file_seen_at,
            }
            if not file_seen_at:
                # exit generator early
                return


class SwmrWatcher(Watcher):
    """
    Watch for hdf5 files written in SWMR mode.

    This will examine the hdf5 master file to determine the number of images
    to watch for, and then look to see whether each image has been written
    to file.
    """

    def __init__(self, parameters: dict, *, log: logging.Logger, **kwargs):
        self.file_handles: dict[str, h5py.File] = {}
        self.log = log

        # List files to wait for
        self.hdf5 = parameters["hdf5"]

        self.filecount = None
        if os.path.isfile(self.hdf5):
            try:
                with h5py.File(self.hdf5, mode="r", swmr=True) as f:
                    d = f["/entry/data/data"]
                    self.dataset_files, self.file_map = h5check.get_real_frames(f, d)
                    self.filecount = len(self.file_map)
            except Exception as e:
                if not is_known_hdf5_exception(e):
                    raise RuntimeError(f"Error reading {self.hdf5}") from e
                # For some reason this means that the .nxs file is probably
                # still being written to, so quietly log the message and
                # continue, leading to the message being resubmitted for
                # another round of processing
                self.log.info(f"Error reading {self.hdf5}", exc_info=True)

        self.burst_limit = int(parameters.get("burst-limit", 100))

    def watch(self, start: int) -> Iterable[dict[str, Any]]:
        if self.filecount is None:
            yield {
                "file": self.hdf5,
                "file-number": 1,
                "file-index": 0,
                "file-seen-at": None,
            }
            # exit generator early
            return

        for i in range(start, min(start + self.burst_limit, self.filecount)):
            m, frame = self.file_map[start]
            h5_data_file, dsetname = self.dataset_files[m]

            file_seen_at = None
            if os.path.isfile(h5_data_file):
                try:
                    if h5_data_file not in self.file_handles:
                        self.file_handles[h5_data_file] = h5py.File(
                            h5_data_file, mode="r", swmr=True
                        )
                    h5_file = self.file_handles[h5_data_file]
                    dataset = h5_file[dsetname]
                    dataset.id.refresh()
                    s = dataset.id.get_chunk_info_by_coord((frame, 0, 0))
                    if s.size > 0:
                        file_seen_at = time.time()
                except Exception as e:
                    if not is_known_hdf5_exception(e):
                        raise RuntimeError(f"Error reading {h5_data_file}") from e
                    # For some reason this means that the .nxs file is probably
                    # still being written to, so quietly log the message and
                    # break, leading to the message being resubmitted for
                    # another round of processing
                    self.log.info(f"Error reading {h5_data_file}", exc_info=True)

            yield {
                "file": self.hdf5,
                "file-number": i + 1,
                "file-index": i,
                "file-seen-at": file_seen_at,
            }
            if not file_seen_at:
                # exit generator early
                return

    def __del__(self):
        # Clean up file handles
        for f in self.file_handles.values():
            f.close()


class FileWatcher(CommonService):
    """
    A service that waits for files to arrive on disk and notifies interested
    parties when they do, or don't.
    """

    # Human readable service name
    _service_name = "DLS Filewatcher"

    # Logger name
    _logger_name = "dlstbx.services.filewatcher"

    watchers = {
        f.name: f.load()
        for f in pkg_resources.iter_entry_points("zocalo.services.filewatcher.watchers")
    }

    def initializing(self):
        """
        Subscribe to the filewatcher queue. Received messages must be
        acknowledged.
        """
        self.log.info("Filewatcher starting")
        workflows.recipe.wrap_subscribe(
            self._transport,
            "filewatcher",
            self.watch_files,
            acknowledgement=True,
            log_extender=self.extend_log,
        )

    @staticmethod
    def _parse_everys(outputs):
        """
        Returns a dictionary of integers to destination names for requested
        file periods.

        :param: outputs: An iterable of strings. Those starting with "every-"
                         are picked out and parsed as "every-$number".
        :return: A dictionary of {$number: "every-$number"} entries.
        """
        # Identify periods to notify for
        everys = [k for k in outputs if isinstance(k, str) and k.startswith("every-")]
        return {int(k[6:]): k for k in everys}

    @staticmethod
    def _parse_selections(outputs):
        """
        Returns a dictionary of integers to destination names for requested
        file selections.

        :param: outputs: An iterable of strings. Those starting with "select-"
                         are picked out and parsed as "select-$number".
        :return: A dictionary of {$number: "select-$number"} entries.
        """
        # Identify selections to notify for
        selections = [
            k for k in outputs if isinstance(k, str) and k.startswith("select-")
        ]
        return {int(k[7:]): k for k in selections}

    @staticmethod
    def _notify_for_found_file(
        nth_file, filecount, selections, everys, notify_function
    ):
        """
        Sends notifications to relevant output streams.

        :param: nth_file: Number of the seen file. 1 if this is the first seen file.
        :param: filecount: Total number of files.
        :param: selections: Dictionary of {int: str} entries pointing to output
                            streams that should receive $int evenly spaced files
                            out of all files.
        :param: everys: Dictionary of {int: str} entries pointing to output
                        streams that should receive every $int file, starting
                        with the first file.
        :param: notify_function: Function called for each triggered output.
        """

        # Notify for first file
        if nth_file == 1:
            notify_function("first")

        # Notify for every file
        notify_function("every")

        # Notify for last file
        if nth_file == filecount:
            notify_function("last")

        # Notify for nth file
        notify_function(nth_file)
        notify_function(str(nth_file))

        # Notify for selections
        for m, dest in selections.items():
            if is_file_selected(nth_file, m, filecount):
                notify_function(dest)

        # Notify for every-n
        for m, dest in everys.items():
            if (nth_file - 1) % m == 0:
                notify_function(dest)

    def watch_files(self, rw, header, message):
        """Check for presence of files."""

        for name in self.watchers:
            if rw.recipe_step["parameters"].get(name):
                watcher_class = self.watchers[name]
                break
        else:
            self.log.error("Rejecting message with unknown watch target")
            rw.transport.nack(header)

        # Check if message body contains partial results from a previous run
        status = {"seen-files": 0, "start-time": time.time()}
        if isinstance(message, dict):
            status.update(message.get("filewatcher-status", {}))

        try:
            watcher = watcher_class(rw.recipe_step["parameters"], log=self.log)
        except Exception as e:
            self.log.error(e, exc_info=True)
            rw.transport.nack(header)
            return
        if watcher.filecount is None:
            pass
        filecount = watcher.filecount

        # Identify everys ('every-N' targets) to notify for
        everys = self._parse_everys(rw.recipe_step["output"])

        # Identify selections ('select-N' targets) to notify for
        selections = self._parse_selections(rw.recipe_step["output"])

        # Conditionally acknowledge receipt of the message
        txn = rw.transport.transaction_begin(subscription_id=header["subscription"])
        rw.transport.ack(header, transaction=txn)

        # Keep a record of os.stat timings
        os_stat_profiler = _Profiler()

        # Look for files
        files_found = 0
        try:
            for file_watcher_info in watcher.watch(status["seen-files"]):

                if file_watcher_info["file-seen-at"] is None:
                    break

                files_found += 1
                status["seen-files"] += 1

                def notify_function(output):
                    rw.send_to(
                        output,
                        file_watcher_info,
                        transaction=txn,
                    )

                self._notify_for_found_file(
                    status["seen-files"], filecount, selections, everys, notify_function
                )
        except Exception as e:
            self.log.error(e, exc_info=True)
            rw.transport.nack(header)
            return

        if not file_watcher_info["file"]:
            # If the only entry in the list is 'None' then there are no files to
            # watch for. Bail out early and only notify on 'finally'.
            if filecount == 1:
                self.log.debug("Empty list encountered")
                rw.send_to(
                    "finally",
                    {"files-expected": 0, "files-seen": 0, "success": True},
                    transaction=txn,
                )
                rw.transport.transaction_commit(txn)
                return

            self.log.info(
                "Filewatcher stopped after encountering empty value at position %d after %.1f seconds",
                file_watcher_info["file-index"],
                time.time() - status["start-time"],
            )

            # Notify for error
            rw.send_to(
                "error",
                {
                    "file": file_watcher_info["file"],
                    "file-index": status["seen-files"] + 1,
                    "success": False,
                },
                transaction=txn,
            )

            # Notify for 'any' target if any file was seen
            if status["seen-files"]:
                rw.send_to(
                    "any",
                    {"files-expected": filecount, "files-seen": status["seen-files"]},
                    transaction=txn,
                )

            # Notify for 'finally' outcome
            rw.send_to(
                "finally",
                {
                    "files-expected": filecount,
                    "files-seen": status["seen-files"],
                    "success": False,
                },
                transaction=txn,
            )
            # Stop processing message
            rw.transport.transaction_commit(txn)
            return

        # Are we done?
        if status["seen-files"] == filecount:
            # Happy days

            self.log.info(
                "All %d files found after %.1f seconds.",
                filecount,
                time.time() - status["start-time"],
                extra={
                    "stat-time-max": os_stat_profiler.max,
                    "stat-time-mean": os_stat_profiler.mean,
                },
            )

            rw.send_to(
                "any",
                {"files-expected": filecount, "files-seen": status["seen-files"]},
                transaction=txn,
            )
            rw.send_to(
                "finally",
                {
                    "files-expected": filecount,
                    "files-seen": status["seen-files"],
                    "success": True,
                },
                transaction=txn,
            )

            rw.transport.transaction_commit(txn)
            return

        message_delay = rw.recipe_step["parameters"].get("burst-wait")
        if files_found == 0:
            # If no files were found, check timeout conditions.
            if status["seen-files"] == 0:
                # For first file: relevant timeout is 'timeout-first', with fallback 'timeout', with fallback 1 hour
                timeout = rw.recipe_step["parameters"].get(
                    "timeout-first", rw.recipe_step["parameters"].get("timeout", 3600)
                )
                timed_out = (status["start-time"] + timeout) < time.time()
            else:
                # For subsequent files: relevant timeout is 'timeout', with fallback 1 hour
                timeout = rw.recipe_step["parameters"].get("timeout", 3600)
                timed_out = (status["last-seen"] + timeout) < time.time()
            if timed_out:
                # File watch operation has timed out.

                # Report all timeouts as warnings unless the recipe specifies otherwise
                timeoutlog = self.log.warning
                if rw.recipe_step["parameters"].get("log-timeout-as-info"):
                    timeoutlog = self.log.info

                timeoutlog(
                    "Filewatcher for file %s (index=%i) timed out after %.1f seconds (%d of %d files found, nothing seen for %.1f seconds)",
                    file_watcher_info["file"],
                    file_watcher_info["file-index"],
                    time.time() - status["start-time"],
                    status["seen-files"],
                    filecount or 0,
                    time.time() - status.get("last-seen", status["start-time"]),
                    extra={
                        "stat-time-max": os_stat_profiler.max,
                        "stat-time-mean": os_stat_profiler.mean,
                    },
                )

                # Notify for timeout
                rw.send_to(
                    "timeout",
                    {
                        "file": file_watcher_info["file"],
                        "file-index": file_watcher_info["file-index"],
                        "success": False,
                    },
                    transaction=txn,
                )
                # Notify for 'any' target if any file was seen
                if status["seen-files"]:
                    rw.send_to(
                        "any",
                        {
                            "files-expected": filecount,
                            "files-seen": status["seen-files"],
                        },
                        transaction=txn,
                    )

                # Notify for 'finally' outcome
                rw.send_to(
                    "finally",
                    {
                        "files-expected": filecount,
                        "files-seen": status["seen-files"],
                        "success": False,
                    },
                    transaction=txn,
                )
                # Stop processing message
                rw.transport.transaction_commit(txn)
                return

            # If no timeouts are triggered, set a minimum waiting time.
            if message_delay:
                message_delay = max(1, message_delay)
            else:
                message_delay = 1
            self.log.debug(
                (
                    "No further files found after a total time of {time:.1f} seconds\n"
                    "{files_seen} of {files_total} files seen so far"
                ).format(
                    time=time.time() - status["start-time"],
                    files_seen=status["seen-files"],
                    files_total=filecount,
                ),
                extra={
                    "stat-time-max": os_stat_profiler.max,
                    "stat-time-mean": os_stat_profiler.mean,
                },
            )
        else:
            # Otherwise note last time progress was made
            status["last-seen"] = time.time()
            self.log.info(
                "%d files with indices %d-%d (out of %d) found within %.1f seconds",
                files_found,
                status["seen-files"] - files_found + 1,
                status["seen-files"],
                filecount,
                time.time() - status["start-time"],
                extra={
                    "stat-time-max": os_stat_profiler.max,
                    "stat-time-mean": os_stat_profiler.mean,
                },
            )

        # Send results to myself for next round of processing
        rw.checkpoint(
            {"filewatcher-status": status}, delay=message_delay, transaction=txn
        )
        rw.transport.transaction_commit(txn)


def is_known_hdf5_exception(exception):
    # Known ephemeral errors that are likely the result of race conditions when
    # an hdf5 file has started been written to, but is not yet valid for reading.
    known_error_messages = {
        "Unable to open object (address of object past end of allocation)",
        "Unable to open file (truncated file: eof =",
        "Unable to open file (file is not already open for SWMR writing)",
        "Can't deserialize object header prefix (bad object header version number)",
    }
    return any(msg in str(exception) for msg in known_error_messages)
