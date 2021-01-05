import contextlib
import h5py
import os
import time

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


class DLSFileWatcher(CommonService):
    """
    A service that waits for files to arrive on disk and notifies interested
    parties when they do, or don't.
    """

    # Human readable service name
    _service_name = "DLS Filewatcher"

    # Logger name
    _logger_name = "dlstbx.services.filewatcher"

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

    def watch_files(self, rw, header, message):
        """Check for presence of files."""
        if rw.recipe_step["parameters"].get("pattern"):
            self.watch_files_pattern(rw, header, message)
        elif rw.recipe_step["parameters"].get("list") is not None:
            self.watch_files_list(rw, header, message)
        elif rw.recipe_step["parameters"].get("hdf5") is not None:
            self.watch_files_swmr(rw, header, message)
        else:
            self.log.error("Rejecting message with unknown watch target")
            rw.transport.nack(header)

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

    def watch_files_list(self, rw, header, message):
        """
        Watch for a given list of files.
        """
        # Check if message body contains partial results from a previous run
        status = {"seen-files": 0, "start-time": time.time()}
        if isinstance(message, dict):
            status.update(message.get("filewatcher-status", {}))

        # List files to wait for
        filelist = rw.recipe_step["parameters"]["list"]
        filecount = len(filelist)

        # If the only entry in the list is 'None' then there are no files to
        # watch for. Bail out early and only notify on 'finally'.
        if filecount == 1 and filelist[0] is None:
            self.log.debug("Empty list encountered")
            txn = rw.transport.transaction_begin()
            rw.transport.ack(header, transaction=txn)
            rw.send_to(
                "finally",
                {"files-expected": 0, "files-seen": 0, "success": True},
                transaction=txn,
            )
            rw.transport.transaction_commit(txn)
            return

        # Identify everys ('every-N' targets) to notify for
        everys = self._parse_everys(rw.recipe_step["output"])

        # Identify selections ('select-N' targets) to notify for
        selections = self._parse_selections(rw.recipe_step["output"])

        # Conditionally acknowledge receipt of the message
        txn = rw.transport.transaction_begin()
        rw.transport.ack(header, transaction=txn)

        # Keep a record of os.stat timings
        os_stat_profiler = _Profiler()

        # Look for files
        files_found = 0
        while (
            status["seen-files"] < filecount
            and files_found < rw.recipe_step["parameters"].get("burst-limit", 100)
            and filelist[status["seen-files"]]
        ):
            with os_stat_profiler.record():
                if not os.path.isfile(filelist[status["seen-files"]]):
                    break

            files_found += 1
            status["seen-files"] += 1

            def notify_function(output):
                rw.send_to(
                    output,
                    {
                        "file": filelist[status["seen-files"] - 1],
                        "file-list-index": status["seen-files"],
                    },
                    transaction=txn,
                )

            self._notify_for_found_file(
                status["seen-files"], filecount, selections, everys, notify_function
            )

        # Are we done?
        if status["seen-files"] == filecount:
            # Happy days

            self.log.info(
                "All %d files in list found after %.1f seconds.",
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

        if not filelist[status["seen-files"]]:
            # 'None' value or empty string encountered. Stop watching here.

            self.log.info(
                "Filewatcher stopped after encountering empty value at position %d after %.1f seconds",
                status["seen-files"] + 1,
                time.time() - status["start-time"],
            )

            # Notify for error
            rw.send_to(
                "error",
                {
                    "file": filelist[status["seen-files"]],
                    "file-list-index": status["seen-files"] + 1,
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
                    "Filewatcher for file %s timed out after %.1f seconds (%d of %d files found, nothing seen for %.1f seconds)",
                    filelist[status["seen-files"]],
                    time.time() - status["start-time"],
                    status["seen-files"],
                    filecount,
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
                        "file": filelist[status["seen-files"]],
                        "file-list-index": status["seen-files"] + 1,
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
                    "No further files in list found after a total time of {time:.1f} seconds\n"
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
                "%d files with list indices %d-%d (out of %d) found within %.1f seconds",
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

    def watch_files_pattern(self, rw, header, message):
        """
        Watch for files where the names follow a linear numeric pattern,
        eg. "template%05d.cbf" with indices 0 to 1800.
        """
        # Check if message body contains partial results from a previous run
        status = {"seen-files": 0, "start-time": time.time()}
        if isinstance(message, dict):
            status.update(message.get("filewatcher-status", {}))

        # List files to wait for
        pattern = rw.recipe_step["parameters"]["pattern"]
        pattern_start = int(rw.recipe_step["parameters"]["pattern-start"])
        filecount = int(rw.recipe_step["parameters"]["pattern-end"]) - pattern_start + 1

        # Sanity check received message
        try:
            pattern % 0
        except TypeError:
            self.log.error(
                "Rejecting message with non-conforming pattern string: %s", pattern
            )
            rw.transport.nack(header)
            return

        # Identify everys ('every-N' targets) to notify for
        everys = self._parse_everys(rw.recipe_step["output"])

        # Identify selections ('select-N' targets) to notify for
        selections = self._parse_selections(rw.recipe_step["output"])

        # Conditionally acknowledge receipt of the message
        txn = rw.transport.transaction_begin()
        rw.transport.ack(header, transaction=txn)

        # Keep a record of os.stat timings
        os_stat_profiler = _Profiler()

        # Look for files
        files_found = 0
        while status["seen-files"] < filecount and files_found < rw.recipe_step[
            "parameters"
        ].get("burst-limit", 100):
            filename = pattern % (pattern_start + status["seen-files"])
            with os_stat_profiler.record():
                if not os.path.isfile(filename):
                    break

            files_found += 1
            status["seen-files"] += 1

            def notify_function(output):
                rw.send_to(
                    output,
                    {
                        "file": filename,
                        "file-number": status["seen-files"],
                        "file-pattern-index": pattern_start + status["seen-files"] - 1,
                    },
                    transaction=txn,
                )

            self._notify_for_found_file(
                status["seen-files"], filecount, selections, everys, notify_function
            )

        # Are we done?
        if status["seen-files"] == filecount:
            # Happy days

            self.log.debug(
                "%d files found for %s with indices %d-%d (all %d files found)",
                files_found,
                pattern,
                pattern_start + status["seen-files"] - files_found,
                pattern_start + status["seen-files"] - 1,
                filecount,
            )

            extra_log = {
                "delay": time.time() - status["start-time"],
                "stat-time-max": os_stat_profiler.max,
                "stat-time-mean": os_stat_profiler.mean,
            }
            if rw.recipe_step["parameters"].get("expected-per-image-delay"):
                # Estimate unexpected delay
                try:
                    expected_delay = (
                        float(rw.recipe_step["parameters"]["expected-per-image-delay"])
                        * filecount
                    )
                except ValueError:
                    # in case the field contains "None" or equivalent un-floatable nonsense
                    self.log.warning(
                        "Ignored invalid expected-per-image-delay value (%r)",
                        rw.recipe_step["parameters"]["expected-per-image-delay"],
                    )
                else:
                    extra_log["unexpected_delay"] = max(
                        0, extra_log["delay"] - expected_delay
                    )

            self.log.info(
                "All %d files found for %s after %.1f seconds.",
                filecount,
                rw.recipe_step["parameters"]["pattern"],
                time.time() - status["start-time"],
                extra=extra_log,
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
                    "Filewatcher for %s timed out after %.1f seconds (%d files found, nothing seen for %.1f seconds)",
                    rw.recipe_step["parameters"]["pattern"],
                    time.time() - status["start-time"],
                    status["seen-files"],
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
                        "file": pattern % (pattern_start + status["seen-files"]),
                        "file-number": status["seen-files"] + 1,
                        "file-pattern-index": pattern_start + status["seen-files"],
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
                    "No further files found for {pattern} after a total time of {time:.1f} seconds\n"
                    "{files_seen} of {files_total} files seen so far"
                ).format(
                    time=time.time() - status["start-time"],
                    pattern=rw.recipe_step["parameters"]["pattern"],
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
                "%d files found for %s with indices %d-%d (total: %d out of %d) within %.1f seconds",
                files_found,
                rw.recipe_step["parameters"]["pattern"],
                pattern_start + status["seen-files"] - files_found,
                pattern_start + status["seen-files"] - 1,
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

    def watch_files_swmr(self, rw, header, message):
        """
        Watch for hdf5 files written in SWMR mode.

        This will examine the hdf5 master file to determine the number of images
        to watch for, and then look to see whether each image has been written
        to file.
        """
        # Check if message body contains partial results from a previous run
        status = {"seen-images": 0, "start-time": time.time(), "image-count": None}
        if isinstance(message, dict):
            status.update(message.get("filewatcher-status", {}))

        # Keep a record of os.stat timings
        os_stat_profiler = _Profiler()

        hdf5 = rw.recipe_step["parameters"]["hdf5"]
        image_count = None
        with os_stat_profiler.record():
            if os.path.isfile(hdf5):
                print(f"Opening {hdf5}")
                try:
                    with h5py.File(hdf5, "r", swmr=True) as f:
                        d = f["/entry/data/data"]
                        t0 = time.time()
                        dataset_files, file_map = h5check.get_real_frames(f, d)
                        image_count = len(file_map)
                        t1 = time.time()
                        self.log.debug(f"Number of data files: {len(dataset_files)}")
                        self.log.debug(f"Number of images: {image_count}")
                        self.log.debug(f"hdf5 setup took {t1-t0:.3f}s")
                        self.log.debug(f"dataset_files: {dataset_files}")
                        self.log.debug(f"file_map: {file_map}")
                except Exception:
                    self.log.warning(f"Error reading {hdf5}", exc_info=True)
                    rw.transport.nack(header)
                    return

        # Identify everys ('every-N' targets) to notify for
        everys = self._parse_everys(rw.recipe_step["output"])

        # Identify selections ('select-N' targets) to notify for
        selections = self._parse_selections(rw.recipe_step["output"])

        # Conditionally acknowledge receipt of the message
        txn = rw.transport.transaction_begin()
        rw.transport.ack(header, transaction=txn)

        # Cache file handles locally to minimise repeatedly re-opening the same data file(s)
        file_handles = {}

        # Look for images
        images_found = 0
        while (
            image_count is not None
            and status["seen-images"] < image_count
            and images_found < rw.recipe_step["parameters"].get("burst-limit", 100)
        ):
            m, frame = file_map[status["seen-images"]]
            h5_data_file, dsetname = dataset_files[m]
            self.log.debug(f"seen-images: {status['seen-images']}")
            self.log.debug(f"m, frame: {m, frame}")
            self.log.debug(f"h5_data_file, dsetname: {h5_data_file, dsetname}")

            with os_stat_profiler.record():
                if not os.path.isfile(h5_data_file):
                    break

            try:
                if h5_data_file not in file_handles:
                    file_handles[h5_data_file] = h5py.File(h5_data_file, "r", swmr=True)
                    self.log.debug(f"Opening file {h5_data_file}")
                h5_file = file_handles[h5_data_file]
                dataset = h5_file[dsetname]
                s = dataset.id.get_chunk_info_by_coord((frame, 0, 0))
                if s.size == 0:
                    break
                self.log.info(f"Found image {status['seen-images']} (size={s.size})")
            except Exception:
                self.log.warning(f"Error reading {h5_data_file}", exc_info=True)
                rw.transport.nack(header)
                return

            images_found += 1

            def notify_function(output):
                rw.send_to(
                    output,
                    {
                        "hdf5": hdf5,
                        "hdf5-index": status["seen-images"],
                        "file": hdf5,
                        "file-number": status["seen-images"],
                        "parameters": {
                            "scan_range": "{0},{0}".format(status["seen-images"] + 1)
                        },
                    },
                    transaction=txn,
                )

            self._notify_for_found_file(
                status["seen-images"] + 1,
                image_count,
                selections,
                everys,
                notify_function,
            )
            status["seen-images"] += 1

        # Are we done?
        if status["seen-images"] == image_count:
            # Happy days

            self.log.debug(f"All {image_count} images found for {hdf5}")

            extra_log = {
                "delay": time.time() - status["start-time"],
                "stat-time-max": os_stat_profiler.max,
                "stat-time-mean": os_stat_profiler.mean,
            }
            if rw.recipe_step["parameters"].get("expected-per-image-delay"):
                # Estimate unexpected delay
                try:
                    expected_delay = (
                        float(rw.recipe_step["parameters"]["expected-per-image-delay"])
                        * image_count
                    )
                except ValueError:
                    # in case the field contains "None" or equivalent un-floatable nonsense
                    self.log.warning(
                        "Ignored invalid expected-per-image-delay value (%r)",
                        rw.recipe_step["parameters"]["expected-per-image-delay"],
                    )
                else:
                    extra_log["unexpected_delay"] = max(
                        0, extra_log["delay"] - expected_delay
                    )

            self.log.info(
                "All %d images found for %s after %.1f seconds.",
                image_count,
                rw.recipe_step["parameters"]["hdf5"],
                time.time() - status["start-time"],
                extra=extra_log,
            )

            rw.send_to(
                "any",
                {"images-expected": image_count, "images-seen": status["seen-images"]},
                transaction=txn,
            )
            rw.send_to(
                "finally",
                {
                    "images-expected": image_count,
                    "images-seen": status["seen-images"],
                    "success": True,
                },
                transaction=txn,
            )

            rw.transport.transaction_commit(txn)
            return

        message_delay = rw.recipe_step["parameters"].get("burst-wait")
        if images_found == 0:
            # If no images were found, check timeout conditions.
            if status["seen-images"] == 0:
                # For first file: relevant timeout is 'timeout-first', with fallback 'timeout', with fallback 1 hour
                timeout = rw.recipe_step["parameters"].get(
                    "timeout-first", rw.recipe_step["parameters"].get("timeout", 3600)
                )
                timed_out = (status["start-time"] + timeout) < time.time()
            else:
                # For subsequent images: relevant timeout is 'timeout', with fallback 1 hour
                timeout = rw.recipe_step["parameters"].get("timeout", 3600)
                timed_out = (status["last-seen"] + timeout) < time.time()
            if timed_out:
                # File watch operation has timed out.

                # Report all timeouts as warnings unless the recipe specifies otherwise
                timeoutlog = self.log.warning
                if rw.recipe_step["parameters"].get("log-timeout-as-info"):
                    timeoutlog = self.log.info

                timeoutlog(
                    "Filewatcher for %s timed out after %.1f seconds (%d images found, nothing seen for %.1f seconds)",
                    rw.recipe_step["parameters"]["hdf5"],
                    time.time() - status["start-time"],
                    status["seen-images"],
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
                        "file": hdf5,
                        "hdf5-index": status["seen-images"],
                        "success": False,
                    },
                    transaction=txn,
                )
                # Notify for 'any' target if any file was seen
                if status["seen-images"]:
                    rw.send_to(
                        "any",
                        {
                            "images-expected": image_count,
                            "images-seen": status["seen-images"],
                        },
                        transaction=txn,
                    )

                # Notify for 'finally' outcome
                rw.send_to(
                    "finally",
                    {
                        "images-expected": image_count,
                        "images-seen": status["seen-images"],
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
                    "No further images found for {hdf5} after a total time of {time:.1f} seconds\n"
                    "{images_seen} of {image_count} images seen so far"
                ).format(
                    time=time.time() - status["start-time"],
                    hdf5=rw.recipe_step["parameters"]["hdf5"],
                    images_seen=status["seen-images"],
                    image_count=image_count,
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
                "%d out of %d images found for %s (total: %d out of %d) within %.1f seconds",
                images_found,
                image_count,
                rw.recipe_step["parameters"]["hdf5"],
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
