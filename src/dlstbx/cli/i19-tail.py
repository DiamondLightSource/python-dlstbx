# LIBTBX_SET_DISPATCHER_NAME i19.tail

import os
import re
import sys
import threading
import time


def is_uuid(s):
    return bool(
        re.match(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", s)
    )


start = time.time()


def ensure_we_are_in_visit_directory():
    current_path = os.getcwd()
    m = re.match(r"(/dls/[^/]+/data/[0-9]+/[^/]+)(/.*)?$", current_path)
    if not m:
        sys.exit(
            "This script must be run inside a visit directory. %s is not a visit "
            "directory." % current_path
        )

    visit_dir = m.group(1)
    print("Looking for screen19 logs for visit directory", visit_dir)
    os.chdir(visit_dir)

    if not os.path.exists("tmp"):
        sys.exit("This visit directory does not have a 'tmp' directory. Can't run.")
    os.chdir("tmp")
    if not os.path.exists("zocalo"):
        print("This visit directory does not yet have a 'tmp/zocalo' directory.")
        print("Waiting up to 10 minutes for first data collection...")
        while not os.path.exists("zocalo") and time.time() < (
            start + 600
        ):  # Limit waiting to 10 minutes
            time.sleep(10)
        if not os.path.exists("zocalo"):
            sys.exit("There is no live data processing in this visit.")
    os.chdir("zocalo")


def header(text):
    print("\n" * 3)
    print(text)
    print("=" * len(text))


ensure_we_are_in_visit_directory()
base_directory = os.getcwd()


def find_screen19_or_newest_entry(directory):
    entries = os.listdir(directory)
    if "screen19" in entries:
        return os.path.join(directory, "screen19")
    if any(map(is_uuid, entries)):
        # if UUID directories in the path then do not descend any further
        return None
    times = {
        os.path.getmtime(os.path.join(directory, entry)): entry
        for entry in entries
        if os.path.isdir(os.path.join(directory, entry))
    }
    if not times:
        return None
    newest_entry = os.path.join(directory, times[max(times)])
    return newest_entry


def recursively_find_most_current_directory(base, last_known_path=None):
    """Recursively find the newest directory underneath 'base'.
    Optionally also find any newer directories between 'base'
    and 'last_known_path'.
    Return a tuple of the most recent directory found and its
    modification timestamp."""
    best_candidate = (None, 0)
    newest_entry = find_screen19_or_newest_entry(base)
    if newest_entry:
        last_modification = os.path.getmtime(newest_entry)
        if last_modification > best_candidate[1]:
            best_candidate = (newest_entry, last_modification)
        if os.path.basename(newest_entry) != "screen19":
            newest_entry = recursively_find_most_current_directory(newest_entry, None)
            if newest_entry:
                newest_entry, last_modification = newest_entry
                if last_modification > best_candidate[1]:
                    best_candidate = (newest_entry, last_modification)
    if last_known_path:
        while last_known_path.startswith(base):
            if os.path.basename(last_known_path) != "screen19":
                newest_entry = find_screen19_or_newest_entry(last_known_path)
                if newest_entry:
                    last_modification = os.path.getmtime(newest_entry)
                    if last_modification > best_candidate[1]:
                        best_candidate = (newest_entry, last_modification)
            last_known_path = os.path.dirname(last_known_path)
    if best_candidate[1]:
        return best_candidate


class _LineAggregator:
    """Buffer that can be filled with stream data and will aggregate complete
    lines."""

    def __init__(self):
        """Create aggregator object."""
        self._buffer = ""

    def add(self, data):
        """Add a single character to buffer. If one or more full lines are found,
        print them (if desired) and pass to callback function."""
        if not data:
            return
        self._buffer += data
        if "\n" in data:
            to_print, remainder = self._buffer.rsplit("\n")
            print(to_print)
            self._buffer = remainder

    def flush(self):
        """Print/send any remaining data to callback function."""
        if self._buffer:
            print(self._buffer)
        self._buffer = ""


class tail_log(threading.Thread):
    def __init__(self, path):
        threading.Thread.__init__(self)
        self.daemon = True
        self._closing = False
        self._path = path
        self.start()

    def close(self):
        self._closing = True

    def run(self):
        header(os.path.dirname(self._path))
        with open(os.path.join(self._path, "screen19.log")) as fh:
            start = time.time()
            la = _LineAggregator()
            while not self._closing and time.time() < start + 900:
                char = fh.read(1)
                if char:
                    la.add(char)
                else:
                    time.sleep(0.3)
            la.flush()
            if time.time() >= start + 900:
                print(" (stopped watching file after 15 minutes)")
            print("." * 76)


active_tail = None
waiting_for_log = None
last_known_path = None
try:
    most_recent_dir = (None, 0)
    while time.time() < start + (24 * 3600):  # Set up a 24hr runtime limit
        better_location = recursively_find_most_current_directory(
            base_directory, last_known_path
        )
        if (
            better_location[0].endswith("screen19")
            and better_location[1] > most_recent_dir[1]
        ):
            most_recent_dir = better_location
            if active_tail:
                active_tail.close()
                active_tail = None
            waiting_for_log = most_recent_dir[0]
            if not os.path.exists(os.path.join(waiting_for_log, "screen19.log")):
                print(
                    "\n\n\n"
                    "New screen19 directory found at %s, waiting for new log to appear"
                    % os.path.dirname(waiting_for_log)
                )
        if waiting_for_log:
            if os.path.exists(os.path.join(waiting_for_log, "screen19.log")):
                active_tail = tail_log(waiting_for_log)
                last_known_path = waiting_for_log
                waiting_for_log = None
        time.sleep(5)
except KeyboardInterrupt:
    print()
