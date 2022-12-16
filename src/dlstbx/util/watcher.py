#!/usr/bin/env python3

from __future__ import annotations

"""
Watcher

Implementation of a utility class to efficiently watch a folder/tree of
folders for new files appearing, with options for ignoring/timeout.
"""

import argparse
import fnmatch
import logging
import os
import re
import time
from collections import defaultdict
from os import walk
from typing import Callable

logger = logging.getLogger(__name__)


def _depth_from(root, subdir: str) -> int:
    """
    Find out how many folders under a root, a subdir is
    """
    assert subdir.startswith(root)
    count = 0
    while subdir != root:
        subdir = os.path.dirname(subdir)
        count += 1
    return count


class Watcher(object):
    def __init__(
        self,
        root: str,
        ignore: list[str] = [],
        timeout: float = 60,
        active_depth: int = 0,
        keep_last_alive: bool = True,
        clock: Callable[[], float] = None,
    ):
        """
        Arguments:
            root: The root path to search for changing folders
            ignore: The (regex) list of paths names to ignore
            timeout:
                The amount of time to keep a folder alive even if no
                changes have been observed.
            active_depth:
                This depth of folder tree will never be timed out
            keep_last_alive:
                If True, the last chain of folders where a new file was
                last detected will never be timed out. If new files
                appear in a new, different directory, then it will
                become a candidate for timing out. If new files appear
                in several different directories, then behaviour is
                undefined.
            clock:
                The function callable to use for time determination. Defaults
                to time.time
        """
        self.root = os.path.abspath(root)
        self.timeout = timeout
        self.keep_active_depth = active_depth
        # Build the ignore matchers
        self.ignore = [re.compile(fnmatch.translate(x)) for x in ignore]
        self.clock = clock or time.time

        self.active_dirs: dict[str, float] = {}
        self.set_active(self.root)
        self.root_dir_count = len(self.active_dirs)
        self.inactive_dirs: set[str] = set()
        self.known_files: defaultdict[str, set[str]] = defaultdict(
            set
        )  # Map from dirs to known files
        self.listeners: list[Callable[[list[str]], None]] = []
        self.dropped_listeners: list[Callable[[list[str]], None]] = []
        self.should_keep_listeners: list[Callable[[str, float], bool]] = []
        self.keep_last_alive = keep_last_alive
        self._last_active: str | None = None
        # Run an initial scan so that we don't flood with files before creation
        # self.scan()

    def drop_folder(self, path: str) -> None:
        """Actively drop a path from scanning"""
        self.inactive_dirs.add(path)
        del self.active_dirs[path]

    def set_active(self, path: str, to_time: float | None = None):
        "Update active timestamps for the whole path tree"
        # Remember the last place so that we can keep it alive
        self._last_active = path

        if to_time is None:
            to_time = self.clock()

        while path != "/":
            self.active_dirs[path] = to_time
            path = os.path.dirname(path)

    def emit(self, filename: list[str]) -> None:
        # logger.info("New file: %s", filename)
        for listener in self.listeners:
            listener(filename)

    def __len__(self) -> int:
        return len(self.active_dirs) - self.root_dir_count

    def preignore_folders(self) -> None:
        """Deliberately ignore everything out of the folder scan depth"""
        print(self.keep_active_depth)
        for (dirpath, dirnames, filenames) in walk(self.root):
            for subdir in dirnames[:]:
                full_path = os.path.join(dirpath, subdir)
                if _depth_from(self.root, full_path) > self.keep_active_depth:
                    logger.debug("Pre-ignoring %s", full_path)
                    self.inactive_dirs.add(full_path)
                    dirnames.remove(subdir)
        logger.debug("Finished pre-ignoring")

    def scan(self, timeout: bool = True) -> tuple[list[str], list[str]]:
        """
        Arguments:
            timeout: Stop watching anything that hasn't changed in timeout
        Returns:
            (new_files, dropped_paths)
        """
        # active = False
        start_count = len(self)
        # Keep track of everything changed
        all_new_files: set[str] = set()
        dropped_paths: set[str] = set()

        # Keep track of everything we've scanned to remove if missing
        scanned_paths = set()

        if not os.path.isdir(self.root):
            logger.debug("Root directory does not exists - skipping")
            return ([], [])

        # List of paths to make active. This ensures that there is no
        # timestamp frame drag from e.g. very long traversals - without
        # this, if a traversal takes > timeout then things that changed
        # can be dropped inadvertantly
        to_make_active = []

        for (dirpath, dirnames, filenames) in walk(self.root):
            # [("/dls/i24/data/2019/mx19458-21/processing", ["processing"], [])]:
            logger.debug("Scanning %s", dirpath)
            for subdir in dirnames[:]:
                full_path = os.path.join(dirpath, subdir)
                scanned_paths.add(full_path)

                # ignore hidden paths
                if subdir.startswith(".") and subdir in dirnames:
                    dirnames.remove(subdir)
                # If inactive or unknown,
                elif full_path in self.inactive_dirs:
                    # If already inactive, then don't walk into it
                    dirnames.remove(subdir)
                elif any(x.search(full_path + "/") for x in self.ignore):
                    # Handle the ignore list
                    logger.debug("Ignoring %s", full_path)
                    dirnames.remove(subdir)
                    self.inactive_dirs.add(full_path)
                elif full_path not in self.active_dirs:
                    # Not in the active list - must be new
                    to_make_active.append(full_path)
                    # Ensure we have a known_files entry, even if there are none
                    self.known_files[full_path] = set()

            # Any new files here?
            all_files = set(filenames)
            new_files = all_files - self.known_files[dirpath]
            if new_files:
                all_new_files.update(os.path.join(dirpath, x) for x in new_files)
                to_make_active.append(dirpath)
                self.known_files[dirpath] = all_files

        # Now mark everything active all at once
        walk_end_time = self.clock()
        for dirpath in to_make_active:
            self.set_active(dirpath, to_time=walk_end_time)

        # Check for paths that have been removed
        for missing_scan in set(self.active_dirs.keys()) - scanned_paths:
            if not os.path.isdir(missing_scan):
                logger.info("Removing missing dir %s", missing_scan)
                del self.active_dirs[missing_scan]
                del self.known_files[missing_scan]
                dropped_paths.add(missing_scan)
                # Handle deletion of the last path
                if self._last_active == missing_scan:
                    self._last_active = None

        # Remove any paths that are now inactive
        for subdir in list(self.active_dirs.keys()):
            logger.debug(
                "Checking %s (%f) for lifetime",
                subdir,
                (self.clock() - self.active_dirs[subdir]),
            )
            if timeout and (self.clock() - self.active_dirs[subdir]) > self.timeout:
                if (
                    subdir != self._last_active
                    and not self.root.startswith(subdir)
                    and _depth_from(self.root, subdir) > self.keep_active_depth
                ):
                    # active = True
                    # Check with listeners if we should drop this
                    if not any(
                        x(subdir, self.active_dirs[subdir])
                        for x in self.should_keep_listeners
                    ):
                        del self.known_files[subdir]
                        del self.active_dirs[subdir]
                        self.inactive_dirs.add(subdir)
                        logger.info("Removing dir %s", subdir)
                        dropped_paths.add(subdir)
        if len(self) != start_count:
            logger.info("%d active watch directories", len(self))

        # Call all the listeners for new files and dropped paths
        self.emit(list(sorted(all_new_files)))
        for listener in self.dropped_listeners:
            listener(list(sorted(dropped_paths)))

        logger.debug("Scan over")
        return (list(sorted(all_new_files)), list(sorted(dropped_paths)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        "Watch for changes in folders",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "target",
        metavar="DIR",
        help="The folder to watch",
        default=os.getcwd(),
        nargs="?",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument(
        "--depth", "-d", help="Scan keepalive depth", type=int, default=1
    )
    parser.add_argument(
        "--fast",
        action="store_true",
        help="Fast start: Ignore all existing folders beyond the search depth",
    )
    options = parser.parse_args()
    logging.basicConfig(level=logging.INFO if not options.verbose else logging.DEBUG)
    watcher = Watcher(
        options.target,
        ignore=["processed/", "spool/", "tmp/", "processing/", "xml/"],
        active_depth=options.depth,
    )
    print(f"Watching {options.target} for changes")
    try:
        if options.fast:
            watcher.preignore_folders()
        # Do an initial scan to get all the first files
        watcher.scan(timeout=False)
        while True:
            logger.debug("Scan!")
            new_f, drop_p = watcher.scan()
            if new_f or drop_p:
                # print("New: {}".format(len(new_f)))
                # for filename in new_f:
                print("  " + "\n  ".join(new_f))
                print(f"New: {len(new_f)}, Dropped: {len(drop_p)}")
                # for dirname in drop_p:
                print("  " + "\n  ".join(drop_p))
            time.sleep(5)
    except KeyboardInterrupt:
        pass


def test_symlink_dir_removal_handled_correctly(tmp_path):
    # Create a symlink to a folder with files
    hard = tmp_path / "hard_path"
    hard.mkdir()
    (hard / "a_file").touch()
    (tmp_path / "link_path").symlink_to(hard)
    elapsed_time = 0
    # Time it out and ensure that it doesn't crash
    watcher = Watcher(tmp_path, clock=lambda: elapsed_time, timeout=1)
    watcher.scan()
    elapsed_time += 2
    watcher.scan()
