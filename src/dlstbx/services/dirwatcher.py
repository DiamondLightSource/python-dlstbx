from __future__ import annotations

import fnmatch
import os
from pathlib import Path

import pydantic
from workflows.services.common_service import CommonService

# Written into a staging directory once its recipe has been sent. Its absence
# marks a group that was claimed but never dispatched, ie. one interrupted by a
# service restart.
DISPATCH_MARKER = ".zocalo-dispatched"


class WatchSpec(pydantic.BaseModel):
    """A holding directory to watch, and where the work it collects should go."""

    name: str
    holding: Path
    staging: Path
    final: Path
    failed: Path
    recipe: str
    marker_suffix: str = ".done"
    member_glob: str = "{prefix}*"
    parameters: dict[str, str] = pydantic.Field(default_factory=dict)

    def members(self, prefix: str, names: list[str]) -> list[str]:
        """Names in 'names' belonging to the group 'prefix'."""
        pattern = self.member_glob.format(prefix=prefix)
        return [name for name in names if fnmatch.fnmatch(name, pattern)]


class DLSDirWatcher(CommonService):
    """A service to watch holding directories for complete groups of files and
    run a recipe against each one.

    A group is a set of files sharing a common prefix, and is complete once its
    '<prefix>.done' marker appears. Since the marker is written only after the
    files have been copied, seeing it is sufficient - no settling period is
    needed.

    Groups are claimed by creating a per-prefix staging directory. That mkdir is
    atomic, so a slow scan overlapping the next one, or a second instance of
    this service, cannot dispatch the same group twice.
    """

    # Human readable service name
    _service_name = "DLS Directory Watcher"

    # Logger name
    _logger_name = "dlstbx.services.dirwatcher"

    def initializing(self):
        """Read the watch list and start scanning."""
        self.log.info("Directory watcher starting")

        storage = getattr(self.config, "storage", None) or {}
        self.watches = [
            WatchSpec(**spec) for spec in storage.get("zocalo.dirwatcher.watches", [])
        ]
        if not self.watches:
            self.log.warning("No watches configured, service will sit idle")

        for watch in self.watches:
            for directory in (
                watch.holding,
                watch.staging,
                watch.final,
                watch.failed,
            ):
                directory.mkdir(parents=True, exist_ok=True)
            self.log.info(
                "Watching %s for '*%s' -> recipe %s",
                watch.holding,
                watch.marker_suffix,
                watch.recipe,
            )

        # The callback runs on the service main loop, so scans cannot overlap:
        # the interval is measured from the end of the previous scan.
        self._register_idle(storage.get("zocalo.dirwatcher.interval", 60), self.scan)

    def scan(self):
        """Look for completed groups in every watched directory."""
        for watch in self.watches:
            try:
                self.scan_watch(watch)
            except OSError:
                self.log.exception("Error scanning %s", watch.holding)

    def scan_watch(self, watch: WatchSpec):
        """Claim and dispatch every completed group in one holding directory."""
        self.resume_interrupted(watch)

        names = os.listdir(watch.holding)  # a single readdir per scan
        prefixes = [
            name[: -len(watch.marker_suffix)]
            for name in names
            if name.endswith(watch.marker_suffix)
        ]

        for prefix in prefixes:
            if not prefix or prefix.startswith(".") or os.sep in prefix:
                self.log.error("Ignoring group with unusable prefix %r", prefix)
                continue
            if self.is_ambiguous(watch, prefix, prefixes):
                continue

            staging = watch.staging / prefix
            try:
                staging.mkdir()
            except FileExistsError:
                # Already claimed, either by an earlier scan or another instance
                continue

            self.log.info("Claimed group %s for watch %s", prefix, watch.name)
            self.assemble(watch, prefix, staging)
            self.dispatch(watch, prefix, staging)

    def is_ambiguous(self, watch: WatchSpec, prefix: str, prefixes: list[str]) -> bool:
        """Check whether this group would swallow files belonging to another.

        With the default '{prefix}*' rule a group named 'run1' would also match
        every file of a group named 'run10'. Rather than mis-group them, skip
        both and ask for a tighter member_glob.
        """
        pattern = watch.member_glob.format(prefix=prefix)
        clashes = [
            other
            for other in prefixes
            if other != prefix and fnmatch.fnmatch(other + watch.marker_suffix, pattern)
        ]
        if clashes:
            self.log.error(
                "Skipping group %s: its member_glob %r also matches group(s) %s. "
                "Set a more specific member_glob for watch %s.",
                prefix,
                pattern,
                ", ".join(sorted(clashes)),
                watch.name,
            )
            return True
        return False

    def assemble(self, watch: WatchSpec, prefix: str, staging: Path):
        """Move a group into its staging directory, marker file last.

        Moving the marker last means an interrupted assembly is self-evident:
        a staging directory without its marker is one we did not finish.
        """
        marker = prefix + watch.marker_suffix
        names = os.listdir(watch.holding)

        moved = 0
        for name in watch.members(prefix, names):
            if name != marker:
                os.rename(watch.holding / name, staging / name)
                moved += 1

        if (watch.holding / marker).exists():
            os.rename(watch.holding / marker, staging / marker)

        self.log.info("Moved %d files of group %s into %s", moved, prefix, staging)

    def dispatch(self, watch: WatchSpec, prefix: str, staging: Path):
        """Start the recipe for one assembled group."""
        parameters = {
            "prefix": prefix,
            "workdir": str(staging),
            "final": str(watch.final),
            "failed": str(watch.failed),
        }
        parameters.update(watch.parameters)

        self._transport.send(
            "processing_recipe",
            {"recipes": [watch.recipe], "parameters": parameters},
        )
        (staging / DISPATCH_MARKER).touch()
        self.log.info("Dispatched recipe %s for group %s", watch.recipe, prefix)

    def resume_interrupted(self, watch: WatchSpec):
        """Carry on with groups whose claim outlived the previous service run.

        A claimed group passes through three states, and the staging directory
        says which one it reached: no marker (assembly unfinished), marker but
        no dispatch marker (assembled but never sent), or both (in flight, or
        finished and already moved away by the wrapper).
        """
        for staging in sorted(p for p in watch.staging.iterdir() if p.is_dir()):
            if (staging / DISPATCH_MARKER).exists():
                continue

            prefix = staging.name
            if (staging / (prefix + watch.marker_suffix)).exists():
                self.log.warning("Group %s was claimed but never sent", prefix)
            else:
                self.log.warning("Resuming interrupted assembly of group %s", prefix)
                self.assemble(watch, prefix, staging)

            self.dispatch(watch, prefix, staging)
