from __future__ import annotations

import os
import shutil
import time
from pathlib import Path

from dlstbx.wrapper import Wrapper


class DirWatchWrapper(Wrapper):
    """Run the analysis for one group of files collected by the directory
    watcher, then move the group out of staging to its final home."""

    _logger_name = "dlstbx.wrap.dirwatch"
    name = "dirwatch"

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]
        prefix = params["prefix"]
        workdir = Path(params["workdir"])
        final = Path(params["final"])
        failed = Path(params["failed"])

        datafiles = sorted(
            path
            for path in workdir.iterdir()
            if path.is_file() and not path.name.startswith(".")
        )
        self.log.info(
            "Processing group %s: %d files in %s", prefix, len(datafiles), workdir
        )

        success = self.analyse(prefix, workdir, datafiles)

        # The group is moved either way, so a failure leaves everything needed
        # to diagnose it - inputs, results and the job's own launch directory -
        # together under the failed directory.
        self.archive(workdir, (final if success else failed) / prefix)
        return success

    def analyse(self, prefix: str, workdir: Path, datafiles: list[Path]) -> bool:
        """Run the analysis for one group. Returns True on success.

        STUB: replace the body with the real work. Write results into workdir so
        that they travel with the group when it is archived. Everything either
        side of this - collecting the group, moving it, reporting the outcome -
        is already handled.
        """
        self.log.warning("dirwatch analysis is a stub, no work was done")
        return True

    def archive(self, workdir: Path, target: Path):
        """Move a finished group out of the staging directory."""
        target.parent.mkdir(parents=True, exist_ok=True)

        if target.exists():
            target = target.with_name(f"{target.name}_{time.strftime('%Y%m%d%H%M%S')}")
            self.log.warning("Destination already existed, using %s instead", target)

        try:
            os.rename(workdir, target)
        except OSError:
            # Staging and destination are on different filesystems, so the move
            # cannot be atomic. Copy it across instead.
            self.log.warning("Falling back to a non-atomic move to %s", target)
            shutil.move(str(workdir), str(target))

        self.log.info("Moved group to %s", target)
