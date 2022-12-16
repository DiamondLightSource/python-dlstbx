from __future__ import annotations

from pathlib import Path

from workflows.services.common_service import CommonService

from dlstbx.util.watcher import Watcher


class SSXZocaloTrigger(CommonService):
    """
    A service that monitors a defined path written to by SSX data collection
    scripts to notify data analysis of start/end of data collections. Trigger
    Zocalo mimas recipe for each new start/end event.
    """

    # Human readable service name
    _service_name = "DLS SSX zocalo trigger"

    _logger_name = "dlstbx.services.ssx_zocalo_trigger"

    def initializing(self):
        self._collect_log_path = Path(
            self.config.storage.get("zocalo.ssx.collect_log_path")
        )

        start_path = self._collect_log_path / "started"
        end_path = self._collect_log_path / "ended"
        if not Path(start_path).is_dir():
            self.log.info(f"Creating log path {start_path}")
            Path(start_path).mkdir(mode=0o1777, parents=True)
        if not Path(end_path).is_dir():
            self.log.info(f"Creating log path {end_path}")
            Path(end_path).mkdir(mode=0o1777, parents=True)

        self.watcher = Watcher(
            self._collect_log_path,
            active_depth=2,
        )
        self.watcher.scan(timeout=False)
        self.log.info(f"Watching {self._collect_log_path} for new changes")
        self._register_idle(3, self.watch)

    def watch(self):
        self.log.debug("Watching")
        new_f, _ = self.watcher.scan()
        new_f = [Path(x) for x in new_f if Path(x).stem.isdigit()]
        if new_f:
            self.log.info(
                "Found DCID files: %s",
                ", ".join(str(x.relative_to(self._collect_log_path)) for x in new_f),
            )
        for dcidfile in new_f:
            dcid = int(dcidfile.stem)
            # It it start or end?
            is_starting = dcidfile.parent.name == "started"
            timestamp = dcidfile.read_text()
            if is_starting:
                self.log.info("New DCID found:      %s started at %s", dcid, timestamp)
                event = "start"
            else:
                self.log.info("Complete DCID found: %s ended at %s", dcid, timestamp)
                event = "end"

            message = {
                "recipes": ["mimas"],
                "parameters": {"event": event, "ispyb_dcid": 2},
            }
            self.transport.send("processing_recipe", message)
