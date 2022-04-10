from __future__ import annotations

import zocalo.wrapper

from dlstbx.util.version import dlstbx_version


class Wrapper(zocalo.wrapper.BaseWrapper):
    def prepare(self, payload):
        super().prepare(payload)
        if getattr(self, "status_thread"):
            self.status_thread.set_static_status_field("dlstbx", dlstbx_version())
