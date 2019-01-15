from __future__ import absolute_import, division, print_function

import dlstbx.util.gda
import workflows.recipe
from workflows.services.common_service import CommonService


class DLSNotifyGDA(CommonService):
    """A service that forwards per-image-analysis results to GDA."""

    # Human readable service name
    _service_name = "DLS GDA Bridge"

    # Logger name
    _logger_name = "dlstbx.services.notifygda"

    def initializing(self):
        """
        Subscribe to the GDA notification queue.
        Received messages must be acknowledged.
        """
        self.log.debug("GDA Bridge starting")
        workflows.recipe.wrap_subscribe(
            self._transport,
            "notify_gda",  # consider transient queue
            self.notify_gda,
            acknowledgement=True,
            log_extender=self.extend_log,
        )

    def notify_gda(self, rw, header, message):
        """Forward some information to GDA."""

        if not isinstance(message, dict):
            self.log.error("message payload must be a dictionary")
            rw.transport.nack(header)
            return

        parameter = rw.recipe_step["parameters"].get
        dcid = parameter("dcid")
        gdahost = parameter("host")
        gdaport = parameter("port")
        if not dcid:
            self.log.error("DataCollectionID not specified")
            rw.transport.nack(header)
            return
        if not gdahost or not gdaport:
            self.log.error("GDA host/port undefined")
            rw.transport.nack(header)
            return

        image_number = message.get("file-pattern-index") or message.get("file-number")
        if not image_number:
            self.log.error("Image number not specified")
            rw.transport.nack(header)
            return

        record = {}
        record["spot_total"] = message.get("n_spots_total")
        record["good_bragg_candidates"] = message.get("n_spots_no_ice")
        record["method1_res"] = message.get("estimated_d_min")
        record["total_integrated_signal"] = message.get("total_intensity")
        if record["spot_total"] is None:
            self.log.error("Message does not contain a spot count")
            rw.transport.nack(header)
            return

        self.log.debug(
            "Forwarding PIA record for image %r in DCID %s: %r",
            image_number,
            dcid,
            record,
        )

        if "{" in gdahost:
            self.log.error("Could not notify GDA, %s is not a valid hostname", gdahost)
            rw.transport.nack(header)
            return
        elif gdahost == "mx-control":
            pass  # skip
        else:
            # We notify according to https://jira.diamond.ac.uk/browse/MXGDA-3243
            # by sending a UDP package containing the actual result record information.
            try:
                dlstbx.util.gda.notify(
                    gdahost,
                    gdaport,
                    (
                        "PIA:{dcid}:{image_number}:{r[spot_total]}"
                        ":{r[good_bragg_candidates]}:{r[method1_res]}:{r[total_integrated_signal]}"
                    ).format(dcid=dcid, image_number=image_number, r=record),
                )
            except Exception as e:
                self.log.error("Could not notify GDA: %s", e, exc_info=True)
                rw.transport.nack(header)
                return
        rw.transport.ack(header)
