from __future__ import annotations

import functools

import dxtbx.model.experiment_list
import pytest
import workflows.recipe
from workflows.services.common_service import CommonService

import dlstbx.util.hdf5 as hdf5_util


class DLSValidation(CommonService):
    """
    A service that validates data collections against ISPyB
    and for internal consistency.
    """

    # Human readable service name
    _service_name = "DLS Validation"

    _logger_name = "dlstbx.services.validation"

    def initializing(self):
        # The main per_image_analysis queue.
        # For every received message a single frame will be analysed.
        workflows.recipe.wrap_subscribe(
            self._transport,
            "validation",
            self.validate,
            acknowledgement=True,
            log_extender=self.extend_log,
        )

    def fail_validation(self, rw, header, output, reason):
        output["reason"] = reason
        txn = rw.transport.transaction_begin(subscription_id=header["subscription"])
        rw.transport.ack(header, transaction=txn)
        rw.send_to("validation_error", output, transaction=txn)
        rw.transport.transaction_commit(txn)
        self.log.error(
            "Image validation of %s failed: %s",
            output["file"],
            reason,
        )

    def validate(self, rw, header, message):
        """Validate an image or a data set

        Recipe parameters:
        { "parameters": { "beamline": "i04-1",
                          "ispyb_wavelength": "0.9119",
                          ... } }
        These are optional and are used for consistency checking.

        Minimum message payload:
        { "file": full file path }

        Output streams:
        "validation_error": This is the default output.

        Output message format:
        { "file": copied over from input message,
          "reason": string describing why validation failed,

          other fields are copied from the parameters dictionary
        }
        """

        filename = message["file"]
        rw.set_default_channel("validation_error")
        output = rw.recipe_step.get("parameters", {})
        output["file"] = filename
        self.log.debug("Starting validation of %s", filename)

        fail = functools.partial(self.fail_validation, rw, header, output)

        # Verify HDF5 file version is _not_ compatible with HDF5 1.8 format
        if filename.endswith((".h5", ".nxs")):
            if not hdf5_util.is_readable(filename):
                return fail(f"{filename} is an invalid HDF5 file")
            try:
                errors = [
                    link
                    for link in hdf5_util.find_all_references(filename)
                    if not hdf5_util.is_readable(link)
                ]
                if errors:
                    return fail(
                        f"HDF5 file {filename} links to invalid file(s) %s"
                        % ", ".join(errors)
                    )
                hdf_18 = [
                    link
                    for link, image_count in hdf5_util.find_all_references(
                        filename
                    ).items()
                    if image_count and hdf5_util.is_HDF_1_8_compatible(link)
                ]
                if hdf_18:
                    return fail(
                        f"HDF5 file {filename} links to HDF5 1.8 format data in %s"
                        % ", ".join(hdf_18)
                    )
            except Exception as e:
                return fail(
                    f"Unhandled {type(e).__name__} exception reading {filename}"
                )

        # Create experiment list
        try:
            el = dxtbx.model.experiment_list.ExperimentListFactory.from_filenames(
                [filename]
            )
        except Exception as e:
            if "unable to open external file" in str(e):
                failname = str(e)
                if failname.split("'")[1:2]:
                    failname = failname.split("'")[1]
                return fail(f"data collection is missing linked file: {failname}")
            self.log.warning(
                f"Unhandled {type(e).__name__} exception reading {filename}",
                exc_info=True,
            )
            return fail(f"Unhandled {type(e).__name__} exception reading {filename}")

        wavelength = el.beams()[0].get_wavelength()

        if wavelength <= 0:
            return fail("wavelength not set in image header")

        if output.get("beamline") == "i04-1":
            if output.get("dc_comments", "").startswith("Simulated datacollection"):
                self.log.debug(
                    "Skipping i04-1 wavelength validation for simulated data collection"
                )
            elif wavelength < 0.9100 or wavelength > 0.9300:
                return fail(
                    f"Image wavelength {wavelength} outside of allowed range for I04-1 (0.9100-0.9300)"
                )

        if output.get("ispyb_wavelength"):
            expected_wavelength = float(output["ispyb_wavelength"])
            if wavelength != pytest.approx(expected_wavelength, rel=0.02):
                return fail(
                    f"Image wavelength {wavelength} deviates from ISPyB wavelength {expected_wavelength} by more than 2%"
                )

        rw.transport.ack(header)
        self.log.debug("%s passed validation", filename)
