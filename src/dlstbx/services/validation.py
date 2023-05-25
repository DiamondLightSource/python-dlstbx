from __future__ import annotations

import functools
import os.path
import pathlib

import dxtbx.model.experiment_list
import h5py
import numpy as np
import pytest
import workflows.recipe

try:
    from dxtbx.format.FormatNXmxDLS import find_meta_filename
except ImportError:
    # backwards compatibility for dials 3.10.X
    from dxtbx.format.FormatNexusEigerDLS import find_meta_filename

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
                linked_files = hdf5_util.find_all_references(filename)
                non_existent_files = [
                    link for link in linked_files if not os.path.exists(link)
                ]
                if non_existent_files:
                    return fail(
                        f"HDF5 file {filename} links to non-existent file(s) {', '.join(non_existent_files)}"
                    )
                errors = [
                    link for link in linked_files if not hdf5_util.is_readable(link)
                ]
                if errors:
                    return fail(
                        f"HDF5 file {filename} links to invalid file(s) {', '.join(errors)}"
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
                try:
                    hdf5_util.validate_pixel_mask(filename)
                except hdf5_util.ValidationError as e:
                    msg = f"HDF5 file {filename} contains invalid pixel_mask: {e}"
                    return fail(msg)
                meta_h5 = find_meta_filename(filename)
                if not os.path.isfile(meta_h5):
                    return fail(f"{meta_h5} not found")
                with h5py.File(meta_h5) as fh:
                    zeros = [
                        f"/_dectris/{name}"
                        for name, d in fh["/_dectris"].items()
                        if len(d) == 0
                    ]
                    if zeros:
                        return fail(
                            f"Empty datasets found in {meta_h5}:\n" + "\n".join(zeros)
                        )

                with h5py.File(filename) as fh:
                    pixel_mask = fh["/entry/instrument/detector/pixel_mask"][()]
                    if "/entry/data/data" not in fh:
                        return fail("Missing VDS /entry/data/data")
                    data = fh["/entry/data/data"]
                    first_image = data[0]
                    max_value = np.max(first_image)
                    if max_value not in (0xFFFF, 0x7FFFFFFF, 0xFFFFFFFF):
                        msg_extra = ""
                        if max_value == 0xFF:
                            msg_extra = " (is the detector in 8-bit mode?)"
                        return fail(
                            f"Unxpected max pixel value found in {filename}: {max_value}{msg_extra}"
                        )
                    unmasked_minus_ones = np.count_nonzero(
                        (first_image == max_value) & (pixel_mask == 0)
                    )
                    if unmasked_minus_ones > 100:
                        return fail(
                            f"{unmasked_minus_ones} unmasked -1 pixel values found in first image for {filename}"
                        )
                    n_masked_pixels = np.count_nonzero(pixel_mask)
                    if n_masked_pixels == pixel_mask.size:
                        return fail(
                            "All pixels are masked (is the detector set to full header mode?)"
                        )
                    if not (
                        np.issubdtype(data.dtype, np.integer)
                        and data.dtype.itemsize in {2, 4}
                    ):
                        return fail(
                            f"Unexpected dtype={data.dtype} for {filename}{data.name} (expected 16-bit or 32-bit int)"
                        )
                    bit_depth_readout = fh[
                        "/entry/instrument/detector/bit_depth_readout"
                    ][()].item()
                    permitted_bit_depth_readout_values = {16, 32}
                    if bit_depth_readout not in permitted_bit_depth_readout_values:
                        return fail(
                            f"Unexpected {bit_depth_readout=} for {filename} (expected values: {permitted_bit_depth_readout_values})"
                        )

                    plist = data.id.get_create_plist()
                    if plist.get_layout() != h5py.h5d.VIRTUAL:
                        return fail(f"Not a VDS: {filename}{data.name}")
                    virtual_count = plist.get_virtual_count()
                    for j in range(virtual_count):
                        dsetname = plist.get_virtual_dsetname(j)
                        link = fh.get(dsetname, getlink=True)
                        dsetname = link.path
                        dset_filename = pathlib.Path(filename).parent / link.filename
                        with h5py.File(dset_filename) as dset_fh:
                            dset = dset_fh[link.path]
                            if (dset.dtype.itemsize * 8) != bit_depth_readout:
                                return fail(
                                    f"{dset_filename}{link.path} dtype ({dset.dtype}) inconsistent with {filename}/entry/instrument/detector/bit_depth_readout {bit_depth_readout}"
                                )

                    if expected_images := output.get("ispyb_expected_images"):
                        expected_images = int(expected_images)
                        n_images = len(data)
                        if expected_images != n_images:
                            return fail(
                                f"Number of images in /entry/data/data ({n_images}) inconsistent with that reported in ISPyB ({expected_images})"
                            )

            except Exception as e:
                self.log.warning(e, exc_info=True)
                return fail(
                    f"Unhandled {type(e).__name__} exception reading {filename}"
                )

        # Create experiment list
        try:
            expt = dxtbx.model.experiment_list.ExperimentListFactory.from_filenames(
                [filename],
                load_models=False,
            )[0]
            expt.load_models(index=0)
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

        wavelength = expt.beam.get_wavelength()

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
