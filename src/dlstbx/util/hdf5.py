# isort: skip_file
import collections
import logging
import os

import dxtbx  # noqa: F401; dxtbx must be imported before h5py is imported
import dxtbx.nexus.nxmx

import h5py
import numpy as np

log = logging.getLogger("dlstbx.util.hdf5")


class ValidationError(Exception):
    pass


def find_all_references(startfile):
    startfile = os.path.abspath(startfile)
    if not os.path.exists(startfile):
        log.error(
            "Can not find references from file %s. This file does not exist.", startfile
        )
        return []
    filepath = os.path.dirname(startfile)

    image_count = collections.defaultdict(int)
    image_count[startfile] = 0
    with h5py.File(startfile, "r") as fh:
        try:
            fhed = fh["/entry/data"]
        except KeyError:
            raise ValueError("Invalid HDF5 master file: file contains no data entries.")
        for entry in fhed.keys():
            entry_link = fhed.get(entry, getlink=True)
            if not isinstance(entry_link, h5py.ExternalLink):
                filename = startfile
            else:
                filename = os.path.abspath(os.path.join(filepath, entry_link.filename))
                assert filename not in image_count
            if not entry.startswith("data_"):
                image_count[filename] += 0
                continue
            try:
                shape = fhed[entry].shape
            except KeyError as e:
                if "unable to open external file" in str(e) and "'" in str(e):
                    log.warning(
                        "Referenced file %s does not exist.", filename, exc_info=True
                    )
                    image_count[filename] = None
                    continue
                raise
            if not shape[0]:
                image_count[filename] += shape[0]
                continue
            chunk_size = (
                fhed[entry]
                .id.get_chunk_info_by_coord(tuple(c - 1 for c in fhed[entry].shape))
                .size
            )
            if not chunk_size:
                log.warning(
                    "Referenced file {filename} has a zero-sized final chunk size.",
                    filename,
                    exc_info=True,
                )
                image_count[filename] += 0
                continue
            image_count[filename] += shape[0]
    return image_count


def is_readable(filename: str) -> bool:
    """Check if a file can be read"""

    try:
        with h5py.File(filename, "r"):
            return True
    except Exception:
        return False


def is_HDF_1_8_compatible(filename: str) -> bool:
    """Check if a file can be read with HDF 1.8. This excludes all SWMR formatted files."""

    try:
        with h5py.File(filename, "r", libver=("earliest", "v108")):
            return True
    except OSError:
        return False


def validate_pixel_mask(filename: str) -> bool:
    with h5py.File(filename, "r") as fh:
        nxmx = dxtbx.nexus.nxmx.NXmx(fh)
        nxinstrument = nxmx.entries[0].instruments[0]
        nxdetector = nxinstrument.detectors[0]
        nxmodule = nxdetector.modules[0]

        pixel_mask = nxdetector.get("pixel_mask")
        data_size = tuple(nxmodule.data_size)
        if pixel_mask is None:
            raise ValidationError("pixel_mask not present")
        elif pixel_mask.shape == (0, 0):
            raise ValidationError(f"pixel_mask is empty ({pixel_mask.shape=})")
        elif pixel_mask.shape != data_size:
            raise ValidationError(
                f"pixel_mask inconsistent with data_size "
                f"({pixel_mask.shape=} {data_size=})"
            )
        elif pixel_mask.dtype not in (np.int32, np.uint32):
            raise ValidationError(
                f"pixel_mask should be of type int32 or uint32 ({pixel_mask.dtype=})"
            )

    return True
