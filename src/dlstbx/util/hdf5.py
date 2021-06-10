# isort: skip_file
import collections
import logging
import os

import dxtbx  # noqa: F401; dxtbx must be imported before h5py is imported
import h5py

log = logging.getLogger("dlstbx.util.hdf5")


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
            else:
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
