from __future__ import absolute_import, division, print_function

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
            log.error(
                "Can not find references from file %s. File contains no data entries.",
                startfile,
            )
            return []
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
                    log.warning("Referenced file %s does not exist.", filename)
                    image_count[filename] = None
                    continue
                raise
            else:
                image_count[filename] += shape[0]
    return image_count
