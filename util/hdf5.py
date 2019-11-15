from __future__ import absolute_import, division, print_function

import logging
import os
import xml.etree.ElementTree

import libtbx.load_env
import procrunner

log = logging.getLogger("dlstbx.util.hdf5")
_h5dump = libtbx.env.under_base("bin/h5dump")


def get_external_references(filename):
    command_line = [_h5dump, "-H", "-x", filename]
    result = procrunner.run(command_line, print_stdout=False, print_stderr=False)
    if result.returncode:
        log.warning(
            "h5dump failed on {} with exitcode {} and output {}".format(
                filename, result.returncode, result.stderr
            )
        )
        raise ValueError("Invalid HDF5 files {}".format(filename))

    xmlroot = xml.etree.ElementTree.fromstring(result["stdout"])
    links = filter(
        lambda tag: tag.tag
        == "{http://hdfgroup.org/HDF5/XML/schema/HDF5-File.xsd}ExternalLink",
        xmlroot.iter(),
    )
    files = filter(None, map(lambda tag: tag.attrib.get("TargetFilename"), links))
    return files


def find_all_references(startfile):
    startfile = os.path.abspath(startfile)
    if not os.path.exists(startfile):
        log.error(
            "Can not find references from file %s. This file does not exist.", startfile
        )
        return []

    known_files = set()
    unchecked_files = {startfile}
    invalid_files = set()

    while unchecked_files:
        filename = unchecked_files.pop()
        filepath = os.path.dirname(filename)
        if not os.path.exists(filename):
            log.warning("Referenced file %s does not exist.", filename)
            invalid_files.add(filename)
            continue
        known_files.add(filename)
        for linked_file in get_external_references(filename):
            linked_file = os.path.abspath(os.path.join(filepath, linked_file))
            if linked_file in known_files or linked_file in invalid_files:
                continue
            else:
                unchecked_files.add(linked_file)

    return sorted(known_files)
