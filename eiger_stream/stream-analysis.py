from __future__ import absolute_import, division, print_function

import os

from dials.command_line.find_spots_server import work
from dlstbx.eiger_stream.data import valid_things
import dxtbx.format.FormatEigerStream
from pprint import pprint

if __name__ == "__main__":
    print(list(valid_things))

    parameters = ["d_max=40"]
    dxtbx.format.FormatEigerStream.injected_data = valid_things

    filename = "eiger.stream"
    try:
        with open(filename, "w") as fh:
            fh.write("EIGERSTREAM")
        results = work(filename, cl=parameters)
    finally:
        os.remove(filename)

    pprint(results)
