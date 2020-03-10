# LIBTBX_SET_DISPATCHER_NAME eiger2xds
from __future__ import absolute_import, division, print_function

import os

import iotbx.phil
from dxtbx.serialize import xds

help_message = """
"""

phil_scope = iotbx.phil.parse(
    """\

""",
    process_includes=True,
)


def run():
    from dials.util.options import OptionParser
    from dials.util.version import dials_version

    usage = "eiger2xds [options] datablock.json"

    if dials_version().startswith("DIALS 2"):
        from dials.util.options import flatten_experiments

        parser = OptionParser(
            usage=usage,
            phil=phil_scope,
            read_experiments=True,
            read_experiments_from_images=True,
            epilog=help_message,
        )
        params, options, args = parser.parse_args(
            show_diff_phil=True, return_unhandled=True
        )
        experiments = flatten_experiments(params.input.experiments)

        if len(experiments) == 0:
            parser.print_help()
            exit(0)

        if len(experiments) > 1:
            exit("Only one experiment can be processed at a time")
        else:
            imagesets = experiments.imagesets()
            assert len(imagesets) == 1, len(imagesets)
            imageset = imagesets[0]

    else:
        # 20190404 backwards compatibility for DIALS 1.x
        from dials.util.options import flatten_datablocks

        parser = OptionParser(
            usage=usage,
            phil=phil_scope,
            read_datablocks=True,
            read_datablocks_from_images=True,
            epilog=help_message,
        )
        params, options, args = parser.parse_args(
            show_diff_phil=True, return_unhandled=True
        )
        datablocks = flatten_datablocks(params.input.datablock)

        if len(datablocks) == 0:
            parser.print_help()
            return

        if len(datablocks) > 1:
            exit("Only one datablock can be processed at a time")
        else:
            imagesets = datablocks[0].extract_imagesets()
            assert len(imagesets) == 1
            imageset = imagesets[0]

    to_xds = xds.to_xds(imageset)
    xds_inp = to_xds.XDS_INP()

    plugin_name = "durin-plugin.so"
    durin_lib = ""
    for d in os.environ["PATH"].split(os.pathsep):
        if os.path.exists(os.path.join(d, plugin_name)):
            durin_lib = os.path.join(d, plugin_name)
    if not durin_lib:
        print("Couldn't find plugin %s in PATH" % plugin_name)

    with open("XDS.INP", "wb") as f:
        print(xds_inp, file=f)
        print("LIB=%s" % durin_lib, file=f)
    print("Written XDS input file to XDS.INP")


if __name__ == "__main__":
    run()
