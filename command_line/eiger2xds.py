# LIBTBX_SET_DISPATCHER_NAME eiger2xds
from __future__ import absolute_import, division, print_function

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
    import libtbx.load_env

    from dials.util.options import OptionParser
    from dials.util.options import flatten_datablocks

    usage = "%s [options] datablock.json" % libtbx.env.dispatcher_name

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
        raise Sorry("Only one datablock can be processed at a time")
    else:
        imagesets = datablocks[0].extract_imagesets()
        assert len(imagesets) == 1
        imageset = imagesets[0]

    to_xds = xds.to_xds(imageset)
    xds_inp = to_xds.XDS_INP()
    with open("XDS.INP", "wb") as f:
        print(xds_inp, file=f)
        print("LIB=/dls_sw/apps/XDS/20180808/durin-plugin.so", file=f)
    print("Written XDS input file to XDS.INP")


if __name__ == "__main__":
    run()
