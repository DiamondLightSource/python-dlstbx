import os

import iotbx.phil
from dials.util.options import OptionParser, flatten_experiments
from dxtbx.serialize import xds

help_message = """
"""

phil_scope = iotbx.phil.parse(
    """\

""",
    process_includes=True,
)


def run(args=None):
    usage = "eiger2xds [options] datablock.json"

    parser = OptionParser(
        usage=usage,
        phil=phil_scope,
        read_experiments=True,
        read_experiments_from_images=True,
        epilog=help_message,
    )
    params, options, args = parser.parse_args(
        args=args, show_diff_phil=True, return_unhandled=True
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

    to_xds = xds.to_xds(imageset)
    xds_inp = to_xds.XDS_INP()

    plugin_name = "durin-plugin.so"
    durin_lib = ""
    for d in os.environ["PATH"].split(os.pathsep):
        if os.path.exists(os.path.join(d, plugin_name)):
            durin_lib = os.path.join(d, plugin_name)
    if not durin_lib:
        print("Couldn't find plugin %s in PATH" % plugin_name)

    with open("XDS.INP", "w") as f:
        print(xds_inp, file=f)
        print("LIB=%s" % durin_lib, file=f)
    print("Written XDS input file to XDS.INP")


if __name__ == "__main__":
    run()
