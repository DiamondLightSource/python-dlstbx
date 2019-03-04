# Returns the parameter dictionary that is available in recipes for a
# given data processing ID or reprocessing ID.

# Examples:
#
# dlstbx.find_in_ispyb 1956161
#   show available dictionary for DCID
#
# dlstbx.find_in_ispyb -p 95
#   show available dictionary for reprocessing ID

from __future__ import absolute_import, division, print_function

import json
import pprint
import sys
from optparse import SUPPRESS_HELP, OptionParser

import workflows.recipe
from dlstbx.ispybtbx import ispyb_filter

if __name__ == "__main__":
    parser = OptionParser(usage="dlstbx.find_in_ispyb [options] dcid")
    parser.add_option("-?", action="help", help=SUPPRESS_HELP)

    parser.add_option(
        "-p",
        "--reprocessing",
        dest="reprocess",
        action="store_true",
        default=False,
        help="Means a reprocessing ID is given rather than a data collection ID",
    )
    parser.add_option(
        "-f",
        "--file",
        dest="recipefile",
        metavar="FILE",
        action="store",
        type="string",
        default="",
        help="Fill in recipe contained in this file with information from ISPyB.",
    )
    parser.add_option(
        "--recipe-pointer",
        dest="recipepointer",
        metavar="NUMBER",
        action="store",
        type="int",
        default=None,
        help="A recipe pointer to output a recipe wrapper.",
    )
    (options, args) = parser.parse_args(sys.argv[1:])

    for arg in args:
        parameters = {}
        if options.reprocess:
            print("Processing ID:", arg)
            parameters["ispyb_process"] = int(arg)
        else:
            print("Data collection ID:", arg)
            parameters["ispyb_dcid"] = int(arg)
        message, parameters = ispyb_filter({}, parameters)

        if options.recipefile:
            with open(options.recipefile, "rb") as f:
                recipe = workflows.recipe.Recipe(json.load(f))
            recipe.apply_parameters(parameters)
            if options.recipepointer:
                d = {
                    "recipe": recipe.recipe,
                    "recipe-pointer": options.recipepointer,
                    "recipe-path": [],
                    "environment": {},
                }
            else:
                d = recipe.recipe
            print(json.dumps(d, indent=2))
        else:
            pprint.pprint(parameters)
        if message.get("default_recipe"):
            print("Default recipes:", ", ".join(sorted(message["default_recipe"])))
        print()
