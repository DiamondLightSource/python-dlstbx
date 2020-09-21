# Returns the parameter dictionary that is available in recipes for a
# given data processing ID or reprocessing ID.

# Examples:
#
# dlstbx.find_in_ispyb 1956161
#   show available dictionary for DCID
#
# dlstbx.find_in_ispyb -p 95
#   show available dictionary for reprocessing ID


import json
import pprint
import re
import sys
from optparse import SUPPRESS_HELP, OptionParser

import dlstbx.ispybtbx
import py
import workflows.recipe

recipe_matcher = re.compile(
    "[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}"
)


def get_dcid_for_recipe_ID(recipe):
    if not recipe_matcher.match(recipe):
        return

    base_path = py.path.local("/dls/tmp/zocalo/dispatcher")
    candidates = base_path.listdir()
    for d in candidates:
        if not d.check(dir=True):
            continue
        recipe_file = d.join(recipe[0:2]).join(recipe[2:])
        if recipe_file.check():
            try:
                lines = iter(recipe_file.readlines(cr=False))
                while next(lines) != "Incoming message body:":
                    pass
                incoming_block = []

                line = next(lines)
                while line:
                    incoming_block.append(line)
                    line = next(lines)
            except StopIteration:
                sys.exit(f"Malformed recipe found in {recipe_file.strpath}")

            incoming_block = json.loads("\n".join(incoming_block))
            if (
                not isinstance(incoming_block, dict)
                or "parameters" not in incoming_block
                or not isinstance(incoming_block["parameters"], dict)
            ):
                sys.exit(
                    "Recipe {} does not contain processing parameters".format(
                        recipe_file.strpath
                    )
                )
            parameters = incoming_block["parameters"]
            if parameters.get("ispyb_dcid"):
                return {"ispyb_dcid": parameters["ispyb_dcid"]}
            sys.exit(f"Recipe {recipe_file.strpath} does not reference a DCID")
    else:
        sys.exit(f"Recipe {recipe} not found.")


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
    parser.add_option(
        "-o",
        "--out",
        dest="outfile",
        metavar="FILE",
        action="store",
        type="string",
        default="",
        help="Output file for the filled-in recipe.",
    )
    (options, args) = parser.parse_args(sys.argv[1:])

    _ispybtbx_instance = None
    for arg in args:
        parameters = get_dcid_for_recipe_ID(arg)
        if parameters:
            print("Recipe ID:", arg)
        else:
            parameters = {}
            if options.reprocess:
                parameters["ispyb_process"] = int(arg)
            else:
                if arg.isdigit():
                    parameters["ispyb_dcid"] = int(arg)
                else:
                    if not _ispybtbx_instance:
                        _ispybtbx_instance = dlstbx.ispybtbx.ispybtbx()
                    parameters["ispyb_dcid"] = _ispybtbx_instance.get_dcid_for_path(arg)

        if parameters.get("ispyb_process"):
            print("Processing ID:", parameters["ispyb_process"])
        else:
            print("Data collection ID:", parameters["ispyb_dcid"])
        message, parameters = dlstbx.ispybtbx.ispyb_filter({}, parameters)

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
            json_str = json.dumps(d, indent=2)
            print(json_str)
            if options.outfile:
                with open(options.outfile, "w") as f:
                    f.write(json_str)
        else:
            pprint.pprint(parameters)
        if message.get("default_recipe"):
            print("Default recipes:", ", ".join(sorted(message["default_recipe"])))
        print(
            "Synchweb: https://ispyb.diamond.ac.uk/dc/visit/{0[ispyb_visit]}/id/{0[ispyb_dcid]}".format(
                parameters
            )
        )
        print()
