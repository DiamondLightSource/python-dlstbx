#
# dlstbx.show_recipeID
#   prints out information about a given recipe ID
#


import py
import sys
from argparse import SUPPRESS, ArgumentParser
from datetime import datetime

import dlstbx.util.colorstreamhandler


def run():
    parser = ArgumentParser(prog="dlstbx.show_recipeID")
    parser.add_argument(
        "--only-incoming",
        action="store_true",
        dest="only_incoming",
        default=False,
        help="show only the incoming message information",
    )
    parser.add_argument(
        "recipe",
        type=str,
        nargs="+",
        help="ID of recipe (in the form of 12345678-90ab-cdef-...) to show information for",
    )
    parser.add_argument("-?", action="help", help=SUPPRESS)
    args = parser.parse_args()

    if not args:
        parser.print_help()
        sys.exit(0)

    c = dlstbx.util.colorstreamhandler.ColorStreamHandler
    base_path = py.path.local("/dls/tmp/zocalo/dispatcher")
    candidates = base_path.listdir()
    directories = [x for x in candidates if x.check(dir=True)]
    isatty = sys.stdout.isatty()
    quiet = False

    for recipe in args.recipe:
        for d in directories:
            recipe_file = d.join(recipe[0:2]).join(recipe[2:])
            if recipe_file.check():
                line = "Recipe {recipe} ({timestamp:%Y-%m-%d %H:%M:%S})".format(
                    recipe=recipe,
                    timestamp=datetime.fromtimestamp(recipe_file.mtime()),
                )
                if isatty:
                    print(f"{c.BOLD}{c.GREEN}{line}{c.DEFAULT}")
                else:
                    print(line)

                for line in recipe_file.readlines(cr=False):
                    if not line.startswith(" ") and line.endswith(":"):
                        if args.only_incoming:
                            quiet = line != "Incoming message body:"
                        if not quiet:
                            if isatty:
                                print(f"{c.BOLD}{c.YELLOW}{line}{c.DEFAULT}")
                            else:
                                print(line)
                    elif not quiet:
                        print(line)
                break
        else:
            print(f"{c.BOLD}{c.RED}Recipe {recipe} not found{c.DEFAULT}")
