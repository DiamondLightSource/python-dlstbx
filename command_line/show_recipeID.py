#
# dlstbx.show_recipeID
#   prints out information about a given recipe ID
#

from __future__ import absolute_import, division, print_function

import py
import sys
from datetime import datetime
from optparse import SUPPRESS_HELP, OptionParser

import dlstbx.util.colorstreamhandler

if __name__ == "__main__":
    parser = OptionParser(usage="dlstbx.show_recipeID [options] recipe-ID (..)")
    parser.add_option("-?", action="help", help=SUPPRESS_HELP)
    (options, args) = parser.parse_args(sys.argv[1:])

    if not args:
        parser.print_help()
        sys.exit(0)

    c = dlstbx.util.colorstreamhandler.ColorStreamHandler
    base_path = py.path.local("/dls/tmp/zocalo/dispatcher")
    candidates = base_path.listdir()
    directories = list(filter(lambda x: x.check(dir=True), candidates))

    for recipe in args:
        for d in directories:
            recipe_file = d.join(recipe[0:2]).join(recipe[2:])
            if recipe_file.check():
                print(
                    "{c.BOLD}{c.GREEN}Recipe {recipe} ({timestamp:%Y-%m-%d %H:%M:%S}){c.DEFAULT}".format(
                        recipe=recipe, c=c, timestamp=datetime.fromtimestamp(recipe_file.mtime())
                    )
                )
                for line in recipe_file.readlines(cr=False):
                    if not line.startswith(" ") and line.endswith(":"):
                        print(
                            "{c.BOLD}{c.YELLOW}{line}{c.DEFAULT}".format(line=line, c=c)
                        )
                    else:
                        print(line)
                break
        else:
            print(
                "{c.BOLD}{c.RED}Recipe {recipe} not found{c.DEFAULT}".format(
                    recipe=recipe, c=c
                )
            )
