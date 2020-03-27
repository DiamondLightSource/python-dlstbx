# Interrogates Mimas given a datacollection ID

# Examples:
#
# dlstbx.mimas 1956161
#   show what would happen for a given DCID

import sys
from optparse import SUPPRESS_HELP, OptionParser

import dlstbx.ispybtbx
import dlstbx.mimas.core
import procrunner

if __name__ == "__main__":
    parser = OptionParser(usage="dlstbx.mimas [options] dcid")
    parser.add_option("-?", action="help", help=SUPPRESS_HELP)

    (options, args) = parser.parse_args(sys.argv[1:])

    if not all(arg.isnumeric() for arg in args):
        parser.error("Arguments must be DCIDs")

    for dcid in map(int, args):
        ispyb_message, ispyb_info = dlstbx.ispybtbx.ispyb_filter(
            {}, {"ispyb_dcid": dcid}
        )
        result = procrunner.run(
            ["/dls_sw/apps/mx-scripts/misc/isItAGridScan_InputDC", str(dcid)],
            print_stdout=False,
        )
        assert result.returncode == 0, (
            "grid scan check failed with %d" % result.returncode
        )
        gridscan = result.stdout.startswith(b"True")

        for event, readable in (
            (dlstbx.mimas.MimasEvent.START, "start of data collection"),
            (dlstbx.mimas.MimasEvent.END, "end of data collection"),
        ):
            scenario = dlstbx.mimas.MimasScenario(
                DCID=dcid,
                event=event,
                beamline=ispyb_info["ispyb_beamline"],
                runstatus=ispyb_info["ispyb_dc_info"]["runStatus"],
                spacegroup=ispyb_info.get("ispyb_space_group"),
                unitcell="",
                default_recipes=ispyb_message["default_recipe"],
                isitagridscan=gridscan,
                getsweepslistfromsamedcg="",
            )
            # from pprint import pprint
            # pprint(scenario._asdict())
            try:
                dlstbx.mimas.validate(scenario)
            except ValueError:
                print(f"Can not generate a valid Mimas scenario for {readable} {dcid}")
                raise

            actions = dlstbx.mimas.core.run(scenario)
            print(f"At the {readable} {dcid}:")
            for a in sorted(actions, key=lambda a: str(type(a)) + " " + a.recipe):
                try:
                    dlstbx.mimas.validate(a)
                except ValueError:
                    print(
                        f"Mimas scenario for DCID {dcid}, {event} returned invalid action {a!r}"
                    )
                    raise
                if isinstance(a, dlstbx.mimas.MimasRecipeInvocation):
                    print(f" - for DCID {a.DCID} call recipe {a.recipe}")
                elif isinstance(a, dlstbx.mimas.MimasISPyBJobInvocation):
                    print(
                        f" - create ISPyB job for DCID {a.DCID} named {a.displayname!r} with recipe {a.recipe}"
                    )
                else:
                    raise RuntimeError(f"Encountered unknown action {a!r}")
            if not actions:
                print(" - do nothing")
            print()
