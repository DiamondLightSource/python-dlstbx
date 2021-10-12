# Interrogates Mimas. Shows what would happen for a given datacollection ID

# Examples:
#
# dlstbx.mimas 4983840  # I19-1 data collection
# dlstbx.mimas 4985704  # I04 gridscan
# dlstbx.mimas 4985701  # I04 rotation with no known space group
# dlstbx.mimas 4985686  # I04-1 rotation with known space group
# dlstbx.mimas 4983807  # I24 gridscan
# dlstbx.mimas 4966986  # I24 rotation with no known space group

import argparse

import dlstbx.ispybtbx
import dlstbx.mimas.core

_readable = {
    dlstbx.mimas.MimasEvent.START: "start of data collection",
    dlstbx.mimas.MimasEvent.END: "end of data collection",
}


def get_scenarios(dcid):
    ispyb_message, ispyb_info = dlstbx.ispybtbx.ispyb_filter({}, {"ispyb_dcid": dcid})
    cell = ispyb_info.get("ispyb_unit_cell")
    if cell:
        cell = dlstbx.mimas.MimasISPyBUnitCell(*cell)
    else:
        cell = None
    spacegroup = ispyb_info.get("ispyb_space_group")
    if spacegroup:
        spacegroup = dlstbx.mimas.MimasISPyBSpaceGroup(spacegroup)
    else:
        spacegroup = None
    anomalous_scatterer = None
    diffraction_plan_info = ispyb_info.get("ispyb_diffraction_plan")
    if diffraction_plan_info:
        anomalous_scatterer = ispyb_info.get("ispyb_diffraction_plan", {}).get(
            "anomalousScatterer"
        )
        if anomalous_scatterer:
            anomalous_scatterer = dlstbx.mimas.MimasISPyBAnomalousScatterer(
                anomalous_scatterer
            )
    dc_class = ispyb_info.get("ispyb_dc_class")
    if dc_class and dc_class["grid"]:
        dc_class_mimas = dlstbx.mimas.MimasDCClass.GRIDSCAN
    elif dc_class and dc_class["screen"]:
        dc_class_mimas = dlstbx.mimas.MimasDCClass.SCREENING
    elif dc_class and dc_class["rotation"]:
        dc_class_mimas = dlstbx.mimas.MimasDCClass.ROTATION
    else:
        dc_class_mimas = dlstbx.mimas.MimasDCClass.UNDEFINED

    detectorclass = (
        dlstbx.mimas.MimasDetectorClass.EIGER
        if ispyb_info["ispyb_detectorclass"] == "eiger"
        else dlstbx.mimas.MimasDetectorClass.PILATUS
    )
    scenarios = []
    for event in (dlstbx.mimas.MimasEvent.START, dlstbx.mimas.MimasEvent.END):
        scenario = dlstbx.mimas.MimasScenario(
            DCID=dcid,
            dcclass=dc_class_mimas,
            event=event,
            beamline=ispyb_info["ispyb_beamline"],
            visit=ispyb_info["ispyb_visit"],
            runstatus=ispyb_info["ispyb_dc_info"]["runStatus"],
            spacegroup=spacegroup,
            unitcell=cell,
            getsweepslistfromsamedcg=tuple(
                dlstbx.mimas.MimasISPyBSweep(*sweep)
                for sweep in ispyb_info["ispyb_related_sweeps"]
            ),
            preferred_processing=ispyb_info.get("ispyb_preferred_processing"),
            detectorclass=detectorclass,
            anomalous_scatterer=anomalous_scatterer,
        )
        try:
            dlstbx.mimas.validate(scenario)
        except ValueError:
            print(
                f"Can not generate a valid Mimas scenario for {_readable.get(scenario.event)} {dcid}"
            )
            raise
        scenarios.append(scenario)
    return scenarios


def run(args=None):
    parser = argparse.ArgumentParser(usage="dlstbx.mimas [options] dcid")
    parser.add_argument("dcids", type=int, nargs="+", help="Data collection ids")
    parser.add_argument("-?", action="help", help=argparse.SUPPRESS)
    parser.add_argument(
        "--commands",
        "-c",
        action="store_true",
        dest="show_commands",
        default=False,
        help="Show commands that would trigger the individual processing steps",
    )

    args = parser.parse_args(args)

    for dcid in args.dcids:
        for scenario in get_scenarios(dcid):
            actions = dlstbx.mimas.core.run(scenario)
            print(f"At the {_readable.get(scenario.event)} {dcid}:")
            for a in sorted(actions, key=lambda a: str(type(a)) + " " + a.recipe):
                try:
                    dlstbx.mimas.validate(a)
                except ValueError:
                    print(
                        f"Mimas scenario for DCID {dcid}, {scenario.event} returned invalid action {a!r}"
                    )
                    raise
                if isinstance(a, dlstbx.mimas.MimasRecipeInvocation):
                    if args.show_commands:
                        print(" - " + dlstbx.mimas.zocalo_command_line(a))
                    else:
                        print(f" - for DCID {a.DCID} call recipe {a.recipe}")
                elif isinstance(a, dlstbx.mimas.MimasISPyBJobInvocation):
                    if args.show_commands:
                        print(" - " + dlstbx.mimas.zocalo_command_line(a))
                    else:
                        print(
                            f" - create ISPyB job for DCID {a.DCID} named {a.displayname!r} with recipe {a.recipe} (autostart={a.autostart})"
                        )
                else:
                    raise RuntimeError(f"Encountered unknown action {a!r}")
            if not actions:
                print(" - do nothing")
            print()
