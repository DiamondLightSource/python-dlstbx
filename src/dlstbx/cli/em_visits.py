from __future__ import annotations

import operator

import ispyb


def run():
    with ispyb.open("/dls_sw/apps/zocalo/secrets/credentials-ispyb-sp.cfg") as i:
        for microscope in range(13):
            beamline = "m%02d" % (microscope + 1)

            try:
                sessions = i.core.retrieve_current_sessions(beamline)
                print(f"{beamline}")
                for session in sorted(sessions, key=operator.itemgetter("session")):
                    location = f"/dls/{beamline}/data/{session['startDate']:%Y}/{session['session']}"
                    print(
                        f"  {session['session']:12s} {session['startDate']:%Y-%m-%d} - {session['endDate']:%Y-%m-%d}   {location}"
                    )
                print()
            except ispyb.NoResult:
                pass
