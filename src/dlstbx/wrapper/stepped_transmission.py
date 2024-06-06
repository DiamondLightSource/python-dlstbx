from __future__ import annotations

import json

from dlstbx.wrapper import Wrapper


class SteppedTransmissionWrapper(Wrapper):
    _logger_name = "dlstbx.wrap.stepped_transmission"

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]
        dcid = params["dcid"]
        beamline = params["beamline"]
        wavelength = float(params["wavelength"])
        resolution = params["resolution"]
        unit_cell = self.recwrap.payload["unit_cell"]
        space_group = self.recwrap.payload["space_group"]

        beamline = params["beamline"]
        if beamline not in ("i03", "i04"):
            # Only generate SteppedTransmission strategy on these beamlines (i.e. beamline has Eiger)
            return True

        # T0 = (20, 100) / wavelength ^ 2 for I03, 4 respectively
        # Recipe 1: T0 / 8, 4, 2, 1 x 3,600 @ 500 Hz @ 0.1 degrees @ distance of screening images
        # Recipe 2: T0 / 8, 4, 2 x 3,600 @ 500 Hz @ 0.1 degrees @ distance of screening images x X = 0, 30

        beamline_t0 = {"i03": 20, "i04": 100}
        t0 = beamline_t0[beamline] / (wavelength**2)
        exposure = 1 / 500  # 500 Hz
        if t0 > 100:
            # Can't have transmission > 100% so scale up exposure times instead
            exposure *= t0 / 100
            t0 = 100

        recipe_base = {
            "resolution": resolution,
            "axisstart": 0,
            "axisend": 360,
            "oscillationrange": 0.1,
            "exposuretime": exposure,
            "noimages": 3600,
            "rotationaxis": "omega",
            "mosaicity": None,
            "completeness": None,
            "rankingResolution": None,
            "phi": "0",
        }

        recipe_1 = [
            {"transmission": t0 / 8, "chi": "0"},
            {"transmission": t0 / 4, "chi": "0"},
            {"transmission": t0 / 2, "chi": "0"},
            {"transmission": t0, "chi": "0"},
        ]
        recipe_1 = [dict(list(r.items()) + list(recipe_base.items())) for r in recipe_1]

        recipe_2 = [
            {"transmission": t0 / 8, "chi": "0"},
            {"transmission": t0 / 8, "chi": "30"},
            {"transmission": t0 / 4, "chi": "0"},
            {"transmission": t0 / 4, "chi": "30"},
            {"transmission": t0 / 2, "chi": "0"},
            {"transmission": t0 / 2, "chi": "30"},
        ]
        recipe_2 = [dict(list(r.items()) + list(recipe_base.items())) for r in recipe_2]

        ispyb_command_list = []

        for i, wedges in enumerate([recipe_1, recipe_2][:1]):
            # Step 1: Add new record to Screening table, keep the ScreeningId
            d = {
                "dcid": dcid,
                "programversion": "Stepped transmission %i" % (i + 1),
                "program": "Stepped transmission %i" % (i + 1),
                "comments": "Stepped transmission %i" % (i + 1),
                "ispyb_command": "insert_screening",
                "store_result": "ispyb_screening_id_%i" % i,
            }
            ispyb_command_list.append(d)

            # Step 2: Store screeningOutput results, linked to the screeningId
            #         Keep the screeningOutputId
            d = {
                "program": "Stepped transmission",
                "ispyb_command": "insert_screening_output",
                "screening_id": "$ispyb_screening_id_%i" % i,
                "store_result": "ispyb_screening_output_id_%i" % i,
            }
            ispyb_command_list.append(d)

            # Step 3: Store screeningOutputLattice results, linked to the screeningOutputId
            #         Keep the screeningOutputLatticeId
            d = {
                "spacegroup": space_group,
                "unitcella": unit_cell[0],
                "unitcellb": unit_cell[1],
                "unitcellc": unit_cell[2],
                "unitcellalpha": unit_cell[3],
                "unitcellbeta": unit_cell[4],
                "unitcellgamma": unit_cell[5],
                "ispyb_command": "insert_screening_output_lattice",
                "screening_output_id": "$ispyb_screening_output_id_%i" % i,
                "store_result": "ispyb_screening_output_lattice_id_%i" % i,
            }
            ispyb_command_list.append(d)

            # Step 4: Store screeningStrategy results, linked to the screeningOutputId
            #         Keep the screeningStrategyId
            d = {
                "program": "Stepped transmission #%i" % (i + 1),
                "anomalous": False,
                "ispyb_command": "insert_screening_strategy",
                "screening_output_id": "$ispyb_screening_output_id_%i" % i,
                "store_result": "ispyb_screening_strategy_id_%i" % i,
            }
            ispyb_command_list.append(d)

            for j, wedge in enumerate(wedges):
                # Step 5: Store screeningStrategyWedge results, linked to the screeningStrategyId
                #         Keep the screeningStrategyWedgeId
                d = {
                    "wedgenumber": j + 1,
                    "ispyb_command": "insert_screening_strategy_wedge",
                    "screening_strategy_id": "$ispyb_screening_strategy_id_%i" % i,
                    "store_result": "ispyb_screening_strategy_wedge_id_%i_%i" % (i, j),
                    "comments": "Stepped transmission #%i.%i" % (i + 1, j + 1),
                }
                ispyb_command_list.append(d)

                # Step 6: Store screeningStrategySubWedge results, linked to the screeningStrategyWedgeId
                #         Keep the screeningStrategySubWedgeId
                d = {
                    "subwedgenumber": 1,
                    "comments": "Stepped transmission #%i.%i" % (i + 1, j + 1),
                    "ispyb_command": "insert_screening_strategy_sub_wedge",
                    "screening_strategy_wedge_id": "$ispyb_screening_strategy_wedge_id_%i_%i"
                    % (i, j),
                    "store_result": "ispyb_screening_strategy_sub_wedge_id_%i_%i"
                    % (i, j),
                }
                for k in (
                    "resolution",
                    "axisstart",
                    "axisend",
                    "oscillationrange",
                    "exposuretime",
                    "noimages",
                    "rotationaxis",
                    "phi",
                    "transmission",
                ):
                    d[k] = wedge[k]
                ispyb_command_list.append(d)

        if ispyb_command_list:
            self.log.debug("Sending %s", json.dumps(ispyb_command_list))
            self.recwrap.send_to("ispyb", {"ispyb_command_list": ispyb_command_list})
            self.log.info("Sent %d commands to ISPyB", len(ispyb_command_list))
        else:
            self.log.warning("No commands to send to ISPyB")

        return True
