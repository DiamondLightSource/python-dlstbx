from __future__ import absolute_import, division, print_function

import json
import logging

import zocalo.wrapper

logger = logging.getLogger("dlstbx.wrap.stepped_transmission")


class SteppedTransmissionWrapper(zocalo.wrapper.BaseWrapper):

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]
        dcid = params["dcid"]
        beamline = params["beamline"]
        resolution = params["resolution"]
        unit_cell = params["unit_cell"]
        space_group = params["space_group"]

        # T0 = (20, 100) / wavelength ^ 2 for I03, 4 respectively
        # Recipe 1: T0 / 8, 4, 2, 1 x 3,600 @ 100 Hz @ 0.1 degrees @ distance of screening images
        # Recipe 2: T0 / 8, 4, 2 x 3,600 @ 100 Hz @ 0.1 degrees @ distance of screening images x X = 0, 30

        beamline_t0 = {'i03': 20, 'i04': 100}
        t0 = beamline_t0[beamline]

        recipe_base = {
            'resolution': resolution,
            'axisstart': 0,
            'axisend': 360,
            'oscillationrange': 0.1,
            'exposuretime': 1/100, # 100 Hz
            'noimages': 3600,
            'rotationaxis': 'omega',
            'mosaicity': None,
            'completeness': None,
            'rankingResolution': None,
            'phi': '0',
        }

        recipe_1 = [
            {
                'comments': 'Stepped transmission #1.1',
                'wedgenumber': '1',
                'transmission': t0/8,
                'chi': '0',
            },
            {
                'comments': 'Stepped transmission #1.2',
                'wedgenumber': '2',
                'transmission': t0/4,
                'chi': '0',
            },
            {
                'comments': 'Stepped transmission #1.3',
                'wedgenumber': '3',
                'transmission': t0/2,
                'chi': '0',
            },
            {
                'comments': 'Stepped transmission #1.4',
                'wedgenumber': '4',
                'transmission': t0,
                'chi': '0',
            },
        ]
        recipe_1 = [dict(r.items() + recipe_base.items()) for r in recipe_1]

        recipe_2 = [
            {
                'comments': 'Stepped transmission #2.1',
                'wedgenumber': '1',
                'transmission': t0/8,
                'chi': '0',
            },
            {
                'comments': 'Stepped transmission #2.2',
                'wedgenumber': '2',
                'transmission': t0/8,
                'chi': '30',
            },
            {
                'comments': 'Stepped transmission #2.3',
                'wedgenumber': '3',
                'transmission': t0/4,
                'chi': '0',
            },
            {
                'comments': 'Stepped transmission #2.4',
                'wedgenumber': '4',
                'transmission': t0/4,
                'chi': '30',
            },
            {
                'comments': 'Stepped transmission #2.5',
                'wedgenumber': '5',
                'transmission': t0/2,
                'chi': '0',
            },
            {
                'comments': 'Stepped transmission #2.6',
                'wedgenumber': '6',
                'transmission': t0/2,
                'chi': '30',
            },
        ]
        recipe_2 = [dict(r.items() + recipe_base.items()) for r in recipe_2]

        recipes = {
            'dcid': dcid,
            'comments': 'Stepped transmission',
            'program': 'Stepped transmission',
            'spacegroup': space_group,
            'unitcella': unit_cell[0],
            'unitcellb': unit_cell[1],
            'unitcellc': unit_cell[2],
            'unitcellalpha': unit_cell[3],
            'unitcellbeta': unit_cell[4],
            'unitcellgamma': unit_cell[5],
            'strategies': [
                {'anomalous': False, 'wedges': recipe_1},
                {'anomalous': False, 'wedges': recipe_2},
            ]
        }

        ispyb_command_list = []

        # Step 1: Add new record to Screening table, keep the ScreeningId
        d = {
            "dcid": dcid,
            "program": "Stepped transmission",
            "comments": "Stepped transmission",
            "ispyb_command": "insert_screening",
            "store_result": "ispyb_screening_id",
        }
        ispyb_command_list.append(d)

        # Step 2: Store screeningOutput results, linked to the screeningId
        #         Keep the screeningOutputId
        d = {
            "program": "Stepped transmission",
            "ispyb_command": "insert_screening_output",
            "screening_id": "$ispyb_screening_id",
            "store_result": "ispyb_screening_output_id",
        }
        ispyb_command_list.append(d)

        # Step 3: Store screeningOutputLattice results, linked to the screeningOutputId
        #         Keep the screeningOutputLatticeId
        d = {
            'spacegroup': spacegroup,
            'unitcella': unitcella,
            'unitcellb': unitcellb,
            'unitcellc': unitcellc,
            'unitcellalpha': unitcellalpha,
            'unitcellbeta': unitcellbeta,
            'unitcellgamma': unitcellgamma,
            "ispyb_command": "insert_screening_output_lattice",
            "screening_output_id": "$ispyb_screening_output_id",
            "store_result": "ispyb_screening_output_lattice_id",
        }
        ispyb_command_list.append(d)

        for i, wedges in enumerate([recipe_1, recipe_2]):

            # Step 4: Store screeningStrategy results, linked to the screeningOutputId
            #         Keep the screeningStrategyId
            d = {
                'program': "Stepped transmission #%i" % (i + 1),
                "anomalous": False,
                "ispyb_command": "insert_screening_strategy",
                "screening_output_id": "$ispyb_screening_output_id",
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
                }
                ispyb_command_list.append(d)

                # Step 6: Store screeningStrategySubWedge results, linked to the screeningStrategyWedgeId
                #         Keep the screeningStrategySubWedgeId
                d = {
                    "subwedgenumber": 1,
                    "ispyb_command": "insert_screening_strategy_sub_wedge",
                    "screening_strategy_wedge_id": "$ispyb_screening_strategy_wedge_id_%i_%i" % (i, j),
                    "store_result": "ispyb_screening_strategy_sub_wedge_id_%i_%i" % (i, j),
                }
                for k in ("resolution", "axisstart", "axisend", "oscillationrange", "exposuretime",
                          "noimages", "rotationaxis", "phi"):
                    d[k] = wedge[k]
                ispyb_command_list.append(d)

        logger.info("Sending %s", json.dumps(ispyb_command_list))
        self.recwrap.send_to("ispyb", {"ispyb_command_list": ispyb_command_list})
        logger.info("Sent %d commands to ISPyB", len(ispyb_command_list))

