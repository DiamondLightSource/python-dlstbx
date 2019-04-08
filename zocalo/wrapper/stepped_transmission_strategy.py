from __future__ import absolute_import, division, print_function

import json
import logging

import zocalo.wrapper

logger = logging.getLogger("dlstbx.wrap.stepped_transmission_strategy")


class SteppedTransmissionWrapper(zocalo.wrapper.BaseWrapper):

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]
        dcid = params["dcid"]
        beamline = params["beamline"]
        resolution = params["resolution"]
        unitcella = params["unitcella"]
        unitcellb = params["unitcellb"]
        unitcellc = params["unitcellc"]
        unitcellalpha = params["unitcellalpha"]
        unitcellbeta = params["unitcellbeta"]
        unitcellgamma = params["unitcellgamma"]
        spacegroup = params["spacegroup"]

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
            'spacegroup': spacegroup,
            'unitcella': unitcella,
            'unitcellb': unitcellb,
            'unitcellc': unitcellc,
            'unitcellalpha': unitcellalpha,
            'unitcellbeta': unitcellbeta,
            'unitcellgamma': unitcellgamma,
            'strategies': [
                {'anomalous': False, 'wedges': recipe_1},
                {'anomalous': False, 'wedges': recipe_2},
            ]
        }
        return self.send_recipes_to_ispyb(dcid, recipes)

    def send_recipes_to_ispyb(self, dcid, recipes):
        logger.info("Inserting strategy recipes into ISPyB: %s" % json.dumps(recipes))
        self.recwrap.send_to("strategy-recipes", recipes)

