from __future__ import annotations

from workflows.recipe import Recipe

from dlstbx.system_test.common import CommonSystemTest

imagepath = "/dls/mx/data/nt24686/nt24686-7/VMXi-AB5081/well_113/images/"


class MimasService(CommonSystemTest):
    """Tests for the per-image-analysis service."""

    def test_i03_gridscan_end(self):
        """Find all files referenced in an example nexus dataset."""

        recipe = {
            "start": [[1, []]],
            "1": {
                "service": "MIMAS Business Logic",
                "queue": "mimas",
                "parameters": {
                    "dcid": "8257178",
                    "event": "end",
                    "beamline": "i03",
                    "visit": "cm31105-2",
                    "detectorclass": "eiger",
                    "dc_class": {
                        "grid": True,
                        "screen": False,
                        "rotation": False,
                        "serial_fixed": False,
                        "serial_jet": False,
                    },
                    "run_status": "DataCollection Successful",
                },
                "output": {"dispatcher": 2, "ispyb": 3},
            },
            "2": {
                "service": "Immediate recipe invocation",
                "queue": self.target_queue,
            },
            "3": {
                "service": "DLS ISPyB connector",
                "queue": self.target_queue,
            },
        }

        recipe = Recipe(recipe)
        recipe.validate()

        self.send_message(
            queue=recipe[1]["queue"],
            message={
                "payload": recipe["start"][0][1],
                "recipe": recipe.recipe,
                "recipe-pointer": "1",
                "environment": {"ID": self.guid},
            },
            headers={"workflows-recipe": True},
        )

        expected_recipe_pointers_and_payloads = (
            (
                2,
                {
                    "recipes": ["generate-crystal-thumbnails"],
                    "parameters": {"ispyb_dcid": 8257178},
                },
            ),
            (
                2,
                {"recipes": ["archive-nexus"], "parameters": {"ispyb_dcid": 8257178}},
            ),
            (
                2,
                {
                    "recipes": ["generate-diffraction-preview"],
                    "parameters": {"ispyb_dcid": 8257178},
                },
            ),
            (
                2,
                {
                    "recipes": ["per-image-analysis-gridscan-i03"],
                    "parameters": {"ispyb_dcid": 8257178},
                },
            ),
        )

        for recipe_pointer, payload in expected_recipe_pointers_and_payloads:
            self.expect_recipe_message(
                environment={"ID": self.guid},
                recipe=recipe,
                recipe_path=[1],
                recipe_pointer=recipe_pointer,
                payload=payload,
                timeout=20,
            )
