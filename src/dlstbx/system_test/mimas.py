from __future__ import annotations

from workflows.recipe import Recipe

from dlstbx.system_test.common import CommonSystemTest

imagepath = "/dls/mx/data/nt24686/nt24686-7/VMXi-AB5081/well_113/images/"


class MimasService(CommonSystemTest):
    """Tests for the per-image-analysis service."""

    def test_i03_rotation_end(self):
        """Find all files referenced in an example nexus dataset."""

        recipe = {
            "start": [[1, []]],
            "1": {
                "service": "MIMAS Business Logic",
                "queue": "mimas",
                "parameters": {
                    "dcid": "8114255",
                    "event": "end",
                    "beamline": "i03",
                    "visit": "cm31105-2",
                    "detectorclass": "eiger",
                    "dc_class": {"grid": False, "screen": False, "rotation": True},
                    "ispyb_images": "",
                    "preferred_processing": "xia2/DIALS",
                    "run_status": "DataCollection Successful",
                    "space_group": "",
                    "sweep_list": [[8114255, 1, 450]],
                    "unit_cell": False,
                    "diffraction_plan_info": None,
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

        ispyb_job_template = {
            "DCID": 8114255,
            "source": "automatic",
            "comment": "",
            "displayname": "",
            "parameters": [],
            "sweeps": [],
            "triggervariables": [],
        }

        expected_recipe_pointers_and_payloads = (
            (2, {"recipes": ["archive-nexus"], "parameters": {"ispyb_dcid": 8114255}}),
            (
                2,
                {
                    "recipes": ["generate-crystal-thumbnails"],
                    "parameters": {"ispyb_dcid": 8114255},
                },
            ),
            (
                2,
                {
                    "recipes": ["generate-diffraction-preview"],
                    "parameters": {"ispyb_dcid": 8114255},
                },
            ),
            (
                2,
                {
                    "recipes": ["per-image-analysis-rotation-swmr"],
                    "parameters": {"ispyb_dcid": 8114255},
                },
            ),
            (
                2,
                {
                    "recipes": ["processing-rlv-eiger"],
                    "parameters": {"ispyb_dcid": 8114255},
                },
            ),
            (
                3,
                ispyb_job_template
                | {"autostart": True, "recipe": "autoprocessing-fast-dp-eiger"},
            ),
            (
                3,
                ispyb_job_template
                | {
                    "autostart": True,
                    "recipe": "autoprocessing-xia2-dials-eiger-cluster",
                    "parameters": [
                        {
                            "key": "resolution.cc_half_significance_level",
                            "value": "0.1",
                        },
                        {"key": "absorption_level", "value": "medium"},
                    ],
                },
            ),
            (
                3,
                ispyb_job_template
                | {
                    "autostart": False,
                    "recipe": "autoprocessing-xia2-3dii-eiger-cluster",
                    "parameters": [
                        {"key": "resolution.cc_half_significance_level", "value": "0.1"}
                    ],
                },
            ),
            (
                3,
                ispyb_job_template
                | {
                    "autostart": False,
                    "recipe": "autoprocessing-autoPROC-eiger-cluster",
                },
            ),
            (
                3,
                ispyb_job_template
                | {
                    "autostart": False,
                    "recipe": "autoprocessing-xia2-3dii-eiger-cloud",
                    "parameters": [
                        {"key": "resolution.cc_half_significance_level", "value": "0.1"}
                    ],
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
