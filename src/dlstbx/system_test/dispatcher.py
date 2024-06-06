from __future__ import annotations

import copy
import json
import os.path
from unittest import mock

from workflows.recipe import Recipe

from dlstbx.system_test.common import CommonSystemTest


class DispatcherService(CommonSystemTest):
    """Tests for the dispatcher service (recipe service)."""

    def test_processing_a_trivial_recipe(self):
        """Passing in a recipe to the service without external dependencies.
        The recipe should be interpreted and a simple message passed back to a
        fixed destination."""

        recipe = {
            1: {
                "service": "DLS system test",
                "queue": self.target_queue,
            },
            "start": [(1, {"purpose": "trivial test for the recipe parsing service"})],
        }

        self.send_message(queue="processing_recipe", message={"custom_recipe": recipe})

        self.expect_recipe_message(
            recipe=Recipe(recipe),
            recipe_path=[],
            recipe_pointer=1,
            payload=recipe["start"][0][1],
        )

    def disabled_test_guid_generation_during_recipe_parsing(self):
        """The guid parameter should be created during parsing of each recipe."""

        recipe = {  # noqa: F841
            1: {
                "service": "DLS system test",
                "queue": self.target_queue,
            },
            "start": [
                (1, {"purpose": "guid generation test for the recipe parsing service"})
            ],
        }

        # TODO: The testing framework actually does not support this atm!

    def test_parsing_a_recipe_and_replacing_parameters(self):
        """Passing in a recipe to the service without external dependencies.
        The recipe should be interpreted, the 'guid' placeholder replaced using
        the parameter field, and the message passed back.
        The message should then contain the recipe and a correctly set pointer."""

        recipe = {
            1: {"service": "DLS system test", "queue": self.target_queue},
            "start": [(1, {"purpose": "test the recipe parsing service"})],
        }
        parameters = {"guid": self.guid}

        self.send_message(
            queue="processing_recipe",
            message={"parameters": parameters, "custom_recipe": recipe},
        )

        expected_recipe = Recipe(recipe)
        expected_recipe.apply_parameters(parameters)
        self.expect_recipe_message(
            recipe=expected_recipe,
            recipe_path=[],
            recipe_pointer=1,
            payload=recipe["start"][0][1],
        )

    def test_loading_a_recipe_from_a_file(self):
        """When a file name is passed to the service the file should be loaded and
        parsed correctly, including parameter replacement."""

        parameters = {"queue": self.target_queue}
        self.send_message(
            queue="processing_recipe",
            message={"parameters": parameters, "recipes": ["test-dispatcher"]},
        )

        recipe_path = self.zc.storage.get("zocalo.recipe_directory")
        with open(os.path.join(recipe_path, "test-dispatcher.json")) as fh:
            recipe = json.load(fh)
        expected_recipe = Recipe(recipe)
        expected_recipe.apply_parameters(parameters)

        self.expect_recipe_message(
            recipe=expected_recipe,
            recipe_path=[],
            recipe_pointer=1,
            payload=recipe["start"][0][1],
        )

    def test_combining_recipes(self):
        """Combine a recipe from a file and a custom recipe."""

        parameters = {"queue": self.target_queue}
        recipe_passed = {
            1: {"service": "DLS system test", "queue": self.target_queue},
            "start": [(1, {"purpose": "test recipe merging"})],
        }
        self.send_message(
            queue="processing_recipe",
            message={
                "parameters": parameters,
                "custom_recipe": recipe_passed,
                "recipes": ["test-dispatcher"],
            },
        )

        recipe_path = self.zc.storage.get("zocalo.recipe_directory")
        with open(os.path.join(recipe_path, "test-dispatcher.json")) as fh:
            recipe_from_file = json.loads(fh.read())

        self.expect_recipe_message(
            recipe=mock.ANY,
            recipe_path=[],
            recipe_pointer=1,
            queue=self.target_queue,
            payload=recipe_passed["start"][0][1],
        )
        self.expect_recipe_message(
            recipe=mock.ANY,
            recipe_path=[],
            recipe_pointer=2,
            queue=self.target_queue,
            payload=recipe_from_file["start"][0][1],
        )

    def test_ispyb_magic(self):
        """Test the ISPyB magic to see that it does what we think it should do"""

        recipe = {
            1: {
                "service": "DLS system test",
                "queue": self.target_queue,
            },
            "start": [
                (
                    1,
                    {
                        "purpose": "testing if ISPyB connection works",
                        "parameters": {"image": "{ispyb_image}"},
                    },
                )
            ],
        }

        self.send_message(
            queue="processing_recipe",
            message={
                "custom_recipe": recipe,
                "parameters": {"ispyb_dcid": 1397955, "ispyb_wait_for_runstatus": True},
            },
        )

        recipe["start"][0][1]["parameters"]["image"] = (
            "/dls/i03/data/2016/cm14451-4/tmp/2016-10-07/fake113556/TRP_M1S6_4_0001.cbf:1:1800"
        )

        self.expect_recipe_message(
            recipe=Recipe(recipe),
            recipe_path=[],
            recipe_pointer=1,
            payload=recipe["start"][0][1],
        )

    def test_wait_for_ispyb_runstatus(self):
        """
        Test the logic to wait for a RunStatus to be set in ISPyB.
        Since we don't touch the database this should run into a timeout condition.
        """

        recipe = {
            1: {
                "service": "DLS system test - should not end up here",
                "queue": self.target_queue,
            },
            "start": [
                [
                    1,
                    {
                        "purpose": "wait for undefined runstatus",
                    },
                ]
            ],
        }

        message = {
            "custom_recipe": recipe,
            "parameters": {
                "ispyb_dcid": 4977408,
                "ispyb_wait_for_runstatus": True,
                "dispatcher_timeout": 10,
                "dispatcher_error_queue": self.target_queue,
            },
        }
        self.send_message(queue="processing_recipe", message=message)

        self.expect_unreached_recipe_step(recipe=recipe, recipe_pointer=1)

        # Emulate recipe mangling
        message = copy.deepcopy(message)
        message["custom_recipe"]["1"] = message["custom_recipe"][1]
        del message["custom_recipe"][1]
        message["parameters"]["guid"] = mock.ANY
        message["parameters"]["dispatcher_expiration"] = mock.ANY

        self.expect_message(
            queue=self.target_queue,
            message=message,
            min_wait=9,
            timeout=30,
        )

    def test_apply_parameters(self):
        """Passing in a recipe to the service without external dependencies.
        The recipe should be interpreted and a simple message passed back to a
        fixed destination."""

        recipe = {
            1: {
                "service": "DLS system test",
                "queue": self.target_queue,
                "parameters": {
                    "list": [
                        "{ispyb_dc_info[imageDirectory]}{ispyb_dc_info[imagePrefix]}_{ispyb_dc_info[dataCollectionNumber]}.nxs"
                    ],
                    "timeout-first": 600,
                },
            },
            "start": [
                (1, {"purpose": "apply parameters test for the recipe parsing service"})
            ],
        }
        message = {"custom_recipe": recipe, "parameters": {"ispyb_dcid": 8652035}}

        self.send_message(queue="processing_recipe", message=message)

        expected_recipe = copy.deepcopy(recipe)
        expected_recipe[1]["parameters"]["list"] = [
            "/dls/i03/data/2022/cm31105-3/xraycentring/manual/xrc_306.nxs"
        ]
        self.expect_recipe_message(
            recipe=Recipe(expected_recipe),
            recipe_path=[],
            recipe_pointer=1,
            payload=recipe["start"][0][1],
        )
