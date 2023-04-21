from __future__ import annotations

from workflows.recipe import Recipe

from dlstbx.system_test.common import CommonSystemTest

imagepath = "/dls/i03/data/2023/cm33866-2/TestInsulin/ins_16/"


class NexusParserService(CommonSystemTest):
    """Tests for the NexusParser service."""

    def test_find_all_referenced_files(self):
        """Find all files referenced in an example nexus dataset."""

        recipe = {
            1: {
                "service": "DLS NexusParser",
                "queue": "nexusparser.find_related_files",
                "output": 2,
            },
            2: {
                "service": "DLS System Test",
                "queue": self.target_queue,
            },
            "start": [(1, {"file": imagepath + "ins_16_4_master.h5"})],
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

        self.expect_recipe_message(
            environment={"ID": self.guid},
            recipe=recipe,
            recipe_path=[1],
            recipe_pointer=2,
            payload={
                "filelist": sorted(
                    imagepath + filename
                    for filename in (
                        "ins_16_4_000001.h5",
                        "ins_16_4_000002.h5",
                        "ins_16_4_000003.h5",
                        "ins_16_4_000004.h5",
                        "ins_16_4_master.h5",
                    )
                )
            },
            timeout=120,
        )
