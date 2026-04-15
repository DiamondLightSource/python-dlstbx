from __future__ import annotations

from workflows.recipe import Recipe

from dlstbx.system_test.common import CommonSystemTest

imagepath = "/dls/science/groups/scisoft/nexusparser_test_data/"


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
            "start": [(1, {"file": imagepath + "protk_4_1_master.h5"})],
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
                        "protk_4_1_000001.h5",
                        "protk_4_1_000002.h5",
                        "protk_4_1_000003.h5",
                        "protk_4_1_000004.h5",
                        "protk_4_1_master.h5",
                        "protk_4_1_meta.h5",
                    )
                )
            },
            timeout=120,
        )
