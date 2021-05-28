import pathlib

from dlstbx.system_test.common import CommonSystemTest
from workflows.recipe import Recipe


class RelionStopService(CommonSystemTest):
    """Test that the Relion stop file generator generates Relion stop files."""

    def test_stop_file_generation(self):
        tmp_basepath = pathlib.Path("/dls/tmp/zocalo/tmp/relion")
        tmp_file = tmp_basepath / self.guid

        if not tmp_basepath.exists():
            tmp_basepath.mkdir(parents=True)
            tmp_basepath.chmod(0o2777)
            self.log.warning(f"{tmp_basepath} newly created. Skipping test")
            return

        recipe = {
            1: {
                "service": "Relion Stop Service",
                "queue": "relion.dev.stop",
                "parameters": {"stop_file": str(tmp_file)},
            },
            "start": [(1, {"purpose": "Generate a Relion stop file"})],
        }
        recipe = Recipe(recipe)
        recipe.validate()

        self.send_message(
            queue="relion.dev.stop",
            message={
                "payload": "",
                "recipe": recipe.recipe,
                "recipe-pointer": "1",
                "environment": {"ID": self.guid},
            },
            headers={"workflows-recipe": True},
        )

        nfs_delay = 30
        self.timer_event(
            at_time=15 + nfs_delay, callback=tmp_file.exists, expect_return=True
        )


if __name__ == "__main__":
    RelionStopService().validate()
