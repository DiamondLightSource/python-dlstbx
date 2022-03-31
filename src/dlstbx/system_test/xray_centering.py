from __future__ import annotations

import os
import pathlib
import random
import time

from workflows.recipe import Recipe

from dlstbx.system_test.common import CommonSystemTest


class XrayCentering(CommonSystemTest):
    """Connect to messaging server and send a message to myself."""

    def test_xray_centering_produces_json(self):
        tmpdir = pathlib.Path(f"/dls/tmp/zocalo/system-tests/xrc/{self.guid}")
        json_file = tmpdir / "Dials5AResults.json"
        log_file = tmpdir / "Dials5AResults.log"

        spot_counts = [0, 1, 2, 3, 4, 5, 4, 3, 2, 1]

        recipe = {
            "start": [[1, []]],
            1: {
                "service": "DLS X-Ray Centering",
                "queue": "reduce.xray_centering",
                "parameters": {
                    "dcid": random.randint(0, 1e6),
                    "experiment_type": "Mesh",
                    "output": os.fspath(json_file),
                    "log": os.fspath(log_file),
                    "beamline": "i03",
                },
                "gridinfo": {
                    "dx_mm": 0.02,
                    "dy_mm": 0.005,
                    "orientation": "vertical",
                    "pixelsPerMicronX": 0.806,
                    "pixelsPerMicronY": 0.806,
                    "snaked": 1,
                    "snapshot_offsetXPixel": 407.593,
                    "snapshot_offsetYPixel": 99.861,
                    "steps_x": 1.0,
                    "steps_y": len(spot_counts),
                },
                "comment": "Xray centring - Diffraction grid scan of 1 by 80 images, Top left [407,99], Bottom right [408,596]",
                "output": 2,
            },
            2: {
                "service": "DLS System Test",
                "queue": self.target_queue,
            },
        }

        recipe = Recipe(recipe)
        recipe.validate()

        for i, count in enumerate(spot_counts):
            message = {
                "n_spots_total": count,
                "file-number": i + 1,
                "file-seen-at": time.time(),
            }
            self.send_message(
                queue=recipe[1]["queue"],
                message={
                    "payload": message,
                    "recipe": recipe.recipe,
                    "recipe-pointer": "1",
                    "environment": {"ID": self.guid},
                },
                headers={"workflows-recipe": True},
            )

        nfs_delay = 30
        self.timer_event(
            at_time=10 + nfs_delay, callback=json_file.exists, expect_return=True
        )
        self.timer_event(
            at_time=10 + nfs_delay, callback=log_file.exists, expect_return=True
        )

        self.expect_recipe_message(
            environment={"ID": self.guid},
            recipe=recipe,
            recipe_path=[1],
            recipe_pointer=2,
            payload={
                "steps": [1, 10],
                "box_size_px": [24.813895781637715, 6.203473945409429],
                "snapshot_offset": [407.593, 99.861],
                "centre_x": 419.9999478908189,
                "centre_y": 133.98010669975187,
                "centre_x_box": 0.5,
                "centre_y_box": 5.5,
                "status": "ok",
                "message": "ok",
                "best_image": 6,
                "reflections_in_best_image": 5,
                "best_region": [[3, 0], [4, 0], [5, 0], [6, 0], [7, 0]],
            },
            timeout=10,
        )
