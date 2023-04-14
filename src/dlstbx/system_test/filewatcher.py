from __future__ import annotations

import os.path
from unittest import mock

from workflows.recipe import Recipe

import dlstbx.util
from dlstbx.system_test.common import CommonSystemTest

tmpdir = dlstbx.util.dls_tmp_folder_date()


class FilewatcherService(CommonSystemTest):
    """Tests for the filewatcher service."""

    def create_temp_dir(self):
        """Create directory for the test."""
        os.makedirs(os.path.join(tmpdir, self.guid))

    def create_next_file(self):
        """Create one more file for the test."""
        self.filecount += 1
        open(self.filepattern % self.filecount, "w").close()

    def test_empty_list_notifications(self):
        """
        Send a recipe containing an empty list to the filewatcher.
        Only the 'finally' output should be triggered from it.
        """

        names = [None]

        recipe = {
            1: {
                "service": "DLS Filewatcher",
                "queue": "filewatcher",
                "parameters": {
                    "list": names,
                    "burst-limit": 3,
                    "timeout": 10,
                    "timeout-first": 10,
                },
                "output": {
                    "first": 3,  # Should not be triggered here
                    "every": 3,  # Should not be triggered here
                    "every-2": 3,  # Should not be triggered here
                    "last": 3,  # Should not be triggered here
                    "select-3": 3,  # Should not be triggered here
                    "0": 3,  # Should not be triggered here
                    "1": 3,  # Should not be triggered here
                    "finally": 2,  # End-of-job
                    "timeout": 3,  # Should not be triggered here
                    "any": 3,  # Should not be triggered here
                },
            },
            2: {"queue": self.target_queue},
            3: {"queue": self.target_queue},
            "start": [(1, "")],
        }
        recipe = Recipe(recipe)
        recipe.validate()

        self.send_message(
            queue="filewatcher",
            message={
                "recipe": recipe.recipe,
                "recipe-pointer": "1",
                "environment": {"ID": self.guid},
            },
            headers={"workflows-recipe": True},
        )

        # Now check for expected messages, marked in the recipe above:

        # Finally ==========================

        self.expect_recipe_message(
            environment={"ID": self.guid},
            recipe=recipe,
            recipe_path=[1],
            recipe_pointer=2,
            payload={"files-expected": 0, "files-seen": 0, "success": True},
            min_wait=0,
            timeout=30,
        )

        # Step 3 in recipe should never be reached

        self.expect_unreached_recipe_step(
            recipe=recipe,
            recipe_pointer=3,
        )

    def test_list_success_notifications(self):
        """
        Send a recipe to the filewatcher based on a list of files.
        Create 10 files and wait for the appropriate notification messages.
        """

        self.create_temp_dir()

        names = [
            os.path.join(tmpdir, self.guid, f)
            for f in (
                "apple",
                "banana",
                "cherry",
                "date",
                "elderberry",
                "fig",
                "grapefruit",
                "hackberry",
                "imbe",
                "jackfruit",
            )
        ]
        self.filecount = 0

        recipe = {
            1: {
                "service": "DLS Filewatcher",
                "queue": "filewatcher",
                "parameters": {
                    "list": names,
                    "burst-limit": 3,
                    "timeout": 120,
                    "timeout-first": 60,
                },
                "output": {
                    "first": 2,  # First
                    "every": 3,  # Every
                    "every-2": 10,  # 1st, 3rd, 5th, ...
                    "last": 4,  # Last
                    "select-3": 5,  # Select
                    "7": 6,  # Specific
                    "finally": 7,  # End-of-job
                    "timeout": 8,  # Should not be triggered here
                    "any": 9,  # End-of-job if at least one file was found
                },
            },
            2: {"queue": self.target_queue},
            3: {"queue": self.target_queue},
            4: {"queue": self.target_queue},
            5: {"queue": self.target_queue},
            6: {"queue": self.target_queue},
            7: {"queue": self.target_queue},
            8: {"queue": self.target_queue},
            9: {"queue": self.target_queue},
            10: {"queue": self.target_queue},
            "start": [(1, "")],
        }
        recipe = Recipe(recipe)
        recipe.validate()

        self.send_message(
            queue="filewatcher",
            message={
                "recipe": recipe.recipe,
                "recipe-pointer": "1",
                "environment": {"ID": self.guid},
            },
            headers={"workflows-recipe": True},
        )

        def create_first_five_files():
            print("creating first five files")
            for file_number in range(0, 5):
                open(names[file_number], "w").close()

        def create_next_five_files():
            print("creating next five files")
            for file_number in range(5, 10):
                open(names[file_number], "w").close()

        # Create 5 files at t=5 seconds
        self.timer_event(at_time=5, callback=create_first_five_files)

        # Create 5 files at t=65 seconds
        self.timer_event(at_time=65, callback=create_next_five_files)

        # Now check for expected messages, marked in the recipe above:

        # First ============================

        self.expect_recipe_message(
            environment={"ID": self.guid},
            recipe=recipe,
            recipe_path=[1],
            recipe_pointer=2,
            payload={"file": names[0], "file-list-index": 1, "file-seen-at": mock.ANY},
            timeout=50,
        )

        # Every ============================

        for file_number in range(10):
            self.expect_recipe_message(
                environment={"ID": self.guid},
                recipe=recipe,
                recipe_path=[1],
                recipe_pointer=3,
                payload={
                    "file": names[file_number],
                    "file-list-index": file_number + 1,
                    "file-seen-at": mock.ANY,
                },
                min_wait=4.5,
                timeout=150,
            )

        # Every-N ==========================

        for file_number in range(0, 10, 2):
            self.expect_recipe_message(
                environment={"ID": self.guid},
                recipe=recipe,
                recipe_path=[1],
                recipe_pointer=10,
                payload={
                    "file": names[file_number],
                    "file-list-index": file_number + 1,
                    "file-seen-at": mock.ANY,
                },
                min_wait=4.5,
                timeout=150,
            )

        # Last =============================

        self.expect_recipe_message(
            environment={"ID": self.guid},
            recipe=recipe,
            recipe_path=[1],
            recipe_pointer=4,
            payload={"file": names[9], "file-list-index": 10, "file-seen-at": mock.ANY},
            min_wait=63,
            timeout=150,
        )

        # Select ===========================

        for file_number in (1, 6, 10):
            self.expect_recipe_message(
                environment={"ID": self.guid},
                recipe=recipe,
                recipe_path=[1],
                recipe_pointer=5,
                payload={
                    "file": names[file_number - 1],
                    "file-list-index": file_number,
                    "file-seen-at": mock.ANY,
                },
                timeout=150,
            )

        # Specific =========================

        self.expect_recipe_message(
            environment={"ID": self.guid},
            recipe=recipe,
            recipe_path=[1],
            recipe_pointer=6,
            payload={
                "file": names[7 - 1],
                "file-list-index": 7,
                "file-seen-at": mock.ANY,
            },
            timeout=150,
        )

        # Finally ==========================

        self.expect_recipe_message(
            environment={"ID": self.guid},
            recipe=recipe,
            recipe_path=[1],
            recipe_pointer=7,
            payload={"files-expected": 10, "files-seen": 10, "success": True},
            min_wait=63,
            timeout=150,
        )

        # Timeout ==========================

        # No timeout message should be sent

        self.expect_unreached_recipe_step(
            recipe=recipe,
            recipe_pointer=8,
        )

        # Any ==============================

        self.expect_recipe_message(
            environment={"ID": self.guid},
            recipe=recipe,
            recipe_path=[1],
            recipe_pointer=9,
            payload={"files-expected": 10, "files-seen": 10},
            min_wait=63,
            timeout=150,
        )

    def test_pattern_success_notifications(self):
        """
        Send a recipe to the filewatcher. Create 200 files and wait for the
        appropriate notification messages.
        """

        self.create_temp_dir()
        self.filepattern = os.path.join(tmpdir, self.guid, "tst_%05d.cbf")
        self.filecount = 0

        recipe = {
            1: {
                "service": "DLS Filewatcher",
                "queue": "filewatcher",
                "parameters": {
                    "pattern": self.filepattern,
                    "pattern-start": 1,
                    "pattern-end": 200,
                    "burst-limit": 40,
                    "timeout": 120,
                    "timeout-first": 60,
                },
                "output": {
                    "first": 2,  # First
                    "every": 3,  # Every
                    "every-7": 10,  # 1st, 8th, 15th, ...
                    "last": 4,  # Last
                    "select-30": 5,  # Select
                    "20": 6,  # Specific
                    "finally": 7,  # End-of-job
                    "timeout": 8,  # Should not be triggered here
                    "any": 9,  # End-of-job if at least one file was found
                },
            },
            2: {"queue": self.target_queue},
            3: {"queue": self.target_queue},
            4: {"queue": self.target_queue},
            5: {"queue": self.target_queue},
            6: {"queue": self.target_queue},
            7: {"queue": self.target_queue},
            8: {"queue": self.target_queue},
            9: {"queue": self.target_queue},
            10: {"queue": self.target_queue},
            "start": [(1, "")],
        }
        recipe = Recipe(recipe)
        recipe.validate()

        self.send_message(
            queue="filewatcher",
            message={
                "recipe": recipe.recipe,
                "recipe-pointer": "1",
                "environment": {"ID": self.guid},
            },
            headers={"workflows-recipe": True},
        )

        # Create 100 files in 0-10 seconds
        for file_number in range(1, 101):
            self.timer_event(at_time=file_number / 10, callback=self.create_next_file)

        # Create 100 files in 60-70 seconds
        for file_number in range(101, 201):
            self.timer_event(
                at_time=50 + (file_number / 10), callback=self.create_next_file
            )

        # Now check for expected messages, marked in the recipe above:

        # First ============================

        self.expect_recipe_message(
            environment={"ID": self.guid},
            recipe=recipe,
            recipe_path=[1],
            recipe_pointer=2,
            payload={
                "file": self.filepattern % 1,
                "file-number": 1,
                "file-pattern-index": 1,
                "file-seen-at": mock.ANY,
            },
            timeout=50,
        )

        # Every ============================

        for file_number in range(200):
            self.expect_recipe_message(
                environment={"ID": self.guid},
                recipe=recipe,
                recipe_path=[1],
                recipe_pointer=3,
                payload={
                    "file": self.filepattern % (file_number + 1),
                    "file-number": file_number + 1,
                    "file-pattern-index": file_number + 1,
                    "file-seen-at": mock.ANY,
                },
                min_wait=max(0, file_number / 10) - 0.5,
                timeout=150,
            )

        # Every-N ==========================

        for file_number in (
            1,
            8,
            15,
            22,
            29,
            36,
            43,
            50,
            57,
            64,
            71,
            78,
            85,
            92,
            99,
            106,
            113,
            120,
            127,
            134,
            141,
            148,
            155,
            162,
            169,
            176,
            183,
            190,
            197,
        ):
            self.expect_recipe_message(
                environment={"ID": self.guid},
                recipe=recipe,
                recipe_path=[1],
                recipe_pointer=10,
                payload={
                    "file": self.filepattern % file_number,
                    "file-number": file_number,
                    "file-pattern-index": file_number,
                    "file-seen-at": mock.ANY,
                },
                min_wait=max(0, file_number / 10) - 0.5,
                timeout=150,
            )

        # Last =============================

        self.expect_recipe_message(
            environment={"ID": self.guid},
            recipe=recipe,
            recipe_path=[1],
            recipe_pointer=4,
            payload={
                "file": self.filepattern % 200,
                "file-number": 200,
                "file-pattern-index": 200,
                "file-seen-at": mock.ANY,
            },
            min_wait=65,
            timeout=150,
        )

        # Select ===========================

        for file_number in (
            1,
            7,
            14,
            21,
            28,
            35,
            42,
            49,
            56,
            63,
            69,
            76,
            83,
            90,
            97,
            104,
            111,
            118,
            125,
            132,
            138,
            145,
            152,
            159,
            166,
            173,
            180,
            187,
            194,
            200,
        ):
            self.expect_recipe_message(
                environment={"ID": self.guid},
                recipe=recipe,
                recipe_path=[1],
                recipe_pointer=5,
                payload={
                    "file": self.filepattern % file_number,
                    "file-number": file_number,
                    "file-pattern-index": file_number,
                    "file-seen-at": mock.ANY,
                },
                timeout=150,
            )

        # Specific =========================

        self.expect_recipe_message(
            environment={"ID": self.guid},
            recipe=recipe,
            recipe_path=[1],
            recipe_pointer=6,
            payload={
                "file": self.filepattern % 20,
                "file-number": 20,
                "file-pattern-index": 20,
                "file-seen-at": mock.ANY,
            },
            timeout=60,
        )

        # Finally ==========================

        self.expect_recipe_message(
            environment={"ID": self.guid},
            recipe=recipe,
            recipe_path=[1],
            recipe_pointer=7,
            payload={"files-expected": 200, "files-seen": 200, "success": True},
            min_wait=65,
            timeout=150,
        )

        # Timeout ==========================

        # No timeout message should be sent

        self.expect_unreached_recipe_step(
            recipe=recipe,
            recipe_pointer=8,
        )

        # Any ==============================

        self.expect_recipe_message(
            environment={"ID": self.guid},
            recipe=recipe,
            recipe_path=[1],
            recipe_pointer=9,
            payload={"files-expected": 200, "files-seen": 200},
            min_wait=65,
            timeout=150,
        )

    def test_list_failure_notification_immediate(self):
        """Send a recipe to the filewatcher. Do not create any files and wait for
        the appropriate timeout notification messages.
        """

        self.create_temp_dir()
        names = [
            os.path.join(tmpdir, self.guid, f)
            for f in (
                "apple",
                "banana",
                "cherry",
                "date",
                "elderberry",
                "fig",
                "grapefruit",
                "hackberry",
                "imbe",
                "jackfruit",
            )
        ]

        recipe = {
            1: {
                "service": "DLS Filewatcher",
                "queue": "filewatcher",
                "parameters": {
                    "list": names,
                    "burst-limit": 3,
                    "timeout": 10,
                    "timeout-first": 60,
                    "log-timeout-as-info": True,
                },
                "output": {
                    "first": 2,  # Should not be triggered here
                    "every": 3,  # Should not be triggered here
                    "every-7": 10,  # Should not be triggered here
                    "last": 4,  # Should not be triggered here
                    "select-3": 5,  # Should not be triggered here
                    "7": 6,  # Should not be triggered here
                    "finally": 7,  # End-of-job
                    "timeout": 8,  # Ran into a timeout condition
                    "any": 9,  # Should not be triggered here
                },
            },
            2: {"queue": self.target_queue},
            3: {"queue": self.target_queue},
            4: {"queue": self.target_queue},
            5: {"queue": self.target_queue},
            6: {"queue": self.target_queue},
            7: {"queue": self.target_queue},
            8: {"queue": self.target_queue},
            9: {"queue": self.target_queue},
            10: {"queue": self.target_queue},
            "start": [(1, "")],
        }
        recipe = Recipe(recipe)
        recipe.validate()

        self.send_message(
            queue="filewatcher",
            message={
                "recipe": recipe.recipe,
                "recipe-pointer": "1",
                "environment": {"ID": self.guid},
            },
            headers={"workflows-recipe": True},
        )

        # Check for expected messages, marked in the recipe above:

        # First ============================
        # Every ============================
        # Every-N ==========================
        # Last =============================
        # Select ===========================
        # Specific =========================

        # No messages should be sent

        for pointer in (2, 3, 4, 5, 6, 10):
            self.expect_unreached_recipe_step(
                recipe=recipe,
                recipe_pointer=pointer,
            )

        # Finally ==========================

        self.expect_recipe_message(
            environment={"ID": self.guid},
            recipe=recipe,
            recipe_path=[1],
            recipe_pointer=7,
            payload={"files-expected": 10, "files-seen": 0, "success": False},
            min_wait=55,
            timeout=90,
        )

        # Timeout ==========================

        self.expect_recipe_message(
            environment={"ID": self.guid},
            recipe=recipe,
            recipe_path=[1],
            recipe_pointer=8,
            payload={"file": names[0], "file-list-index": 1, "success": False},
            min_wait=55,
            timeout=90,
        )

        # Any ==============================

        # No messages should be sent
        self.expect_unreached_recipe_step(
            recipe=recipe,
            recipe_pointer=9,
        )

    def test_pattern_failure_notification_immediate(self):
        """Send a recipe to the filewatcher. Do not create any files and wait for
        the appropriate timeout notification messages.
        """

        self.create_temp_dir()
        failpattern = os.path.join(tmpdir, self.guid, "tst_fail_%05d.cbf")

        recipe = {
            1: {
                "service": "DLS Filewatcher",
                "queue": "filewatcher",
                "parameters": {
                    "pattern": failpattern,
                    "pattern-start": 1,
                    "pattern-end": 200,
                    "burst-limit": 40,
                    "timeout": 10,
                    "timeout-first": 60,
                    "log-timeout-as-info": True,
                },
                "output": {
                    "first": 2,  # Should not be triggered here
                    "every": 3,  # Should not be triggered here
                    "every-7": 10,  # Should not be triggered here
                    "last": 4,  # Should not be triggered here
                    "select-30": 5,  # Should not be triggered here
                    "20": 6,  # Should not be triggered here
                    "finally": 7,  # End-of-job
                    "timeout": 8,  # Ran into a timeout condition
                    "any": 9,  # Should not be triggered here
                },
            },
            2: {"queue": self.target_queue},
            3: {"queue": self.target_queue},
            4: {"queue": self.target_queue},
            5: {"queue": self.target_queue},
            6: {"queue": self.target_queue},
            7: {"queue": self.target_queue},
            8: {"queue": self.target_queue},
            9: {"queue": self.target_queue},
            10: {"queue": self.target_queue},
            "start": [(1, "")],
        }
        recipe = Recipe(recipe)
        recipe.validate()

        self.send_message(
            queue="filewatcher",
            message={
                "recipe": recipe.recipe,
                "recipe-pointer": "1",
                "environment": {"ID": self.guid},
            },
            headers={"workflows-recipe": True},
        )

        # Check for expected messages, marked in the recipe above:

        # First ============================
        # Every ============================
        # Every-N ==========================
        # Last =============================
        # Select ===========================
        # Specific =========================

        # No messages should be sent

        for pointer in (2, 3, 4, 5, 6, 10):
            self.expect_unreached_recipe_step(
                recipe=recipe,
                recipe_pointer=pointer,
            )

        # Finally ==========================

        self.expect_recipe_message(
            environment={"ID": self.guid},
            recipe=recipe,
            recipe_path=[1],
            recipe_pointer=7,
            payload={"files-expected": 200, "files-seen": 0, "success": False},
            min_wait=55,
            timeout=80,
        )

        # Timeout ==========================

        self.expect_recipe_message(
            environment={"ID": self.guid},
            recipe=recipe,
            recipe_path=[1],
            recipe_pointer=8,
            payload={
                "file": failpattern % 1,
                "file-number": 1,
                "file-pattern-index": 1,
                "success": False,
            },
            min_wait=55,
            timeout=80,
        )

        # Any ==============================

        # No messages should be sent

        self.expect_unreached_recipe_step(
            recipe=recipe,
            recipe_pointer=9,
            min_wait=3,
        )

    def test_list_failure_notification_delayed(self):
        """
        Send a recipe to the filewatcher. Creates a single file and waits for
        the appropriate initial success and subsequent timeout notification
        messages.
        """

        self.create_temp_dir()
        names = [
            os.path.join(tmpdir, self.guid, f)
            for f in (
                "apple",
                "banana",
                "cherry",
                "date",
                "elderberry",
                "fig",
                "grapefruit",
                "hackberry",
                "imbe",
                "jackfruit",
            )
        ]

        recipe = {
            1: {
                "service": "DLS Filewatcher",
                "queue": "filewatcher",
                "parameters": {
                    "list": names,
                    "burst-limit": 3,
                    "timeout": 10,
                    "timeout-first": 60,
                    "log-timeout-as-info": True,
                },
                "output": {
                    "first": 2,  # First
                    "every": 3,  # Every
                    "every-3": 10,  # 1st, 4th only.
                    "last": 4,  # Should not be triggered here
                    "select-3": 5,  # Select
                    "7": 6,  # Should not be triggered here
                    "finally": 7,  # End-of-job
                    "timeout": 8,  # Ran into a timeout condition
                    "any": 9,  # End-of-job if at least one file was found
                },
            },
            2: {"queue": self.target_queue},
            3: {"queue": self.target_queue},
            4: {"queue": self.target_queue},
            5: {"queue": self.target_queue},
            6: {"queue": self.target_queue},
            7: {"queue": self.target_queue},
            8: {"queue": self.target_queue},
            9: {"queue": self.target_queue},
            10: {"queue": self.target_queue},
            "start": [(1, "")],
        }
        recipe = Recipe(recipe)
        recipe.validate()

        self.send_message(
            queue="filewatcher",
            message={
                "recipe": recipe.recipe,
                "recipe-pointer": "1",
                "environment": {"ID": self.guid},
            },
            headers={"workflows-recipe": True},
        )

        # Create first four files after 5 seconds
        def create_four_files():
            for file_number in range(0, 4):
                open(names[file_number], "w").close()

        self.timer_event(at_time=5, callback=create_four_files)

        # Check for expected messages, marked in the recipe above:

        # First ============================

        self.expect_recipe_message(
            environment={"ID": self.guid},
            recipe=recipe,
            recipe_path=[1],
            recipe_pointer=2,
            payload={"file": names[0], "file-list-index": 1, "file-seen-at": mock.ANY},
            timeout=65,
        )

        # Every ============================

        for file_number in range(4):
            self.expect_recipe_message(
                environment={"ID": self.guid},
                recipe=recipe,
                recipe_path=[1],
                recipe_pointer=3,
                payload={
                    "file": names[file_number],
                    "file-list-index": file_number + 1,
                    "file-seen-at": mock.ANY,
                },
                min_wait=4,
                timeout=80,
            )

        # Every-N ==========================

        for file_number in (0, 3):
            self.expect_recipe_message(
                environment={"ID": self.guid},
                recipe=recipe,
                recipe_path=[1],
                recipe_pointer=10,
                payload={
                    "file": names[file_number],
                    "file-list-index": file_number + 1,
                    "file-seen-at": mock.ANY,
                },
                min_wait=4,
                timeout=80,
            )

        # Last =============================

        # No messages should be sent

        self.expect_unreached_recipe_step(
            recipe=recipe,
            recipe_pointer=4,
        )

        # Select ===========================

        self.expect_recipe_message(
            environment={"ID": self.guid},
            recipe=recipe,
            recipe_path=[1],
            recipe_pointer=5,
            payload={"file": names[0], "file-list-index": 1, "file-seen-at": mock.ANY},
            timeout=80,
        )

        # Specific =========================

        # No messages should be sent

        self.expect_unreached_recipe_step(
            recipe=recipe,
            recipe_pointer=6,
        )

        # Finally ==========================

        self.expect_recipe_message(
            environment={"ID": self.guid},
            recipe=recipe,
            recipe_path=[1],
            recipe_pointer=7,
            payload={"files-expected": 10, "files-seen": 4, "success": False},
            min_wait=14,
            timeout=80,
        )

        # Timeout ==========================

        self.expect_recipe_message(
            environment={"ID": self.guid},
            recipe=recipe,
            recipe_path=[1],
            recipe_pointer=8,
            payload={"file": names[4], "file-list-index": 5, "success": False},
            min_wait=14,
            timeout=80,
        )

        # Any ==============================

        self.expect_recipe_message(
            environment={"ID": self.guid},
            recipe=recipe,
            recipe_path=[1],
            recipe_pointer=9,
            payload={"files-expected": 10, "files-seen": 4},
            min_wait=14,
            timeout=80,
        )

    def test_pattern_failure_notification_delayed(self):
        """
        Send a recipe to the filewatcher. Creates a single file and waits for
        the appropriate initial success and subsequent timeout notification
        messages.
        """

        self.create_temp_dir()
        semifailpattern = os.path.join(tmpdir, self.guid, "tst_semi_%05d.cbf")
        delayed_fail_file = semifailpattern % 5

        recipe = {
            1: {
                "service": "DLS Filewatcher",
                "queue": "filewatcher",
                "parameters": {
                    "pattern": semifailpattern,
                    "pattern-start": 5,
                    "pattern-end": 204,
                    "burst-limit": 40,
                    "timeout": 10,
                    "timeout-first": 60,
                    "log-timeout-as-info": True,
                },
                "output": {
                    "first": 2,  # First
                    "every": 3,  # Every
                    "every-3": 10,  # 1st only
                    "last": 4,  # Should not be triggered here
                    "select-30": 5,  # 1st only
                    "20": 6,  # Should not be triggered here
                    "finally": 7,  # End-of-job
                    "timeout": 8,  # Ran into a timeout condition
                    "any": 9,  # End-of-job if at least one file was found
                },
            },
            2: {"queue": self.target_queue},
            3: {"queue": self.target_queue},
            4: {"queue": self.target_queue},
            5: {"queue": self.target_queue},
            6: {"queue": self.target_queue},
            7: {"queue": self.target_queue},
            8: {"queue": self.target_queue},
            9: {"queue": self.target_queue},
            10: {"queue": self.target_queue},
            "start": [(1, "")],
        }
        recipe = Recipe(recipe)
        recipe.validate()

        self.send_message(
            queue="filewatcher",
            message={
                "recipe": recipe.recipe,
                "recipe-pointer": "1",
                "environment": {"ID": self.guid},
            },
            headers={"workflows-recipe": True},
        )

        # Create first file after 30 seconds
        def create_delayed_failure_file():
            open(delayed_fail_file, "w").close()

        self.timer_event(at_time=30, callback=create_delayed_failure_file)

        # Check for expected messages, marked in the recipe above:

        # First ============================

        self.expect_recipe_message(
            environment={"ID": self.guid},
            recipe=recipe,
            recipe_path=[1],
            recipe_pointer=2,
            payload={
                "file": delayed_fail_file,
                "file-number": 1,
                "file-pattern-index": 5,
                "file-seen-at": mock.ANY,
            },
            min_wait=25,
            timeout=60,
        )

        # Every ============================

        self.expect_recipe_message(
            environment={"ID": self.guid},
            recipe=recipe,
            recipe_path=[1],
            recipe_pointer=3,
            payload={
                "file": delayed_fail_file,
                "file-number": 1,
                "file-pattern-index": 5,
                "file-seen-at": mock.ANY,
            },
            min_wait=25,
            timeout=60,
        )

        # Every-N ==========================

        self.expect_recipe_message(
            environment={"ID": self.guid},
            recipe=recipe,
            recipe_path=[1],
            recipe_pointer=10,
            payload={
                "file": delayed_fail_file,
                "file-number": 1,
                "file-pattern-index": 5,
                "file-seen-at": mock.ANY,
            },
            min_wait=25,
            timeout=60,
        )

        # Last =============================

        # No message should be sent
        self.expect_unreached_recipe_step(
            recipe=recipe,
            recipe_pointer=4,
        )

        # Select ===========================

        self.expect_recipe_message(
            environment={"ID": self.guid},
            recipe=recipe,
            recipe_path=[1],
            recipe_pointer=5,
            payload={
                "file": delayed_fail_file,
                "file-number": 1,
                "file-pattern-index": 5,
                "file-seen-at": mock.ANY,
            },
            min_wait=25,
            timeout=60,
        )

        # Specific =========================

        # No message should be sent
        self.expect_unreached_recipe_step(
            recipe=recipe,
            recipe_pointer=6,
        )

        # Finally ==========================

        self.expect_recipe_message(
            environment={"ID": self.guid},
            recipe=recipe,
            recipe_path=[1],
            recipe_pointer=7,
            payload={"files-expected": 200, "files-seen": 1, "success": False},
            min_wait=25,
            timeout=65,
        )

        # Timeout ==========================

        self.expect_recipe_message(
            environment={"ID": self.guid},
            recipe=recipe,
            recipe_path=[1],
            recipe_pointer=8,
            payload={
                "file": semifailpattern % 6,
                "file-number": 2,
                "file-pattern-index": 6,
                "success": False,
            },
            min_wait=25,
            timeout=65,
        )

        # Any ==============================

        self.expect_recipe_message(
            environment={"ID": self.guid},
            recipe=recipe,
            recipe_path=[1],
            recipe_pointer=9,
            payload={"files-expected": 200, "files-seen": 1},
            min_wait=25,
            timeout=65,
        )
