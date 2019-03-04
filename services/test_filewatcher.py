from __future__ import absolute_import, division, print_function

import collections

import pytest


@pytest.mark.parametrize("select_n_images", (151, 250))
def test_file_selection(select_n_images):
    import dlstbx.services.filewatcher

    select_n_images = 250
    for filecount in range(1, 255) + range(3600, 3700):
        selection = lambda x: dlstbx.services.filewatcher.is_file_selected(
            x, select_n_images, filecount
        )
        l = list(filter(selection, range(1, filecount + 1)))

        # Check that correct number of images were selected
        assert len(l) == min(filecount, select_n_images)

        # Check that selection was evenly distributed
        if filecount > 1:
            diffs = [n - l[i - 1] for i, n in enumerate(l) if i]
            assert 1 <= len(collections.Counter(diffs)) <= 2, (filecount, diffs)
