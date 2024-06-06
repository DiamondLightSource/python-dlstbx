from __future__ import annotations

from unittest import mock

import pytest
import zocalo.configuration
from dlstbx import mimas


@pytest.fixture
def get_zocalo_commands():
    def get_zocalo_commands_(scenario):
        mock_zc = mock.MagicMock(zocalo.configuration.Configuration, autospec=True)

        commands = set()
        actions = mimas.handle_scenario(scenario, zc=mock_zc)
        for a in actions:
            mimas.validate(a)
            commands.add(mimas.zocalo_command_line(a).strip())
        return commands

    return get_zocalo_commands_
