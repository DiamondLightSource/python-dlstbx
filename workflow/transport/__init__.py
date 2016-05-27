from __future__ import division

import dlstbx.workflow.transport.stomp

def add_command_line_options(parser):
  dlstbx.workflow.transport.stomp.Transport().add_command_line_options(parser)
