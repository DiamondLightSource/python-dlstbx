from __future__ import division

import dlstbx.workflow.transport.stomp

def lookup(transport):
  if transport is not None and \
    transport.lower() == "stomp":
      return dlstbx.workflow.transport.stomp.Transport
  # fallback to default
  return dlstbx.workflow.transport.stomp.Transport

def add_command_line_options(parser):
  dlstbx.workflow.transport.stomp.Transport().add_command_line_options(parser)
