from __future__ import absolute_import, division

from workflows.services.common_service import CommonService

class DLSDummy(CommonService):
  '''A dlstbx dummy service that does nothing.'''

  # Human readable service name
  _service_name = "DLS Dummy service"
