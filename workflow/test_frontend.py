import frontend
import pytest
import dlstbx.workflow.services.sample_service

def test_instantiate_new_frontend():
  '''Create a new frontend.'''
  frontend.Frontend()

def test_start_service_in_frontend():
  fe = frontend.Frontend()
  fe.switch_service(dlstbx.workflow.services.sample_service.Waiter)
