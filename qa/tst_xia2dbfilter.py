import unittest
from xia2dbfilter import *

class xia2dbTests(unittest.TestCase):

  def test_filters(self):
    self.assertTrue(xia2dbfilter(''))
    self.assertTrue(xia2dbfilter('some.entry'))
    self.assertTrue(xia2dbfilter('_long.entries.234234._something'))

    self.assertFalse(xia2dbfilter('somewhere._sweeps'))
    self.assertFalse(xia2dbfilter('somewhere._sweeps.subentry'))
    self.assertFalse(xia2dbfilter('crystal._sweep_information'))
    self.assertFalse(xia2dbfilter('crystal._sweep_information.stuff'))
    self.assertFalse(xia2dbfilter('crystal._scalr_integraters'))
    self.assertFalse(xia2dbfilter('crystal._scalr_integraters.stuff'))
