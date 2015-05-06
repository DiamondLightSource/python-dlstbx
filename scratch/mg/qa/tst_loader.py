import mock
import unittest
import tests

class LoaderTests(unittest.TestCase):

  @unittest.skip('not implemented yet')
  @mock.patch('tests.os')
  def test_loader_file_enumeration(self, mock_os):
    test_list = tests.list_all_modules()
    print test_list
#    self.assertTrue(C(5))
#    self.assertFalse(C(6))

if __name__ == '__main__':
  unittest.main()
