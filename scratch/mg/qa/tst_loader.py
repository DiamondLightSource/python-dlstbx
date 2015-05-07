import mock
import os
import tests
import unittest

class LoaderTests_for_tests__init__py(unittest.TestCase):

  def test_loader_file_enumeration(self):
    test_directory = os.path.dirname(os.path.abspath(tests.__file__))
    expected = set()
    for f in os.listdir(test_directory):
      if f.endswith('.py') and not f.startswith('__'):
        expected.add(os.path.join(test_directory, f))

    actual = set(tests.list_all_modules().itervalues())

    self.assertEqual(expected, actual)

class LoaderTests_for_loader_py(unittest.TestCase):

  @unittest.skip('not implemented yet')
  def test_for_loader_py_still_missing(self):
    pass

if __name__ == '__main__':
  unittest.main()
