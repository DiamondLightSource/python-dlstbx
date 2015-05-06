import mock
import unittest
import database

class DatabaseTests(unittest.TestCase):

  @unittest.skip('not implemented yet')
  @mock.patch('database.sqlite3')
  def test_create_database(self, mock_sqlite3):
    db = database.DB('stuff')
    pass

  @unittest.skip('not implemented yet')
  @mock.patch('database.sqlite3')
  def test_load_database(self, mock_sqlite3):
    pass

  @unittest.skip('not implemented yet')
  @mock.patch('database.sqlite3')
  def test_store_new_key_values_in_database(self, mock_sqlite3):
    pass

  @unittest.skip('not implemented yet')
  @mock.patch('database.sqlite3')
  def test_store_existing_key_values_in_database(self, mock_sqlite3):
    pass

  @unittest.skip('not implemented yet')
  @mock.patch('database.sqlite3')
  def test_retrieve_key_values_from_database(self, mock_sqlite3):
    pass

  def test_transform_data_structure_to_key_values(self):
    datastructure = { 'key': [ { 'a' : 1 }, { 'b' : 2 } , { 'c' : [ 'x', 'y' ] } ] }
    expected = { 'key.1.a': 1, 'key.2.b': 2, 'key.3.c.1': 'x', 'key.3.c.2': 'y' }

    actual = database.transform_to_values(datastructure)

    self.assertEqual(actual, expected)

if __name__ == '__main__':
  unittest.main()
