import database
import mock
import unittest

class DatabaseTests(unittest.TestCase):

  @mock.patch('database.sqlite3')
  @mock.patch('database.os')
  @mock.patch('database.DB._initialize_database')
  def test_initialize_database_when_the_file_does_not_exist_yet(self, mock_db_init, mock_os, mock_sqlite3):
    mock_os.path.isfile.return_value = False

    db = database.DB(mock.sentinel.filename)

    mock_sqlite3.connect.assert_called_with(mock.sentinel.filename)
    self.assertTrue(mock_db_init.called)

  @mock.patch('database.sqlite3')
  @mock.patch('database.os')
  @mock.patch('database.DB._initialize_database')
  def test_do_not_initialize_database_when_the_file_is_already_present(self, mock_db_init, mock_os, mock_sqlite3):
    mock_os.path.isfile.return_value = True

    db = database.DB(mock.sentinel.filename)

    mock_sqlite3.connect.assert_called_with(mock.sentinel.filename)
    self.assertFalse(mock_db_init.called)

  @mock.patch('database.sqlite3')
  @mock.patch('database.os')
  @mock.patch('database.DB._initialize_database')
  def test_database_can_be_run_in_memory_and_with_the_python__with__statement(self, mock_db_init, mock_os, mock_sqlite3):
    mock_connection = mock.Mock()
    mock_sqlite3.connect.return_value = mock_connection

    with database.DB(database.DB.memory) as db:
      pass

    self.assertFalse(mock_os.path.isfile.called)
    mock_sqlite3.connect.assert_called_with(database.DB.memory)
    self.assertTrue(mock_db_init.called)
    self.assertTrue(mock_connection.close.called)

  def test_store_retrieve_and_update_run_result(self):
    (dataset, test) = ('qa', 'test')
    (lastseenA, successA, outputA) = (1, True, 'output information')
    (lastseenB, successB, outputB) = (2, False, 'failure information')
    expectedA = { 'id': 1, 'dataset': dataset, 'test': test, 'lastseen': lastseenA,
                  'success': 1 if successA else 0, 'output': outputA }
    expectedB = { 'id': 1, 'dataset': dataset, 'test': test, 'lastseen': lastseenB,
                  'success': 1 if successB else 0, 'output': outputB }

    with database.DB(database.DB.memory) as db:
      db.processed_dataset(dataset, test, lastseenA, successA, outputA)
      rowsA = db.select_dataset()
      actualA = dict(rowsA[0])
      db.processed_dataset(dataset, test, lastseenB, successB, outputB)
      rowsB = db.select_dataset()
      actualB = dict(rowsB[0])

    self.assertEqual(len(rowsA), 1)
    self.assertEqual(actualA, expectedA)
    self.assertEqual(len(rowsB), 1)
    self.assertEqual(actualB, expectedB)

  @mock.patch('database.sqlite3')
  def test_store_new_key_values_in_database(self, mock_sqlite3):
    (dataset, test, timestamp, key, value) = ('qa-test', 'store', 1, 'some.key', 'value')

    with database.DB(database.DB.memory) as db:
      db.store_keys(dataset, test, timestamp, { key : value })

  @mock.patch('database.sqlite3')
  def test_store_existing_key_values_in_database(self, mock_sqlite3):
    (dataset, test, timestamp, key, value) = ('qa-test', 'store', 1, 'some.key', 'value')

    with database.DB(database.DB.memory) as db:
      db.store_keys(dataset, test, timestamp, { key : value })

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
