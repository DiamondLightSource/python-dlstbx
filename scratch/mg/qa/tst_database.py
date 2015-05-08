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

  def test_register_test_in_database_and_retrieve_testid(self):
    (dataset, test) = ('qa', 'test')

    with database.DB(database.DB.memory) as db:
      id_1 = db.register_test(dataset, test)
      id_2 = db.get_testid(dataset, test)
      test_db = db.get_test(id_1)

      id_3 = db.register_test(dataset, test)

    self.assertEqual(id_1, id_2)
    self.assertEqual(test_db['dataset'], dataset)
    self.assertEqual(test_db['test'], test)
    self.assertEqual(id_1, id_3)

  def test_store_test_results_in_database(self):
    (dataset, test) = ('qa', 'test')
    (lastseenA, successA, stdoutA, stderrA, jsonA, xia2errorA) = (1, True, 'a', 'b', 'c', None)
    (lastseenB, successB, stdoutB, stderrB, jsonB, xia2errorB) = (2, False, '', None, 'y', 'z')
    expectedA = { 'id': 1, 'dataset': dataset, 'test': test, 'lastseen': lastseenA,
                  'success': 1 if successA else 0, 'stdout': stdoutA, 'stderr': stderrA,
                  'json': jsonA, 'xia2error': xia2errorA }
    expectedB = { 'id': 1, 'dataset': dataset, 'test': test, 'lastseen': lastseenB,
                  'success': 1 if successB else 0, 'stdout': stdoutB, 'stderr': stderrB,
                  'json': jsonB, 'xia2error': xia2errorB }

    with database.DB(database.DB.memory) as db:
      testid = db.register_test(dataset, test)

      db.store_test_result(testid, lastseenA, successA, stdoutA, stderrA, jsonA, xia2errorA)
      rowsA = db.select_tests()
      actualA = dict(rowsA[0])

      db.store_test_result(testid, lastseenB, successB, stdoutB, stderrB, jsonB, xia2errorB)
      rowsB = db.select_tests()
      actualB = dict(rowsB[0])

    self.assertEqual(len(rowsA), 1)
    self.assertEqual(actualA, expectedA)
    self.assertEqual(len(rowsB), 1)
    self.assertEqual(actualB, expectedB)

  @unittest.skip('not implemented yet')
  def test_store_test_runs_in_database_and_retrieve_ordered_list(self):
    (dataset, test, timestampA, timestampB) = ('qa', 'test', 2,  1)

    with database.DB(database.DB.memory) as db:
      testid = db.register_test(dataset, test)
      runidA = db.register_testrun(testid, timestampA)
      runidB = db.register_testrun(testid, timestampB)

      runs = db.lookup_testrun_ids(testid)

      self.assertEqual(set(runs), set([runidA, runidB]))

      actual = db.retrieve_keys(testids=ids) # [ {testid: n, timestamp: t, keyid: v, keyid#2: v2} ]


  @unittest.skip('not implemented yet')
  @mock.patch('database.sqlite3')
  def test_store_new_key_values_in_database(self, mock_sqlite3):
    (dataset, test, timestamp, key, value) = ('qa', 'test', 1, 'some key', 'some value')
#    expected = [ { 'timestamp': timestamp, '

    with database.DB(database.DB.memory) as db:
      db.processed_dataset(dataset, test, None, None, None, None, None, None)
      db.store_keys(dataset, test, timestamp, { key : value })

      ids = db.lookup_test_ids(dataset=dataset, test=test)
      self.assertEqual(len(ids), 1)

      actual = db.retrieve_keys(testids=ids) # [ {testid: n, timestamp: t, keyid: v, keyid#2: v2} ]
      #self.assertEquals

  @unittest.skip('not implemented yet')
  @mock.patch('database.sqlite3')
  def test_store_existing_key_values_in_database(self, mock_sqlite3):
    (datasetA, testA, timestampA) = ('qa', 'test1', 1)
    (datasetB, testB, timestampB) = ('qa', 'test2', 2)
    (key, value) = ('some key', 'some value')

    with database.DB(database.DB.memory) as db:
      db.processed_dataset(datasetA, testA, None, None, None, None, None, None)
      db.processed_dataset(datasetB, testB, None, None, None, None, None, None)
      db.store_keys(datasetA, testA, timestampA, { key : value })
      db.retrieve_keys(dataset=datasetA, test=testA) # [ {testid: n, timestamp: t, keyid: v, keyid#2: v2} ]

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
