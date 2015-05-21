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
      test_db = db.get_tests(test_id=id_1, all_columns=True)

      id_3 = db.register_test(dataset, test)

    self.assertEqual(id_1, id_2)
    self.assertEqual(test_db['dataset'], dataset)
    self.assertEqual(test_db['test'], test)
    self.assertEqual(id_1, id_3)

  # test = one test function running on one test dataset, stored together with its last results
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
      rowsA = db.get_tests(all_columns=True)
      actualA = dict(rowsA[0])

      db.store_test_result(testid, lastseenB, successB, stdoutB, stderrB, jsonB, xia2errorB)
      rowsB = db.get_tests(all_columns=True)
      actualB = dict(rowsB[0])

    self.assertEqual(len(rowsA), 1)
    self.assertEqual(actualA, expectedA)
    self.assertEqual(len(rowsB), 1)
    self.assertEqual(actualB, expectedB)

  # test run = one single run at a particular time of a given test
  def test_store_test_runs_in_database_and_read_them_back(self):
    (dataset, test, timestampA, timestampB) = ('qa', 'test', 2,  1)

    with database.DB(database.DB.memory) as db:
      testid = db.register_test(dataset, test)
      runidA = db.register_testrun(testid, timestampA)
      runidB = db.register_testrun(testid, timestampB)

      runs1 = db.get_testruns(testid)
      runs2 = db.get_testruns(testid, limit=1)
      runs3 = db.get_testruns(testid, after_timestamp=1.5)

      self.assertEqual(runs1, { runidA: timestampA, runidB: timestampB })
      self.assertEqual(runs2, { runidA: timestampA })
      self.assertEqual(runs3, { runidA: timestampA })

  def test_store_new_key_values_in_database(self):
    (dataset, test, timestamp) = ('qa', 'test', 1)
    (keyA, valueA) = ('some key', 'some value')
    (keyB, valueB) = ('other key', 'other value')

    with database.DB(database.DB.memory) as db:
      testid = db.register_test(dataset, test)
      runid  = db.register_testrun(testid, timestamp)

      db.store_keys(runid, { keyA: valueA, keyB: valueB })
      keys = db.get_keys(runid)

      self.assertEqual(keys, { keyA: valueA, keyB: valueB })

  def test_store_multiple_key_values_in_database_and_read_back(self):
    (dataset, test) = ('qa', 'test')
    (timestampA, timestampB) = (1, 2)
    (key, valueA, valueB) = ('some key', 'some value', 'other value')

    with database.DB(database.DB.memory) as db:
      testid = db.register_test(dataset, test)
      runidA = db.register_testrun(testid, timestampA)
      db.store_keys(runidA, { key: valueA })
      runidB = db.register_testrun(testid, timestampB)
      db.store_keys(runidB, { key: valueB })
      expected_one = { runidB: (valueB, timestampB) }
      expected_two = { runidA: (valueA, timestampA), runidB: (valueB, timestampB) }

      keyids = db.get_key_ids([key])
      keyid = keyids[key]
      keys_all         = db.get_key_values(key=keyid)
      keys_this_test   = db.get_key_values(key=keyid, test=testid)
      keys_other_test  = db.get_key_values(key=keyid, test=testid+1)
      keys_limit_1     = db.get_key_values(key=keyid, limit=1)
      keys_limit_2     = db.get_key_values(key=keyid, limit=2)
      keys_timestamp_1 = db.get_key_values(key=keyid, after_timestamp=1.5)
      keys_timestamp_2 = db.get_key_values(key=keyid, after_timestamp=0.5)

      # reformat output for comparing
      keys_all         = { r['runid']: r['value'] for r in keys_all }
      keys_this_test   = { r['runid']: (r['value'], r['timestamp']) for r in keys_this_test }
      keys_other_test  = { r['runid']: (r['value'], r['timestamp']) for r in keys_other_test }
      keys_limit_1     = { r['runid']: (r['value'], r['timestamp']) for r in keys_limit_1 }
      keys_limit_2     = { r['runid']: (r['value'], r['timestamp']) for r in keys_limit_2 }
      keys_timestamp_1 = { r['runid']: (r['value'], r['timestamp']) for r in keys_timestamp_1 }
      keys_timestamp_2 = { r['runid']: (r['value'], r['timestamp']) for r in keys_timestamp_2 }

      self.assertEqual(keys_all, { runidA: valueA, runidB: valueB })
      self.assertEqual(keys_this_test, expected_two)
      self.assertEqual(keys_other_test, {})
      self.assertEqual(keys_limit_1, expected_one)
      self.assertEqual(keys_limit_2, expected_two)
      self.assertEqual(keys_timestamp_1, expected_one)
      self.assertEqual(keys_timestamp_2, expected_two)

  def test_transform_data_structure_to_key_values(self):
    datastructure = { 'key': [ { 'a' : 1 }, { 'b' : 2 } , { 'c' : [ 'x', 'y' ] } ] }
    expected = { 'key.1.a': 1, 'key.2.b': 2, 'key.3.c.1': 'x', 'key.3.c.2': 'y' }

    actual = database.transform_to_values(datastructure)

    self.assertEqual(actual, expected)

if __name__ == '__main__':
  unittest.main()
