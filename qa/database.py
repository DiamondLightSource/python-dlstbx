import os
import sqlite3

class DB(object):
  memory = ':memory:'

  def __init__(self, file):
    _needs_initialization = (file == self.memory) or not os.path.isfile(file)
    self.sql = sqlite3.connect(file)
    self.sql.row_factory = sqlite3.Row
    self.sql.execute('PRAGMA foreign_keys=1')
    if _needs_initialization:
      self._initialize_database()

  def __enter__(self):
    return self  # for use with python 'with' statement

  def __exit__(self, type, value, traceback):
    self.close() # for use with python 'with' statement

  def __del__(self):
    self.close() # destructor

  def close(self):
    if self.sql is not None:
      self.sql.close()
    self.sql = None

  def _initialize_database(self):
    with self.sql as sql:
      cur = self.sql.cursor()
      cur.execute("CREATE TABLE Tests(id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, dataset TEXT NOT NULL, test TEXT NOT NULL, lastseen INTEGER, success INT, stdout TEXT, stderr TEXT, json TEXT, xia2error TEXT)")
      cur.execute("CREATE TABLE TestRuns(id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, testid INTEGER NOT NULL, timestamp INTEGER NOT NULL, FOREIGN KEY(testid) REFERENCES Tests(id) ON DELETE CASCADE)")
      cur.execute("CREATE TABLE Observables(id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, key TEXT NOT NULL UNIQUE)")
      cur.execute("CREATE TABLE Observations(runid INTEGER NOT NULL, observableid INTEGER NOT NULL, value TEXT, FOREIGN KEY(runid) REFERENCES TestRuns(id) ON DELETE CASCADE, FOREIGN KEY(observableid) REFERENCES Observables(id) ON DELETE CASCADE)")
      cur.execute("CREATE UNIQUE INDEX dataset_test ON Tests (dataset, test)")

  def register_test(self, dataset, test):
    id = self.get_testid(dataset, test)
    if id is not None:
      return id
    with self.sql as sql:
      cur = sql.cursor()
      cur.execute('INSERT INTO Tests(dataset, test) VALUES (?, ?)', (dataset, test))
      sql.commit()
      return self.get_testid(dataset, test)

  def get_testid(self, dataset, test):
    with self.sql as sql:
      cur = sql.cursor()
      cur.execute('SELECT id FROM Tests WHERE dataset = :ds AND test = :t', {'ds': dataset, 't': test})
      existing_id = cur.fetchone()
      if existing_id is None:
        return None
      else:
        return existing_id['id']

  def get_tests(self, test_id=None, all_columns=False, group_by_dataset=False, order_by_name=False):
    query = { 'select': [], 'where': [], 'group': [], 'order': [] }

    query['select'] = [ 'id', 'dataset', 'test', 'lastseen', 'success' ]
    if order_by_name:
      query['order'].append('dataset ASC')
      query['order'].append('test ASC')
    if group_by_dataset:
      query['group'].append('dataset')
      query['select'].remove('id')
      query['select'].remove('test')
      if order_by_name:
        query['order'].remove('test ASC')
      query['select'].remove('success')
      query['select'].remove('lastseen')
      query['select'].append('MIN(success) as success')
      query['select'].append('MAX(lastseen) as lastseen')
      all_columns = False
    if all_columns:
      query['select'].extend([ 'stdout', 'stderr', 'json', 'xia2error' ])
    if test_id:
      query['where'].append('id = :testid')

    query['select'] = ', '.join(query['select'])
    query['where'] = ') AND ('.join(query['where'])
    query['group'] = ', '.join(query['group'])
    query['order'] = ', '.join(query['order'])

    if query['select'] != '':
      query['select'] = 'SELECT %s FROM Tests' % query['select']
    if query['where'] != '':
      query['where'] = 'WHERE (%s)' % query['where']
    if query['group'] != '':
      query['group'] = 'GROUP BY %s' % query['group']
    if query['order'] != '':
      query['order'] = 'ORDER BY %s' % query['order']

    query = " ".join([query[n] for n in ['select', 'where', 'group', 'order']])

    with self.sql as sql:
      cur = sql.cursor()
      cur.execute(query, {'testid': test_id} )
      if test_id:
        return cur.fetchone()
      return cur.fetchall()

  def register_testrun(self, testid, timestamp):
    with self.sql as sql:
      cur = sql.cursor()
      cur.execute('INSERT INTO TestRuns(testid, timestamp) VALUES (?, ?)', (testid, timestamp))
      sql.commit()
      return cur.lastrowid

  def get_testruns(self, testid, limit=None, after_timestamp=None):
    with self.sql as sql:
      cur = sql.cursor()
      sql_command = 'SELECT id, timestamp FROM TestRuns WHERE testid = :testid'
      if after_timestamp:
        sql_command += ' AND timestamp > :timestamp'
      if limit:
        sql_command += ' LIMIT :limit'
      rows = cur.execute( sql_command, {'testid': testid, 'timestamp': after_timestamp, 'limit': limit})
      results = {}
      for row in rows:
        results[row['id']] = row['timestamp']
      return results

  def store_test_result(self, testid, lastseen, success, stdout, stderr, json, xia2error):
    with self.sql as sql:
      cur = sql.cursor()
      success = 1 if success else 0
      cur.execute('UPDATE Tests SET lastseen = ?, success = ?, stdout = ?, stderr = ?, json = ?, xia2error = ? WHERE id = ?', (lastseen, success, stdout, stderr, json, xia2error, testid))
      sql.commit()

  def get_key_ids(self, keys, register_keys=False):
    with self.sql as sql:
      cur = sql.cursor()
      keyids = {}
      for k in sorted(keys):
        cur.execute('SELECT id FROM Observables WHERE key = :key', { 'key': k })
        keyid = cur.fetchone()
        if (keyid is None) and register_keys:
          cur.execute('INSERT INTO Observables (key) VALUES (:key)', { 'key': k })
          keyid = cur.lastrowid
        else:
          keyid = keyid['id']
        keyids[k] = keyid
      if register_keys:
        sql.commit()
    return keyids

  def store_keys(self, runid, values):
    keyids = self.get_key_ids(values.keys(), register_keys=True)
    data = [ (runid, keyids[key], values[key]) for key in values ]
    with self.sql as sql:
      cur = sql.cursor()
      cur.executemany('INSERT INTO Observations (runid, observableid, value) VALUES (?, ?, ?)', data)
      sql.commit()

  def get_keys(self, runid):
    with self.sql as sql:
      cur = sql.cursor()
      cur.execute('SELECT key, value FROM Observations JOIN Observables ON (Observables.id = Observations.observableid) WHERE runid = :runid', { 'runid': runid })
      return { k:v for (k, v) in cur.fetchall() }

  def get_key_values(self, key, test=None, limit=None, after_timestamp=None):
    select_vars = [ 'runid', 'value' ]
    whereclause = [ 'observableid = :key' ]
    limitclause = []

    join_testruns = False
    with self.sql as sql:
      cur = sql.cursor()
      if not test is None:
        join_testruns = True
        whereclause.append('testid = :test')
      if not after_timestamp is None:
        join_testruns = True
        whereclause.append('timestamp > :timestamp')
      if not limit is None:
        join_testruns = True
        limitclause = [ 'ORDER BY timestamp DESC LIMIT :limit' ]

      if join_testruns:
        select_vars.extend(['testid', 'timestamp'])
      sql_command = [ 'SELECT', ', '.join(select_vars), 'FROM Observations' ]

      if join_testruns:
        sql_command.append('JOIN TestRuns ON (Observations.runid = TestRuns.id)')

      sql_command.append('WHERE (' + (") AND (".join(whereclause)) + ')')
      sql_command.extend(limitclause)

      cur.execute(' '.join(sql_command), { 'key': key, 'test': test, 'timestamp': after_timestamp, 'limit': limit } )
      return cur.fetchall()

  def transform_to_values(self, datastructure):
    global transform_to_values
    return transform_to_values(datastructure)

def transform_to_values(datastructure):
  from collections import Mapping
  if isinstance(datastructure, Mapping):
    recursive = {}
    for key, value in datastructure.iteritems():
      kv = transform_to_values(value)
      for kvkey, kvvalue in kv.iteritems():
        recursive[ key + ('.' + kvkey if kvkey != '' else '') ] = kvvalue
    return recursive
  elif isinstance(datastructure, basestring):
    return { '': datastructure }
  else:
    try:
      z = 1
      recursive = {}
      for n in datastructure:
       kv = transform_to_values(n)
       for kvkey, kvvalue in kv.iteritems():
         recursive[ str(z) + ('.' + kvkey if kvkey != '' else '') ] = kvvalue
       z += 1
      return recursive
    except:
      pass
  return { '': datastructure }
