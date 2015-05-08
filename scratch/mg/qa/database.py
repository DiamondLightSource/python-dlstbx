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

  def get_test(self, testid):
    with self.sql as sql:
      cur = sql.cursor()
      cur.execute('SELECT * FROM Tests WHERE id = :testid', {'testid': testid})
      existing_id = cur.fetchone()
      return existing_id

  def store_test_result(self, testid, lastseen, success, stdout, stderr, json, xia2error):
    with self.sql as sql:
      cur = sql.cursor()
      success = 1 if success else 0
      cur.execute('UPDATE Tests SET lastseen = ?, success = ?, stdout = ?, stderr = ?, json = ?, xia2error = ? WHERE id = ?', (lastseen, success, stdout, stderr, json, xia2error, testid))
      sql.commit()

  def get_dataset(self, dataset=None, test=None, testid=None):
    pass

  def select_tests(self, whereclause=''):
    with self.sql as sql:
      cur = sql.cursor()
      cur.execute('SELECT * FROM Tests %s' % whereclause)
      return cur.fetchall()

  def store_keys(self, dataset, test, timestamp, values):
    with self.sql as sql:
      pass

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
