import os
import sqlite3

class DB(object):
  memory = ':memory:'

  def __init__(self, file):
    _needs_initialization = (file == self.memory) or not os.path.isfile(file)
    self.sql = sqlite3.connect(file)
    self.sql.row_factory = sqlite3.Row
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
    cur = self.sql.cursor()
    cur.execute("CREATE TABLE TestModules(id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, dataset TEXT NOT NULL, test TEXT NOT NULL, lastseen INTEGER NOT NULL, success INT NOT NULL, stdout TEXT, stderr TEXT, json TEXT, xia2error TEXT)")
    cur.execute("CREATE UNIQUE INDEX dataset_test ON TestModules (dataset, test)")

  def processed_dataset(self, dataset, test, lastseen, success, stdout, stderr, json, xia2error):
    cur = self.sql.cursor()
    cur.execute('SELECT id FROM TestModules WHERE dataset = :ds AND test = :t', {'ds': dataset, 't': test})
    existing_id = cur.fetchone()
    success = 1 if success else 0
    if existing_id is not None:
      cur.execute('UPDATE TestModules SET lastseen = ?, success = ?, stdout = ?, stderr = ?, json = ?, xia2error = ? WHERE id = ?', (lastseen, success, stdout, stderr, json, xia2error, existing_id['id']))
    else:
      cur.execute('INSERT INTO TestModules(dataset, test, lastseen, success, stdout, stderr, json, xia2error) VALUES (?, ?, ?, ?, ?, ?, ?, ?)', (dataset, test, lastseen, success, stdout, stderr, json, xia2error))
    self.sql.commit()

  def select_dataset(self, whereclause=''):
    cur = self.sql.cursor()
    cur.execute('SELECT * FROM TestModules %s' % whereclause)
    return cur.fetchall()

  def store_keys(self, dataset, test, timestamp, values):
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
