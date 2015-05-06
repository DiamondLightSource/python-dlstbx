import os
import sqlite3

class DB(object):
  memory = ':memory:'

  def __init__(self, file):
    _needs_initialization = (file == self.memory) or not os.path.isfile(file)
    self.sql = sqlite3.connect(file)
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
    pass

  def save(self, dataset, test, timestamp, values):
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
