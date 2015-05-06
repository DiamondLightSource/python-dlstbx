import sqlite3

class DB(object):
  def __init__(self, file):
#   print sqlite3
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
