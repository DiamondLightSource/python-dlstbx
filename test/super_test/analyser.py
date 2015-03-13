
from __future__ import division

class Analyser(object):
  '''
  Class to analyse the results

  '''

  def __init__(self, basepath, runpath):
    '''
    Initialise the class

    '''
    from os.path import join
    import json

    # Set the paths
    self.basepath = basepath
    self.runpath = runpath
    assert(self.basepath != self.runpath)
    self.datasetpath = join(self.basepath, "datasets.json")

    # load the datasets
    with open(self.datasetpath, "r") as infile:
      self.datasets = json.load(infile)

      # Convert keys to integer
    self.datasets = dict((int(k), v) for k, v in self.datasets.iteritems())

    # Check paths are unique
    paths = [join(d['directory'], d['template']) for d in
             self.datasets.itervalues()]
    assert(len(paths) == len(set(paths)))

    # Print some output
    print "Loaded %d datasets" % len(paths)

  def analyse(self):
    '''
    Analyse the results

    '''
    from os.path import join, exists

    # Find out which failed
    results = []
    indices = sorted(self.datasets.iterkeys())
    for i in indices:

      dataset = self.datasets[i]

      # Initialize the result
      result = {
        'id'         : i,
        'import'     : False,
        'find_spots' : False,
        'index'      : False,
        'refine'     : False,
        'integrate'  : False,
        'export'     : False
      }

      # The processing path
      directory = join(self.runpath, "%d" % i)
      if exists(directory):
        if exists(join(directory, "datablock.json")):
          result['import'] = True
        else:
          pass
        if exists(join(directory, "strong.pickle")):
          result['find_spots'] = True
        else:
          pass
        if (exists(join(directory, "experiments.json")) and
            exists(join(directory, "indexed.pickle"))):
          result['index'] = True
        else:
          pass
        if (exists(join(directory, "refined_experiments.json")) and
            exists(join(directory, "refined.pickle"))):
          result["refine"] = True
        else:
          pass
        if exists(join(directory, "integrated.pickle")):
          result["integrate"] = True
        else:
          pass
        if exists(join(directory, "hklout.mtz")):
          result["export"] = True
        else:
          pass
        results.append(result)
      else:
        results.append(result)

    # Print failure table
    rows = [['#',
             'Import',
             'Find Spots',
             'Index',
             'Refine',
             'Integrate',
             'Export']]
    for r in results:
      if list(r.itervalues()).count(False) > 0:
        rows.append([
          str(r['id']),
          str(r['import']),
          str(r['find_spots']),
          str(r['index']),
          str(r['refine']),
          str(r['integrate']),
          str(r['export'])])
    from libtbx.table_utils import format as table
    print table(rows, has_header=True)

    # Print the summary
    nimport = [r['import'] for r in results].count(True)
    nfind_spots = [r['find_spots'] for r in results].count(True)
    nindex = [r['index'] for r in results].count(True)
    nrefine = [r['refine'] for r in results].count(True)
    nintegrate = [r['integrate'] for r in results].count(True)
    nexport = [r['export'] for r in results].count(True)
    print "Num Import: %d" % nimport
    print "Num Find Spots: %d" % nfind_spots
    print "Num Index: %d" % nindex
    print "Num Refine: %d" % nrefine
    print "Num Integrate: %d" % nintegrate
    print "Num Export: %d" % nexport
