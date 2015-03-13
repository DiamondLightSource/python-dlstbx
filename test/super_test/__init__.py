
from __future__ import division
import matplotlib

# Use agg backend
matplotlib.use('Agg')

class BulkTest(object):
  '''
  Test case using large number of datasets.

  '''

  def __init__(self, directory):
    '''
    Initialise the test case reading data from file

    '''
    from os.path import join
    import json

    # The base and run directory
    self.basepath = join(directory, "bulk")
    self.runpath = join(directory, "run", "bulk")
    #self.runpath = join(self.basepath, "run")
    #self.bulkpath = join(self.basepath, "bulk")
    #self.datasetpath = join(self.basepath, "bulk", "datasets.json")
    #self.resultspath = join(self.basepath, "bulk", "results.json")

    # Read the datasets
    #with open(self.datasetpath, "r") as infile:
    #  self._datasets = json.load(infile)

    # Ensure data collection ofids are unique
    #ids = [d['id'] for d in self._datasets]
    #if len(ids) != len(set(ids)):
    #  raise RuntimeError('Collection ID must be unique')

    # Read the results
    #with open(self.resultspath, "r") as infile:
    #  self._results = json.load(infile)
#
#  def info(self, show='summary'):
#    '''
#    Get a string with information about the test
#
#    :param show: The stuff to show
#    :return: The info string
#
#    '''
#    from os.path import join
#    from StringIO import StringIO
#    from libtbx.table_utils import format as table
#
#    # Setup the buffer
#    output = StringIO()
#    print >> output, '-' * 80
#    print >> output, 'Bulk Test'
#    print >> output, '-' * 80
#    print >> output, ''
#
#    # Print out a list of datasets
#    if show == 'all' or show == 'paths':
#      print >> output, ' Datasets'
#      rows = [[
#        '#',
#        'ID',
#        'TEMPLATE'
#      ]]
#      for i, dataset in enumerate(self._datasets):
#        rows.append([
#          str(i),
#          str(dataset['id']),
#          str(join(dataset['directory'], dataset['template']))
#        ])
#      print >> output, table(rows, has_header=True, justify='left', prefix=' ')
#      print >> output, ''
#
#    # Print out the results
#    if show == 'all' or show == 'results':
#      print >> output, ' Results'
#      rows = [[
#        '#',
#        'ID',
#        'FOUND SPOTS',
#        'INDEXED',
#        'REFINED',
#        'INTEGRATED',
#        'SCALED'
#        'RMERGE',
#        'CC 1/2',
#      ]]
#      for i, dataset in enumerate(self._datasets):
#        rows.append([
#          str(i),
#          str(dataset['id']),
#          str(self._results[-1][dataset['id']]['find_spots']),
#          str(self._results[-1][dataset['id']]['index']),
#          str(self._results[-1][dataset['id']]['refine']),
#          str(self._results[-1][dataset['id']]['integrate']),
#          str(self._results[-1][dataset['id']]['scale']),
#          str(self._results[-1][dataset['id']]['rmerge']),
#          str(self._results[-1][dataset['id']]['cchalf'])
#        ])
#      print >> output, table(rows, has_header=True, justify='left', prefix=' ')
#      print >> output, ''
#
#    # Print summary results
#    if show == 'all' or show == 'summary':
#      print >> output, ' Summary'
#      passed_find_spots = []
#      passed_index = []
#      passed_refine = []
#      passed_integrate = []
#      passed_scale = []
#      better_rmerge = []
#      better_cchalf = []
#      for dataset in enumerate(self._datasets):
#        r = self._results[-1][dataset['id']]
#        passed_find_spots.append(r['find_spots'])
#        passed_index.append(r['index'])
#        passed_refine.append(r['refine'])
#        passed_integrate.append(r['integrate'])
#        passed_scale.append(r['scale'])
#        r1 = r['rmerge']
#        c1 = r['cchalf']
#        if len(self._results) == 1:
#          r0 = r1
#          c0 = c1
#        else:
#          r0 = self._results[-2][dataset['id']]['rmerge']
#          c0 = self._results[-2][dataset['id']]['cchalf']
#        better_rmerge.append(r1 <= r0)
#        better_cchalf.append(c1 >= c0)
#      rows = [
#        ['# Datasets'            , str(len(self._datasets))],
#        ['# Failed to find spots', str(passed_find_spots.count(False))],
#        ['# Failed to indexing'  , str(passed_index.count(False))],
#        ['# Failed to refine'    , str(passed_refine.count(False))],
#        ['# Failed to integrate' , str(passed_integrate.count(False))],
#        ['# Failed to scale'     , str(passed_scale.count(False))],
#        ['# Better Rmerge'       , str(better_rmerge.count(True))]
#        ['# Better CC 1/2'       , str(better_cchalf.count(True))]
#      ]
#      print >> output, table(rows, has_header=False, justify='left', prefix=' ')
#      print >> output, ''
#
#    # Return the info
#    return output.getvalue()
#
#  def print_figures(self):
#    '''
#    Print figures for the processing.
#
#    '''
#    self.print_last_rmerge()
#    self.print_last_cchalf()
#    self.print_success_over_time()
#    self.print_num_better_rmerge_over_time()
#    self.print_num_better_cchalf_over_time()
#
#  def print_last_rmerge(self):
#    pass
#
#  def print_last_cchalf(self):
#    pass
#
#  def print_success_over_time(self):
#    pass
#
#  def print_num_better_rmerge_over_time(self):
#    pass
#
#  def print_num_better_cchalf_over_time(self):
#    pass
#
  def run(self, resume=False, dataset=None, date=None):
    '''
    Run the test

    :param use: The datasets to run on

    '''
    from dlstbx.test.super_test.runner import Runner
    from dlstbx.test.super_test.analyser import Analyser

    # Run the test
    runner = Runner(self.basepath, self.runpath)
    runner.run(resume, dataset=dataset, date=date)

    # Analyse the results
    analyser = Analyser(runner.basepath, runner.runpath)
    analyser.analyse()


def get_tests(name, directory):
  '''
  Get a list of the test objects

  :param name: The name of the test (can be all)
  :returns: A list of tests

  '''
  if name == 'all':
    return [BulkTest(directory)]
  elif name == 'bulk':
    return [BulkTest(directory)]
  else:
    raise RuntimeError('Unknown test')
