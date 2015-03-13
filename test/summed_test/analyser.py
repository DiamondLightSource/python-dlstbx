
from __future__ import division

class Analyser(object):
  '''
  Class to analyse the results

  '''

  def __init__(self, datasets, runpath):
    '''
    Initialise the class

    '''
    self.datasets = datasets
    self.runpath = runpath
    self.failed = []

  def analyse(self):
    '''
    Analyse the results

    '''
    from dlstbx.util import aimless
    from dials.array_family import flex
    from dials.algorithms.statistics import pearson_correlation_coefficient
    from dials.algorithms.statistics import spearman_correlation_coefficient

    nsweep = []
    rmerge_sum = []
    rmerge_prf = []
    rmerge_cmb = []
    cchalf_sum = []
    cchalf_prf = []
    cchalf_cmb = []
    cc_sum_prf = []
    rc_sum_prf = []

    # Loop through all the datasets
    for i in range(2, 32):

      print i

      # Read the reflections
      reflections = flex.reflection_table.from_pickle("%d/integrated.pickle" % i)

      # Get only those reflections integrated
      reflections = reflections.select(
        reflections.get_flags(
          reflections.flags.integrated, all=True))

      # Get the summation and profile fitted intensities
      Isum = reflections['intensity.sum.value']
      Iprf = reflections['intensity.prf.value']

      # Compute the correlations
      cc_sum_prf.append(pearson_correlation_coefficient(Isum, Iprf))
      rc_sum_prf.append(spearman_correlation_coefficient(Isum, Iprf))

      # Read the files
      reader_sum = aimless.AimlessLogReader("%d/summation/aimless.log" % i)
      reader_prf = aimless.AimlessLogReader("%d/profile/aimless.log" % i)
      reader_cmb = aimless.AimlessLogReader("%d/combine/aimless.log" % i)

      # Append the data
      nsweep.append(i)
      rmerge_sum.append(reader_sum.rmerge[2])
      rmerge_prf.append(reader_prf.rmerge[2])
      rmerge_cmb.append(reader_cmb.rmerge[2])
      cchalf_sum.append(reader_sum.cchalf[2])
      cchalf_prf.append(reader_prf.cchalf[2])
      cchalf_cmb.append(reader_cmb.cchalf[2])

    print "Rmerge"
    print "------"
    for n, sum, prf, cmb in zip(nsweep, rmerge_sum, rmerge_prf, rmerge_cmb):
      print "%02d  %.2f  %.2f  %.2f" % (n, sum, prf, cmb)

    print "CChalf"
    print "------"
    for n, sum, prf, cmb in zip(nsweep, cchalf_sum, cchalf_prf, cchalf_cmb):
      print "%02d  %.2f  %.2f  %.2f" % (n, sum, prf, cmb)

    print "CC/RC"
    print "------"
    for n, cc, rc in zip(nsweep, cc_sum_prf, rc_sum_prf):
      print "%02d  %.2f  %.2f" % (n, cc, rc)

    # Set the matplotlib backend
    import matplotlib
    matplotlib.use("Agg")

    # Create the figure
    from matplotlib import pylab
    print "Writing figure rmerge.png"
    pylab.plot(nsweep, rmerge_sum, color='black')
    pylab.plot(nsweep, rmerge_prf, color='blue')
    pylab.plot(nsweep, rmerge_cmb, color='red')
    pylab.savefig("rmerge.png")
    pylab.clf()

    print "Writing figure cchalf.png"
    pylab.plot(nsweep, cchalf_sum, color='black')
    pylab.plot(nsweep, cchalf_prf, color='blue')
    pylab.plot(nsweep, cchalf_cmb, color='red')
    pylab.savefig("cchalf.png")
    pylab.clf()

    print "Writing figure cc_sum_prf.png"
    pylab.plot(nsweep, cc, color='blue')
    pylab.savefig('cc_sum_prf.png')
    pylab.clf()

    print "Writing figure rc_sum_prf.png"
    pylab.plot(nsweep, rc, color='blue')
    pylab.savefig('rc_sum_prf.png')
    pylab.clf()
