
from __future__ import division

class Reader(object):

  def __init__(self, filename):
    self.success = False
    self.command = None
    with open(filename, "r") as infile:
      for line in infile:
        if line.startswith("Command line:"):
          tokens = line.split()
          self.command = tokens[2:]
        if line.startswith("High resolution limit"):
          tokens = line.split()
          self.high_resolution = map(float,tuple(tokens[3:]))
          assert(len(self.high_resolution) == 3)
        elif line.startswith("Low resolution limit"):
          tokens = line.split()
          self.low_resolution = map(float,tuple(tokens[3:]))
          assert(len(self.low_resolution) == 3)
        elif line.startswith("Completeness"):
          tokens = line.split()
          self.completeness = map(float,tuple(tokens[1:]))
          assert(len(self.completeness) == 3)
        elif line.startswith("Multiplicity"):
          tokens = line.split()
          self.multiplicity = map(float,tuple(tokens[1:]))
          assert(len(self.multiplicity) == 3)
        elif line.startswith("I/sigma"):
          tokens = line.split()
          self.i_over_sigma = map(float,tuple(tokens[1:]))
          assert(len(self.i_over_sigma) == 3)
        elif line.startswith("Rmerge"):
          tokens = line.split()
          self.rmerge = map(float,tuple(tokens[1:]))
          assert(len(self.rmerge) == 3)
        elif line.startswith("Rmeas(I)"):
          tokens = line.split()
          self.rmeas_i = map(float,tuple(tokens[1:]))
          assert(len(self.rmeas_i) == 3)
        elif line.startswith("Rmeas(I+/-)"):
          tokens = line.split()
          self.rmeas_i_plus_minus = map(float,tuple(tokens[1:]))
          assert(len(self.rmeas_i_plus_minus) == 3)
        elif line.startswith("Rpim(I)"):
          tokens = line.split()
          self.rpim_i = map(float,tuple(tokens[1:]))
          assert(len(self.rpim_i) == 3)
        elif line.startswith("Rpim(I+/-)"):
          tokens = line.split()
          self.rpim_i_plus_minus = map(float,tuple(tokens[1:]))
          assert(len(self.rpim_i_plus_minus) == 3)
        elif line.startswith("CC half"):
          tokens = line.split()
          self.cc_half = map(float,tuple(tokens[2:]))
          assert(len(self.cc_half) == 3)
        elif line.startswith("Wilson B factor"):
          tokens = line.split()
          self.wilson_b_factor = float(tokens[3])
        elif line.startswith("Anomalous completeness"):
          tokens = line.split()
          self.anomalous_completeness = map(float,tuple(tokens[2:]))
          assert(len(self.anomalous_completeness) == 3)
        elif line.startswith("Anomalous multiplicity"):
          tokens = line.split()
          self.anomalous_multiplicity = map(float,tuple(tokens[2:]))
          assert(len(self.anomalous_multiplicity) == 3)
        elif line.startswith("Anomalous correlation"):
          tokens = line.split()
          self.anomalous_correlation = map(float,tuple(tokens[2:]))
          assert(len(self.anomalous_correlation) == 3)
        elif line.startswith("Anomalous slope"):
          tokens = line.split()
          self.anomalous_slope = map(float,tuple(tokens[2:]))
          assert(len(self.anomalous_slope) == 3)
        elif line.startswith("dF/F"):
          tokens = line.split()
          self.df_f = float(tokens[1])
        elif line.startswith("dI/s(dI)"):
          tokens = line.split()
          self.di_s = float(tokens[1])
        elif line.startswith("Total observations"):
          tokens = line.split()
          self.total_observations = map(float,tuple(tokens[2:]))
          assert(len(self.total_observations) == 3)
        elif line.startswith("Total unique"):
          tokens = line.split()
          self.total_unique = map(float,tuple(tokens[2:]))
          assert(len(self.total_unique) == 3)
        elif line.startswith("Assuming spacegroup:"):
          tokens = line.split()
          self.space_group = ' '.join(tokens[2:])
        elif line.startswith("Status: normal termination"):
          self.success = True
