from qa import *

@Data
def data_are_complete():
  images(7950)

@Test(timeout=minutes(120))
def run_with_dials():
  xia2('-dials', '-small_molecule')

  runtime(minutes(25))
#  spacegroup("P 21 21 21")
#  unitcell(between(4,7), between(5,8), between(10,20))
  resolution(0.7, 10)
  completeness(90)
  multiplicity(12)
  uniquereflections(3000)

@Test(timeout=minutes(120))
def run_with_3dii():
  xia2('-3dii', '-small_molecule')

  runtime(minutes(4))
#  spacegroup("P 21 21 21")
#  unitcell(between(4,7), between(5,8), between(10,20))
  resolution(0.7, 10)
  completeness(90)
  multiplicity(12)
  uniquereflections(3000)
