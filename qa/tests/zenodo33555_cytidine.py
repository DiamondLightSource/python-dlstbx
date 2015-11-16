from qa import *

@Data
def data_are_complete():
  images(7950)

@Test(timeout=minutes(120))
def run_with_dials():
  xia2('-dials')

  runtime(minutes(4))
#  spacegroup("P 21 21 21")
#  unitcell(between(4,7), between(5,8), between(10,20))
  resolution(2.1, 25)
  completeness(40)
  multiplicity(1.2)
  uniquereflections(3700)

@Test(timeout=minutes(120))
def run_with_3dii():
  xia2('-3dii')

  runtime(minutes(4))
#  spacegroup("P 21 21 21")
#  unitcell(between(4,7), between(5,8), between(10,20))
  resolution(2.1, 25)
  completeness(40)
  multiplicity(1.2)
  uniquereflections(3700)
