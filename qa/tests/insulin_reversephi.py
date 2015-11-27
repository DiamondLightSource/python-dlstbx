from qa import *

@Data
def data_are_complete():
  images(180)

@Test(timeout=minutes(60))
def run_with_dials():
  xia2('-dials')

  runtime(minutes(20))
#  spacegroup("P 21 21 21")
#  unitcell(between(4,7), between(5,8), between(10,20))
#  resolution(0.7, 10)
#  completeness(90)
#  multiplicity(12)
#  uniquereflections(3000)

@Test(timeout=minutes(60))
def run_with_3dii():
  xia2('-3dii')

  runtime(minutes(20))
#  spacegroup("P 21 21 21")
#  unitcell(between(4,7), between(5,8), between(10,20))
#  resolution(0.7, 10)
#  completeness(90)
#  multiplicity(12)
#  uniquereflections(3000)
