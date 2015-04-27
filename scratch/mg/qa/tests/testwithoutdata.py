from qa import *

@Test
def runwithdials():
  xia2('-dials')

  spacegroup("P 21 21 21")
  unitcell(between(4,7), between(5,8), between(10,20))

  resolution(1.5, 20)
  highresolution(1.5)
  lowresolution(20)

  completeness(60)
  multiplicity(4)
  uniquereflections(100)
  multiplicity(between(3, 5))
  runtime(minutes(10))


