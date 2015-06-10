from qa import *

@Data
def checkthatdataiscomplete():
  images(atleast(220))

@Test(timeout=minutes(20))
def runwithdials():
  xia2('-dials')

  runtime(minutes(8))
  spacegroup("C 2 2 2")
  unitcell(between(45.63, 49.63), between(101.23, 105.23), between(41.23, 45.23))
  resolution(1.4, 27)

@Test(timeout=minutes(20))
def runwith3dii():
  xia2('-3dii')

  runtime(minutes(10)
