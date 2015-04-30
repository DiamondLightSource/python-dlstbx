from qa import *

@Data
def checkthatdataiscomplete():
  images(300, 
         between(100,300),
         between(500,300),
         atLeast(300),
         atMost(300),
         moreThan(299),
         lessThan(301))

@Test
def runwithdials():
  xia2('-dials')

  runtime(minutes(4))
#  spacegroup("P 21 21 21")
#  unitcell(between(4,7), between(5,8), between(10,20))
  resolution(2.1, 25)
  completeness(40)
  multiplicity(1.2)
  uniquereflections(3700)

@Test
def failwithinvalidparameters():
  xia2('-stuff', 'barf=1')

@Test(timeout=minutes(20))
def runwith3dii():
  xia2('-3dii')

  runtime(minutes(4))
#  spacegroup("P 21 21 21")
#  unitcell(between(4,7), between(5,8), between(10,20))
  resolution(2.1, 25)
  completeness(40)
  multiplicity(1.2)
  uniquereflections(3700)

if 0:
  checkthatdataiscomplete()
  #runwithdials()
  #runwith3dii()
  print
  print "dir():"
  print dir()

