from qa import *

@Data
def checkthatdataiscomplete():
  images(600,
         between(500,700),
         between(700,500),
         at_least(500),
         at_most(700),
         more_than(599),
         less_than(601)) # http://en.wikipedia.org/wiki/Rabbit_of_Caerbannog#Holy_Hand_Grenade_of_Antioch

@Test
def runwithdials():
  xia2('-dials')

  runtime(minutes(4))
#  spacegroup("P 1 21 1")
#  unitcell(between(4,7), between(5,8), between(10,20))
  resolution(2.1, 25)
  completeness(40)
  multiplicity(1.2)
  uniquereflections(3700)

@Test(timeout=minutes(20))
def runwith3dii():
  xia2('-3dii')

  runtime(minutes(4))
#  spacegroup("P 1 21 1")
#  unitcell(between(4,7), between(5,8), between(10,20))
  resolution(2.1, 25)
  completeness(40)
  multiplicity(1.2)
  uniquereflections(3700)
