from qa import *

@Data
def checkthatdataiscomplete():
  images(300,
         between(100,300),
         between(500,300),
         at_least(300),
         at_most(300),
         more_than(299),
         less_than(301)) # http://en.wikipedia.org/wiki/Rabbit_of_Caerbannog#Holy_Hand_Grenade_of_Antioch

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
