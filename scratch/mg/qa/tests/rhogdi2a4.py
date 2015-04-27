from qa import *

@Data
def checkthatdataiscomplete():
  images(300, 
         between(100,300),
         between(500,300),
         atLeast(300),
         atMost(300),)

@Test
def runwithdials():
  xia2('-dials')

  spacegroup("P 21 21 21")
  unitcell(between(4,7), between(5,8), between(10,20))

  resolution(1.5, 20)

  completeness(60)
  multiplicity(4)
  uniquereflections(100)
  multiplicity(between(3, 5))
  runtime(minutes(10))

@Test
def showparameters():
  xia2()

@Test
def earlytest():
  resolution(5)
  xia2('bla')
  pass

@Test(15)
def runwith3dii15():
  xia2('-3dii15')
  resolution(12)

@Test(timeout=minutes(20))
def runwith3dii():
  xia2('-3dii')

if 0:
  checkthatdataiscomplete()
  print
  print "Calling test without parameters"
  runwithdials()
  print
  print "Calling test with parameters"
  runwith3dii15()
  print
  print "Calling test with named parameters"
  runwith3dii()
  print
  print "dir():"
  print dir()

