from qa import *

@Data
def checkthatdataiscomplete():
  images(moreThan(30), lessThan(100))
  if has_images(moreThan(200)):
    output("more than 200")
  else:
    output("less than 200")
  if has_images(moreThan(30)):
    output("more than 30")
  else:
    output("less than 30")

@Test(timeout=minutes(20))
def runwith3d():
  xia2('-3d')

  runtime(minutes(1))
  spacegroup("P 21 21 21")
  unitcell(between(4,7), between(5,8), between(10,20))
  resolution(2.1, 25)

  if has_resolution(1.0):
    output("ultra-high resolution")
  else:
    output("meh")
  if has_resolution(5.0):
    output("ultra-normal resolution")
  else:
    output("anormal resolution")
  completeness(40)
  multiplicity(1.2)
  uniquereflections(3700)
