from qa import *

@Data(timeout=seconds(3),failFast=True)
def checkAllDataThere():
  resolution(2)
#  images(400)

@Test
def runwithdials():
  xia2('-dials')
