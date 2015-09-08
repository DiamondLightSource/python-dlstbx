from qa import *

@Data
def check_that_data_is_complete():
  images(at_least(220))

@Test(timeout=minutes(20))
def run_with_dials():
  xia2('-dials', '-atom', 'Se')

  runtime(minutes(8))
  spacegroup("C 2 2 2")
  unitcell(between(45.63, 49.63), between(101.23, 105.23), between(41.23, 45.23))
  resolution(1.35, 30)

@Test(timeout=minutes(20))
def run_with_3dii():
  xia2('-3dii')

  runtime(minutes(10))
