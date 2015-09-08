from qa import *

@Data
def check_that_data_is_complete():
  images(360)

@Test(timeout=minutes(20))
def run_with_dials():
  xia2('-dials')

  runtime(minutes(8))

@Test(timeout=minutes(30))
def run_with_3dii():
  xia2('-3dii')

  runtime(minutes(20))
