from qa import *

@Data
def checkthatdataiscomplete():
  images(90)

@Test(timeout=minutes(20))
def run_with_dials():
  xia2('-dials', 'nproc=1')

  expected_data_files = [
    'AUTOMATIC_DEFAULT_free.mtz',
    'AUTOMATIC_DEFAULT_scaled.sca',
    'AUTOMATIC_DEFAULT_scaled_unmerged.mtz',
    'AUTOMATIC_DEFAULT_scaled_unmerged.sca']
  skip('Data file expectation not supported yet')

  expected_summary = """\
Project: AUTOMATIC
Crystal: DEFAULT
Sequence length: 0
Wavelength: NATIVE (0.97950)
Sweep: SWEEP1
Files %s/X4_wide_M1S4_2_####.cbf
Images: 1 to 90
For AUTOMATIC/DEFAULT/NATIVE:
High resolution limit                       1.32    5.9     1.32
Low resolution limit                        28.99   28.99   1.35
Completeness                                99.9    99.4    100.0
Multiplicity                                5.6     5.2     5.4
I/sigma                                     7.6     15.9    1.9
Rmerge                                      0.085   0.03    0.504
CC half                                     0.997   0.998   0.869
Anomalous completeness                      99.3    100.0   98.9
Anomalous multiplicity                      3.0     3.7     2.8
Cell:  42.416  42.416  39.712  90.000  90.000  90.000
Spacegroup: P 41 21 2
""" % "data_dir"
  skip('Summary expectation not supported yet')

@Test(timeout=minutes(20))
def split_run_with_dials():
  split_xinfo_template = """/
BEGIN PROJECT AUTOMATIC
BEGIN CRYSTAL DEFAULT

BEGIN WAVELENGTH NATIVE
WAVELENGTH 0.979500
END WAVELENGTH NATIVE

BEGIN SWEEP SWEEP1
WAVELENGTH NATIVE
DIRECTORY %s
IMAGE X4_wide_M1S4_2_0001.cbf
START_END 1 40
BEAM 219.84 212.65
END SWEEP SWEEP1

BEGIN SWEEP SWEEP2
WAVELENGTH NATIVE
DIRECTORY %s
IMAGE X4_wide_M1S4_2_0001.cbf
START_END 45 90
BEAM 219.84 212.65
END SWEEP SWEEP2

END CRYSTAL DEFAULT
END PROJECT AUTOMATIC
"""
#  with open(xinfo_file, 'wb') as f:
#    print >> f, split_xinfo_template %(data_dir, data_dir)

#  xia2('-dials', 'nproc=1', 'njob=2', 'mode=parallel')
#  command_line_args = ['-dials', 'nproc=1', 'njob=2', 'mode=parallel',
#                       'xinfo=%s' %xinfo_file]
  skip('Running with xinfo file not supported yet')

  expected_summary = """\
Project: AUTOMATIC
Crystal: DEFAULT
Sequence length: 0
Wavelength: NATIVE (0.97950)
Sweep: SWEEP1
Files %s/X4_wide_M1S4_2_####.cbf
Images: 1 to 40
Sweep: SWEEP2
Files %s/X4_wide_M1S4_2_####.cbf
Images: 45 to 90
For AUTOMATIC/DEFAULT/NATIVE:
High resolution limit                       1.33    5.95    1.33
Low resolution limit                        29.03   29.03   1.36
Completeness                                98.3    99.4    85.5
Multiplicity                                4.9     4.7     3.3
I/sigma                                     7.6     14.9    1.8
Rmerge                                      0.077   0.023   0.394
CC half                                     0.997   0.999   0.756
Anomalous completeness                      96.2    96.2    78.2
Anomalous multiplicity                      2.6     3.3     1.8
Cell:  42.406  42.406  39.827  90.000  90.000  90.000
Spacegroup: P 41 21 2
""" %("data_dir", "data_dir")
  skip('Data file expectation not supported yet')
  skip('Summary expectation not supported yet')

@Test(timeout=minutes(20))
def run_with_xds():
  xia2('-3di', 'nproc=1')
 
  expected_data_files = [
    'AUTOMATIC_DEFAULT_free.mtz',
    'AUTOMATIC_DEFAULT_scaled.sca',
    'AUTOMATIC_DEFAULT_scaled_unmerged.mtz',
    'AUTOMATIC_DEFAULT_scaled_unmerged.sca']
  skip('Data file expectation not supported yet')

  expected_summary = """\
Project: AUTOMATIC
Crystal: DEFAULT
Sequence length: 0
Wavelength: NATIVE (0.97950)
Sweep: SWEEP1
Files %s/X4_wide_M1S4_2_####.cbf
Images: 1 to 90
For AUTOMATIC/DEFAULT/NATIVE:
High resolution limit                           1.31    5.86    1.31
Low resolution limit                            21.14   21.14   1.34
Completeness                                    99.8    98.3    99.7
Multiplicity                                    5.9     5.0     5.8
I/sigma                                         17.6    51.4    2.5
Rmerge                                          0.047   0.015   0.6
CC half                                         0.999   1.0     0.8
Anomalous completeness                          98.7    81.5    99.1
Anomalous multiplicity                          3.1     3.7     3.0
Cell:  42.280  42.280  39.590  90.000  90.000  90.000
Spacegroup: P 41 21 2
""" % "data_dir"
  skip('Summary expectation not supported yet')

@Test(timeout=minutes(20))
def split_run_with_xds():
  split_xinfo_template = """/
BEGIN PROJECT AUTOMATIC
BEGIN CRYSTAL DEFAULT

BEGIN WAVELENGTH NATIVE
WAVELENGTH 0.979500
END WAVELENGTH NATIVE

BEGIN SWEEP SWEEP1
WAVELENGTH NATIVE
DIRECTORY %s
IMAGE X4_wide_M1S4_2_0001.cbf
START_END 1 40
BEAM 219.84 212.65
END SWEEP SWEEP1

BEGIN SWEEP SWEEP2
WAVELENGTH NATIVE
DIRECTORY %s
IMAGE X4_wide_M1S4_2_0001.cbf
START_END 45 90
BEAM 219.84 212.65
END SWEEP SWEEP2

END CRYSTAL DEFAULT
END PROJECT AUTOMATIC
"""

#  with open(xinfo_file, 'wb') as f:
#    print >> f, split_xinfo_template %(data_dir, data_dir)

#  xia2('-3di', 'nproc=1', 'njob=2', 'mode=parallel')
  skip('Running with xinfo file not supported yet')
#  command_line_args = ['-3di', 'nproc=1', 'njob=2', 'mode=parallel',
#                       'xinfo=%s' %xinfo_file]

  expected_summary = """\
Project: AUTOMATIC
Crystal: DEFAULT
Sequence length: 0
Wavelength: NATIVE (0.97950)
Sweep: SWEEP1
Files %s/X4_wide_M1S4_2_####.cbf
Images: 1 to 40
Sweep: SWEEP2
Files %s/X4_wide_M1S4_2_####.cbf
Images: 45 to 90
For AUTOMATIC/DEFAULT/NATIVE:
High resolution limit                           1.29    5.77    1.29
Low resolution limit                            23.88   23.88   1.32
Completeness                                    98.8    98.9    89.9
Multiplicity                                    5.3     4.7     2.8
I/sigma                                         15.7    45.3    2.1
Rmerge                                          0.051   0.016   0.551
CC half                                         0.999   1.0     0.717
Anomalous completeness                          94.2    78.9    63.0
Anomalous multiplicity                          2.8     3.3     1.7
Cell:  42.300  42.300  39.650  90.000  90.000  90.000
Spacegroup: P 41 21 2
""" %("data_dir", "data_dir")
  skip('Data file expectation not supported yet')
  skip('Summary expectation not supported yet')

@Test(timeout=minutes(20))
def run_with_xds_ccp4a():
  xia2('-3di', 'nproc=1', 'scaler=ccp4a')

  expected_data_files = [
    'AUTOMATIC_DEFAULT_free.mtz',
    'AUTOMATIC_DEFAULT_scaled.sca',
    'AUTOMATIC_DEFAULT_scaled_unmerged.mtz',
    'AUTOMATIC_DEFAULT_scaled_unmerged.sca']
  skip('Data file expectation not supported yet')

  expected_summary = """\
Project: AUTOMATIC
Crystal: DEFAULT
Sequence length: 0
Wavelength: NATIVE (0.97950)
Sweep: SWEEP1
Files %s/X4_wide_M1S4_2_####.cbf
Images: 1 to 90
For AUTOMATIC/DEFAULT/NATIVE:
High resolution limit                           1.35    4.48    1.35
Low resolution limit                            23.85   23.85   1.42
Completeness                                    99.9    98.7    100.0
Multiplicity                                    6.0     5.3     6.1
I/sigma                                         13.2    40.2    2.7
Rmerge                                          0.063   0.021   0.467
CC half                                         0.999   0.999   0.884
Anomalous completeness                          99.7    99.3    99.8
Anomalous multiplicity                          3.3     3.4     3.2
Cell:  42.273  42.273  39.579  90.000  90.000  90.000
Spacegroup: P 41 21 2
""" % "data_dir"
  skip('Summary expectation not supported yet')

@Test(timeout=minutes(20))
def split_run_with_xds_ccp4a():
  split_xinfo_template = """/
BEGIN PROJECT AUTOMATIC
BEGIN CRYSTAL DEFAULT

BEGIN WAVELENGTH NATIVE
WAVELENGTH 0.979500
END WAVELENGTH NATIVE

BEGIN SWEEP SWEEP1
WAVELENGTH NATIVE
DIRECTORY %s
IMAGE X4_wide_M1S4_2_0001.cbf
START_END 1 40
BEAM 219.84 212.65
END SWEEP SWEEP1

BEGIN SWEEP SWEEP2
WAVELENGTH NATIVE
DIRECTORY %s
IMAGE X4_wide_M1S4_2_0001.cbf
START_END 45 90
BEAM 219.84 212.65
END SWEEP SWEEP2

END CRYSTAL DEFAULT
END PROJECT AUTOMATIC
"""

#  xia2('-3di', 'nproc=1', 'scaler=ccp4a', 'njob=2', 'mode=parallel')
  skip('Running with xinfo file not supported yet')
#  command_line_args = ['-3di', 'nproc=1', 'scaler=ccp4a', 'njob=2',
#                       'mode=parallel', 'xinfo=%s' %xinfo_file]

  expected_summary = """\
Project: AUTOMATIC
Crystal: DEFAULT
Sequence length: 0
Wavelength: NATIVE (0.97950)
Sweep: SWEEP1
Files %s/X4_wide_M1S4_2_####.cbf
Images: 1 to 40
Sweep: SWEEP2
Files %s/X4_wide_M1S4_2_####.cbf
Images: 45 to 90
For AUTOMATIC/DEFAULT/NATIVE:
High resolution limit                           1.37    6.13    1.37
Low resolution limit                            23.88   23.88   1.41
Completeness                                    99.9    98.8    100.0
Multiplicity                                    5.7     5.1     5.9
I/sigma                                         11.7    27.8    3.0
Rmerge                                          0.065   0.028   0.41
CC half                                         0.998   0.996   0.883
Anomalous completeness                          99.4    100.0   99.8
Anomalous multiplicity                          3.1     3.4     3.1
Cell:  42.305  42.305  39.654  90.000  90.000  90.000
Spacegroup: P 41 21 2
""" %("data_dir", "data_dir")
  skip('Data file expectation not supported yet')
  skip('Summary expectation not supported yet')
