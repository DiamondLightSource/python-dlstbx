import unittest
from units import *

class UnitsTests(unittest.TestCase):

  def test_check_named_time_to_second_functions(self):
    self.assertEqual(milliseconds(100), 100 / 1000)
    self.assertEqual(seconds(13), 13)
    self.assertEqual(minutes(20), 20 * 60)
    self.assertEqual(hours(3), 3 * 60*60)
    self.assertEqual(days(5), 5 * 24*60*60)
    self.assertEqual(weeks(2), 2 * 7*24*60*60)

  def test_readable_timespans_are_correct(self):
    times = { 1: '1 second',
             10: '10 seconds',
             50: '50 seconds',
            119: '119 seconds',
            120: '2 minutes',
           1800: '30 minutes',
           3599: '59 minutes',
           5400: '90 minutes',
           6000: '100 minutes',
           7199: '119 minutes',
           7200: '2 hours',
          72000: '20 hours',
         129600: '36 hours',
         172800: '2 days',
       17280000: '200 days',
            0.9: 'less than 1 second',
            }
    timeslist = times.keys()
    expected = [ times[t] for t in timeslist ]

    actual = [ readable_time(t) for t in timeslist ]

    self.assertEqual(actual, expected)

if __name__ == '__main__':
  unittest.main()
