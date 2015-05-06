import unittest
import comparators

class ComparatorTests(unittest.TestCase):

  def test_comparator_Between_is_working(self):
    C = comparators.between(3, 5)

    self.assertFalse(C(-1))
    self.assertFalse(C(0))
    self.assertFalse(C(2))
    self.assertTrue(C(3))
    self.assertTrue(C(4))
    self.assertTrue(C(5))
    self.assertFalse(C(6))
    self.assertFalse(C(None))
    self.assertFalse(C("cookie"))

  def test_comparator_MoreThan_is_working(self):
    C = comparators.more_than(3)

    self.assertFalse(C(-1))
    self.assertFalse(C(0))
    self.assertFalse(C(2))
    self.assertFalse(C(3))
    self.assertTrue(C(4))
    self.assertTrue(C(5))
    self.assertTrue(C(6))
    self.assertFalse(C(None))
    self.assertFalse(C("parrot"))

  def test_comparator_LessThan_is_working(self):
    C = comparators.less_than(3)

    self.assertTrue(C(-1))
    self.assertTrue(C(0))
    self.assertTrue(C(2))
    self.assertFalse(C(3))
    self.assertFalse(C(4))
    self.assertFalse(C(5))
    self.assertFalse(C(6))
    self.assertFalse(C(None))
    self.assertFalse(C("airplane"))

  def test_comparator_AtLeast_is_working(self):
    C = comparators.at_least(3)

    self.assertFalse(C(-1))
    self.assertFalse(C(0))
    self.assertFalse(C(2))
    self.assertTrue(C(3))
    self.assertTrue(C(4))
    self.assertTrue(C(5))
    self.assertTrue(C(6))
    self.assertFalse(C(None))
    self.assertFalse(C("carrot"))

  def test_comparator_AtMost_is_working(self):
    C = comparators.at_most(3)

    self.assertTrue(C(-1))
    self.assertTrue(C(0))
    self.assertTrue(C(2))
    self.assertTrue(C(3))
    self.assertFalse(C(4))
    self.assertFalse(C(5))
    self.assertFalse(C(6))
    self.assertFalse(C(None))
    self.assertFalse(C("ship"))

if __name__ == '__main__':
  unittest.main()
