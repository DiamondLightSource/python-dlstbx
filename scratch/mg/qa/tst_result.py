import unittest
import result

class ResultTests(unittest.TestCase):

  def test_result_log_mechanism_with_debug_output(self):
    (t1, t2) = ('some text', 'some more text')

    r = result.Result()
    r.log_message(t1)
    r.log_message(t2)

    self.assertEqual(r.stdout, "\n".join([t1, t2]))
    self.assertEqual(r.stderr, None)
    self.assertEqual(r.failure_message, None)
    self.assertEqual(r.failure_output, None)
    self.assertEqual(r.skipped_message, None)
    self.assertEqual(r.skipped_output, None)
    self.assertTrue(r.is_success())
    self.assertFalse(r.is_failure())
    self.assertFalse(r.is_skipped())

  def test_result_log_mechanism_with_error_output(self):
    (t1, t2) = ('some text', 'some more text')

    r = result.Result()
    r.log_error(t1)
    r.log_error(t2)

    self.assertEqual(r.stdout, None)
    self.assertEqual(r.stderr, "\n".join([t1, t2]))
    self.assertEqual(r.failure_message, t1)
    self.assertEqual(r.failure_output, None)
    self.assertEqual(r.skipped_message, None)
    self.assertEqual(r.skipped_output, None)
    self.assertFalse(r.is_success())
    self.assertTrue(r.is_failure())
    self.assertFalse(r.is_skipped())

  def test_result_log_mechanism_with_skipped_output(self):
    (t1, t2) = ('some text', 'some more text')

    r = result.Result()
    r.log_skip(t1)
    r.log_skip(t2)

    self.assertEqual(r.stdout, None)
    self.assertEqual(r.stderr, None)
    self.assertEqual(r.failure_message, None)
    self.assertEqual(r.failure_output, None)
    self.assertEqual(r.skipped_message, t1)
    self.assertEqual(r.skipped_output, "\n".join([t1, t2]))
    self.assertFalse(r.is_success())
    self.assertFalse(r.is_failure())
    self.assertTrue(r.is_skipped())

  def test_result_log_mechanism_with_stacktrace(self):
    (t1, t2, t3) = ('some text', 'some more text', 'third line')
    traceback = "\n".join([t1, t2, t3])

    r = result.Result()
    r.log_trace(traceback)

    self.assertEqual(r.stdout, None)
    self.assertEqual(r.stderr, None)
    self.assertEqual(r.failure_message, t1)
    self.assertEqual(r.failure_output, traceback)
    self.assertEqual(r.skipped_message, None)
    self.assertEqual(r.skipped_output, None)
    self.assertFalse(r.is_success())
    self.assertTrue(r.is_failure())
    self.assertFalse(r.is_skipped())

  def test_result_internal_timer(self):
    r = result.Result()
    self.assertEqual(r.elapsed_sec, 0)
    import time
    time.sleep(0.1)
    r.update_timer()
    self.assertAlmostEqual(r.elapsed_sec, 0.1, places=2)

  def test_junit_specific_result_features(self):
    (n1, n2) = ('name1', 'name2')

    r = result.Result()
    r.set_name(n1)
    r.set_classname(n2)

    self.assertEqual(r.name, n1)
    self.assertEqual(r.classname, n2)

  @unittest.skip('not implemented yet')
  def test_inorder_logging_of_debug_and_error_messages(self):
    self.fail('Not implemented yet')

  @unittest.skip('not implemented yet')
  def test_appending_one_result_object_to_another(self):
    self.fail('Not implemented yet')

if __name__ == '__main__':
  unittest.main()
