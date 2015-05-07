import unittest
import result
import time

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

  def test_inorder_logging_of_debug_and_error_messages(self):
    (t1, t2, t3) = ('some text', 'some more text', 'third line')

    r = result.Result()
    r.log_message(t1)
    r.log_error(t2)
    r.log_message(t3)
    r.log_skip(t1)
    r.log_message(t2)
    r.log_error(t3)
    r.log_skip(t1)

    (msg, skp, err) = (0, 1, 2)
    self.assertEqual([l for (l, _) in r.log], [msg, err, msg, skp, msg, err, skp])
    self.assertEqual([t for (_, t) in r.log], [t1, t2, t3, t1, t2, t3, t1])

  def test_appending_one_result_object_to_another(self):
    (t1, t2) = ('some text', 'some more text')

    r = result.Result()
    r.log_message(t1)

    s = result.Result()
    s.log_error(t1)

    t = result.Result()
    t.log_message(t2)
    t.log_error(t2)
    t.log_skip(t2)
    t.log_trace(t2)

    s.append(t)
    r.append(s)

    self.assertTrue(r.is_failure())
    self.assertEqual(r.failure_message, t1)
    self.assertEqual(r.failure_output, t2)
    self.assertEqual(r.skipped_message, t2)
    self.assertEqual(r.skipped_output, t2)
    self.assertEqual(r.stderr, "\n".join([t1,t2]))

if __name__ == '__main__':
  unittest.main()
