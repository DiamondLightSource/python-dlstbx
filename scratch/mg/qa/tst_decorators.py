import unittest
import decorators

class DecoratorTests(unittest.TestCase):

  def test_function_discovery_through_decorators(self):
    decorators.getDiscoveredTestFunctions()
    decorators.getDiscoveredDataFunctions()

    @decorators.Test
    def function_test_one():
      pass
    @decorators.Data
    def function_data_one():
      pass
    @decorators.Test
    def function_test_two():
      pass

    test_function_names = [ x[0] for x in decorators.getDiscoveredTestFunctions() ]
    self.assertEqual(test_function_names, ['function_test_one', 'function_test_two'] )
    data_function_names = [ x[0] for x in decorators.getDiscoveredDataFunctions() ]
    self.assertEqual(data_function_names, ['function_data_one'] )

    self.assertEqual(decorators.getDiscoveredTestFunctions(), [])
    self.assertEqual(decorators.getDiscoveredDataFunctions(), [])


  def test_argument_passing_through_decorators(self):
    self.parameters = ()

    @decorators.Test
    @decorators.Data
    def function_store_arguments(*args, **kwargs):
      self.parameters = (args, kwargs)

    self.assertEqual(self.parameters, ())
    function_store_arguments(21, 42, someargument = 1)
    self.assertEqual(self.parameters, ((21, 42), {'someargument': 1}))


  def test_function_disabling_through_decorators(self):
    self.called = False
    decorators.disabledCalls()

    @decorators.Test
    def set_call_flag():
      self.called = True

    @decorators.Data
    def reset_call_flag():
      self.called = False

    self.assertEqual(self.called, False)
    set_call_flag()
    self.assertEqual(self.called, True)
    reset_call_flag()
    self.assertEqual(self.called, False)

    decorators.disableDecoratorFunctions()
    set_call_flag()
    self.assertEqual(self.called, False)

    decorators.enableDecoratorFunctions()
    set_call_flag()
    self.assertEqual(self.called, True)

    decorators.disableDecoratorFunctions()
    reset_call_flag()
    self.assertEqual(self.called, True)

    decorators.enableDecoratorFunctions()
    reset_call_flag()
    self.assertEqual(self.called, False)

    self.assertEqual(decorators.disabledCalls(), ['set_call_flag', 'reset_call_flag'])
    self.assertEqual(decorators.disabledCalls(), [])

if __name__ == '__main__':
  unittest.main()
