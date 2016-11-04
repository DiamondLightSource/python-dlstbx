from __future__ import absolute_import, division
from dlstbx.system_test.common import CommonSystemTest
from workflows.recipe import Recipe

class FilewatcherService(CommonSystemTest):
  '''Tests for the filewatcher service.'''

  def test_different_types_of_notifications(self):
    '''Passing in a recipe to the service without external dependencies.
       The recipe should be interpreted, the 'guid' placeholder replaced using
       the parameter field, and the message passed back.
       The message should then contain the recipe and a correctly set pointer.'''

    recipe = {
        1: { 'service': 'DLS Filewatcher',
             'queue': 'filewatcher',
             'parameters': { 'pattern': '/dls/tmp/{guid}/tst_%05d.cbf',
                             'pattern-start': 1,
                             'pattern-end': 200,
                             'burst-limit': 40,
                             'timeout': 10,
                             'timeout-first': 60,
                           },
             'output': { 'first': 2,
                         'every': 3,
                         'last': 4,
                         'select-30': 5,
                         20: 6,
                       }
           },
        2: { 'queue': 'transient.system_test.{guid}.2' },
        3: { 'queue': 'transient.system_test.{guid}.3' },
        4: { 'queue': 'transient.system_test.{guid}.4' },
        5: { 'queue': 'transient.system_test.{guid}.5' },
        6: { 'queue': 'transient.system_test.{guid}.6' },
        'start': [ (1, '') ]
      }
    parameters = { 'guid': self.guid }

#    self.send_message(
#      queue='processing_recipe',
#      message={
#        'parameters': parameters,
#        'custom_recipe': recipe,
#      }
#    )

    recipe = Recipe(recipe)
    recipe.validate()
    recipe.apply_parameters(parameters)
#    self.expect_message(
#      queue='transient.system_test.' + self.guid,
#      message=recipe['start'][0][1],
#      headers={ 'recipe': expected_recipe,
#                'recipe-pointer': '1',
#              },
#      timeout=3,
#    )


if __name__ == "__main__":
  FilewatcherService().validate()
