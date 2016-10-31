from __future__ import absolute_import, division
from dlstbx.system_test.common import CommonSystemTest
from workflows.recipe import Recipe

class RecipeService(CommonSystemTest):
  '''Tests for the recipe service.'''

  def test_recipe_parsing(self):
    '''Passing in a recipe to the service without external dependencies.
       The recipe should be interpreted, the 'guid' placeholder replaced using
       the parameter field, and the message passed back.
       The message should then contain the recipe and a correctly set pointer.'''

    recipe = {
        1: { 'service': 'DLS system test',
             'queue': 'transient.system_test.{guid}'
           },
        'start': [
           (1, { 'purpose': 'test the recipe parsing service' }),
        ]
      }

    self.send_message(
      queue='processing_recipe',
      message={
        'parameters': { 'guid': self.guid },
        'selected_recipes': [],
        'custom_recipe': recipe,
      }
    )

    self.expect_message(
      queue='transient.system_test.' + self.guid,
      message=recipe['start'][0][1],
      headers={ 'recipe': Recipe(recipe),
                'recipe-pointer': '1',
              },
      timeout=3,
    )

if __name__ == "__main__":
  RecipeService().validate()
