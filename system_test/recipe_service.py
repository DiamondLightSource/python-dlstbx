from __future__ import absolute_import, division
from dlstbx.system_test.common import CommonSystemTest

class TestRecipeService(CommonSystemTest):

  def test_recipe_parsing(self):

    self.send_message(
        'transient.recipe_service',
        { 'recipe': 'etc' }
    )
    self.expect_message(
        queue='transient.system_test.${guid}',
        content={ 'asdf': 1 },
        timeout=300
    )

print "LOAD"
print TestRecipeService()
                   
