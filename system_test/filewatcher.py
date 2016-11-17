from __future__ import absolute_import, division
from dlstbx.system_test.common import CommonSystemTest
import dlstbx.util
import mock
import os.path
from workflows.recipe import Recipe

tmpdir = dlstbx.util.dls_tmp_folder()

class FilewatcherService(CommonSystemTest):
  '''Tests for the filewatcher service.'''

  def create_next_file(self):
    '''Create one more file for the test.'''
    self.filecount += 1
    open(self.filepattern % self.filecount, 'w').close()

  def test_success_notifications(self):
    '''Send a recipe to the filewatcher. Create 200 files and wait for the
       appropriate notification messages.'''

    os.makedirs(os.path.join(tmpdir, self.guid))
    self.filepattern = os.path.join(tmpdir, self.guid, 'tst_%05d.cbf')
    self.filecount = 0

    recipe = {
        1: { 'service': 'DLS Filewatcher',
             'queue': 'filewatcher',
             'parameters': { 'pattern': self.filepattern,
                             'pattern-start': 1,
                             'pattern-end': 200,
                             'burst-limit': 40,
                             'timeout': 10,
                             'timeout-first': 60,
                           },
             'output': { 'first': 2,     # First
                         'every': 3,     # Every
                         'last': 4,      # Last
                         'select-30': 5, # Select
                         20: 6,          # Specific
                       }
           },
        2: { 'queue': 'transient.system_test.' + self.guid + '.2' },
        3: { 'queue': 'transient.system_test.' + self.guid + '.3' },
        4: { 'queue': 'transient.system_test.' + self.guid + '.4' },
        5: { 'queue': 'transient.system_test.' + self.guid + '.5' },
        6: { 'queue': 'transient.system_test.' + self.guid + '.6' },
        'start': [ (1, '') ]
      }
    recipe = Recipe(recipe)
    recipe.validate()
    recipe = recipe.serialize()

    self.send_message(
      queue='filewatcher',
      message='',
      headers={ 'recipe': recipe,
                'recipe-pointer': '1',
              }
    )

    # Create 200 files in 5 seconds
    for file_number in range(200):
      self.timer_event(at_time=(file_number + 1) / 40, callback=self.create_next_file)

    # Now check for expected messages, marked in the recipe above:

    # First ============================

    self.expect_message(
      queue='transient.system_test.' + self.guid + '.2',
      message=self.filepattern % 1,
      headers={ 'recipe': recipe,
                'recipe-pointer': '2',
              },
      timeout=10,
    )

    # Every ============================

    for file_number in range(200):
      self.expect_message(
        queue='transient.system_test.' + self.guid + '.3',
        message=self.filepattern % (file_number + 1),
        headers={ 'recipe': recipe,
                  'recipe-pointer': '3',
                },
        min_wait=max(0, file_number / 50) - 0.5,
        timeout=15,
      )

    # Last =============================

    self.expect_message(
      queue='transient.system_test.' + self.guid + '.4',
      message=self.filepattern % 200,
      headers={ 'recipe': recipe,
                'recipe-pointer': '4',
              },
      min_wait=4,
      timeout=15,
    )

    # Select ===========================

    for file_number in (1, 7, 14, 21, 28, 35, 42, 49, 56, 63, 69, 76, 83, 90, 97, 104, 111, 118, 125, 132, 138, 145, 152, 159, 166, 173, 180, 187, 194, 200):
      self.expect_message(
        queue='transient.system_test.' + self.guid + '.5',
        message=self.filepattern % file_number,
        headers={ 'recipe': recipe,
                  'recipe-pointer': '5',
                },
        timeout=15,
      )

    # Specific =========================

    self.expect_message(
      queue='transient.system_test.' + self.guid + '.6',
      message=self.filepattern % 20,
      headers={ 'recipe': recipe,
                'recipe-pointer': '6',
              },
      timeout=15,
    )


if __name__ == "__main__":
  FilewatcherService().validate()
