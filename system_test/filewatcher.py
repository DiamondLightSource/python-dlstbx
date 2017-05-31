from __future__ import absolute_import, division
from dlstbx.system_test.common import CommonSystemTest
import dlstbx.util
import mock
import os.path
from workflows.recipe import Recipe

tmpdir = dlstbx.util.dls_tmp_folder_date()

class FilewatcherService(CommonSystemTest):
  '''Tests for the filewatcher service.'''

  def create_temp_dir(self):
    '''Create directory for the test.'''
    os.makedirs(os.path.join(tmpdir, self.guid))

  def create_next_file(self):
    '''Create one more file for the test.'''
    self.filecount += 1
    open(self.filepattern % self.filecount, 'w').close()

  def test_success_notifications(self):
    '''Send a recipe to the filewatcher. Create 200 files and wait for the
       appropriate notification messages.'''

    self.create_temp_dir()
    self.filepattern = os.path.join(tmpdir, self.guid, 'tst_%05d.cbf')
    self.filecount = 0

    recipe = {
        1: { 'service': 'DLS Filewatcher',
             'queue': 'filewatcher',
             'parameters': { 'pattern': self.filepattern,
                             'pattern-start': 1,
                             'pattern-end': 200,
                             'burst-limit': 40,
                             'timeout': 120,
                             'timeout-first': 60,
                           },
             'output': { 'first': 2,     # First
                         'every': 3,     # Every
                         'last': 4,      # Last
                         'select-30': 5, # Select
                         '20': 6,        # Specific
                         'finally': 7,   # End-of-job
                         'timeout': 8    # Should not be triggered here
                       }
           },
        2: { 'queue': 'transient.system_test.' + self.guid + '.pass.2' },
        3: { 'queue': 'transient.system_test.' + self.guid + '.pass.3' },
        4: { 'queue': 'transient.system_test.' + self.guid + '.pass.4' },
        5: { 'queue': 'transient.system_test.' + self.guid + '.pass.5' },
        6: { 'queue': 'transient.system_test.' + self.guid + '.pass.6' },
        7: { 'queue': 'transient.system_test.' + self.guid + '.pass.7' },
        8: { 'queue': 'transient.system_test.' + self.guid + '.pass.8' },
        'start': [ (1, '') ]
      }
    recipe = Recipe(recipe)
    recipe.validate()

    self.send_message(
      queue='filewatcher',
      message={ 'recipe': recipe.recipe,
                'recipe-pointer': '1',
              },
      headers={ 'workflows-recipe': True }
    )

    # Create 100 files in 0-10 seconds
    for file_number in range(1, 101):
      self.timer_event(at_time=file_number / 10, callback=self.create_next_file)

    # Create 100 files in 60-70 seconds
    for file_number in range(101, 201):
      self.timer_event(at_time=50 + (file_number / 10), callback=self.create_next_file)

    # Now check for expected messages, marked in the recipe above:

    # First ============================

    self.expect_recipe_message(
      recipe=recipe,
      recipe_path=[ 1 ],
      recipe_pointer=2,
      payload={ 'file': self.filepattern % 1, 'file-number': 1, 'file-pattern-index': 1 },
      timeout=50,
    )

    # Every ============================

    for file_number in range(200):
      self.expect_recipe_message(
        recipe=recipe,
        recipe_path=[ 1 ],
        recipe_pointer=3,
        payload={ 'file': self.filepattern % (file_number + 1), 'file-number': file_number + 1, 'file-pattern-index': file_number + 1 },
        min_wait=max(0, file_number / 10) - 0.5,
        timeout=150,
      )

    # Last =============================

    self.expect_recipe_message(
      recipe=recipe,
      recipe_path=[ 1 ],
      recipe_pointer=4,
      payload={ 'file': self.filepattern % 200, 'file-number': 200, 'file-pattern-index': 200 },
      min_wait=65,
      timeout=150,
    )

    # Select ===========================

    for file_number in (1, 7, 14, 21, 28, 35, 42, 49, 56, 63, 69, 76, 83, 90, 97, 104, 111, 118, 125, 132, 138, 145, 152, 159, 166, 173, 180, 187, 194, 200):
      self.expect_recipe_message(
        recipe=recipe,
        recipe_path=[ 1 ],
        recipe_pointer=5,
        payload={ 'file': self.filepattern % file_number, 'file-number': file_number, 'file-pattern-index': file_number },
        timeout=150,
      )

    # Specific =========================

    self.expect_recipe_message(
      recipe=recipe,
      recipe_path=[ 1 ],
      recipe_pointer=6,
      payload={ 'file': self.filepattern % 20, 'file-number': 20, 'file-pattern-index': 20 },
      timeout=60,
    )

    # Finally ==========================

    self.expect_recipe_message(
      recipe=recipe,
      recipe_path=[ 1 ],
      recipe_pointer=7,
      payload={ 'files-expected': 200,
                'files-seen': 200,
                'success': True },
      min_wait=65,
      timeout=150,
    )

    # Timeout ==========================

    # No timeout message should be sent


  def test_failure_notification_immediate(self):
    '''Send a recipe to the filewatcher. Do not create any files and wait for
       the appropriate timeout notification messages.'''

    self.create_temp_dir()
    failpattern = os.path.join(tmpdir, self.guid, 'tst_fail_%05d.cbf')

    recipe = {
        1: { 'service': 'DLS Filewatcher',
             'queue': 'filewatcher',
             'parameters': { 'pattern': failpattern,
                             'pattern-start': 1,
                             'pattern-end': 200,
                             'burst-limit': 40,
                             'timeout': 10,
                             'timeout-first': 60,
                             'log-timeout-as-info': True,
                           },
             'output': { 'first': 2,     # Should not be triggered here
                         'every': 3,     # Should not be triggered here
                         'last': 4,      # Should not be triggered here
                         'select-30': 5, # Should not be triggered here
                         '20': 6,        # Should not be triggered here
                         'finally': 7,   # End-of-job
                         'timeout': 8    # Ran into a timeout condition
                       }
           },
        2: { 'queue': 'transient.system_test.' + self.guid + '.fail.2' },
        3: { 'queue': 'transient.system_test.' + self.guid + '.fail.3' },
        4: { 'queue': 'transient.system_test.' + self.guid + '.fail.4' },
        5: { 'queue': 'transient.system_test.' + self.guid + '.fail.5' },
        6: { 'queue': 'transient.system_test.' + self.guid + '.fail.6' },
        7: { 'queue': 'transient.system_test.' + self.guid + '.fail.7' },
        8: { 'queue': 'transient.system_test.' + self.guid + '.fail.8' },
        'start': [ (1, '') ]
      }
    recipe = Recipe(recipe)
    recipe.validate()

    self.send_message(
      queue='filewatcher',
      message={ 'recipe': recipe.recipe,
                'recipe-pointer': '1',
              },
      headers={ 'workflows-recipe': True }
    )

    # Check for expected messages, marked in the recipe above:

    # First ============================
    # Every ============================
    # Last =============================
    # Select ===========================
    # Specific =========================

    # No messages should be sent

    # Finally ==========================

    self.expect_message(
      queue='transient.system_test.' + self.guid + '.fail.7',
      message={ 'payload': { 'files-expected': 200,
                             'files-seen': 0,
                             'success': False },
                'recipe': recipe,
                'recipe-path': [ 1 ],
                'recipe-pointer': 7,
                'environment': mock.ANY,
              },
      headers={ 'workflows-recipe': 'True' },
      min_wait=55,
      timeout=80,
    )


    # Timeout ==========================

    self.expect_message(
      queue='transient.system_test.' + self.guid + '.fail.8',
      message={ 'payload': { 'file': failpattern % 1, 'file-number': 1, 'file-pattern-index': 1,
                             'success': False },
                'recipe': recipe,
                'recipe-path': [ 1 ],
                'recipe-pointer': 8,
                'environment': mock.ANY,
              },
      headers={ 'workflows-recipe': 'True' },
      min_wait=55,
      timeout=80,
    )

  def create_delayed_failure_file(self):
    '''Create one file for the test.'''
    open(self.delayed_fail_file, 'w').close()

  def test_failure_notification_delayed(self):
    '''Send a recipe to the filewatcher. Creates a single file and waits for
       the appropriate initial success and subsequent timeout notification messages.'''

    self.create_temp_dir()
    semifailpattern = os.path.join(tmpdir, self.guid, 'tst_semi_%05d.cbf')
    self.delayed_fail_file = semifailpattern % 5

    recipe = {
        1: { 'service': 'DLS Filewatcher',
             'queue': 'filewatcher',
             'parameters': { 'pattern': semifailpattern,
                             'pattern-start': 5,
                             'pattern-end': 204,
                             'burst-limit': 40,
                             'timeout': 10,
                             'timeout-first': 60,
                             'log-timeout-as-info': True,
                           },
             'output': { 'first': 2,     # First
                         'every': 3,     # Every
                         'last': 4,      # Should not be triggered here
                         'select-30': 5, # Should not be triggered here
                         '20': 6,        # Should not be triggered here
                         'finally': 7,   # End-of-job
                         'timeout': 8    # Ran into a timeout condition
                       }
           },
        2: { 'queue': 'transient.system_test.' + self.guid + '.semi.2' },
        3: { 'queue': 'transient.system_test.' + self.guid + '.semi.3' },
        4: { 'queue': 'transient.system_test.' + self.guid + '.semi.4' },
        5: { 'queue': 'transient.system_test.' + self.guid + '.semi.5' },
        6: { 'queue': 'transient.system_test.' + self.guid + '.semi.6' },
        7: { 'queue': 'transient.system_test.' + self.guid + '.semi.7' },
        8: { 'queue': 'transient.system_test.' + self.guid + '.semi.8' },
        'start': [ (1, '') ]
      }
    recipe = Recipe(recipe)
    recipe.validate()

    self.send_message(
      queue='filewatcher',
      message={ 'recipe': recipe.recipe,
                'recipe-pointer': '1',
              },
      headers={ 'workflows-recipe': True }
    )

    # Create first file after 30 seconds
    self.timer_event(at_time=30, callback=self.create_delayed_failure_file)

    # Check for expected messages, marked in the recipe above:

    # First ============================

    self.expect_message(
      queue='transient.system_test.' + self.guid + '.semi.2',
      message={ 'payload': { 'file': self.delayed_fail_file, 'file-number': 1, 'file-pattern-index': 5 },
                'recipe': recipe,
                'recipe-path': [ 1 ],
                'recipe-pointer': 2,
                'environment': mock.ANY,
              },
      headers={ 'workflows-recipe': 'True' },
      min_wait=25,
      timeout=50,
    )

    # Every ============================

    self.expect_message(
      queue='transient.system_test.' + self.guid + '.semi.3',
      message={ 'payload': { 'file': self.delayed_fail_file, 'file-number': 1, 'file-pattern-index': 5 },
                'recipe': recipe,
                'recipe-path': [ 1 ],
                'recipe-pointer': 3,
                'environment': mock.ANY,
              },
      headers={ 'workflows-recipe': 'True' },
      min_wait=25,
      timeout=50,
    )

    # Last =============================
    # Select ===========================
    # Specific =========================

    # No messages should be sent

    # Finally ==========================

    self.expect_message(
      queue='transient.system_test.' + self.guid + '.semi.7',
      message={ 'payload': { 'files-expected': 200,
                             'files-seen': 1,
                             'success': False },
                'recipe': recipe,
                'recipe-path': [ 1 ],
                'recipe-pointer': 7,
                'environment': mock.ANY,
              },
      headers={ 'workflows-recipe': 'True' },
      min_wait=25,
      timeout=55,
    )

    # Timeout ==========================

    self.expect_message(
      queue='transient.system_test.' + self.guid + '.semi.8',
      message={ 'payload': { 'file': semifailpattern % 6, 'file-number': 2, 'file-pattern-index': 6,
                             'success': False },
                'recipe': recipe,
                'recipe-path': [ 1 ],
                'recipe-pointer': 8,
                'environment': mock.ANY,
              },
      headers={ 'workflows-recipe': 'True' },
      min_wait=25,
      timeout=55,
    )

if __name__ == "__main__":
  FilewatcherService().validate()
