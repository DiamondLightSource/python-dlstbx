from __future__ import division, absolute_import

class BaseWrapper(object):
 def set_recipe_wrapper(self, recwrap):
   self.recwrap = recwrap

 def prepare(self, payload=''):
   if getattr(self, 'recwrap', None):
     self.recwrap.send_to('starting', payload)

 def update(self, payload=''):
   if getattr(self, 'recwrap', None):
     self.recwrap.send_to('updates', payload)

 def done(self, payload=''):
   if getattr(self, 'recwrap', None):
     self.recwrap.send_to('completed', payload)

 def success(self, payload=''):
   if getattr(self, 'recwrap', None):
     self.recwrap.send_to('success', payload)

 def failure(self, payload=''):
   if getattr(self, 'recwrap', None):
     self.recwrap.send_to('failure', str(payload))

 def run(self):
   raise NotImplementedError()
