# A comparator class holds a function and a plain text description of it

class Comparator():
  def __init__(self, function, description):
    self.f = function
    self.description = description

  def __call__(self, x):
    return self.f(x)

  def __str__(self):
    return self.description

  def __repr__(self):
    return self.description

def _is_numeric(x):
  import numbers
  return isinstance(x, numbers.Number)

def _assert_parameter_is_numeric(source, arg):
  if not _is_numeric(arg):
    raise ValueError('Comparator %s expects numerical parameters ("%s" is not numerical)' % (source, arg))

def between(boundaryA, boundaryB):
  _assert_parameter_is_numeric('between', boundaryA)
  _assert_parameter_is_numeric('between', boundaryB)
  if (boundaryA > boundaryB):
    boundaryA, boundaryB = boundaryB, boundaryA
  def comparator(x):
    return (x is not None) and _is_numeric(x) and (x >= boundaryA) and (x <= boundaryB)
  return Comparator(comparator, "between %.1f and %.1f" % (boundaryA, boundaryB))

def more_than(boundary):
  _assert_parameter_is_numeric('moreThan', boundary)
  def comparator(x):
    return (x is not None) and _is_numeric(x) and (x > boundary)
  return Comparator(comparator, "more than %d" % (boundary))

def less_than(boundary):
  _assert_parameter_is_numeric('lessThan', boundary)
  def comparator(x):
    return (x is not None) and _is_numeric(x) and (x < boundary)
  return Comparator(comparator, "less than %d" % (boundary))

def at_least(boundary):
  _assert_parameter_is_numeric('atLeast', boundary)
  def comparator(x):
    return (x is not None) and _is_numeric(x) and (x >= boundary)
  return Comparator(comparator, "at least %d" % (boundary))

def at_most(boundary):
  _assert_parameter_is_numeric('atMost', boundary)
  def comparator(x):
    return (x is not None) and _is_numeric(x) and (x <= boundary)
  return Comparator(comparator, "at most %d" % (boundary))
