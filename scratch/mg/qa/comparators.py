# A comparator class holds a function and a plain text description of it

class Comparator():
  def __init__(self, function, description):
    self.f = function
    self.description = description

  def eval(self,x):
    return self.f(x)

  def __str__(self):
    return self.description

  def __repr__(self):
    return self.description

def _assertNumericParameters(source, args):
  import numbers
  if any([not isinstance(r, numbers.Number) for r in args]):
    raise ValueError('%s test called with non-numerical parameters' % source)

def between(boundaryA, boundaryB):
  _assertNumericParameters('between', [boundaryA, boundaryB])
  if (boundaryA > boundaryB):
    boundaryA, boundaryB = boundaryB, boundaryA
  def comparator(x):
    return (x >= boundaryA) and (x <= boundaryB)
  return Comparator(comparator, "between %.1f and %.1f" % (boundaryA, boundaryB))

def moreThan(boundary):
  _assertNumericParameters('moreThan', [boundary])
  def comparator(x):
    return (x > boundary)
  return Comparator(comparator, "more than %d" % (boundary))

def lessThan(boundary):
  _assertNumericParameters('lessThan', [boundary])
  def comparator(x):
    return (x < boundary)
  return Comparator(comparator, "less than %d" % (boundary))

def atLeast(boundary):
  _assertNumericParameters('atLeast', [boundary])
  def comparator(x):
    return (x >= boundary)
  return Comparator(comparator, "at least %d" % (boundary))

def atMost(boundary):
  _assertNumericParameters('atMost', [boundary])
  def comparator(x):
    return (x <= boundary)
  return Comparator(comparator, "at most %d" % (boundary))
