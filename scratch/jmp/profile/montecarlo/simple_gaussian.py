

from random import normalvariate
from math import sqrt

s1 = 3
s2 = 6
xx = []

for i in range(100000):

  m = normalvariate(0, s1)
  x = normalvariate(m, s2)
  xx.append(x)


# from matplotlib import pylab
# pylab.hist(xx, bins=100)
# pylab.show()

sum1 = sum(xx)
mean = sum1 / len(xx)
sum2 = sum([(x - mean)**2 for x in xx])
sdev = sqrt(sum2 / len(xx))
print mean, sdev, sqrt(s1*s1+s2*s2)

s1 = 3
s2 = 6
xx = []

for i in range(100000):

  x1 = normalvariate(0, s1)
  x2 = normalvariate(0, s2)
  xx.append(x1+x2)


from matplotlib import pylab
pylab.hist(xx, bins=100)
pylab.show()

sum1 = sum(xx)
mean = sum1 / len(xx)
sum2 = sum([(x - mean)**2 for x in xx])
sdev = sqrt(sum2 / len(xx))
print mean, sdev, sqrt(s1*s1+s2*s2)
