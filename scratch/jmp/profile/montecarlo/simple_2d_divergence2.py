



if __name__ == '__main__':

  from dials.array_family import flex
  from math import pi, exp, sqrt, cos, atan2, sin

  mt = -pi
  k = 100
  r = 1.0

  width, height = 1000, 1000
  f = flex.double(flex.grid(height, width))

  for j in range(height):
    for i in range(width):
      x = -1.0 + 2.0*i/width
      y = -1.0 + 2.0*j/height
      fx = -r*sin(t)*exp(k*cos(t - mt)) / (2*pi*iv(0,k))
      fy = r*cos(t)*exp(k*cos(t - mt)) / (2*pi*iv(0,k))
      f[j,i] = fx*fy
      # phi = 2*pi*j/height
      # theta =2*pi*i/width
      # x = (cos(phi) + cos(theta))
      # y = (sin(phi) + sin(theta))
      # f[j,i] = x

  # for j in range(height):
  #   for i in range(width):
  #     x = -1.0 + 2.0*i/width
  #     y = -1.0 + 2.0*j/height

  # r = 0.1
  # t1 = -(pi/2+pi/4)
  # t2 =  (pi/2+pi/4)

  from matplotlib import pylab
  # circle1 = pylab.Circle((r*cos(t1),r*sin(t1)),r)
  # circle2 = pylab.Circle((r*cos(t2),r*sin(t2)),r)
  # fig = pylab.gcf()
  # fig.gca().add_artist(circle1)
  # fig.gca().add_artist(circle2)
  # pylab.xlim(-2,2)
  # pylab.ylim(-2,2)
  # pylab.show()
  pylab.imshow(f.as_numpy_array())
  pylab.show()
