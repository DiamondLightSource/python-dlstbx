from __future__ import absolute_import, division

#
# Simple test to do a montecarlo integration of a 2d gaussian moving through a
# circle.
#

def pdf(a, b, sigma, phi, theta):
  from math import pi, exp, cos, sin

  C1 = 1.0 / (2*pi*sigma**2)
  C2 = (2*a**2+b**2) / (2.0*sigma**2)
  C3 = (a*a*cos(phi) + a*b*sin(phi)*sin(theta)) / (sigma**2)#(cos(phi)*cos(theta)+sin(phi)*sin(theta)) /(sigma**2)
  # print -(C2-C3)
  C4 = exp(-(C2-C3))
  return C1*C4

def pdf2(a, b, sigma, phi0, phi, theta):
  from math import pi, exp, cos, sin
  # from scipy.special import iv
  # D1 = a*a*cos(phi0) + a*b*cos(theta)
  # D2 = a*b*sin(phi0) + a*b*sin(theta)
  # D3 = 2*pi*iv(0, sqrt(D1*D1+D2*D2))
  # D4 = 1.0 / (2*pi*sigma**2)
  # D5 = exp(-(2*a*a+b*b+2*a*b*cos(theta-phi0))/(2*sigma**2))
  # D6 = D4*D5
  # D7 = D3 * D6

  C1 = 1.0 / (2*pi*sigma**2)
  C2 = 2.0*a**2 + b**2
  C3 = 2*a*b*cos(theta - phi0)
  C4 = 2*a*a*cos(phi - phi0)
  C5 = 2*a*b*cos(phi - theta)
  C6 = -1.0 / (2*sigma**2)
  C8 = C1*exp(C6*(C2+C3-C4-C5))
  return C8

def pdf3(a, b, sigma, phi0, phi, theta):
  from math import pi, exp, cos, sin
  from scipy.special import iv
  D1 = (a*a*cos(phi0) + a*b*cos(theta)) / (sigma**2)
  D2 = (a*a*sin(phi0) + a*b*sin(theta)) / (sigma**2)
  D3 = 2*pi*iv(0, sqrt(D1*D1+D2*D2))
  D4 = 1.0 / (2*pi*sigma**2)
  D5 = exp(-(2*a*a+b*b+2*a*b*cos(theta-phi0))/(2*sigma**2))
  D6 = D4*D5
  # D7 = D3 * D6
  return D6* exp(D1*cos(phi) + D2*sin(phi))

if __name__ == '__main__':

  from dials.array_family import flex
  from matplotlib import pylab
  from math import pi, cos, atan2, sqrt, exp

  A  = 1.0                     # The radius of the ewald sphere
  B  = 0.5           # The radius out to the lattice point
  T0 = pi/2                    # The initial angle of the lattice point
  S  = 0.05                     # The standard deviation of the gaussian
  XC = A                      # The X offset of the ewald sphere centre
  YC = 0                       # The Y offset of the ewald sphere centre
  TC = atan2(YC, XC)           # The angle
  RC = sqrt(XC*XC + YC*YC)     # The radius

  width, height = 1000, 1000
  z = flex.double(flex.grid(height, width))
  maxz = 0
  maxphi = 0
  maxtheta = 0
  for j in range(height):
    for i in range(width):
      phi = i*2*pi / width
      theta = j*2*pi/height
      z[j,i] = pdf3(A,B,S,0,phi,theta)
      if z[j,i] > maxz:
        maxz = z[j,i]
        maxphi = phi
        maxtheta= theta

  print "Max Phi: ", maxphi * 180.0 / pi
  print "Max Theta: ", maxtheta * 180.0 / pi
  print "Total: ", flex.sum(z)


  # z = flex.double(flex.grid(100, 100))

  # for j in range(100):
  #   for i in range(100):
  #     x = -2.0 + 4.0 *i/100.0
  #     y = -2.0 + 4.0 *j/100.0
  #     r = sqrt(x*x + y*y)
  #     t = atan2(y, x)
  #     c = 1.0 / (2*pi*S*S)
  #     z[j,i] = c*exp(-(1.0/(2*S*S))*(r*r + B*B +2*r*B*cos(T0-t)))
  #     z[j,i] += c*exp(-(1.0/(2*S*S))*(r*r + B*B -2*r*B*cos(-t)))
      # z[j,i] = abs(r - 2*RC*cos(t - TC))

  pylab.imshow(z.as_numpy_array(), origin='bottom')
  pylab.show()
