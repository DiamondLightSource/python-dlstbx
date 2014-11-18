/*
 * model.h
 *
 *  Copyright (C) 2013 Diamond Light Source
 *
 *  Author: James Parkhurst
 *
 *  This code is distributed under the BSD license, a copy of which is
 *  included in the root directory of this package.
 */

#ifndef DLSTBX_ALGORITHMS_PROFILE_MODEL_NAVE_MODEL_H
#define DLSTBX_ALGORITHMS_PROFILE_MODEL_NAVE_MODEL_H

#include <dlstbx/algorithms/profile_model/nave/spherical_cap.h>
#include <dials/error.h>

namespace dlstbx {
namespace algorithms {
namespace profile_model {
namespace nave {

  template <typename T>
  T min3(T a, T b, T c) {
    return std::min(std::min(a, b), c);
  }

  template <typename T>
  T min6(T a, T b, T c, T d, T e, T f) {
    return std::min(min3(a, b, c), min3(d, e, f));
  }

  template <typename T>
  T max3(T a, T b, T c) {
    return std::max(std::max(a, b), c);
  }

  template <typename T>
  T max6(T a, T b, T c, T d, T e, T f) {
    return std::max(max3(a, b, c), max3(d, e, f));
  }

  /**
   * A helper class to compute the elliptical parameters of the projections of
   * the circle of intersection between the ewald sphere and sphere of rotation
   * projected onto a plane.
   */
  class EwaldCirclePlaneProjection {
  public:

    /**
     * Initialise the projection
     * @param s0 The beam vector
     * @param d The detector d matrix
     */
    EwaldCirclePlaneProjection(vec3<double> s0, mat3<double> d)
      : s0_(s0),
        d0_(d[0], d[3], d[6]),
        d1_(d[1], d[4], d[7]),
        d2_(d[2], d[5], d[8]),
        d0s0_(d0_ * s0_),
        d1s0_(d1_ * s0_),
        d2s0_(d2_ * s0_),
        d0d2_(d0_ * d2_),
        d1d2_(d1_ * d2_),
        d2d2_(d2_.length_sq()) {

    }

    /**
     * Do the projection and return the elliptical parameters such that for x
     * and y on the virtual detector plane, the ellipse is given as
     *
     * AX^2 + BXY + CY^2 + DX + EY + F = 0
     *
     * @param rl The length of the reciprocal lattice vector
     * @returns The elliptical parameters
     */
    af::small<double, 6> operator()(double rl) const {
      double K = s0_.length_sq() - rl*rl / 2.0;
      double KK = K * K;
      af::small<double, 6> result(6);
      result[0] = d0s0_*d0s0_ - KK;
      result[1] = d0s0_*d1s0_*2.0;
      result[2] = d1s0_*d1s0_ - KK;
      result[3] = d0s0_*d2s0_*2.0 - d0d2_*KK*2.0;
      result[4] = d1s0_*d2s0_*2.0 - d1d2_*KK*2.0;
      result[5] = d2s0_*d2s0_ - KK*d2d2_;
      return result;
    }

  private:

    vec3<double> s0_;
    vec3<double> d0_;
    vec3<double> d1_;
    vec3<double> d2_;
    double d0s0_;
    double d1s0_;
    double d2s0_;
    double d0d2_;
    double d1d2_;
    double d2d2_;
  };


  /**
   * A class to represent the model in reciprocal space.
   */
  class Model {
  public:

    /**
     * @param s0 The incident beam vector
     * @param m2 The rotation axis
     * @param s1 The diffracted beam vector
     * @param phi The rotation angle
     * @param s The mosaic block size
     * @param da The spread of unit cell sizes da/a
     * @param w The angular spread of mosaic blocks
     */
    Model(vec3<double> s0,
          vec3<double> m2,
          vec3<double> s1,
          double phi,
          double s,
          double da,
          double w)
      : s0_(s0),
        m2_(m2.normalize()),
        s1_(s1.normalize()*s0.length()),
        e1_(s1_.cross(s0_).normalize()),
        e2_(s1_.cross(e1_).normalize()),
        e3_((s1_ + s0_).normalize()),
        zeta_(m2_ * e1_),
        phi_(phi),
        s_(s),
        da_(da),
        w_(w),
        cap_(s1_ - s0_, w_) {
      DIALS_ASSERT(s0_.length() > 0);
      DIALS_ASSERT(s1_.length() > 0);
      DIALS_ASSERT(s > 0);
      DIALS_ASSERT(da >= 0);
      DIALS_ASSERT(w >= 0);
      DIALS_ASSERT(w <= pi);
      thickness_ = 1.0 / s + r().length() * da;
      rocking_width_ =
        w +
        2.0 * std::atan2(0.5 / s, r().length()) +
        2.0 * std::atan2(0.5 * da, 1.0);
    }

    /**
     * @returns The incident beam vector
     */
    vec3<double> s0() const {
      return s0_;
    }

    /**
     * @returns The rotation axis
     */
    vec3<double> m2() const {
      return m2_;
    }

    /**
     * @returns The e1 axis
     */
    vec3<double> e1() const {
      return e1_;
    }

    /**
     * @returns The e2 axis
     */
    vec3<double> e2() const {
      return e2_;
    }

    /**
     * @returns The e3 axis
     */
    vec3<double> e3() const {
      return e3_;
    }

    /**
     * @returns zeta
     */
    double zeta() const {
      return zeta_;
    }

    /**
     * @returns The diffracted beam vector
     */
    vec3<double> s1() const {
      return s1_;
    }

    /**
     * @returns The reciprocal lattice vector
     */
    vec3<double> r() const {
      return cap_.axis();
    }

    /**
     * @returns The rotation angle of the centre
     */
    double phi() const {
      return phi_;
    }

    /**
     * @returns The mosaic block size
     */
    double s() const {
      return s_;
    }

    /**
     * @returns The spread of unit cell sizes
     */
    double da() const {
      return da_;
    }

    /**
     * @returns The angular spread of mosaic blocks
     */
    double w() const {
      return w_;
    }

    /**
     * @returns The thickness of the profile
     */
    double thickness() const {
      return thickness_;
    }

    /**
     * @returns The rocking width
     */
    double rocking_width() const {
      return rocking_width_;
    }

    /**
     * @returns The distance of the point from the profile.
     */
    double distance(vec3<double> r) const {
      return cap_.distance(r);
    }

    /**
     * @returns Is the point inside the profile bounds
     */
    bool inside(vec3<double> r) const {
      return distance(r) < thickness();
    }

    /**
     * @returns The first phi angle
     */
    vec2<double> phi_range() const {
      vec3<double> p = r();
      double pl2 = p.length_sq();
      double m2p = m2_ * p;
      double m2p2 = m2p*m2p;
      double a = m2p2 - pl2;
      DIALS_ASSERT(a != 0);
      DIALS_ASSERT(rocking_width_ > 0);
      double b = m2p2 - pl2 * std::cos(rocking_width_ * 0.5);
      double cosdphi = b / a;
      if (cosdphi >  1) cosdphi =  1.0;
      if (cosdphi < -1) cosdphi = -1.0;
      double dphi = std::acos(cosdphi);
      return vec2<double>(phi_ - dphi, phi_ + dphi);
    }

    /**
     * @returns The shortest distance along the axis
     */
    double z0() const {
      return cap_.h2() - thickness();
    }

    /**
     * @returns The longest distance along the axis
     */
    double z1() const {
      return cap_.radius() + thickness();
    }

    /**
     * @returns fraction of expected intensity between two rotation angles
     */
    double intensity_fraction(double phia, double phib) const {
      DIALS_ASSERT(rocking_width_ > 0);
      if (phia > phib) {
        std::swap(phia, phib);
      }
      double c = std::abs(zeta_) / (std::sqrt(2.0) * (0.5 * rocking_width_ / 3.0));
      double p = 0.5 * (erf(c * (phib - phi_)) - erf(c * (phia - phi_)));
      DIALS_ASSERT(p >= 0.0 && p <= 1.0);
      return p;
    }

    /**
     * @returns The angles defining the ewald intersection range
     */
    vec2<double> ewald_intersection_angles() const {
      double rl = r().length();
      vec2<double> angles(
          ewald_intersection_angle(rl - thickness() / 2.0),
          ewald_intersection_angle(rl + thickness() / 2.0));
      if (angles[0] > angles[1]) {
        std::swap(angles[0], angles[1]);
      }
      return angles;
    }

    /**
     * @returns The minimum box as 8 s vectors
     */
    af::small< vec3<double>, 8> minimum_box() const {

      const double EPS = 1e-7;

      // The offset along the s0 axis of the two circles
      double rl = r().length();
      double rl_min = rl - thickness() / 2.0;
      double rl_max = rl + thickness() / 2.0;
      double a1_min = s0_.length_sq() - rl_min*rl_min / 2.0;
      double a1_max = s0_.length_sq() - rl_max*rl_max / 2.0;
      double a1_mid = s0_.length_sq() - rl * rl / 2.0;

      // The three axes
      vec3<double> zp = s0_.normalize();
      vec3<double> yp = s0_.cross(s1_).normalize();
      vec3<double> xp = yp.cross(zp);

      // The radius and inclination
      double r = s0_.length();
      double theta1 = std::acos(a1_min / s0_.length_sq());
      double theta2 = std::acos(a1_max / s0_.length_sq());
      double theta3 = std::acos(a1_mid / s0_.length_sq());
      DIALS_ASSERT(theta1 <= s0_.angle(s1_));
      DIALS_ASSERT(theta2 >= s0_.angle(s1_));
      DIALS_ASSERT(std::abs(theta3 - s0_.angle(s1_)) < EPS);

      // The azimuth angles
      double phi1 = 0;
      double phi2 = rocking_width() / 2.0;
      double phi3 = -rocking_width() / 2.0;

      // Compute the cartesian coordinates at the different extrema of r_min
      vec3<double> v1(
        r*std::sin(theta1)*std::cos(phi1),
        r*std::sin(theta1)*std::sin(phi1),
        r*std::cos(theta1));
      vec3<double> v2(
        r*std::sin(theta1)*std::cos(phi2),
        r*std::sin(theta1)*std::sin(phi2),
        r*std::cos(theta1));
      vec3<double> v3(
        r*std::sin(theta1)*std::cos(phi3),
        r*std::sin(theta1)*std::sin(phi3),
        r*std::cos(theta1));

      // Compute the cartesian coordinates at the different extrema of r_max
      vec3<double> v4(
        r*std::sin(theta2)*std::cos(phi1),
        r*std::sin(theta2)*std::sin(phi1),
        r*std::cos(theta2));
      vec3<double> v5(
        r*std::sin(theta2)*std::cos(phi2),
        r*std::sin(theta2)*std::sin(phi2),
        r*std::cos(theta2));
      vec3<double> v6(
        r*std::sin(theta2)*std::cos(phi3),
        r*std::sin(theta2)*std::sin(phi3),
        r*std::cos(theta2));

      // Compute cartesian coordinates
      vec3<double> w1 = v1[0]*xp + v1[1]*yp + v1[2]*zp;
      vec3<double> w2 = v2[0]*xp + v2[1]*yp + v2[2]*zp;
      vec3<double> w3 = v3[0]*xp + v3[1]*yp + v3[2]*zp;
      vec3<double> w4 = v4[0]*xp + v4[1]*yp + v4[2]*zp;
      vec3<double> w5 = v5[0]*xp + v5[1]*yp + v5[2]*zp;
      vec3<double> w6 = v6[0]*xp + v6[1]*yp + v6[2]*zp;

      // Get the min/max x, y, z
      double minx = min6(w1[0], w2[0], w3[0], w4[0], w5[0], w6[0]);
      double maxx = max6(w1[0], w2[0], w3[0], w4[0], w5[0], w6[0]);
      double miny = min6(w1[1], w2[1], w3[1], w4[1], w5[1], w6[1]);
      double maxy = max6(w1[1], w2[1], w3[1], w4[1], w5[1], w6[1]);
      double minz = min6(w1[2], w2[2], w3[2], w4[2], w5[2], w6[2]);
      double maxz = max6(w1[2], w2[2], w3[2], w4[2], w5[2], w6[2]);

      // Return the vectors
      af::small< vec3<double>, 8> result(8);
      result[0] = vec3<double>(minx, miny, minz);
      result[1] = vec3<double>(minx, miny, maxz);
      result[2] = vec3<double>(minx, maxy, minz);
      result[3] = vec3<double>(minx, maxy, maxz);
      result[4] = vec3<double>(maxx, miny, minz);
      result[5] = vec3<double>(maxx, miny, maxz);
      result[6] = vec3<double>(maxx, maxy, minz);
      result[7] = vec3<double>(maxx, maxy, maxz);
      return result;
    }

    /**
     * Check if the vector is within the profile
     */
    bool inside2(vec3<double> s, double angle0, double angle1) const {

      vec3<double> rr = s.normalize() * s0_.length() - s0_;
      vec3<double> rr1 = rr.unit_rotate_around_origin(m2_, angle0 - phi_);
      vec3<double> rr2 = rr.unit_rotate_around_origin(m2_, angle1 - phi_);
      vec3<double> rr3 = rr.unit_rotate_around_origin(m2_, (angle0 + angle1) / 2.0 - phi_);
      bool in = inside(rr1) || inside(rr2) || inside(rr3);
      return in;
    }

    af::small<double, 6> equation(mat3<double> d, double phi) const {

      // Get the plane vectors
      vec3<double> dx(d[0], d[3], d[6]);
      vec3<double> dy(d[1], d[4], d[7]);
      vec3<double> dz(d[2], d[5], d[8]);

      // Compute the rotated vector
      /* vec3<double> p = r().unit_rotate_around_origin(m2_, phi - phi_); */
      vec3<double> p = r();

      // The radius
      double t2 = thickness() * thickness() / 4.0;
      double sl2 = s0_.length_sq();
      double pl2 = p.length_sq();
      double s0p = s0_ * p;

      // Compute the constant
      /* double GR = sl2 + s0p + (pl2 - t2) / 2.0; */
      double GR = (sl2 + s0p + (pl2 - t2) / 2.0);
      double G = GR * GR / sl2;

      // Compute the stereographic constants
      double C1 = dx * dx;
      double C2 = dx * dy * 2.0;
      double C3 = dy * dy;
      double C4 = dx * dz * 2.0;
      double C5 = dy * dz * 2.0;
      double C6 = dz * dz;

      // Compute the intersection constants
      double dxs0p = dx * (s0_ + p);
      double dys0p = dy * (s0_ + p);
      double dzs0p = dz * (s0_ + p);
      double K1 = dxs0p * dxs0p;
      double K2 = dxs0p * dys0p * 2.0;
      double K3 = dys0p * dys0p;
      double K4 = dxs0p * dzs0p * 2.0;
      double K5 = dys0p * dzs0p * 2.0;
      double K6 = dzs0p * dzs0p;

      // Compute the ellipse coefficients
      double L1 = K1 - G * C1;
      double L2 = K2 - G * C2;
      double L3 = K3 - G * C3;
      double L4 = K4 - G * C4;
      double L5 = K5 - G * C5;
      double L6 = K6 - G * C6;

      // Return as an array
      af::small<double,6> result(6);
      result[0] = L1;
      result[1] = L2;
      result[2] = L3;
      result[3] = L4;
      result[4] = L5;
      result[5] = L6;
      return result;
    }

    af::small<double, 5> parametric(mat3<double> d, double phi) const {
      af::small<double, 6> coeffs = equation(d, phi);
      double A = coeffs[0];
      double B = coeffs[1] / 2.0;
      double C = coeffs[2];
      double D = coeffs[3] / 2.0;
      double E = coeffs[4] / 2.0;
      double F = coeffs[5];

      // Compute ellipse centre
      double den0 = B*B - A*C;
      DIALS_ASSERT(den0 != 0);
      double xc = (C*D - B*E) / den0;
      double yc = (A*E - B*D) / den0;

      // Compute axes
      double den1 = (A - C)*(A - C) + 4*B*B;
      DIALS_ASSERT(den1 >= 0);
      double den2 = std::sqrt(den1);
      double den3 = den0 * (den2 - (A + C));
      double den4 = den0 * (-den2 - (A + C));
      DIALS_ASSERT(den3 != 0 && den4 != 0);
      double num = 2*(A*E*E+C*D*D+F*B*B-2*B*D*E-A*C*F);
      double aa = num / den3;
      double bb = num / den4;
      DIALS_ASSERT(aa >= 0 && bb >= 0);
      double a = std::sqrt(aa);
      double b = std::sqrt(bb);

      // Compute the angle
      double theta = 0;
      if (B == 0) {
        if (A < C) {
          theta = 0;
        } else {
          theta = pi / 2.0;
        }
      } else {
        if (A < C) {
          theta = 0.5 * std::atan2(2.0*B, (A - C));
        } else {
          theta = 0.5 * pi + 0.5 * std::atan2(2.0*B, (A - C));
        }
      }

      af::small<double,5> result(5);
      result[0] = xc;
      result[1] = yc;
      result[2] = a;
      result[3] = b;
      result[4] = theta;
      return result;
    }

  private:

    /**
     * @returns The angle defining the ewald intersection
     */
    double ewald_intersection_angle(double r) const {
      double sl = s0_.length();
      double h2 = r*r / (2.0 * sl);
      double h1 = sl - h2;
      double sl2 = sl*sl;
      double h12 = h1*h1;
      DIALS_ASSERT(sl2 >= h12);
      return std::atan2(std::sqrt(sl2 - h12), h1);
    }

    vec3<double> s0_;
    vec3<double> m2_;
    vec3<double> s1_;
    vec3<double> e1_;
    vec3<double> e2_;
    vec3<double> e3_;
    double zeta_;
    double phi_;
    double s_;
    double da_;
    double w_;
    double thickness_;
    double rocking_width_;
    SphericalCap cap_;
  };

}}}} // namespace dlstbx::algorithms::profile_model::nave

#endif // DLSTBX_ALGORITHMS_PROFILE_MODEL_NAVE_MODEL_H
