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
