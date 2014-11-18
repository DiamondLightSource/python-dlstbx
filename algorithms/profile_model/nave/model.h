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
     * @param s1 The diffracted beam vector
     * @returns The elliptical parameters
     */
    af::small<double, 6> operator()(vec3<double> s1) const {
      double K = s0_.length() - (s1 - s0_).length_sq() / 2.0;
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
     * @param r The reciprocal lattive vector
     * @param s The mosaic block size
     * @param da The spread of unit cell sizes
     * @param w The angular spread of mosaic blocks
     */
    Model(vec3<double> s0,
          vec3<double> m2,
          vec3<double> s1,
          double phi,
          double d,
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
        d_(d),
        s_(s),
        da_(da),
        w_(w),
        cap_(s1_ - s0_, w_) {
      DIALS_ASSERT(s0_.length() > 0);
      DIALS_ASSERT(s1_.length() > 0);
      DIALS_ASSERT(d > 0);
      DIALS_ASSERT(s > 0);
      DIALS_ASSERT(da >= 0);
      DIALS_ASSERT(w >= 0);
      DIALS_ASSERT(w <= pi);
      thickness_ = 1.0 / s;// + n * da / (a*a);
      rocking_width_ = 2.0 * std::atan2(1.0, (2.0 * s * r().length())) + w;
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
     * @returns The resolution of the reflection.
     */
    double d() const {
      return d_;
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
    double d_;
    double s_;
    double da_;
    double w_;
    double thickness_;
    double rocking_width_;
    SphericalCap cap_;
  };

}}}} // namespace dlstbx::algorithms::profile_model::nave

#endif // DLSTBX_ALGORITHMS_PROFILE_MODEL_NAVE_MODEL_H
