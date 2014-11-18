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
          vec3<double> s1,
          double phi,
          double d,
          double s,
          double da,
          double w)
      : s0_(s0),
        s1_(s1.normalize()*s0.length()),
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
      rocking_width_ = d / s + w;
    }

    /**
     * @returns The incident beam vector
     */
    vec3<double> s0() const {
      return s0_;
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
    double phi0() const {
      return phi_ - rocking_width_ * 0.5;
    }

    /**
     * @returns The last phi angle
     */
    double phi1() const {
      return phi_ + rocking_width_ * 0.5;
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
     * @returns The angles defining the ewald intersection range
     */
    vec2<double> ewald_intersection_angles() const {
      double rl = r().length();
      return vec2<double>(
          ewald_intersection_angle(rl - thickness() / 2.0),
          ewald_intersection_angle(rl + thickness() / 2.0));
    }
    /**
     * @returns Does the line intersect the profile model
     */
    /* bool line_intersects(vec3<double> x0, vec3<double> x1) const { */
    /*   double r1 = x0.length(); */
    /*   double r2 = x1.length(); */
    /*   double i1 = cap_.inclination(x0); */
    /*   double i2 = cap_.inclination(x1); */
    /*   if (i1 <= cap_.angle() && i2 <= cap_.angle()) { */

    /*   } */
    /* } */

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
    vec3<double> s1_;
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
