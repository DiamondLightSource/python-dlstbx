/*
 * spherical_cap.h
 *
 *  Copyright (C) 2013 Diamond Light Source
 *
 *  Author: James Parkhurst
 *
 *  This code is distributed under the BSD license, a copy of which is
 *  included in the root directory of this package.
 */

#ifndef DLSTBX_ALGORITHMS_PROFILE_MODEL_NAVE_SPHERICAL_CAP_H
#define DLSTBX_ALGORITHMS_PROFILE_MODEL_NAVE_SPHERICAL_CAP_H

#include <cmath>
#include <scitbx/vec3.h>
#include <scitbx/constants.h>
#include <dials/error.h>

namespace dlstbx {
namespace algorithms {
namespace profile_model {
namespace nave {

  using scitbx::vec3;
  using scitbx::constants::pi;

  /**
   * A class to represent a spherical cap. The sphere has its origin at zero and
   * has a radius given in the constructor. The cap is assumed to be centred on
   * the axis vector.
   */
  class SphericalCap {
  public:

    /**
     * @param axis The axis around which the cap is centred.
     * @param angle The angle which the cap covers (must be 0 <= angle < pi).
     */
    SphericalCap(vec3<double> axis, double angle)
      : axis_(axis),
        radius_(axis.length()),
        angle_(angle) {
      DIALS_ASSERT(radius_ >= 0);
      DIALS_ASSERT(angle_ >= 0);
      DIALS_ASSERT(angle_ <= pi);
    }

    /**
     * @returns The axis of the cap
     */
    vec3<double> axis() const {
      return axis_;
    }

    /**
     * @returns The radius of the sphere
     */
    double radius() const {
      return radius_;
    }

    /**
     * @returns The angle covered by the cap
     */
    double angle() const {
      return angle_;
    }

    /**
     * @returns The radius of the cap circle
     */
    double a() const {
      return radius_ * std::sin(angle_);
    }

    /**
     * @returns The distance from the top of the sphere to the cap plane
     */
    double h1() const {
      return radius_ - h2();
    }

    /**
     * @returns The signed distance from the centre to the cap plane
     */
    double h2() const {
      return radius_ * std::cos(angle_);
    }

    /**
     * Compute the distance to the spherical cap. Where the inclination of the
     * point is < the angle that the cap covers, this is just the distance from
     * the origin of the sphere to the point minus the radius of the sphere.
     * Where the inclination is > than the angle that the cap covers, this is
     * the distance to the circle limiting the cap.
     * @param p The point
     * @returns The distance to the cap
     */
    double distance(vec3<double> p) const {
      double d = 0;
      if (angle_ > 0) {
        double l = p.length();
        if (l > 0) {
          double costheta = (axis_ * p) / (radius_ * l);
          if (costheta >  1) costheta =  1;
          if (costheta < -1) costheta = -1;
          double theta = std::acos(costheta);
          if (theta <= angle_) {
            d = std::abs(l - radius_);
          } else {
            DIALS_ASSERT(theta > angle_);
            double a = radius_*radius_ + l*l;
            double b = 2.0 * radius_*l*std::cos(theta - angle_);
            DIALS_ASSERT(a >= b);
            d = std::sqrt(a - b);
          }
        } else {
          d = radius_;
        }
      } else {
        d = (axis_ - p).length();
      }
      return d;
    }

  private:

    vec3<double> axis_;
    double radius_;
    double angle_;
  };

}}}} // namespace dlstbx::algorithms::profile_model::nave

#endif // DLSTBX_ALGORITHMS_PROFILE_MODEL_NAVE_SPHERICAL_CAP_H
