/*
 * profile_model_support.h
 *
 *  Copyright (C) 2013 Diamond Light Source
 *
 *  Author: James Parkhurst
 *
 *  This code is distributed under the BSD license, a copy of which is
 *  included in the root directory of this package.
 */

#ifndef DLSTBX_ALGORITHMS_PROFILE_MODEL_NAVE_PROFILE_MODEL_SUPPORT_H
#define DLSTBX_ALGORITHMS_PROFILE_MODEL_NAVE_PROFILE_MODEL_SUPPORT_H

#include <scitbx/array_family/tiny_types.h>
#include <dlstbx/algorithms/profile_model/nave/model.h>
#include <dials/error.h>

namespace dlstbx {
namespace algorithms {
namespace profile_model {
namespace nave {

  using scitbx::af::int6;

  class ProfileModelSupport {
  public:

    ProfileModelSupport(
        const Beam &beam,
        const Detector &detector,
        const Goniometer &goniometer,
        const Scan &scan,
        double s,
        double da,
        double w)
      : scan_(scan),
        s0_(beam.get_s0()),
        m2_(goniometer.get_rotation_axis()),
        s_(s),
        da_(da),
        w_(w) {}

    double compute_partiality(
        vec3<double> s1,
        double phi,
        double d,
        int6 bbox) const {

      // Create the model
      Model model(s0_, m2_, s1, phi, d, s_, da_, w_);

      // Ensure our values are ok
      DIALS_ASSERT(bbox[4] < bbox[5]);

      // Get the rotation angle
      double phia = scan_.get_angle_from_array_index(bbox[4]);
      double phib = scan_.get_angle_from_array_index(bbox[5]);

      // Compute and return the fraction of intensity
      return model.intensity_fraction(phia, phib);
    }

    /* int6 compute_bbox() const { */
    /*   Model model; */

    /*   // The angles where the ewald sphere is intersected */
    /*   vec2<double> angles = model.ewald_intersection_angles(); */



    /*   // Compute the z range of the reflection */
    /*   double z0 = scan_.array_index_from_angle(model.phi0()); */
    /*   double z1 = scan_.array_index_from_angle(model.phi1()); */

    /*   // Return the roi in the following form: */
    /*   // (minx, maxx, miny, maxy, minz, maxz) */
    /*   // Min's are rounded down to the nearest integer, Max's are rounded up */
    /*   double4 x(xy1[0], xy2[0], xy3[0], xy4[0]); */
    /*   double4 y(xy1[1], xy2[1], xy3[1], xy4[1]); */
    /*   double2 z(z1, z2); */
    /*   int6 bbox( */
    /*     (int)std::floor(min(x)), (int)std::ceil(max(x)), */
    /*     (int)std::floor(min(y)), (int)std::ceil(max(y)), */
    /*     (int)std::floor(min(z)), (int)std::ceil(max(z)) */
    /*   ); */

    /*   // Check the bbox ranges */
    /*   vec2<int> array_range = scan_.get_array_range(); */
    /*   DIALS_ASSERT(bbox[4] <= frame && frame < bbox[5]); */
    /*   bbox[4] = std::max(bbox[4], array_range[0]); */
    /*   bbox[4] = std::min(bbox[4], array_range[1]-1); */
    /*   bbox[5] = std::min(bbox[5], array_range[1]); */
    /*   bbox[5] = std::max(bbox[5], array_range[0]+1); */
    /*   DIALS_ASSERT(bbox[1] > bbox[0]); */
    /*   DIALS_ASSERT(bbox[3] > bbox[2]); */
    /*   DIALS_ASSERT(bbox[5] > bbox[4]); */
    /*   return bbox; */
    /* } */

    /* void compute_mask() const { */
    /*     /1* int6 bbox, *1/ */
    /*     /1* af::ref< int, af::c_grid<3> > &mask) const { *1/ */

    /*   // Check the input */
    /*   DIALS_ASSERT(bbox[1] > bbox[0]); */
    /*   DIALS_ASSERT(bbox[3] > bbox[2]); */
    /*   DIALS_ASSERT(bbox[5] > bbox[4]); */
    /*   DIALS_ASSERT(mask.accessor()[0] == bbox[5] - bbox[4]); */
    /*   DIALS_ASSERT(mask.accessor()[1] == bbox[3] - bbox[2]); */
    /*   DIALS_ASSERT(mask.accessor()[2] == bbox[1] - bbox[0]); */

    /*   // Loop through all the pixels in the mask region */
    /*   for (std::size_t k = 0; k < mask.accessor()[0]; ++k) { */
    /*     for (std::size_t j = 0; j < mask.accessor()[1]; ++j) { */
    /*       for (std::size_t i = 0; i < mask.accessor()[2]; ++i) { */

    /*         // Get the diffracted beam vectors */
    /*         vec3<double> s00 = p.get_pixel_lab_coordinate(vec2<double>(x,y)); */
    /*         vec3<double> s01 = p.get_pixel_lab_coordinate(vec2<double>(x+1,y)); */
    /*         vec3<double> s10 = p.get_pixel_lab_coordinate(vec2<double>(x,y+1)); */
    /*         vec3<double> s11 = p.get_pixel_lab_coordinate(vec2<double>(x+1,y+1)); */
    /*         s00 = s00.normalize() * s1_length; */
    /*         s01 = s01.normalize() * s1_length; */
    /*         s10 = s10.normalize() * s1_length; */
    /*         s11 = s11.normalize() * s1_length; */

    /*         // Get the range of oscillation angles */
    /*         double phi00 = scan_.get_angle_from_array_index(z); */
    /*         double phi01 = scan_.get_angle_from_array_index(z+1); */
    /*         double phi10 = model.phi0(); */
    /*         double phi11 = model.phi1(); */
    /*         if ((phi00 < phi10 && phi01 > phi10) || */
    /*             (phi00 < phi11 && phi01 > phi11)) { */

    /*         } else { */
    /*           mask(k,j,i) |= Background; */
    /*         } */

    /*         // Compute the reciprocal space vectors */
    /*         af::small< vec3<double>, 8 > vel(8); */
    /*         vel[0] = (s00 - s0_).unit_rotate_around_axis(m2_, phi0); */
    /*         vel[1] = (s00 - s0_).unit_rotate_around_axis(m2_, phi0); */
    /*         vel[2] = (s00 - s0_).unit_rotate_around_axis(m2_, phi0); */
    /*         vel[3] = (s00 - s0_).unit_rotate_around_axis(m2_, phi0); */
    /*         vel[4] = (s00 - s0_).unit_rotate_around_axis(m2_, phi1); */
    /*         vel[5] = (s00 - s0_).unit_rotate_around_axis(m2_, phi1); */
    /*         vel[6] = (s00 - s0_).unit_rotate_around_axis(m2_, phi1); */
    /*         vel[7] = (s00 - s0_).unit_rotate_around_axis(m2_, phi1); */

    /*         // If the reciprocal space point is within the profile region then */
    /*         // set the mask to foreground, otherwise background */
    /*         mask(k,j,i) = model.inside(vel) ? Foreground : Background; */
    /*       } */
    /*     } */
    /*   } */
    /* } */

  private:

    Scan scan_;
    vec3<double> s0_;
    vec3<double> m2_;
    double s_;
    double da_;
    double w_;
  };

}}}} // namespace dlstbx::algorithms::profile_model::nave

#endif // DLSTBX_ALGORITHMS_PROFILE_MODEL_NAVE_PROFILE_MODEL_SUPPORT_H
