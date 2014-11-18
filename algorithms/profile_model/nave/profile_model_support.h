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
  using dxtbx::model::Panel;
  using dials::model::Background;
  using dials::model::Foreground;

  /**
   * A class to provide methods needed by profile model
   */
  class ProfileModelSupport {
  public:

    /**
     * Initialise support functions
     * @param beam The beam model
     * @param detector The detector model
     * @param goniometer The goniometer model
     * @param scan The scan model
     * @param s The size of mosaic blocks
     * @param da The range of unit cell sizes
     * @param w The angular spread of mosaic blocks
     */
    ProfileModelSupport(
        const Beam &beam,
        const Detector &detector,
        const Goniometer &goniometer,
        const Scan &scan,
        double s,
        double da,
        double w)
      : detector_(detector),
        scan_(scan),
        s0_(beam.get_s0()),
        m2_(goniometer.get_rotation_axis()),
        s_(s),
        da_(da),
        w_(w) {}

    /**
     * Compute the reflection partiality
     * @param s1 The diffracted beam vector
     * @param phi The rotation angle
     * @param d The resolution
     * @param bbox The bounding box
     * @returns The partiality
     */
    double compute_partiality(
        vec3<double> s1,
        double phi,
        int6 bbox) const {

      // Create the model
      Model model(s0_, m2_, s1, phi, s_, da_, w_);

      // Ensure our values are ok
      DIALS_ASSERT(bbox[4] < bbox[5]);

      // Get the rotation angle
      double phia = scan_.get_angle_from_array_index(bbox[4]);
      double phib = scan_.get_angle_from_array_index(bbox[5]);

      // Compute and return the fraction of intensity
      return model.intensity_fraction(phia, phib);
    }

    int6 compute_bbox(
        std::size_t panel,
        vec3<double> s1,
        double phi) const {

      // Get the panel
      const Panel &p = detector_[panel];

      // Create the model
      Model model(s0_, m2_, s1, phi, s_, da_, w_);

      // Get the angular range
      vec2<double> phi_range = model.phi_range();
      double z0 = scan_.get_array_index_from_angle(phi_range[0]);
      double z1 = scan_.get_array_index_from_angle(phi_range[1]);
      double zm = scan_.get_array_index_from_angle(phi);
      vec2<double> z(z0, z1);

      // Get the minimum box of s1 vectors
      af::small< vec3<double>, 8 > s1_box = model.minimum_box();
      af::small<double, 8> x(8);
      af::small<double, 8> y(8);
      for (std::size_t i = 0; i < 8; ++i) {
        vec2<double> xy = p.get_ray_intersection_px(s1_box[i]);
        x[i] = xy[0];
        y[i] = xy[1];
      }

      // Return the roi in the following form:
      // (minx, maxx, miny, maxy, minz, maxz)
      // Min's are rounded down to the nearest integer, Max's are rounded up
      int6 bbox(
        (int)std::floor(min(x)), (int)std::ceil(max(x)),
        (int)std::floor(min(y)), (int)std::ceil(max(y)),
        (int)std::floor(min(z)), (int)std::ceil(max(z))
      );

      // Check the bbox ranges
      vec2<int> array_range = scan_.get_array_range();
      DIALS_ASSERT(bbox[4] <= zm && zm < bbox[5]);
      bbox[4] = std::max(bbox[4], array_range[0]);
      bbox[4] = std::min(bbox[4], array_range[1]-1);
      bbox[5] = std::min(bbox[5], array_range[1]);
      bbox[5] = std::max(bbox[5], array_range[0]+1);
      DIALS_ASSERT(bbox[1] > bbox[0]);
      DIALS_ASSERT(bbox[3] > bbox[2]);
      DIALS_ASSERT(bbox[5] > bbox[4]);
      return bbox;
    }

    void compute_mask(
        std::size_t panel,
        vec3<double> s1,
        double phi,
        int6 bbox,
        af::ref<int, af::c_grid<3> > mask) const {

      // Get the panel
      const Panel &p = detector_[panel];

      // Create the model
      Model model(s0_, m2_, s1, phi, s_, da_, w_);

      // Check the input
      DIALS_ASSERT(bbox[1] > bbox[0]);
      DIALS_ASSERT(bbox[3] > bbox[2]);
      DIALS_ASSERT(bbox[5] > bbox[4]);
      DIALS_ASSERT(mask.accessor()[0] == bbox[5] - bbox[4]);
      DIALS_ASSERT(mask.accessor()[1] == bbox[3] - bbox[2]);
      DIALS_ASSERT(mask.accessor()[2] == bbox[1] - bbox[0]);

      for (std::size_t k = 0; k < mask.accessor()[0]; ++k) {
        for (std::size_t j = 0; j < mask.accessor()[1]; ++j) {
          for (std::size_t i = 0; i < mask.accessor()[2]; ++i) {

            int z = bbox[4] + (int)k;
            int y = bbox[2] + (int)j;
            int x = bbox[0] + (int)i;

            // The rotation angles
            double phi0 = scan_.get_angle_from_array_index(z);
            double phi1 = scan_.get_angle_from_array_index(z+1);

            // The beam vectors
            vec3<double> sp0 = p.get_pixel_lab_coord(vec2<double>(x,y));
            vec3<double> sp1 = p.get_pixel_lab_coord(vec2<double>(x+1,y));
            vec3<double> sp2 = p.get_pixel_lab_coord(vec2<double>(x,y+1));
            vec3<double> sp3 = p.get_pixel_lab_coord(vec2<double>(x+1,y+1));
            vec3<double> sp4 = p.get_pixel_lab_coord(vec2<double>(x+0.5,y+0.5));

            // Check if point is inside
            if (model.inside2(sp0, phi0, phi1) ||
                model.inside2(sp1, phi0, phi1) ||
                model.inside2(sp2, phi0, phi1) ||
                model.inside2(sp3, phi0, phi1) ||
                model.inside2(sp4, phi0, phi1)) {
              mask(k,j,i) |= Foreground;
            } else {
              /* mask(k,j,i) |= Background; */
            }
          }
        }
      }
    }

  private:

    Detector detector_;
    Scan scan_;
    vec3<double> s0_;
    vec3<double> m2_;
    double s_;
    double da_;
    double w_;
  };

}}}} // namespace dlstbx::algorithms::profile_model::nave

#endif // DLSTBX_ALGORITHMS_PROFILE_MODEL_NAVE_PROFILE_MODEL_SUPPORT_H
