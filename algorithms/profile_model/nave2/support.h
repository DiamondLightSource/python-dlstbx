/*
 * support.h
 *
 *  Copyright (C) 2013 Diamond Light Source
 *
 *  Author: James Parkhurst
 *
 *  This code is distributed under the BSD license, a copy of which is
 *  included in the root directory of this package.
 */

#ifndef DLSTBX_ALGORITHMS_PROFILE_MODEL_NAVE_SUPPORT_H
#define DLSTBX_ALGORITHMS_PROFILE_MODEL_NAVE_SUPPORT_H

#include <stack>
#include <boost/unordered_set.hpp>
#include <boost/math/distributions/chi_squared.hpp>
#include <scitbx/array_family/tiny_types.h>
#include <dxtbx/model/beam.h>
#include <dxtbx/model/detector.h>
#include <dxtbx/model/goniometer.h>
#include <dxtbx/model/scan.h>
#include <dials/model/data/shoebox.h>
#include <dlstbx/algorithms/profile_model/nave2/model.h>

namespace scitbx{
std::size_t hash_value(vec3<int> a) {
    std::size_t seed = 0;
    boost::hash_combine(seed, a[0]);
    boost::hash_combine(seed, a[1]);
    boost::hash_combine(seed, a[2]);
    return seed;
  }
}

namespace dlstbx { namespace algorithms {

  using scitbx::af::int6;
  using dxtbx::model::Beam;
  using dxtbx::model::Detector;
  using dxtbx::model::Panel;
  using dxtbx::model::Goniometer;
  using dxtbx::model::Scan;
  using dials::model::Shoebox;
  using dials::model::Valid;
  using dials::model::Foreground;
  using dials::model::Background;

  /**
   * A class to provide some support functions for the nave profile model
   */
  class Support {
  public:

    /**
     * Initialise the support class
     * @param beam The beam model
     * @param detector The detector model
     * @param goniometer The goniometer model
     * @param scan The scan model
     * @param A The UB matrix
     * @param sig_s The mosiac block size parameters
     * @param sig_a The unit_cell size parameters
     * @param sig_w The angular spread parameters
     * @param p The probability
     */
    Support(
        const Beam &beam,
        const Detector &detector,
        const Goniometer &goniometer,
        const Scan &scan,
        const mat3<double> &A,
        const vec3<double> &sig_s,
        const vec3<double> &sig_a,
        const vec3<double> &sig_w,
        double p)
      : detector_(detector),
        scan_(scan),
        A_(A),
        s0_(beam.get_s0()),
        m2_(goniometer.get_rotation_axis()),
        sig_s_(sig_s),
        sig_a_(sig_a),
        sig_w_(sig_w),
        chi2p_(quantile(boost::math::chi_squared(3), p)) {
      DIALS_ASSERT(chi2p_ > 0);
    }

    /**
     * Compute the bounding box
     * @param panel The panel number
     * @param s1 The s1 vector
     * @param phi0 The rotation angle
     * @returns The bounding box
     */
    int6 compute_bbox(std::size_t panel, vec3<double> s1, double phi0) const {

      // Get the panel
      const Panel &p = detector_[panel];
      mat3<double> D = p.get_d_matrix();

      // Construct the model
      Model model(D, A_, s0_, m2_, s1, phi0, sig_s_, sig_a_, sig_w_);

      // Get the centre
      vec2<double> xyc = p.get_ray_intersection_px(s1);
      double xc = xyc[0];
      double yc = xyc[1];
      double zc = scan_.get_array_index_from_angle(phi0);

      // The initial ranges
      int xmin = (int)std::floor(xc);
      int xmax = xmin + 1;
      int ymin = (int)std::floor(yc);
      int ymax = ymin + 1;
      int zmin = (int)std::floor(zc);
      int zmax = zmin + 1;

      // This list of those processed
      boost::unordered_set< vec3<int>, boost::hash< vec3<int> > > processed;

      // Flood fill to find the bounding box
      std::stack< vec3<int> > s;
      s.push(vec3<int>(xmin, ymin, zmin));
      while (!s.empty()) {
        vec3<int> a = s.top();
        s.pop();
        if (processed.count(a)) {
          continue;
        }
        processed.insert(a);
        vec2<double> xymm = p.pixel_to_millimeter(vec2<double>(a[0], a[1]));
        double b = scan_.get_angle_from_array_index(a[2]);
        if (model.Dm(xymm[0], xymm[1], b) < chi2p_) {
          if (a[0] <= xmin) xmin = a[0]-1;
          if (a[1] <= ymin) ymin = a[1]-1;
          if (a[2] <= zmin) zmin = a[2]-1;
          if (a[0] >= xmax) xmax = a[0]+1;
          if (a[1] >= ymax) ymax = a[1]+1;
          if (a[2] >= zmax) zmax = a[2]+1;
          s.push(vec3<int>(a[0]+1, a[1], a[2]));
          s.push(vec3<int>(a[0], a[1]+1, a[2]));
          s.push(vec3<int>(a[0], a[1], a[2]+1));
          s.push(vec3<int>(a[0]-1, a[1], a[2]));
          s.push(vec3<int>(a[0], a[1]-1, a[2]));
          s.push(vec3<int>(a[0], a[1], a[2]-1));
        }
      }

      return int6(xmin-1, xmax+1, ymin-1, ymax+1, zmin, zmax);
    }

    /**
     * Compute the reflection mask
     * @param panel The panel number
     * @param s1 The s1 vector
     * @param phi0 The rotation angle
     * @param sbox The shoebox
     */
    void compute_mask(
        std::size_t panel,
        vec3<double> s1,
        double phi0,
        Shoebox<> &sbox) const {

      // Check the input
      DIALS_ASSERT(sbox.is_consistent());

      // Get the panel
      const Panel &p = detector_[panel];
      mat3<double> D = p.get_d_matrix();

      // Get the mask
      af::ref< int, af::c_grid<3> > mask = sbox.mask.ref();

      // Get the bounding box values
      int x0 = sbox.bbox[0];
      int y0 = sbox.bbox[2];
      int z0 = sbox.bbox[4];

      // Construct the model
      Model model(D, A_, s0_, m2_, s1, phi0, sig_s_, sig_a_, sig_w_);

      // Loop through all the pixels
      for (std::size_t y = 0; y < mask.accessor()[1]; ++y) {
        for (std::size_t x = 0; x < mask.accessor()[2]; ++x) {
          double xx0 = (double)(x0 + (int)x);
          double xx2 = xx0 + 1.0;
          double xx1 = (xx0 + xx2) / 2.0;
          double yy0 = (double)(y0 + (int)y);
          double yy2 = yy0 + 1.0;
          double yy1 = (yy0 + yy2) / 2.0;
          af::small< vec2<double>, 9 > xy(9);
          xy[0] = p.pixel_to_millimeter(vec2<double>(xx0, yy0));
          xy[1] = p.pixel_to_millimeter(vec2<double>(xx0, yy1));
          xy[2] = p.pixel_to_millimeter(vec2<double>(xx0, yy2));
          xy[3] = p.pixel_to_millimeter(vec2<double>(xx1, yy0));
          xy[4] = p.pixel_to_millimeter(vec2<double>(xx1, yy1));
          xy[5] = p.pixel_to_millimeter(vec2<double>(xx1, yy2));
          xy[6] = p.pixel_to_millimeter(vec2<double>(xx2, yy0));
          xy[7] = p.pixel_to_millimeter(vec2<double>(xx2, yy1));
          xy[8] = p.pixel_to_millimeter(vec2<double>(xx2, yy2));
          for (std::size_t z = 0; z < mask.accessor()[0]; ++z) {
            double zz0 = (double)(z0 + (int)z);
            double zz2 = zz0 + 1.0;
            double zz1 = (zz0 + zz2) / 2.0;
            af::small< double, 3> a(3);
            a[0] = scan_.get_angle_from_array_index(zz0);
            a[1] = scan_.get_angle_from_array_index(zz1);
            a[2] = scan_.get_angle_from_array_index(zz2);
            int mask_code = Background;
            for (std::size_t j = 0; j < 3 && (mask_code == Background); ++j) {
              for (std::size_t i = 0; i < 9; ++i) {
                if (model.Dm(xy[i][0], xy[i][1], a[j]) < chi2p_) {
                  mask_code = Foreground;
                  break;
                }
              }
            }
            mask(z,y,x) |= mask_code;
          }
        }
      }
    }

    /**
     * Compute the reflection profile
     * @param panel The panel number
     * @param s1 The s1 vector
     * @param phi0 The rotation angle
     * @param profile The profile array
     */
    void compute_prof(
        std::size_t panel,
        vec3<double> s1,
        double phi0,
        int6 bbox,
        af::ref< double, af::c_grid<3> > &profile) const {

      // Get the panel
      const Panel &p = detector_[panel];
      mat3<double> D = p.get_d_matrix();

      // Get the bounding box values
      int x0 = bbox[0];
      int x1 = bbox[1];
      int y0 = bbox[2];
      int y1 = bbox[3];
      int z0 = bbox[4];
      int z1 = bbox[5];

      // Check the input
      DIALS_ASSERT(x1 > x0);
      DIALS_ASSERT(y1 > y0);
      DIALS_ASSERT(z1 > z0);
      DIALS_ASSERT(profile.accessor()[0] == (x1 - x0));
      DIALS_ASSERT(profile.accessor()[1] == (y1 - y0));
      DIALS_ASSERT(profile.accessor()[2] == (z1 - z0));

      // Construct the model
      Model model(D, A_, s0_, m2_, s1, phi0, sig_s_, sig_a_, sig_w_);

      // Loop through all the pixels
      for (int y = y0; y < y1; ++y) {
        for (int x = x0; x < x1; ++x) {
          vec2<double> xy = p.pixel_to_millimeter(vec2<double>(x+0.5, y+0.5));
          for (int z = z0; z < z1; ++z) {
            double p = scan_.get_angle_from_array_index(z0+0.5);
            profile(z-z0,y-y0,x-x0) = model.P(xy[0], xy[1], p);
          }
        }
      }
    }

    /**
     * Compute the reflection profile
     * @param mask The mask array
     * @param panel The panel number
     * @param z The image number
     * @param s1 The s1 vector
     * @param phi0 The rotation angle
     * @param bbox The bounding box
     */
    void compute_image_mask(
        af::ref< int, af::c_grid<2> > mask,
        std::size_t panel,
        int z,
        vec3<double> s1,
        double phi0,
        int6 bbox) const {

      // Get the panel
      const Panel &p = detector_[panel];
      mat3<double> D = p.get_d_matrix();

      // Get the bounding box values
      int x0 = bbox[0];
      int x1 = bbox[1];
      int y0 = bbox[2];
      int y1 = bbox[3];
      int z0 = bbox[4];
      int z1 = bbox[5];
      DIALS_ASSERT(x1 > x0);
      DIALS_ASSERT(y1 > y0);
      DIALS_ASSERT(z1 > z0);
      if (x0 < 0) x0 = 0;
      if (y0 < 0) y0 = 0;
      if (x1 > mask.accessor()[1]) x1 = mask.accessor()[1];
      if (y1 > mask.accessor()[0]) y1 = mask.accessor()[0];
      if (x1 <= x0 || y1 <= y0 || z0 > z || z1 < z) {
        return;
      }
      std::size_t xs = x1 - x0;
      std::size_t ys = y1 - y0;

      // Construct the model
      Model model(D, A_, s0_, m2_, s1, phi0, sig_s_, sig_a_, sig_w_);

      // Loop through all the pixels
      for (std::size_t y = 0; y < ys; ++y) {
        for (std::size_t x = 0; x < xs; ++x) {
          double xx0 = (double)(x0 + (int)x);
          double xx2 = xx0 + 1.0;
          double xx1 = (xx0 + xx2) / 2.0;
          double yy0 = (double)(y0 + (int)y);
          double yy2 = yy0 + 1.0;
          double yy1 = (yy0 + yy2) / 2.0;
          af::small< vec2<double>, 9 > xy(9);
          xy[0] = p.pixel_to_millimeter(vec2<double>(xx0, yy0));
          xy[1] = p.pixel_to_millimeter(vec2<double>(xx0, yy1));
          xy[2] = p.pixel_to_millimeter(vec2<double>(xx0, yy2));
          xy[3] = p.pixel_to_millimeter(vec2<double>(xx1, yy0));
          xy[4] = p.pixel_to_millimeter(vec2<double>(xx1, yy1));
          xy[5] = p.pixel_to_millimeter(vec2<double>(xx1, yy2));
          xy[6] = p.pixel_to_millimeter(vec2<double>(xx2, yy0));
          xy[7] = p.pixel_to_millimeter(vec2<double>(xx2, yy1));
          xy[8] = p.pixel_to_millimeter(vec2<double>(xx2, yy2));
          double zz0 = (double)(z);
          double zz2 = zz0 + 1.0;
          double zz1 = (zz0 + zz2) / 2.0;
          af::small< double, 3> a(3);
          a[0] = scan_.get_angle_from_array_index(zz0);
          a[1] = scan_.get_angle_from_array_index(zz1);
          a[2] = scan_.get_angle_from_array_index(zz2);
          int mask_code = Background;
          for (std::size_t j = 0; j < 3 && (mask_code == Background); ++j) {
            for (std::size_t i = 0; i < 9; ++i) {
              if (model.Dm(xy[i][0], xy[i][1], a[j]) < chi2p_) {
                mask_code = Foreground;
                break;
              }
            }
          }
          mask(y0+y,x0+x) |= mask_code;
        }
      }

    }


  private:

    Detector detector_;
    Scan scan_;
    mat3<double> D_;
    mat3<double> A_;
    vec3<double> s0_;
    vec3<double> m2_;
    vec3<double> sig_s_;
    vec3<double> sig_a_;
    vec3<double> sig_w_;
    double chi2p_;
  };


}} // namespace dlstbx::algorithms

#endif // DLSTBX_ALGORITHMS_PROFILE_MODEL_NAVE_SUPPORT_H
