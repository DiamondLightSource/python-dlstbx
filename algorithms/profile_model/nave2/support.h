
#ifndef DLSTBX_ALGORITHMS_PROFILE_MODEL_NAVE_SUPPORT_H
#define DLSTBX_ALGORITHMS_PROFILE_MODEL_NAVE_SUPPORT_H

#include <scitbx/array_family/tiny_types.h>
#include <dxtbx/model/beam.h>
#include <dxtbx/model/detector.h>
#include <dxtbx/model/goniometer.h>
#include <dxtbx/model/scan.h>
#include <dials/model/data/shoebox.h>
#include <dlstbx/algorithms/profile_model/nave2/model.h>

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

  class Support {
  public:

    Support(
        const Beam &beam,
        const Detector &detector,
        const Goniometer &goniometer,
        const Scan &scan,
        const mat3<double> &A,
        const vec3<double> &sig_s,
        const vec3<double> &sig_a,
        const vec3<double> &sig_w,
        double chi2p) 
      : detector_(detector),
        scan_(scan),
        A_(A),
        s0_(beam.get_s0()),
        m2_(goniometer.get_rotation_axis()),
        sig_s_(sig_s),
        sig_a_(sig_a),
        sig_w_(sig_w),
        chi2p_(chi2p) {
      DIALS_ASSERT(chi2p > 0);    
    }

    int6 compute_bbox(std::size_t panel, vec3<double> s1, double phi0) const {

      /* // Get the panel */
      /* const Panel &p = detector_[panel]; */
      /* mat3<double> D = p.get_d_matrix(); */

      /* // Construct the model */
      /* Model model(D, A_, s0_, m2_, s1, phi0, sig_s_, sig_a_, sig_w_); */

      /* // Get the centre */
      /* vec2<double> xyc = p.ray_intersection_px(s1); */
      /* double xc = xyc[0]; */
      /* double yc = xyc[1]; */
      /* double zc = scan_.get_array_index_from_angle(phi); */

      

      /* std::vector<int2> xrange; */
      
      /* int xmin = (int)xc; */
      /* int xmax = xmin+1; */
      /* for (;;--xmin) { */
      /*   vec2<double> xymm = p.pixel_to_millimeter(vec2<double>(xmin, yc)); */
      /*   if (model.Dm(xymm[0], xymm[1], phi) > chi2p_) { */
      /*     break; */
      /*   } */
      /* } */
      /* for (;;++xmax) { */
      /*   vec2<double> xymm = p.pixel_to_millimeter(vec2<double>(xmax, yc)); */
      /*   if (model.Dm(xymm[0], xymm[1], phi) > chi2p_) { */
      /*     break; */
      /*   } */
      /* } */
      return int6();
    }

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
      int x1 = sbox.bbox[1];
      int y0 = sbox.bbox[2];
      int y1 = sbox.bbox[3];
      int z0 = sbox.bbox[4];
      int z1 = sbox.bbox[5];

      // Construct the model
      Model model(D, A_, s0_, m2_, s1, phi0, sig_s_, sig_a_, sig_w_);

      // Loop through all the pixels
      for (int y = y0; y < y1; ++y) {
        for (int x = x0; x < x1; ++x) {
          double xx0 = (double)x;
          double xx2 = xx0 + 1.0;
          double xx1 = (xx0 + xx2) / 2.0;
          double yy0 = (double)y;
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
          int mask_code = Background;
          for (int z = z0; z < z1; ++z) {
            double z0 = (double)z;
            double z2 = z0 + 1.0;
            double z1 = (z0 + z2) / 2.0;
            af::small< double, 3> p(3);
            p[0] = scan_.get_angle_from_array_index(z0);
            p[1] = scan_.get_angle_from_array_index(z1);
            p[2] = scan_.get_angle_from_array_index(z2);
            for (std::size_t j = 0; j < 3 && (mask_code == Background); ++j) {
              for (std::size_t i = 0; i < 9; ++i) {
                if (model.Dm(xy[i][0], xy[i][1], p[j]) < chi2p_) {
                  mask_code = Foreground;
                  break;
                }
              }
            }
            mask(z-z0,y-y0,x-x0) |= mask_code;
          }
        }
      }
    }

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
