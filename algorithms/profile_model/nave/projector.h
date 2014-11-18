

#ifndef DLSTBX_ALGORITHMS_PROFILE_MODEL_NAVE_PROJECTOR_H
#define DLSTBX_ALGORITHMS_PROFILE_MODEL_NAVE_PROJECTOR_H

#include <cmath>
#include <dxtbx/model/beam.h>
#include <dxtbx/model/detector.h>
#include <dxtbx/model/goniometer.h>
#include <dxtbx/model/scan.h>
#include <scitbx/mat3.h>
#include <scitbx/vec3.h>
#include <scitbx/vec2.h>
#include <dials/array_family/scitbx_shared_and_versa.h>
#include <dials/error.h>

namespace af = dials::af;

namespace dlstbx {
namespace algorithms {
namespace profile_model {
namespace nave {


  using dxtbx::model::Beam;
  using dxtbx::model::Detector;
  using dxtbx::model::Goniometer;
  using dxtbx::model::Scan;
  using scitbx::mat3;
  using scitbx::vec3;
  using scitbx::vec2;

  template <typename T>
  T min4(T x1, T x2, T x3, T x4) {
    return std::min(std::min(x1, x2), std::min(x3, x4));
  }

  template <typename T>
  T max4(T x1, T x2, T x3, T x4) {
    return std::max(std::max(x1, x2), std::max(x3, x4));
  }

  template <typename T>
  T min8(T x1, T x2, T x3, T x4, T x5, T x6, T x7, T x8) {
    return std::min(min4(x1, x2, x3, x4), min4(x5, x6, x7, x8));
  }

  template <typename T>
  T max8(T x1, T x2, T x3, T x4, T x5, T x6, T x7, T x8) {
    return std::max(max4(x1, x2, x3, x4), max4(x5, x6, x7, x8));
  }

  class Projector {
  public:

    Projector(
        const Beam &beam,
        const Detector &detector,
        const Goniometer &goniometer,
        const Scan &scan,
        mat3<double> A,
        double s,
        double da,
        double w)
      : s0_(beam.get_s0()),
        m2_(goniometer.get_rotation_axis()),
        A_(A),
        detector_(detector),
        scan_(scan),
        s_(s),
        da_(da),
        w_(w) {
    }

    af::versa< double, af::c_grid<2> > image(std::size_t index) const {

      // Get the width and height
      DIALS_ASSERT(detector_.size() == 1);
      std::size_t height = detector_[0].get_image_size()[1];
      std::size_t width  = detector_[0].get_image_size()[0];

      // Allocate result array
      af::versa< double, af::c_grid<2> > result(af::c_grid<2>(height, width), 0);

      // Get the matrix and inverse
      mat3<double> Ai = A_.inverse();
      mat3<double> A = A_;

      // Get the angles
      double phi0 = scan_.get_angle_from_array_index(index);
      double phi1 = scan_.get_angle_from_array_index(index+1);

      // Loop through all the pixels and set pixel to 1 if inside
      for (std::size_t j = 0; j < height; ++j) {
        for (std::size_t i = 0; i < width; ++i) {

          // Compute the diffracted beam vectors for the edge of the pixel
          vec3<double> s00 = detector_[0].get_pixel_lab_coord(vec2<double>(j,i));
          vec3<double> s01 = detector_[0].get_pixel_lab_coord(vec2<double>(j,i+1));
          vec3<double> s10 = detector_[0].get_pixel_lab_coord(vec2<double>(j+1,i));
          vec3<double> s11 = detector_[0].get_pixel_lab_coord(vec2<double>(j+1,i+1));
          s00 = s00.normalize() * s0_.length();
          s01 = s01.normalize() * s0_.length();
          s10 = s10.normalize() * s0_.length();
          s11 = s11.normalize() * s0_.length();

          // Compute the reciprocal space vectors
          vec3<double> ps00 = s00 - s0_;
          vec3<double> ps01 = s01 - s0_;
          vec3<double> ps10 = s10 - s0_;
          vec3<double> ps11 = s11 - s0_;

          // Compute rotated vectors to give rs quad
          vec3<double> p000 = ps00.unit_rotate_around_origin(m2_, -phi0);
          vec3<double> p001 = ps01.unit_rotate_around_origin(m2_, -phi0);
          vec3<double> p010 = ps10.unit_rotate_around_origin(m2_, -phi0);
          vec3<double> p011 = ps11.unit_rotate_around_origin(m2_, -phi0);
          vec3<double> p100 = ps00.unit_rotate_around_origin(m2_, -phi1);
          vec3<double> p101 = ps01.unit_rotate_around_origin(m2_, -phi1);
          vec3<double> p110 = ps10.unit_rotate_around_origin(m2_, -phi1);
          vec3<double> p111 = ps11.unit_rotate_around_origin(m2_, -phi1);

          // Compute the fraction index
          vec3<double> h000 = Ai * p000;
          vec3<double> h001 = Ai * p001;
          vec3<double> h010 = Ai * p010;
          vec3<double> h011 = Ai * p011;
          vec3<double> h100 = Ai * p100;
          vec3<double> h101 = Ai * p101;
          vec3<double> h110 = Ai * p110;
          vec3<double> h111 = Ai * p111;

          // Compute the neartest hkl
          vec3<int> hn0((int)std::floor(h000[0]+0.5),
                        (int)std::floor(h000[1]+0.5),
                        (int)std::floor(h000[2]+0.5));
          vec3<int> hn1((int)std::floor(h001[0]+0.5),
                        (int)std::floor(h001[1]+0.5),
                        (int)std::floor(h001[2]+0.5));
          vec3<int> hn2((int)std::floor(h010[0]+0.5),
                        (int)std::floor(h010[1]+0.5),
                        (int)std::floor(h010[2]+0.5));
          vec3<int> hn3((int)std::floor(h011[0]+0.5),
                        (int)std::floor(h011[1]+0.5),
                        (int)std::floor(h011[2]+0.5));
          vec3<int> hn4((int)std::floor(h100[0]+0.5),
                        (int)std::floor(h100[1]+0.5),
                        (int)std::floor(h100[2]+0.5));
          vec3<int> hn5((int)std::floor(h101[0]+0.5),
                        (int)std::floor(h101[1]+0.5),
                        (int)std::floor(h101[2]+0.5));
          vec3<int> hn6((int)std::floor(h110[0]+0.5),
                        (int)std::floor(h110[1]+0.5),
                        (int)std::floor(h110[2]+0.5));
          vec3<int> hn7((int)std::floor(h111[0]+0.5),
                        (int)std::floor(h111[1]+0.5),
                        (int)std::floor(h111[2]+0.5));

          // Get proper vectors
          vec3<double> q0 = (A * hn0).rotate_around_origin(m2_, phi0);
          vec3<double> q1 = (A * hn1).rotate_around_origin(m2_, phi0);
          vec3<double> q2 = (A * hn2).rotate_around_origin(m2_, phi0);
          vec3<double> q3 = (A * hn3).rotate_around_origin(m2_, phi0);
          vec3<double> q4 = (A * hn4).rotate_around_origin(m2_, phi1);
          vec3<double> q5 = (A * hn5).rotate_around_origin(m2_, phi1);
          vec3<double> q6 = (A * hn6).rotate_around_origin(m2_, phi1);
          vec3<double> q7 = (A * hn7).rotate_around_origin(m2_, phi1);

          DIALS_ASSERT(1.0 / s_ < s0_.length());

          double d0 = (q0 - p000).length();
          double d1 = (q1 - p001).length();
          double d2 = (q2 - p010).length();
          double d3 = (q3 - p011).length();
          double d4 = (q4 - p100).length();
          double d5 = (q5 - p101).length();
          double d6 = (q6 - p110).length();
          double d7 = (q7 - p111).length();

          double mind = min8(d0, d1, d2, d3, d4, d5, d6, d7);
          if (mind < 1.0 / s_) {
            result(j,i) = 1;
          }

          /* double rad1 = s0_.length() - 1.0 / s_; */
          /* double rad2 = s0_.length() + 1.0 / s_; */
          /* if (minr < rad1 && maxr > rad1 || */
          /*     minr < rad2 && maxr < rad2) { */
          /*   if (mina < 1.0 / s_) { */
          /*     result(j,i) = 1; */
          /*   } */
          /* } */


          /*     // Loop through frame limits */
      /*     for (std::size_t k = 0; k < 2; ++k) { */

      /*       // Get the rotation angle */
      /*       double phi = scan_.get_angle_from_array_index(index); */

      /*       // Rotate back to position */
      /*       vec3<double> ps0 = ps.unit_rotate_around_origin(m2_, -phi); */

      /*       // Compute the fractional miller indices */
      /*       vec3<double> h0 = Ai * ps0; */

      /*       // Get the closest miller index */
      /*       vec3<int> h1( */
      /*           (int)std::floor(h0[0]+0.5), */
      /*           (int)std::floor(h0[1]+0.5), */
      /*           (int)std::floor(h0[2]+0.5)); */

      /*       vec3<double> ps2 = (A * h1).unit_rotate_around_origin(m2_, phi); */

      /*       double size = 1 / s + */

      /*       double radius = ps.length(); */
      /*       double angle = ps2.angle(ps); */
      /*       if (radius > r1 && radius < r2 && angle < nu) { */
      /*         if (j > 0 && i > 0) result(j-1,i-1) = 1; */
      /*         if (j > 0) result(j-1,i) = 1; */
      /*         if (i > 0) result(j,i-1) = 1; */
      /*         result(j,i) = 1; */
      /*       } */
      /*     } */
        }
      }

      // Return the result
      return result;
    }

  private:

    vec3<double> s0_;
    vec3<double> m2_;
    mat3<double> A_;
    Detector detector_;
    Scan scan_;
    double s_;
    double da_;
    double w_;
  };

}}}}

#endif // DLSTBX_ALGORITHMS_PROFILE_MODEL_NAVE_PROJECTOR_H
