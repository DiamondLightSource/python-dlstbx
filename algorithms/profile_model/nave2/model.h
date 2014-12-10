

#ifndef DLSTBX_ALGORITHMS_PROFILE_MODEL_NAVE_MODEL_H
#define DLSTBX_ALGORITHMS_PROFILE_MODEL_NAVE_MODEL_H

#include <scitbx/vec3.h>
#include <scitbx/mat3.h>
#include <scitbx/math/r3_rotation.h>
#include <scitbx/array_family/small.h>
#include <cctbx/miller.h>
#include <dials/algorithms/spot_prediction/rotation_angles.h>
#include <dxtbx/model/ray_intersection.h>

namespace af = scitbx::af;

namespace dlstbx { namespace algorithms {

  using scitbx::vec2;
  using scitbx::vec3;
  using scitbx::mat3;
  using scitbx::math::r3_rotation::axis_and_angle_as_matrix;
  using dxtbx::model::plane_ray_intersection;
  using dials::algorithms::rotation_angles;

  class Model {
  public:

    Model(mat3<double> D,
          mat3<double> A,
          vec3<double> s0,
          vec3<double> m2,
          cctbx::miller::index<> h,
          vec3<double> sig_s,
          vec3<double> sig_da,
          vec3<double> sig_w)
      : D_(D),
        D1_(D.inverse()),
        A_(A),
        s0_(s0),
        m2_(m2.normalize()),
        h_(h),
        rlp_(A * h),
        phi0_(rotation_angles(s0_, m2_, rlp_)),
        s1_entr_(s0_ + rlp_.unit_rotate_around_origin(m2_, phi0_[0])),
        s1_exit_(s0_ + rlp_.unit_rotate_around_origin(m2_, phi0_[1])) {
      DIALS_ASSERT(s0_.length() > 0);
      DIALS_ASSERT(sig_s.const_ref().all_ge(0));
      DIALS_ASSERT(sig_da.const_ref().all_ge(0));

      // The covariance matrix for the mosaic block size component in the
      // reciprocal lattice coordinate system
      mat3<double> sigma_s(
        sig_s[0]*sig_s[0], 0, 0,
        0, sig_s[1]*sig_s[1], 0,
        0, 0, sig_s[2]*sig_s[2]
      );
      
      // The covariance matrix for the spread in unit cell size component in the
      // reciprocal lattice coordinate system
      mat3<double> sigma_a(
        h[0]*h[0]*sig_da[0]*sig_da[0], 0, 0,
        0, h[1]*h[1]*sig_da[1]*sig_da[1], 0,
        0, 0, h[2]*h[1]*sig_da[2]*sig_da[2]
      );

      // Compute the covariance in orthognal coordinate system
      sigma_s = A * sigma_s * A.transpose();
      sigma_a = A * sigma_a * A.transpose();

      // Select two vectors orthogonal to the rlp
      vec3<double> rn = rlp_.normalize();
      vec3<double> v1 = std::abs(rn[0]) > std::abs(rn[2])
        ? vec3<double>(-rn[1], rn[0], 0.0).normalize()
        : vec3<double>(0.0, -rn[2], rn[1]).normalize();
      vec3<double> v2 = rn.cross(v1).normalize();
      vec3<double> v3 = rn.cross(v2).normalize();
      
      // Construct an eigenvector matrix
      mat3<double> U(
        v2[0], v3[0], rn[0],
        v2[1], v3[1], rn[1],
        v2[2], v3[2], rn[2]
      );

      // Compute the angular spread
      double w = ((A * mat3<double>(
          sig_w[0], 0, 0,
          0, sig_w[1], 0,
          0, 0, sig_w[2])) * rlp_).length();

      // Construct an eigenvalue matrix
      mat3<double> V(
        w*w, 0, 0,
        0, w*w, 0,
        0,   0, 0
      );
      
      // Compute the covariance matrix for the angular spread of mosaic blocks
      // in the orthogonal lab coordinate system using the eigenvectors and
      // eigenvalues to produce a 2d gaussian in the plane normal to the rlp. 
      mat3<double> sigma_w = U*V*U.transpose();

      // The full covariance matrix and its inverse
      sigma_ = sigma_s + sigma_a + sigma_w;
      sigma_inv_ = sigma_.inverse();
    }

    mat3<double> D() const {
      return D_;
    }

    mat3<double> D1() const {
      return D1_;
    }

    mat3<double> A() const {
      return A_;
    }

    vec3<double> s0() const {
      return s0_;
    }

    vec3<double> m2() const {
      return m2_;
    }

    cctbx::miller::index<> h() const {
      return h_;
    }
    
    vec3<double> rlp() const {
      return rlp_;
    }
    
    vec2<double> phi0_entering() const {
      return phi0_;
    }
    
    vec2<double> phi0_exiting() const {
      return phi0_;
    }

    vec3<double> s1_entering() const {
      return s1_entr_; 
    }

    vec3<double> s1_exiting() const {
      return s1_exit_; 
    }

    mat3<double> sigma() const {
      return sigma_;
    }

    mat3<double> sigma_inv() const {
      return sigma_inv_;
    }

    mat3<double> R(double phi) const {
      return axis_and_angle_as_matrix(m2_, phi); 
    }

    vec3<double> h_frac(double x, double y, double phi) const {
      vec3<double> v = D_ * vec3<double>(x, y, 1.0);
      double slen = s0_.length();
      double vlen = v.length();
      DIALS_ASSERT(vlen > 0);
      return R(phi).transpose() * (v * slen / vlen - s0_);
    }

    double Dm(double x, double y, double phi) const {
      vec3<double> dh = h_frac(x, y, phi) - rlp_;
      return dh * sigma_inv_ * dh;
    }

    double P(double x, double y, double phi) const {
      return std::exp(-0.5 * Dm(x, y, phi)); 
    }

  private:

    mat3<double> D_;
    mat3<double> D1_;
    mat3<double> A_;
    vec3<double> s0_;
    vec3<double> m2_;
    cctbx::miller::index<> h_;
    vec3<double> rlp_;
    vec2<double> phi0_;
    vec3<double> s1_entr_;
    vec3<double> s1_exit_;
    mat3<double> sigma_;
    mat3<double> sigma_inv_;
  };

}}

#endif // DLSTBX_ALGORITHMS_PROFILE_MODEL_NAVE_MODEL_H
