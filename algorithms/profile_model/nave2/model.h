

#ifndef DLSTBX_ALGORITHMS_PROFILE_MODEL_NAVE_MODEL_H
#define DLSTBX_ALGORITHMS_PROFILE_MODEL_NAVE_MODEL_H

#include <scitbx/vec3.h>
#include <scitbx/mat3.h>
#include <scitbx/math/r3_rotation.h>
#include <cctbx/miller.h>

namespace dlstbx { namespace algorithms {

  using scitbx::vec3;
  using scitbx::mat3;
  using scitbx::math::r3_rotation::axis_and_angle_as_matrix;

  class Model {
  public:

    Model(mat3<double> D,
          mat3<double> A,
          vec3<double> s0,
          vec3<double> m2,
          cctbx::miller::index<> h,
          vec3<double> sig_s,
          vec3<double> sig_da)
      : D_(D),
        A_(A),
        A1_(A.inverse()),
        s0_(s0),
        m2_(m2.normalize()),
        h_(h) {
      DIALS_ASSERT(s0_.length() > 0);
      DIALS_ASSERT(sig_s.const_ref().all_ge(0));
      DIALS_ASSERT(sig_da.const_ref().all_ge(0));
      double siga2 = sig_s[0]*sig_s[0] + h[0]*h[0]*sig_da[0]*sig_da[0];
      double sigb2 = sig_s[1]*sig_s[1] + h[1]*h[1]*sig_da[1]*sig_da[1];
      double sigc2 = sig_s[2]*sig_s[2] + h[2]*h[1]*sig_da[2]*sig_da[2];
      sigma_inv_ = mat3<double>(
          siga2, 0, 0,
          0, sigb2, 0,
          0, 0, sigc2).inverse();
    }

    mat3<double> D() const {
      return D_;
    }

    mat3<double> A() const {
      return A_;
    }

    mat3<double> A1() const {
      return A1_;
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

    mat3<double> sigma_inv() const {
      return sigma_inv_;
    }

    mat3<double> R(double phi) const {
      return axis_and_angle_as_matrix(m2_, phi); 
    }

    mat3<double> AR(double phi) const {
      return A1_ * R(phi).transpose();
    }

    vec3<double> h_frac(double x, double y, double phi) const {
      vec3<double> v = D_ * vec3<double>(x, y, 1.0);
      double slen = s0_.length();
      double vlen = v.length();
      DIALS_ASSERT(vlen > 0);
      return AR(phi) * (v * slen / vlen - s0_);
    }

    double Dm(double x, double y, double phi) const {
      vec3<double> h(h_[0], h_[1], h_[2]);
      vec3<double> dh = h_frac(x, y, phi) - h;
      return dh * sigma_inv_ * dh;
    }

    double P(double x, double y, double phi) const {
      return std::exp(-0.5 * Dm(x, y, phi)); 
    }

  private:

    mat3<double> D_;
    mat3<double> A_;
    mat3<double> A1_;
    vec3<double> s0_;
    vec3<double> m2_;
    cctbx::miller::index<> h_;
    mat3<double> sigma_inv_;
  };

}}

#endif // DLSTBX_ALGORITHMS_PROFILE_MODEL_NAVE_MODEL_H
