/*
 * nave_ext.cc
 *
 *  Copyright (C) 2013 Diamond Light Source
 *
 *  Author: James Parkhurst
 *
 *  This code is distributed under the BSD license, a copy of which is
 *  included in the root directory of this package.
 */
#include <boost/python.hpp>
#include <boost/python/def.hpp>
#include <dials/array_family/reflection_table.h>
#include <dlstbx/algorithms/profile_model/nave2/model.h>
#include <dlstbx/algorithms/profile_model/nave2/support.h>

namespace dlstbx {
namespace algorithms {
namespace profile_model {
namespace nave {
namespace boost_python {

  using namespace boost::python;

  BOOST_PYTHON_MODULE(dlstbx_algorithms_profile_model_nave2_ext)
  {
    class_<Model>("Model", no_init)
      .def(init<mat3<double>,
                mat3<double>,
                vec3<double>,
                vec3<double>,
                vec3<double>,
                double,
                vec3<double>,
                vec3<double>,
                vec3<double> >())
      .def("D", &Model::D)
      .def("D1", &Model::D1)
      .def("A", &Model::A)
      .def("s0", &Model::s0)
      .def("m2", &Model::m2)
      .def("s1", &Model::s1)
      .def("phi0", &Model::phi0)
      .def("rlp", &Model::rlp)
      .def("sigma", &Model::sigma)
      .def("sigma_inv", &Model::sigma_inv)
      .def("R", &Model::R)
      .def("r", &Model::r)
      .def("Dm", &Model::Dm)
      .def("P", &Model::P)
      ;

    class_<Support>("Support", no_init)
      .def(init<const Beam&,
                const Detector&,
                const Goniometer&,
                const Scan&,
                const mat3<double>&,
                const vec3<double>&,
                const vec3<double>&,
                const vec3<double>&,
                double>())
      .def("compute_bbox", &Support::compute_bbox)
      .def("compute_mask", &Support::compute_mask)
      .def("compute_prof", &Support::compute_prof)
      .def("compute_image_mask", &Support::compute_image_mask)
      ;
  }

}}}}} // namespace = dlstbx::algorithms::profile_model::nave::boost_python
