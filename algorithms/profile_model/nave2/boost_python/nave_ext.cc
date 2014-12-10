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
                cctbx::miller::index<>,
                vec3<double>,
                vec3<double> >())
      .def("D", &Model::D)
      .def("A", &Model::A)
      .def("A1", &Model::A1)
      .def("s0", &Model::s0)
      .def("m2", &Model::m2)
      .def("h", &Model::h)
      .def("sigma_inv", &Model::sigma_inv)
      .def("R", &Model::R)
      .def("AR", &Model::AR)
      .def("h_frac", &Model::h_frac)
      .def("Dm", &Model::Dm)
      .def("P", &Model::P)
      ;
  }

}}}}} // namespace = dlstbx::algorithms::profile_model::nave::boost_python
