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
#include <dlstbx/algorithms/profile_model/nave/projector.h>
#include <dlstbx/algorithms/profile_model/nave/spherical_cap.h>
#include <dlstbx/algorithms/profile_model/nave/model.h>

namespace dlstbx {
namespace algorithms {
namespace profile_model {
namespace nave {
namespace boost_python {

  using namespace boost::python;

  BOOST_PYTHON_MODULE(dlstbx_algorithms_profile_model_nave_ext)
  {
    class_<SphericalCap>("SphericalCap", no_init)
      .def(init< vec3<double>, double>())
      .def("axis", &SphericalCap::axis)
      .def("radius", &SphericalCap::radius)
      .def("angle", &SphericalCap::angle)
      .def("distance", &SphericalCap::distance)
      ;

    class_<Model>("Model", no_init)
      .def(init< vec3<double>,
                 double,
                 double,
                 double >())
      .def("r", &Model::r)
      .def("s", &Model::s)
      .def("da", &Model::da)
      .def("w", &Model::w)
      .def("thickness", &Model::thickness)
      .def("rocking_width", &Model::rocking_width)
      .def("distance", &Model::distance)
      .def("inside", &Model::inside)
      ;

    class_<Projector>("Projector", no_init)
      .def(init< const Beam&,
                 const Detector&,
                 const Goniometer&,
                 const Scan&,
                 mat3<double>,
                 double,
                 double,
                 double>())
      .def("image", &Projector::image)
      ;

  }

}}}}} // namespace = dlstbx::algorithms::profile_model::nave::boost_python
