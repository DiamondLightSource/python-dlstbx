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
#include <dlstbx/algorithms/profile_model/nave/projector.h>
#include <dlstbx/algorithms/profile_model/nave/spherical_cap.h>
#include <dlstbx/algorithms/profile_model/nave/model.h>
#include <dlstbx/algorithms/profile_model/nave/profile_model_support.h>

namespace dlstbx {
namespace algorithms {
namespace profile_model {
namespace nave {
namespace boost_python {

  using dials::model::Shoebox;

  using namespace boost::python;

  void profile_model_support_compute_partiality(
      const ProfileModelSupport &self,
      af::reflection_table data) {

    // Check input
    DIALS_ASSERT(data.contains("s1"));
    DIALS_ASSERT(data.contains("xyzcal.mm"));
    DIALS_ASSERT(data.contains("d"));

    // Get exisiting columns
    af::const_ref< vec3<double> > s1 = data["s1"];
    af::const_ref< vec3<double> > xyz = data["xyzcal.mm"];
    af::const_ref< int6 > bbox = data["bbox"];

    // Create new column
    af::ref< double > partiality = data["partiality"];

    // Compute all values
    for (std::size_t i = 0; i < partiality.size(); ++i) {
      partiality[i] = self.compute_partiality(
          s1[i],
          xyz[i][2],
          bbox[i]);
    }
  }

  void profile_model_support_compute_bbox(
      const ProfileModelSupport &self,
      af::reflection_table data) {

    // Check input
    DIALS_ASSERT(data.contains("panel"));
    DIALS_ASSERT(data.contains("s1"));
    DIALS_ASSERT(data.contains("xyzcal.mm"));

    // Get exisiting columns
    af::const_ref< std::size_t > panel = data["panel"];
    af::const_ref< vec3<double> > s1 = data["s1"];
    af::const_ref< vec3<double> > xyz = data["xyzcal.mm"];

    // Create new column
    af::ref< int6 > bbox = data["bbox"];

    // Compute all values
    for (std::size_t i = 0; i < bbox.size(); ++i) {
      bbox[i] = self.compute_bbox(
          panel[i],
          s1[i],
          xyz[i][2]);
    }
  }

  void profile_model_support_compute_mask(
      const ProfileModelSupport &self,
      af::reflection_table data) {

    // Check input
    DIALS_ASSERT(data.contains("panel"));
    DIALS_ASSERT(data.contains("s1"));
    DIALS_ASSERT(data.contains("xyzcal.mm"));
    DIALS_ASSERT(data.contains("bbox"));
    DIALS_ASSERT(data.contains("shoebox"));

    // Get exisiting columns
    af::const_ref< std::size_t > panel = data["panel"];
    af::const_ref< vec3<double> > s1 = data["s1"];
    af::const_ref< vec3<double> > xyz = data["xyzcal.mm"];
    af::const_ref< int6> bbox = data["bbox"];
    af::ref< Shoebox<> > shoebox = data["shoebox"];

    // Compute all values
    for (std::size_t i = 0; i < bbox.size(); ++i) {
      self.compute_mask(
          panel[i],
          s1[i],
          xyz[i][2],
          bbox[i],
          shoebox[i].mask.ref());
    }
  }

  BOOST_PYTHON_MODULE(dlstbx_algorithms_profile_model_nave_ext)
  {
    class_<SphericalCap>("SphericalCap", no_init)
      .def(init< vec3<double>, double>())
      .def("axis", &SphericalCap::axis)
      .def("radius", &SphericalCap::radius)
      .def("angle", &SphericalCap::angle)
      .def("distance", &SphericalCap::distance)
      .def("h1", &SphericalCap::h1)
      .def("h2", &SphericalCap::h2)
      .def("a", &SphericalCap::a)
      .def("inclination", &SphericalCap::inclination)
      ;

    class_<Model>("Model", no_init)
      .def(init< vec3<double>,
                 vec3<double>,
                 vec3<double>,
                 double,
                 double,
                 double,
                 double >())
      .def("s0", &Model::s0)
      .def("m2", &Model::m2)
      .def("s1", &Model::s1)
      .def("e1", &Model::e1)
      .def("e2", &Model::e2)
      .def("e3", &Model::e3)
      .def("r", &Model::r)
      .def("phi", &Model::phi)
      .def("zeta", &Model::zeta)
      .def("s", &Model::s)
      .def("da", &Model::da)
      .def("w", &Model::w)
      .def("thickness", &Model::thickness)
      .def("rocking_width", &Model::rocking_width)
      .def("distance", &Model::distance)
      .def("inside", &Model::inside)
      .def("inside2", &Model::inside2)
      .def("phi_range", &Model::phi_range)
      .def("z0", &Model::z0)
      .def("z1", &Model::z1)
      .def("intensity_fraction", &Model::intensity_fraction)
      .def("ewald_intersection_angles", &Model::ewald_intersection_angles)
      .def("minimum_box", &Model::minimum_box)
      .def("equation", &Model::equation)
      .def("parametric", &Model::parametric)
      ;

    class_<ProfileModelSupport>("ProfileModelSupport", no_init)
      .def(init< const Beam&,
                 const Detector&,
                 const Goniometer&,
                 const Scan&,
                 double,
                 double,
                 double>())
      .def("compute_partiality",
          &profile_model_support_compute_partiality)
      .def("compute_bbox",
          &profile_model_support_compute_bbox)
      .def("compute_mask",
          &profile_model_support_compute_mask)
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
