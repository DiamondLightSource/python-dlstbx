from __future__ import absolute_import, division, print_function

import ast
import imp
import pkgutil

import dlstbx.services
import libtbx.pkg_utils

try:
    from dlstbx.util.version import dlstbx_version

    print(dlstbx_version())
except Exception:
    pass

try:
    import dials.precommitbx.nagger

    dials.precommitbx.nagger.nag()
except ImportError:
    pass

# --- workflows service registration exploration ---

print("Enumerating workflow services:")
service_list = []
service_list.append("DLSISPyBPIA = dlstbx.services.ispybsvc_pia:DLSISPyBPIA")
for _, name, _ in pkgutil.iter_modules(dlstbx.services.__path__):
    if name.startswith("test_") or name.startswith("_"):
        continue
    try:
        fid, pathname, desc = imp.find_module(name, dlstbx.services.__path__)
    except Exception:
        fid = None
    if not fid:
        print("  *** Could not read %s" % name)
        continue
    if desc[0] == ".pyc":
        print("  *** %s only present in compiled form, ignoring" % name)
        continue
    content = fid.read()
    fid.close()
    try:
        parsetree = ast.parse(content)
    except Exception as e:
        print("  *** Could not parse %s" % name)
        continue
    for top_level_def in parsetree.body:
        if not isinstance(top_level_def, ast.ClassDef):
            continue
        base_names = [
            baseclass.id
            for baseclass in top_level_def.bases
            if isinstance(baseclass, ast.Name)
        ]
        if "CommonService" in base_names:
            classname = top_level_def.name
            service_list.append(
                "{classname} = dlstbx.services.{modulename}:{classname}".format(
                    classname=classname, modulename=name
                )
            )
            print("  found", classname)
libtbx.pkg_utils.define_entry_points(
    {
        "workflows.services": sorted(service_list),
        "dlstbx.wrappers": sorted(
            [
                "align_crystal = dlstbx.zocalo.wrapper.dlstbx_align_crystal:AlignCrystalWrapper",
                "autoproc = dlstbx.zocalo.wrapper.autoPROC:autoPROCWrapper",
                "big_ep = dlstbx.zocalo.wrapper.big_ep:BigEPWrapper",
                "dc_sim = dlstbx.zocalo.wrapper.dc_sim:DCSimWrapper",
                "dozor = dlstbx.zocalo.wrapper.dozor:DozorWrapper",
                "edna = dlstbx.zocalo.wrapper.edna:EdnaWrapper",
                "fast_dp = dlstbx.zocalo.wrapper.fast_dp:FastDPWrapper",
                "fast_rdp = dlstbx.zocalo.wrapper.fast_rdp:FastRDPWrapper",
                "fast_ep = dlstbx.zocalo.wrapper.fast_ep:FastEPWrapper",
                "dimple = dlstbx.zocalo.wrapper.dimple:DimpleWrapper",
                "anode = dlstbx.zocalo.wrapper.anode:AnodeWrapper",
                "i19screen = dlstbx.zocalo.wrapper.i19screen:I19ScreenWrapper",
                "stepped_transmission = dlstbx.zocalo.wrapper.stepped_transmission:SteppedTransmissionWrapper",
                "mosflm_strategy = dlstbx.zocalo.wrapper.mosflm_strategy:MosflmStrategyWrapper",
                "rlv = dlstbx.zocalo.wrapper.rlv:RLVWrapper",
                "xia2.multiplex = dlstbx.zocalo.wrapper.xia2_multiplex:Xia2MultiplexWrapper",
                "spotcounts = dlstbx.zocalo.wrapper.spot_counts_per_image:SCPIWrapper",
                "xia2 = dlstbx.zocalo.wrapper.xia2:Xia2Wrapper",
                "xia2.strategy = dlstbx.zocalo.wrapper.xia2_strategy:Xia2StrategyWrapper",
                "xoalign = dlstbx.zocalo.wrapper.xoalign:XOalignWrapper",
            ]
        ),
    }
)
