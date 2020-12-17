import ast
import imp
import pkgutil

import dlstbx.requirements
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
    except Exception:
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

known_wrappers = [  # please keep alphabetically sorted
    "align_crystal = dlstbx.wrapper.dlstbx_align_crystal:AlignCrystalWrapper",
    "autobuild = dlstbx.wrapper.autobuild:AutoBuildWrapper",
    "autosharp = dlstbx.wrapper.autoSHARP:autoSHARPWrapper",
    "autoproc = dlstbx.wrapper.autoPROC:autoPROCWrapper",
    "best = dlstbx.wrapper.best:BESTWrapper",
    "big_ep = dlstbx.wrapper.big_ep:BigEPWrapper",
    "big_ep_report = dlstbx.wrapper.big_ep_report:BigEPReportWrapper",
    "crank2 = dlstbx.wrapper.crank2:Crank2Wrapper",
    "dc_sim = dlstbx.wrapper.dc_sim:DCSimWrapper",
    "dimple = dlstbx.wrapper.dimple:DimpleWrapper",
    "dozor = dlstbx.wrapper.dozor:DozorWrapper",
    "edna = dlstbx.wrapper.edna:EdnaWrapper",
    "ep_predict = dlstbx.wrapper.ep_predict:EPPredictWrapper",
    "fast_dp = dlstbx.wrapper.fast_dp:FastDPWrapper",
    "fast_ep = dlstbx.wrapper.fast_ep:FastEPWrapper",
    "fast_rdp = dlstbx.wrapper.fast_rdp:FastRDPWrapper",
    "mosflm_strategy = dlstbx.wrapper.mosflm_strategy:MosflmStrategyWrapper",
    "mrbump = dlstbx.wrapper.mrbump:MrBUMPWrapper",
    "mr_predict = dlstbx.wrapper.mr_predict:MRPredictWrapper",
    "phaser_ellg  = dlstbx.wrapper.phaser_ellg:PhasereLLGWrapper",
    "relion = dlstbx.wrapper.relion:RelionWrapper",
    "rlv = dlstbx.wrapper.rlv:RLVWrapper",
    "screen19 = dlstbx.wrapper.screen19:Screen19Wrapper",
    "screen19_mx = dlstbx.wrapper.screen19_mx:Screen19MXWrapper",
    "shelxc_stats = dlstbx.wrapper.shelxc_stats:ShelxcStatsWrapper",
    "spotcounts = dlstbx.wrapper.spot_counts_per_image:SCPIWrapper",
    "stepped_transmission = dlstbx.wrapper.stepped_transmission:SteppedTransmissionWrapper",
    "timg = dlstbx.wrapper.timg:TopazWrapper",
    "topaz3 = dlstbx.wrapper.topaz3_wrapper:Topaz3Wrapper",
    "xia2 = dlstbx.wrapper.xia2:Xia2Wrapper",
    "xia2.multiplex = dlstbx.wrapper.xia2_multiplex:Xia2MultiplexWrapper",
    "xia2.strategy = dlstbx.wrapper.xia2_strategy:Xia2StrategyWrapper",
    "xia2.to_shelxcde = dlstbx.wrapper.xia2_to_shelxcde:Xia2toShelxcdeWrapper",
    "xoalign = dlstbx.wrapper.xoalign:XOalignWrapper",
]

dxtbx_formats = [
    "FormatEiger0MQDump:Format = dlstbx.format.FormatEiger0MQDump:FormatEiger0MQDump",
]

libtbx.pkg_utils.define_entry_points(
    {
        "dlstbx.wrappers": sorted(known_wrappers),
        "dxtbx.format": sorted(dxtbx_formats),
        "workflows.services": sorted(service_list),
        "zocalo.wrappers": sorted(known_wrappers),
    }
)
if dlstbx.requirements.check():
    print(
        "You can run 'python -m dlstbx.requirements' to update all packages indiscriminately."
    )
    print(
        "Note that this will overwrite any 'pip install -e' local installations you may have."
    )
