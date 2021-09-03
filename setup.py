import os
import subprocess

from setuptools import find_packages, setup

# Version number is determined either by git revision (which takes precendence)
# or a static version number which is updated by bump2version
__version_tag__ = "1.0.dev"

console_scripts = [
    "dials.swirly_eyes=dlstbx.cli.swirly_eyes:run",
    "dlstbx.align_crystal=dlstbx.cli.align_crystal:run",
    "dlstbx.create_monitoring_table=dlstbx.cli.create_cluster_monitor_table:run",
    "dlstbx.cluster_monitor_server=dlstbx.cluster_mobitor.server:run",
    "dlstbx.dc_sim_verify=dlstbx.cli.dc_sim_verify:run",
    "dlstbx.dlq_check=dlstbx.cli.dlq_check:run",
    "dlstbx.dlq_purge=dlstbx.cli.dlq_purge:run",
    "dlstbx.dlq_reinject=dlstbx.cli.dlq_reinject:run",
    "dlstbx.ep_predict_phase=dlstbx.cli.ep_predict_phase:run",
    "dlstbx.ep_predict_results=dlstbx.cli.ep_predict_results:runmain",
    "dlstbx.find_funny_eiger_frames=dlstbx.cli.find_funny_eiger_frames:run",
    "dlstbx.find_in_ispyb=dlstbx.cli.find_in_ispyb:run",
    "dlstbx.fix_cluster_jobs=dlstbx.cli.fix_cluster_jobs:run",
    "dlstbx.get_activemq_statistics=dlstbx.cli.get_activemq_statistics:run",
    "dlstbx.get_graylog_statistics=dlstbx.cli.get_graylog_statistics:run",
    "dlstbx.go=dlstbx.cli.go:run",
    "dlstbx.graylog=dlstbx.cli.graylog:run",
    "dlstbx.gridscan3d=dlstbx.cli.gridscan3d:run",
    "dlstbx.h5rewrite=dlstbx.cli.h5rewrite:cli",
    "dlstbx.hdf5_missing_frames=dlstbx.cli.hdf5_missing_frames:run",
    "dlstbx.last_data_collections_on=dlstbx.cli.last_data_collections_on:main",
    "dlstbx.log=dlstbx.cli.log:run",
    "dlstbx.mimas=dlstbx.cli.mimas:run",
    "dlstbx.monitor_beamline=dlstbx.cli.monitor_beamline:run",
    "dlstbx.mr_predict_results=dlstbx.cli.mr_predict_results:runmain",
    "dlstbx.pickup=dlstbx.cli.pickup:run",
    "dlstbx.pilatus_settings_check=dlstbx.cli.pilatus_settings_check:run",
    "dlstbx.plot_reflections=dlstbx.cli.plot_reflections:run",
    "dlstbx.prometheus_job_register=dlstbx.cli.prometheus_cluster_capture:run",
    "dlstbx.queue_drain=dlstbx.cli.queue_drain:run",
    "dlstbx.queue_monitor=dlstbx.cli.queue_monitor:run",
    "dlstbx.queue_monitor_rmq=dlstbx.cli.queue_monitor_rmq:run",
    "dlstbx.run_dozor=dlstbx.cli.run_dozor:run",
    "dlstbx.run_health_checks=dlstbx.cli.run_health_checks:run",
    "dlstbx.run_system_tests=dlstbx.cli.run_system_tests:run",
    "dlstbx.service=dlstbx.cli.service:run",
    "dlstbx.show_recipeID=dlstbx.cli.show_recipeID:run",
    "dlstbx.shutdown=dlstbx.cli.shutdown:run",
    "dlstbx.status_monitor=dlstbx.cli.status_monitor:run",
    "dlstbx.trim_pdb_bfactors=dlstbx.cli.trim_pdb_bfactors:run",
    "dlstbx.tumbleweed=dlstbx.cli.tumbleweed:run",
    "dlstbx.version=dlstbx.cli.version:run",
    "dlstbx.wrap=dlstbx.cli.wrap:run",
    "dlstbx.wrap_fast_dp=dlstbx.cli.wrap_fast_dp:main",
    "dlstbx.wrap_multi_xia2=dlstbx.cli.wrap_multi_xia2:main",
    "eiger2xds=dlstbx.cli.eiger2xds:run",
    "em.visits=dlstbx.cli.em_visits:run",
    "em.running=dlstbx.cli.em_running:run",
    "i19.tail=dlstbx.cli.i19_tail:run",
    "it.status=dlstbx.cli.it_status:run",
]

# Console scripts that will have libtbx dispatchers generated in the release
swirltbx_hacks = [
    "ispyb.job",
    "ispyb.last_data_collections_on",
]

known_wrappers = [  # please keep alphabetically sorted
    "align_crystal = dlstbx.wrapper.dlstbx_align_crystal:AlignCrystalWrapper",
    "AlphaFold = dlstbx.wrapper.alphafold:AlphaFoldWrapper",
    "AutoBuild = dlstbx.wrapper.autobuild:AutoBuildWrapper",
    "autoproc = dlstbx.wrapper.autoPROC:autoPROCWrapper",
    "autoSHARP = dlstbx.wrapper.autoSHARP:autoSHARPWrapper",
    "best = dlstbx.wrapper.best:BESTWrapper",
    "big_ep = dlstbx.wrapper.big_ep:BigEPWrapper",
    "big_ep_report = dlstbx.wrapper.big_ep_report:BigEPReportWrapper",
    "Crank2 = dlstbx.wrapper.crank2:Crank2Wrapper",
    "dc_sim = dlstbx.wrapper.dc_sim:DCSimWrapper",
    "dimple = dlstbx.wrapper.dimple:DimpleWrapper",
    "dozor = dlstbx.wrapper.dozor:DozorWrapper",
    "edna = dlstbx.wrapper.edna:EdnaWrapper",
    "ep_predict = dlstbx.wrapper.ep_predict:EPPredictWrapper",
    "fast_dp = dlstbx.wrapper.fast_dp:FastDPWrapper",
    "fast_ep = dlstbx.wrapper.fast_ep:FastEPWrapper",
    "fast_rdp = dlstbx.wrapper.fast_rdp:FastRDPWrapper",
    "mosflm_strategy = dlstbx.wrapper.mosflm_strategy:MosflmStrategyWrapper",
    "mr_predict = dlstbx.wrapper.mr_predict:MRPredictWrapper",
    "mrbump = dlstbx.wrapper.mrbump:MrBUMPWrapper",
    "phaser_ellg  = dlstbx.wrapper.phaser_ellg:PhasereLLGWrapper",
    "rlv = dlstbx.wrapper.rlv:RLVWrapper",
    "screen19 = dlstbx.wrapper.screen19:Screen19Wrapper",
    "screen19_mx = dlstbx.wrapper.screen19_mx:Screen19MXWrapper",
    "shelxc_stats = dlstbx.wrapper.shelxc_stats:ShelxcStatsWrapper",
    "spotcounts = dlstbx.wrapper.spot_counts_per_image:SCPIWrapper",
    "stepped_transmission = dlstbx.wrapper.stepped_transmission:SteppedTransmissionWrapper",
    # "timg = dlstbx.wrapper.timg:TopazWrapper",  # tentatively disabled
    "topaz3 = dlstbx.wrapper.topaz3_wrapper:Topaz3Wrapper",
    "xia2 = dlstbx.wrapper.xia2:Xia2Wrapper",
    "xia2.multiplex = dlstbx.wrapper.xia2_multiplex:Xia2MultiplexWrapper",
    "xia2.strategy = dlstbx.wrapper.xia2_strategy:Xia2StrategyWrapper",
    "xia2.to_shelxcde = dlstbx.wrapper.xia2_to_shelxcde:Xia2toShelxcdeWrapper",
    "xoalign = dlstbx.wrapper.xoalign:XOalignWrapper",
]

dxtbx_formats = [
    "FormatNXmx:FormatNexus = dlstbx.format.FormatNXmx:FormatNXmx",
    "FormatNXmxDLS:FormatNXmx = dlstbx.format.FormatNXmxDLS:FormatNXmxDLS",
]

service_list = [
    "DLSArchiver = dlstbx.services.archiver:DLSArchiver",
    "DLSCluster = dlstbx.services.cluster:DLSCluster",
    "DLSClusterMonitor = dlstbx.services.cluster_monitor:DLSClusterMonitor",
    "DLSController = dlstbx.services.controller:DLSController",
    "DLSDispatcher = dlstbx.services.dispatcher:DLSDispatcher",
    "DLSDropfilePickup = dlstbx.services.dropfile_pickup:DLSDropfilePickup",
    "DLSFileWatcher = dlstbx.services.filewatcher:DLSFileWatcher",
    "DLSISPyB = dlstbx.services.ispybsvc:DLSISPyB",
    "DLSISPyBPIA = dlstbx.services.ispybsvc_pia:DLSISPyBPIA",
    "DLSImages = dlstbx.services.images:DLSImages",
    "DLSMailer = dlstbx.services.mailer:DLSMailer",
    "DLSMimas = dlstbx.services.mimas:DLSMimas",
    "DLSMimasBacklog = dlstbx.services.mimas_backlog:DLSMimasBacklog",
    "DLSNexusParser = dlstbx.services.nexusparser:DLSNexusParser",
    "DLSNotifyGDA = dlstbx.services.notifygda:DLSNotifyGDA",
    "DLSPerImageAnalysis = dlstbx.services.per_image_analysis:DLSPerImageAnalysis",
    "DLSStatistics = dlstbx.services.statistics:DLSStatistics",
    "DLSTrigger = dlstbx.services.trigger:DLSTrigger",
    "DLSValidation = dlstbx.services.validation:DLSValidation",
    "DLSXRayCentering = dlstbx.services.xray_centering:DLSXRayCentering",
    "DLSBridge = dlstbx.services.bridge:DLSBridge",
    "DLSReverseBridge = dlstbx.services.bridge_reverse:DLSReverseBridge",
    # "LoadProducer = dlstbx.services.load_producer:LoadProducer",  # tentatively disabled
    # "LoadReceiver = dlstbx.services.load_receiver:LoadReceiver",  # tentatively disabled
]

health_checks = [
    "it.filesystem.gpfs-expulsion = dlstbx.health_checks.graylog:check_gfps_expulsion",
    "it.filesystem.responsiveness = dlstbx.health_checks.graylog:check_filesystem_is_responsive",
    "services.dls.cas = dlstbx.health_checks.network:check_cas",
    "services.dls.epics = dlstbx.health_checks.epics:get_diamond_ring_status",
    "services.dls.gitlab = dlstbx.health_checks.network:check_gitlab",
    "services.dls.jira = dlstbx.health_checks.network:check_jira",
    "services.dls.synchweb = dlstbx.health_checks.network:check_synchweb",
    "services.dls.uas = dlstbx.health_checks.network:check_uas",
    "services.graylog.alive = dlstbx.health_checks.graylog:check_graylog_is_alive",
    "services.graylog.healthy = dlstbx.health_checks.graylog:check_graylog_is_healthy",
    "services.graylog.history = dlstbx.health_checks.graylog:check_graylog_has_history",
    "services.mx.agamemnon = dlstbx.health_checks.network:check_agamemnon",
    "services.mx.dbserver = dlstbx.health_checks.network:check_dbserver",
    "services.zocalo.stash = dlstbx.health_checks.zocalo:check_zocalo_stash",
    "dls.network.internet = dlstbx.health_checks.network:check_internet",
    "zocalo.dlq.activemq = dlstbx.health_checks.activemq:check_activemq_dlq",
]


def get_git_revision():
    """Try to obtain the current git revision number"""
    xia2_root_path = os.path.split(os.path.realpath(__file__))[0]

    if not os.path.exists(os.path.join(xia2_root_path, ".git")):
        return None

    try:
        result = subprocess.run(
            ("git", "describe", "--long"),
            check=True,
            cwd=xia2_root_path,
            encoding="latin-1",
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
        version = result.stdout.rstrip()
    except Exception:
        return None
    if version.startswith("v"):
        version = version[1:].replace(".0-", ".")

    try:
        result = subprocess.run(
            ("git", "describe", "--contains", "--all", "HEAD"),
            check=True,
            cwd=xia2_root_path,
            encoding="latin-1",
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
        branch = result.stdout.rstrip()
        if branch != "" and branch != "master" and not branch.endswith("/master"):
            version = version + "-" + branch
    except Exception:
        pass

    return version


setup(
    install_requires=[
        "procrunner",
    ],
    entry_points={
        "console_scripts": sorted(console_scripts),
        "dxtbx.format": sorted(dxtbx_formats),
        "libtbx.dispatcher.script": [
            "%s=%s" % (x.split("=")[0], x.split("=")[0]) for x in console_scripts
        ]
        + ["%s=%s" % (x, x) for x in swirltbx_hacks],
        "libtbx.precommit": ["dlstbx=dlstbx"],
        "workflows.services": sorted(service_list),
        "zocalo.health_checks": sorted(health_checks),
        "zocalo.services.images.plugins": [
            "diffraction = dlstbx.services.images:diffraction",
            "thumbnail = dlstbx.services.images:thumbnail",
        ],
        "zocalo.wrappers": sorted(known_wrappers),
    },
    packages=find_packages("src"),
    package_dir={"": "src"},
    data_files=[("dlstbx", ["libtbx_refresh.py"])],
    test_suite="tests",
    tests_require=[
        "pytest>=3.1",
        "pytest-mock",
    ],
    version=get_git_revision() or __version_tag__,
)
