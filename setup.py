from __future__ import annotations

import os
import subprocess

from setuptools import find_packages, setup

# Version number is determined either by git revision (which takes precendence)
# or a static version number which is updated by bump2version
__version_tag__ = "1.0.dev"

console_scripts = [
    "dials.swirly_eyes=dlstbx.cli.swirly_eyes:run",
    "dlstbx.align_crystal=dlstbx.cli.align_crystal:run",
    "dlstbx.dc_sim_verify=dlstbx.cli.dc_sim_verify:run",
    "dlstbx.ep_predict_phase=dlstbx.cli.ep_predict_phase:run",
    "dlstbx.ep_predict_results=dlstbx.cli.ep_predict_results:runmain",
    "dlstbx.find_funny_eiger_frames=dlstbx.cli.find_funny_eiger_frames:run",
    "dlstbx.find_in_ispyb=dlstbx.cli.find_in_ispyb:run",
    "dlstbx.fix_cluster_jobs=dlstbx.cli.fix_cluster_jobs:run",
    "dlstbx.get_activemq_statistics=dlstbx.cli.get_activemq_statistics:run",
    "dlstbx.get_graylog_statistics=dlstbx.cli.get_graylog_statistics:run",
    "dlstbx.get_rabbitmq_statistics=dlstbx.cli.get_rabbitmq_statistics:run",
    "dlstbx.graylog=dlstbx.cli.graylog:run",
    "dlstbx.gridscan3d=dlstbx.cli.gridscan3d:run",
    "dlstbx.h5rewrite=dlstbx.cli.h5rewrite:cli",
    "dlstbx.hdf5_missing_frames=dlstbx.cli.hdf5_missing_frames:run",
    "dlstbx.mimas=dlstbx.cli.mimas:run",
    "dlstbx.mr_predict_results=dlstbx.cli.mr_predict_results:runmain",
    "dlstbx.pickup=dlstbx.cli.pickup:run",
    "dlstbx.pilatus_settings_check=dlstbx.cli.pilatus_settings_check:run",
    "dlstbx.plot_reflections=dlstbx.cli.plot_reflections:run",
    "dlstbx.queue_monitor=dlstbx.cli.queue_monitor:run",
    "dlstbx.run_dozor=dlstbx.cli.run_dozor:run",
    "dlstbx.run_health_checks=dlstbx.cli.run_health_checks:run",
    "dlstbx.run_system_tests=dlstbx.cli.run_system_tests:run",
    "dlstbx.service=dlstbx.cli.service:run",
    "dlstbx.show_recipeID=dlstbx.cli.show_recipeID:run",
    "dlstbx.status_monitor=dlstbx.cli.status_monitor:run",
    "dlstbx.trim_pdb_bfactors=dlstbx.cli.trim_pdb_bfactors:run",
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
    "autoproc = dlstbx.wrapper.autoPROC:autoPROCWrapper",
    "autoproc_setup = dlstbx.wrapper.autoPROC_setup:autoPROCSetupWrapper",
    "autoproc_run = dlstbx.wrapper.autoPROC_run:autoPROCRunWrapper",
    "autoproc_results = dlstbx.wrapper.autoPROC_results:autoPROCResultsWrapper",
    "best = dlstbx.wrapper.best:BESTWrapper",
    "big_ep_run = dlstbx.wrapper.big_ep_run:BigEPRunWrapper",
    "big_ep_setup = dlstbx.wrapper.big_ep_setup:BigEPSetupWrapper",
    "big_ep_report = dlstbx.wrapper.big_ep_report:BigEPReportWrapper",
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
    "xia2_run = dlstbx.wrapper.xia2_run:Xia2RunWrapper",
    "xia2_setup = dlstbx.wrapper.xia2_setup:Xia2SetupWrapper",
    "xia2_results = dlstbx.wrapper.xia2_results:Xia2ResultsWrapper",
    "xia2.multiplex = dlstbx.wrapper.xia2_multiplex:Xia2MultiplexWrapper",
    "xia2.strategy = dlstbx.wrapper.xia2_strategy:Xia2StrategyWrapper",
    "xia2.to_shelxcde = dlstbx.wrapper.xia2_to_shelxcde:Xia2toShelxcdeWrapper",
    "xoalign = dlstbx.wrapper.xoalign:XOalignWrapper",
]

service_list = [
    "DLSArchiver = dlstbx.services.archiver:DLSArchiver",
    "DLSBridge = dlstbx.services.bridge:DLSBridge",
    "DLSCluster = dlstbx.services.cluster:DLSCluster",
    "DLSClusterMonitor = dlstbx.services.cluster_monitor:DLSClusterMonitor",
    "DLSController = dlstbx.services.controller:DLSController",
    "DLSDispatcher = dlstbx.services.dispatcher:DLSDispatcher",
    "DLSDropfilePickup = dlstbx.services.dropfile_pickup:DLSDropfilePickup",
    "DLSFileWatcher = dlstbx.services.filewatcher:DLSFileWatcher",
    "DLSISPyB = dlstbx.services.ispybsvc:DLSISPyB",
    "DLSISPyBPIA = dlstbx.services.ispybsvc_pia:DLSISPyBPIA",
    "DLSImages = dlstbx.services.images:DLSImages",
    "DLSMimas = dlstbx.services.mimas:DLSMimas",
    "DLSMimasBacklog = dlstbx.services.mimas_backlog:DLSMimasBacklog",
    "DLSNexusParser = dlstbx.services.nexusparser:DLSNexusParser",
    "DLSNotifyGDA = dlstbx.services.notifygda:DLSNotifyGDA",
    "DLSPerImageAnalysis = dlstbx.services.per_image_analysis:DLSPerImageAnalysis",
    "DLSReverseBridge = dlstbx.services.bridge_reverse:DLSReverseBridge",
    "DLSStatistics = dlstbx.services.statistics:DLSStatistics",
    "DLSTrigger = dlstbx.services.trigger:DLSTrigger",
    "DLSValidation = dlstbx.services.validation:DLSValidation",
    "DLSXRayCentering = dlstbx.services.xray_centering:DLSXRayCentering",
    "HTCondorWatcher = dlstbx.services.htcondorwatcher:HTCondorWatcher",
    # "LoadProducer = dlstbx.services.load_producer:LoadProducer",  # tentatively disabled
    # "LoadReceiver = dlstbx.services.load_receiver:LoadReceiver",  # tentatively disabled
]

health_checks = [
    "it.filesystem = dlstbx.health_checks.filesystem:check_filesystems",
    "it.filesystem.gpfs-expulsion = dlstbx.health_checks.graylog:check_gfps_expulsion",
    "it.filesystem.responsiveness = dlstbx.health_checks.graylog:check_filesystem_is_responsive",
    "it.filesystem.space = dlstbx.health_checks.filesystem:check_free_space",
    "it.internet = dlstbx.health_checks.network:check_internet",
    "it.quota = dlstbx.health_checks.quota:check_quota",
    "remote.github = dlstbx.health_checks.network:check_github",
    "services.activemq = dlstbx.health_checks.activemq:check_activemq_health",
    "services.cas = dlstbx.health_checks.network:check_cas",
    "services.epics = dlstbx.health_checks.epics:get_diamond_ring_status",
    "services.gitlab = dlstbx.health_checks.network:check_gitlab",
    "services.graylog.alive = dlstbx.health_checks.graylog:check_graylog_is_alive",
    "services.graylog.healthy = dlstbx.health_checks.graylog:check_graylog_is_healthy",
    "services.graylog.history = dlstbx.health_checks.graylog:check_graylog_has_history",
    "services.ispyb = dlstbx.health_checks.ispyb:check_ispyb_servers",
    "services.jenkins = dlstbx.health_checks.network:check_jenkins",
    "services.jenkins.certificate = dlstbx.health_checks.jenkins:check_jenkins_certificate",
    "services.jira = dlstbx.health_checks.network:check_jira",
    "services.mx.agamemnon = dlstbx.health_checks.network:check_agamemnon",
    "services.mx.dbserver = dlstbx.health_checks.network:check_dbserver",
    "services.mx.amqrmqbridge = dlstbx.health_checks.activemq_rabbitmq_migration:check_PIA_bridge_runs",
    "services.rabbitmq = dlstbx.health_checks.rabbitmq:check_rabbitmq_health",
    "services.synchweb = dlstbx.health_checks.network:check_synchweb",
    "services.uas = dlstbx.health_checks.network:check_uas",
    "services.zocalo.stash = dlstbx.health_checks.zocalo:check_zocalo_stash",
    "vmxi.hold = dlstbx.health_checks.filesystem:check_vmxi_holding_area",
    "zocalo.dlq.activemq = dlstbx.health_checks.activemq:check_activemq_dlq",
    "zocalo.dlq.rabbitmq = dlstbx.health_checks.rabbitmq:check_rabbitmq_dlq",
]

mimas_scenario_handlers = [
    "cloud = dlstbx.mimas.cloud:handle_cloud",
    "eiger_screening = dlstbx.mimas.core:handle_eiger_screening",
    "eiger_start = dlstbx.mimas.core:handle_eiger_start",
    "eiger_end = dlstbx.mimas.core:handle_eiger_end",
    "i19_pilatus_start = dlstbx.mimas.i19:handle_i19_start_pilatus",
    "i19_pilatus_end = dlstbx.mimas.i19:handle_i19_end_pilatus",
    "i19_eiger_start = dlstbx.mimas.i19:handle_i19_start_eiger",
    "i19_eiger_end = dlstbx.mimas.i19:handle_i19_end_eiger",
    "i19_end = dlstbx.mimas.i19:handle_i19_end",
    "pilatus_end = dlstbx.mimas.core:handle_pilatus_end",
    "pilatus_gridscan_start = dlstbx.mimas.core:handle_pilatus_gridscan_start",
    "pilatus_not_gridscan_start = dlstbx.mimas.core:handle_pilatus_not_gridscan_start",
    "pilatus_screening = dlstbx.mimas.core:handle_pilatus_screening",
    "rotation_end = dlstbx.mimas.core:handle_rotation_end",
    "vmxi_end = dlstbx.mimas.vmxi:handle_vmxi_end",
    "vmxi_gridscan = dlstbx.mimas.vmxi:handle_vmxi_gridscan",
    "vmxi_rotation = dlstbx.mimas.vmxi:handle_vmxi_rotation_scan",
    "vmxi_start = dlstbx.mimas.vmxi:handle_vmxi_start",
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
        "libtbx.dispatcher.script": [
            "%s=%s" % (x.split("=")[0], x.split("=")[0]) for x in console_scripts
        ]
        + [f"{x}={x}" for x in swirltbx_hacks],
        "libtbx.precommit": ["dlstbx=dlstbx"],
        "workflows.services": sorted(service_list),
        "zocalo.health_checks": sorted(health_checks),
        "zocalo.services.images.plugins": [
            "diffraction = dlstbx.services.images:diffraction",
            "thumbnail = dlstbx.services.images:thumbnail",
        ],
        "zocalo.wrappers": sorted(known_wrappers),
        "zocalo.mimas.handlers": sorted(mimas_scenario_handlers),
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
