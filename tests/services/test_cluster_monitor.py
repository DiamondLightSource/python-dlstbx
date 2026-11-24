from __future__ import annotations

from unittest import mock

import pytest
import zocalo.configuration

from dlstbx.services.cluster_monitor import DLSClusterMonitor, DLSIrisMonitor


@pytest.mark.parametrize(
    "service_class,scheduler",
    [(DLSClusterMonitor, "slurm"), (DLSIrisMonitor, "iris")],
)
def test_cluster_monitor_uses_its_scheduler(mocker, service_class, scheduler):
    zc = mocker.MagicMock(zocalo.configuration.Configuration)
    zc.storage = {}
    from_zocalo_configuration = mocker.patch(
        "dlstbx.services.cluster_monitor.slurm.SlurmRestApi.from_zocalo_configuration"
    )
    service = service_class(environment={"config": zc})
    mocker.patch.object(service, "_register_idle")
    service.initializing()

    from_zocalo_configuration.assert_called_once_with(zc, cluster=scheduler)
    assert service.scheduler == scheduler

    mocker.patch("dlstbx.services.cluster_monitor.time.sleep")
    service._transport = mock.Mock()
    service.update_cluster_statistics()
    from_zocalo_configuration.return_value.get_jobs.assert_called_once_with()
    message = service._transport.broadcast.call_args.args[1]
    assert message["statistic-cluster"] == scheduler


def test_cluster_monitor_deprecated_scheduler_setting(mocker):
    zc = mocker.MagicMock(zocalo.configuration.Configuration)
    zc.storage = {"scheduler": "iris"}
    from_zocalo_configuration = mocker.patch(
        "dlstbx.services.cluster_monitor.slurm.SlurmRestApi.from_zocalo_configuration"
    )
    service = DLSClusterMonitor(environment={"config": zc})
    mocker.patch.object(service, "_register_idle")
    service.initializing()
    from_zocalo_configuration.assert_called_once_with(zc, cluster="iris")
