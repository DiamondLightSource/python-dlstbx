from __future__ import annotations

from dlstbx.system_test.common import CommonSystemTest


class Cluster(CommonSystemTest):
    """Test cluster job submission and monitoring."""

    def test_submitting_jobs(self):
        """Submitting a job should
        - Run qsub command
        - Put job information into the output queue"""

    def test_keep_monitoring_a_running_job(self):
        """Monitoring a job should
        - Run qstat command
        - Put job information back into the waiting queue with updated timing information
        """

    def test_handle_successful_job(self):
        """When a job successfully completed
        - qstat will fail
        - run qacct instead
        - look for exit_status 0, failed 0"""

    def test_handle_failed_job(self):
        """When a job failed with exit code
        - qstat will fail
        - run qacct instead
        - look for exit_status != 0, failed 0"""

    def test_handle_deleted_job(self):
        """When a job was deleted
        - qstat will fail
        - run qacct instead
        - look for failed != 0, deleted_by != NONE"""
