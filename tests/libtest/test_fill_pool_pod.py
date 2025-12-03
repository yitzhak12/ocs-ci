import logging
import time

from ocs_ci.framework.testlib import (
    ManageTest,
    ignore_leftovers,
    libtest,
)
from ocs_ci.helpers.ceph_helpers import wait_for_percent_used_capacity_reached
from ocs_ci.ocs.cluster import get_percent_used_capacity, CephCluster

log = logging.getLogger(__name__)


@libtest
@ignore_leftovers
class TestFillPoolPod(ManageTest):
    """
    Test the Fill Pool Pod functionalities
    """

    def test_fill_pool_pod_with_both_modes(self, fill_pod_factory):
        """
        Run Fill Pool Pod using both modes to fill the cluster to a target usage.
        Verifies that the workload completes, logs capacity usage and elapsed time.

        """
        ceph_cluster = CephCluster()
        ceph_capacity = ceph_cluster.get_ceph_capacity()
        log.info(f"Ceph Cluster capacity: {ceph_capacity}")
        if ceph_capacity > 500:
            storage_to_fill = 100  # in GiB
        else:
            storage_to_fill = 50  # in GiB

        storage_to_fill_zero_mode = storage_to_fill // 2
        storage_to_fill_random_mode = storage_to_fill - storage_to_fill_zero_mode
        log.info(
            f"Storage to fill in zero mode: {storage_to_fill_zero_mode}Gi, "
            f"Storage to fill in random mode: {storage_to_fill_random_mode}Gi"
        )

        start = time.time()
        fill_pod_factory(
            fill_mode="zero",
            storage=f"{storage_to_fill_zero_mode}Gi",
        )
        fill_pod_factory(
            fill_mode="random",
            storage=f"{storage_to_fill_random_mode}Gi",
        )
        wait_for_percent_used_capacity_reached(50, timeout=120, sleep=5)
        end = time.time()
        fill_up_time = end - start
        used_capacity = get_percent_used_capacity()
        log.info(
            f"The current percent used capacity = {used_capacity}%, "
            f"The time took to fill up the cluster = {fill_up_time}"
        )
