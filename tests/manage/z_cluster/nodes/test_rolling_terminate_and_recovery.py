import logging
import pytest
import random


from ocs_ci.framework.testlib import (
    tier4b,
    ignore_leftovers,
    ManageTest,
    skipif_external_mode,
    skipif_ibm_cloud,
    managed_service_required,
    ipi_deployment_required,
)
from ocs_ci.ocs.machine import (
    get_machine_from_node_name,
    get_machineset_from_machine_name,
    get_ready_replica_count,
    wait_for_ready_replica_count_to_reach_expected_value,
    set_replica_count,
)
from ocs_ci.ocs.node import (
    get_ocs_nodes,
    get_node_objs,
    recover_node_to_ready_state,
    consumers_verification_steps_after_provider_node_replacement,
    generate_nodes_for_provider_worker_node_tests,
    wait_for_new_worker_node_ipi,
    get_worker_nodes,
    add_new_node_and_label_it,
)
from ocs_ci.ocs.resources.pod import (
    check_pods_after_node_replacement,
)
from ocs_ci.helpers.sanity_helpers import SanityManagedService, Sanity
from ocs_ci.ocs.cluster import is_ms_provider_cluster, is_managed_service_cluster
from ocs_ci.framework import config


log = logging.getLogger(__name__)


@tier4b
@skipif_ibm_cloud
@skipif_external_mode
@ignore_leftovers
@pytest.mark.polarion_id("OCS-4661")
class TestRollingWorkerNodeTerminateAndRecovery(ManageTest):
    """
    Test rolling terminate and recovery of the OCS worker nodes
    """

    @pytest.fixture(autouse=True)
    def setup(self, create_scale_pods_and_pvcs_using_kube_job_on_ms_consumers):
        """
        Initialize the Sanity instance for the Managed Service

        """
        if is_managed_service_cluster():
            self.sanity_helpers = SanityManagedService(
                create_scale_pods_and_pvcs_using_kube_job_on_ms_consumers
            )
        else:
            self.sanity_helpers = Sanity()

    @pytest.fixture(autouse=True)
    def teardown(self, request, nodes):
        """
        Make sure all nodes are up again
        """

        def finalizer():
            ocp_nodes = get_node_objs()
            for n in ocp_nodes:
                recover_node_to_ready_state(n)

            # If the cluster is an MS provider cluster, and we also have MS consumer clusters in the run
            if is_ms_provider_cluster() and config.is_consumer_exist():
                log.info(
                    "Execute the the consumers verification steps before starting the next test"
                )
                consumers_verification_steps_after_provider_node_replacement()

        request.addfinalizer(finalizer)

    def rolling_terminate_and_recovery_of_ocs_worker_nodes(self, nodes):
        # Get OCS worker node objects
        if is_ms_provider_cluster():
            ocs_node_objs = generate_nodes_for_provider_worker_node_tests()
        else:
            # If it's not a provider cluster, test rolling terminate two ocs worker nodes will suffice
            ocs_node_objs = random.choices(get_ocs_nodes(), k=2)

        # Start rolling terminate and recovery of OCS worker nodes
        for node_obj in ocs_node_objs:
            old_wnodes = get_worker_nodes()
            log.info(f"Current worker nodes: {old_wnodes}")
            machine_name = get_machine_from_node_name(node_obj.name)
            machineset = get_machineset_from_machine_name(machine_name)
            log.info(f"machineset name: {machineset}")
            old_ready_rc = get_ready_replica_count(machineset)

            nodes.terminate_nodes(nodes=[node_obj], wait=True)
            log.info(f"Successfully terminated the node: {node_obj.name}")
            if is_managed_service_cluster():
                new_ocs_node = wait_for_new_worker_node_ipi(machineset, old_wnodes)
            else:
                expected_ready_rc = old_ready_rc - 1
                wait_for_ready_replica_count_to_reach_expected_value(
                    machineset, expected_ready_rc
                )
                # Change the current replica count to the expected ready replica count
                set_replica_count(machineset, expected_ready_rc)
                new_ocs_node_names = add_new_node_and_label_it(machineset)
                new_ocs_node = get_node_objs(new_ocs_node_names)[0]

            log.info(f"The new ocs node is: {new_ocs_node.name}")
            log.info("Waiting for all the pods to be running")
            assert check_pods_after_node_replacement(), "Not all the pods are running"

            # If the cluster is an MS provider cluster, and we also have MS consumer clusters in the run
            if is_ms_provider_cluster() and config.is_consumer_exist():
                assert consumers_verification_steps_after_provider_node_replacement()
            if is_managed_service_cluster():
                self.sanity_helpers.health_check_ms(cluster_check=False, tries=40)
            else:
                self.sanity_helpers.health_check(cluster_check=False, tries=40)

    @managed_service_required
    def test_rolling_terminate_and_recovery_in_controlled_fashion_ms(self, nodes):
        """
        Test rolling terminate and recovery of the OCS worker nodes, when waiting for the pods to
        be running and Ceph Health OK between the iterations. This test is for the Managed Service

        """
        self.rolling_terminate_and_recovery_of_ocs_worker_nodes(nodes)
        # Check basic cluster functionality by creating some resources
        self.sanity_helpers.create_resources_on_ms_consumers()

    @ipi_deployment_required
    def test_rolling_terminate_and_recovery_in_controlled_fashion_ipi(
        self, nodes, pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
    ):
        """
        Test rolling terminate and recovery of the OCS worker nodes, when waiting for the pods to
        be running and Ceph Health OK between the iterations. This test is for the ipi deployment

        """
        self.rolling_terminate_and_recovery_of_ocs_worker_nodes(nodes)
        # Check basic cluster functionality by creating some resources
        self.sanity_helpers.create_resources(
            pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
        )
