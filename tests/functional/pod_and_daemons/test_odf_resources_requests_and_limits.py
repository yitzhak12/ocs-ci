import json
import logging

from ocs_ci.ocs import constants

from ocs_ci.framework.testlib import (
    tier1,
    skipif_ocs_version,
    BaseTest,
    polarion_id,
    runs_on_provider,
    skipif_external_mode,
)
from ocs_ci.framework.pytest_customization.marks import brown_squad, jira
from ocs_ci.ocs.resources.pod import (
    get_all_pods,
    get_osd_pods,
)
from ocs_ci.helpers.pod_helpers import (
    get_all_pods_container_resource_details,
    get_pod_container_resource_details,
    validate_all_pods_container_resources,
    validate_pod_containers_requests_equal_limits,
)
from ocs_ci.ocs.resources.pod import is_pod_owned_by_job

logger = logging.getLogger(__name__)


@tier1
@skipif_ocs_version("<4.20")
@jira("DFBUGS-5080")
class TestLiveResourcesPresenceAndFormat(BaseTest):
    """
    Functional test to verify that live pod resource values (requests/limits)
    exist and start with a digit (no None/'N/A'/'null'/non-numeric prefixes).
    """

    @brown_squad
    @polarion_id("OCS-7362")
    def test_live_resources_presence_and_format(self):
        """
        Steps:
        1) Get live ODF pods (exclude transient patterns).
        2) Extract per-container resource details.
        3) Check that each live value exists and starts with a digit.
        4) Assert no invalid values were found.

        """
        pod_name_exclude_patterns = [
            "storageclient-",
            "rook-ceph-tools-external-",
            "rook-ceph-osd-prepare-",
            "pod-test-",
            "test",
            "session",
            "debug",
            "must-gather",
            "ocs-ci",
            "java-s3",
        ]
        logger.info(
            f"Retrieving live pod objects from the cluster, "
            f"excluding patterns: {pod_name_exclude_patterns}"
        )
        pod_objs = get_all_pods(namespace=constants.OPENSHIFT_STORAGE_NAMESPACE)
        filtered_pods = []
        for p in pod_objs:
            # Exclude the pods whose name starts with or contains the above mentioned pattern
            if any(keyword in p.name for keyword in pod_name_exclude_patterns):
                continue
            # Exclude pods owned by Jobs
            if is_pod_owned_by_job(p):
                continue
            filtered_pods.append(p)

        logger.info(f"Found {len(filtered_pods)} pods after filtering.")

        logger.info("Extracting live resource details for validation.")
        pods_resources_details_dict = get_all_pods_container_resource_details(
            filtered_pods
        )

        logger.info("Checking live pod resource values (exist + start with digit).")
        validation = validate_all_pods_container_resources(pods_resources_details_dict)

        if not validation["result"]:
            pretty = json.dumps(validation["invalid_values"], indent=2, sort_keys=True)
            error_message = (
                "Invalid or missing live resource values detected for one or more containers.\n"
                f"Details:\n{pretty}"
            )
        else:
            error_message = ""

        assert validation["result"], error_message
        logger.info("All live pod resource values exist and are well-formed.")

    @brown_squad
    @skipif_external_mode
    @runs_on_provider
    @skipif_ocs_version("<4.22")
    @jira("DFBUGS-6784")
    def test_osd_log_collector_guaranteed_qos(self):
        """
        Verify that OSD pods have Guaranteed QoS class by checking that all
        containers (including log-collector) have CPU and memory requests
        equal to limits.

        Steps:
        1) Get all OSD pods.
        2) For each OSD pod, extract per-container resource details.
        3) Verify the log-collector container is present.
        4) Verify CPU requests == CPU limits for every container.
        5) Verify memory requests == memory limits for every container.
        6) Verify the pod QoS class is Guaranteed.

        """
        logger.test_step("Get all OSD pods")
        osd_pods = get_osd_pods()
        assert osd_pods, "No OSD pods found in the cluster"
        logger.info("Found %s OSD pods", len(osd_pods))

        failures = []

        logger.test_step(
            "Verify log-collector container is present and all containers "
            "have requests == limits"
        )
        for pod_obj in osd_pods:
            pod_name = pod_obj.name
            containers = get_pod_container_resource_details(pod_obj)
            container_names = [c["container"] for c in containers]

            logger.info("OSD pod '%s' containers: %s", pod_name, container_names)

            if constants.LOG_COLLECTOR_CONTAINER not in container_names:
                failures.append(
                    f"{pod_name}: '{constants.LOG_COLLECTOR_CONTAINER}'"
                    " container not found"
                )

            failures.extend(
                validate_pod_containers_requests_equal_limits(pod_name, containers)
            )

        logger.test_step("Verify OSD pod QoS class is Guaranteed")
        for pod_obj in osd_pods:
            pod_name = pod_obj.name
            qos = pod_obj.get()["status"].get("qosClass", "")
            logger.info("OSD pod '%s' QoS class: %s", pod_name, qos)
            if qos != constants.POD_QOS_GUARANTEED:
                failures.append(
                    f"{pod_name}: QoS class is '{qos}',"
                    f" expected '{constants.POD_QOS_GUARANTEED}'"
                )

        assert not failures, "OSD pod resource validation failed:\n" + "\n".join(
            f"  - {f}" for f in failures
        )
