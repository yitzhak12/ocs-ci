import random
import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import (
    ManageTest,
    tier1,
    acceptance,
    skipif_ocs_version,
    skipif_ocp_version,
)
from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    provider_mode,
    run_on_all_clients,
)
from ocs_ci.ocs.ocp import OCP


from ocs_ci.ocs.resources import pod, pvc
from ocs_ci.helpers import helpers
from ocs_ci.ocs.resources.pvc import verify_pvc_failed_with_different_volume_mode_change

log = logging.getlog(__name__)


@green_squad
@skipif_ocs_version("<4.9")
@skipif_ocp_version("<4.9")
class TestRestoreSnapshotUsingDifferentMode(ManageTest):
    """
    Verify that restoring a snapshot to a different volumeMode is blocked
    until the VolumeSnapshotContent is annotated:
      snapshot.storage.kubernetes.io/allow-volume-mode-change: "true"
    Then verify it succeeds, and (optionally) round-trip back to Filesystem
    and validate data integrity.
    """

    @provider_mode
    @acceptance
    @run_on_all_clients
    @pytest.mark.polarion_id("OCS-VSC-ALLOW-MODE-CHANGE")
    @pytest.mark.parametrize(
        "interface_type,access_mode",
        [
            # Only RBD here, since CephFS doesn't support Block volume mode
            pytest.param(constants.CEPHBLOCKPOOL, constants.ACCESS_MODE_RWO),
        ],
    )
    @tier1
    def test_snapshot_vsc_annotation_allows_mode_change(
        self,
        interface_type,
        access_mode,
        pvc_factory,
        pod_factory,
        teardown_factory,
    ):
        """
        Steps:
          1) Create FS PVC + Pod; write a file; compute md5
          2) Take VolumeSnapshot (snap-1)
          3) Attempt restore to Block PVC (pvc-2) -> expect ProvisioningFailed with missing-annotation error
          4) Patch bound VolumeSnapshotContent with allow-volume-mode-change
          5) Recreate restore PVC (Block) -> expect Bound
          6) (Optional) Take snap-2 from pvc-2, patch, restore to FS (pvc-3) and verify md5
        """
        log.info(f"Running IO on pod {self.pod_obj.name}")
        file_name = self.pod_obj.name
        log.info(f"File created during IO {file_name}")
        self.pod_obj.run_io(storage_type="fs", size="1G", fio_filename=file_name)

        # Wait for fio to finish
        fio_result = self.pod_obj.get_fio_results()
        err_count = fio_result.get("jobs")[0].get("error")
        assert err_count == 0, (
            f"IO error on pod {self.pod_obj.name}. " f"FIO result: {fio_result}"
        )
        log.info(f"Verified IO on pod {self.pod_obj.name}.")

        # Verify presence of the file
        file_path = pod.get_file_path(self.pod_obj, file_name)
        log.info(f"Actual file path on the pod {file_path}")
        assert pod.check_file_existence(
            self.pod_obj, file_path
        ), f"File {file_name} doesn't exist"
        log.info(f"File {file_name} exists in {self.pod_obj.name}")

        # Calculate md5sum
        orig_md5_sum = pod.cal_md5sum(self.pod_obj, file_name)

        # Take a snapshot
        snap_yaml = constants.CSI_RBD_SNAPSHOT_YAML
        if interface_type == constants.CEPHFILESYSTEM:
            snap_yaml = constants.CSI_CEPHFS_SNAPSHOT_YAML

        snap_name = helpers.create_unique_resource_name("test", "snapshot")
        snap_obj1 = pvc.create_pvc_snapshot(
            self.pvc_obj.name,
            snap_yaml,
            snap_name,
            self.pvc_obj.namespace,
            helpers.default_volumesnapshotclass(interface_type).name,
        )
        snap_obj1.ocp.wait_for_resource(
            condition="true",
            resource_name=snap_obj1.name,
            column=constants.STATUS_READYTOUSE,
            timeout=60,
        )
        teardown_factory(snap_obj1)

        # Same Storage class of the original PVC
        sc_name = self.pvc_obj.backed_sc

        # Size should be same as of the original PVC
        pvc_size = str(self.pvc_obj.size) + "Gi"

        # Create pvc out of the snapshot
        # Both, the snapshot and the restore PVC should be in same namespace
        restore_pvc_name = helpers.create_unique_resource_name(
            "test", "restore-pvc-fail"
        )
        restore_pvc_yaml = constants.CSI_RBD_PVC_RESTORE_YAML
        if interface_type == constants.CEPHFILESYSTEM:
            restore_pvc_yaml = constants.CSI_CEPHFS_PVC_RESTORE_YAML

        restore_pvc_obj = pvc.create_restore_pvc(
            sc_name=sc_name,
            snap_name=snap_obj1.name,
            namespace=snap_obj1.namespace,
            size=pvc_size,
            pvc_name=restore_pvc_name,
            restore_pvc_yaml=restore_pvc_yaml,
            volume_mode=constants.VOLUME_MODE_BLOCK,
        )
        helpers.wait_for_resource_state(
            restore_pvc_obj, constants.STATUS_BOUND, timeout=180
        )
        restore_pvc_obj.reload()
        teardown_factory(restore_pvc_obj)

        verify_pvc_failed_with_different_volume_mode_change(pvc_obj=restore_pvc_obj)

        # Get the VolumeSnapshotContent object
        vsc_obj = helpers.get_snapshot_content_obj(snap_obj1)
        # Patch the bound VolumeSnapshotContent
        log.info(
            f"Patching VolumeSnapshotContent {vsc_obj.name} with allow-volume-mode-change"
        )
        params = '{"metadata":{"annotations":{"snapshot.storage.kubernetes.io/allow-volume-mode-change":"true"}}}'
        OCP(kind=constants.VOLUMESNAPSHOTCONTENT).patch(
            resource_name=vsc_obj.name,
            params=params,
            format_type="merge",
        )

        # Create and attach pod to the pvc
        restore_pod_obj = helpers.create_pod(
            interface_type=interface_type,
            pvc_name=restore_pvc_obj.name,
            namespace=snap_obj1.namespace,
            pod_dict_path=constants.NGINX_POD_YAML,
        )

        # Confirm that the pod is running
        helpers.wait_for_resource_state(
            resource=restore_pod_obj, state=constants.STATUS_RUNNING, timeout=120
        )
        restore_pod_obj.reload()
        teardown_factory(restore_pod_obj)

        # Verify that the file is present on the new pod
        log.info(
            f"Checking the existence of {file_name} "
            f"on restore pod {restore_pod_obj.name}"
        )
        assert pod.check_file_existence(
            restore_pod_obj, file_path
        ), f"File {file_name} doesn't exist"
        log.info(f"File {file_name} exists in {restore_pod_obj.name}")

        # Verify that the md5sum matches
        log.info(
            f"Verifying that md5sum of {file_name} "
            f"on pod {self.pod_obj.name} matches with md5sum "
            f"of the same file on restore pod {restore_pod_obj.name}"
        )
        assert pod.verify_data_integrity(
            restore_pod_obj, file_name, orig_md5_sum
        ), "Data integrity check failed"
        log.info("Data integrity check passed, md5sum are same")

        log.info("Running IO on new pod")
        # Run IO on new pod
        restore_pod_obj.run_io(storage_type="fs", size="1G", runtime=20)

        # Wait for fio to finish
        restore_pod_obj.get_fio_results()
        log.info("IO finished o new pod")

        # # Recreate the restore PVC
        # pvc_ocp = OCP(kind=constants.PVC, namespace=ns)
        # pvc_ocp.delete(resource_name=pvc2_fail_name)
        # pvc_ocp.wait_for_delete(resource_name=pvc2_fail_name, timeout=180)
        #
        # pvc2_ok_name = helpers.create_unique_resource_name("restore", "block-ok")
        # pvc2_ok_obj = pvc.create_restore_pvc(
        #     sc_name=src_pvc.backed_sc,
        #     snap_name=snap1_obj.name,
        #     namespace=ns,
        #     size="1Gi",
        #     pvc_name=pvc2_ok_name,
        #     restore_pvc_yaml=constants.CSI_RBD_PVC_RESTORE_YAML,
        #     volume_mode=constants.VOLUME_MODE_BLOCK,
        #     access_mode=constants.ACCESS_MODE_RWO,
        # )
        # teardown_factory(pvc2_ok_obj)
        #
        # helpers.wait_for_resource_state(
        #     pvc2_ok_obj, constants.STATUS_BOUND, timeout=300
        # )
        # pvc2_ok_obj.reload()
        # log.info(f"Restore PVC (Block) is Bound: {pvc2_ok_obj.name}")
        #
        # # ---------- 6) Optional round-trip: Block -> FS and verify md5 ----------
        # snap2_obj = pvc2_ok_obj.create_snapshot(wait=True)
        # teardown_factory(snap2_obj)
        # log.info(f"Created snapshot {snap2_obj.name} from Block PVC {pvc2_ok_obj.name}")
        #
        # # Patch VSC for snap-2 as well (each snapshot has its own VSC)
        # vsc2_name = (
        #     OCP(kind=constants.VOLUMESNAPSHOT, namespace=ns)
        #     .get(resource_name=snap2_obj.name)["status"]["boundVolumeSnapshotContentName"]
        # )
        # OCP(kind=constants.VOLUMESNAPSHOTCONTENT).patch(
        #     resource_name=vsc2_name,
        #     params='{"metadata":{"annotations":{"snapshot.storage.kubernetes.io/allow-volume-mode-change":"true"}}}',
        #     format_type="merge",
        # )
        #
        # pvc3_name = helpers.create_unique_resource_name("restore", "fs-ok")
        # pvc3_obj = pvc.create_restore_pvc(
        #     sc_name=src_pvc.backed_sc,
        #     snap_name=snap2_obj.name,
        #     namespace=ns,
        #     size="1Gi",
        #     pvc_name=pvc3_name,
        #     restore_pvc_yaml=constants.CSI_RBD_PVC_RESTORE_YAML,
        #     volume_mode=constants.VOLUME_MODE_FILESYSTEM,  # back to FS
        #     access_mode=constants.ACCESS_MODE_RWO,
        # )
        # teardown_factory(pvc3_obj)
        # helpers.wait_for_resource_state(pvc3_obj, constants.STATUS_BOUND, timeout=300)
        # pvc3_obj.reload()
        # log.info(f"Restore PVC (Filesystem) is Bound: {pvc3_obj.name}")
        #
        # # Mount and verify md5 matches original
        # reader_pod = helpers.create_pod(
        #     interface_type=interface_type,
        #     pvc_name=pvc3_obj.name,
        #     namespace=ns,
        #     pod_dict_path=constants.NGINX_POD_YAML,
        # )
        # helpers.wait_for_resource_state(reader_pod, constants.STATUS_RUNNING)
        # reader_pod.reload()
        # teardown_factory(reader_pod)
        #
        # assert pod.check_file_existence(
        #     reader_pod, file_path
        # ), f"File {file_path} missing on final restored pod"
        # assert pod.verify_data_integrity(
        #     reader_pod, file_name, orig_md5
        # ), "Data integrity mismatch after FS->Block->FS round-trip"
        #
        # log.info("Mode-change gate enforced, patch honored, and data integrity verified.")


import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier2,
    skipif_ocp_version,
    skipif_managed_service,
    skipif_hci_provider_and_client,
)
from ocs_ci.ocs.resources.pod import (
    cal_md5sum,
    verify_data_integrity,
    calculate_md5sum_of_pod_files,
)
from ocs_ci.helpers.helpers import (
    storagecluster_independent_check,
    wait_for_resource_state,
)
from ocs_ci.utility import version

log = logging.getLogger(__name__)


@green_squad
@skipif_managed_service
@skipif_hci_provider_and_client
@skipif_ocs_version("<4.6")
@skipif_ocp_version("<4.6")
@pytest.mark.polarion_id("OCS-2424")
class TestRestoreSnapshotUsingDifferentSc(ManageTest):
    """
    Tests to verify snapshot restore using an SC different than that of parent

    """

    @pytest.fixture(autouse=True)
    def setup(
        self,
        project_factory,
        secret_factory,
        storageclass_factory,
        snapshot_restore_factory,
        create_pvcs_and_pods,
    ):
        """
        Create PVCs and pods

        """
        self.pvc_size = 3
        self.pvcs, self.pods = create_pvcs_and_pods(
            pvc_size=self.pvc_size,
            access_modes_rbd=[constants.ACCESS_MODE_RWO],
            access_modes_cephfs=[constants.ACCESS_MODE_RWO],
        )

    def run_io_on_pods(self, pods, size="1G", runtime=30):
        """
        Run IO on the pods

        Args:
            pods (list): The list of pods for running the IO
            size (str): Size in MB or Gi, e.g. '200M'. Default value is '1G'
            runtime (int): The number of seconds IO should run for

        """
        log.info("Starting IO on all pods")
        for pod_obj in pods:
            storage_type = (
                "block"
                if pod_obj.pvc.volume_mode == constants.VOLUME_MODE_BLOCK
                else "fs"
            )
            rate = f"{random.randint(1, 5)}M"
            pod_obj.run_io(
                storage_type=storage_type,
                size=size,
                runtime=runtime,
                rate=rate,
                fio_filename=self.pod_file_name,
                end_fsync=1,
            )
            log.info(f"IO started on pod {pod_obj.name}")
        log.info("Started IO on all pods")

    def prepare_data(self, interface_type):
        """
        Prepare the data before resizing the osd

        """
        pvc_size = random.randint(2, 5)
        # Source: RBD + Filesystem + RWO
        pvcs, pods = self.create_pvcs_and_pods(
            access_modes_rbd=[constants.ACCESS_MODE_RWO],  # => Filesystem
            num_of_rbd_pvc=1,
            access_modes_cephfs=[],  # not needed
            num_of_cephfs_pvc=0,
        )
        # Create one RBD PVC in Block mode (RWO)
        pvcs, pods = self.create_pvcs_and_pods(
            access_modes_rbd=[f"{constants.ACCESS_MODE_RWO}-Block"],  # Block mode
            num_of_rbd_pvc=1,
            access_modes_cephfs=[],  # Skip CephFS
            num_of_cephfs_pvc=0,
        )

        self.pvcs1, self.pods_for_integrity_check = self.create_pvcs_and_pods(
            pvc_size=pvc_size, num_of_rbd_pvc=3, num_of_cephfs_pvc=3
        )
        pvc_size = random.randint(3, 8)
        self.pvcs2, self.pods_for_run_io = self.create_pvcs_and_pods(
            pvc_size=pvc_size, num_of_rbd_pvc=5, num_of_cephfs_pvc=5
        )
        log.info("Run IO on the pods for integrity check")
        self.run_io_on_pods(self.pods_for_integrity_check)
        log.info("Calculate the md5sum of the pods for integrity check")
        calculate_md5sum_of_pod_files(self.pods_for_integrity_check, self.pod_file_name)
        runtime = 180
        log.info(f"Run IO on the pods in the test background for {runtime} seconds")
        self.run_io_on_pods(self.pods_for_run_io, size="2G", runtime=runtime)

    @tier2
    def test_snapshot_restore_using_different_sc(
        self,
        storageclass_factory,
        snapshot_factory,
        snapshot_restore_factory,
        pod_factory,
    ):
        """
        Test to verify snapshot restore using an SC different than that of parent

        """
        snap_objs = []
        file_name = "file_snapshot"
        # Run IO
        log.info("Start IO on all pods")
        for pod_obj in self.pods:
            pod_obj.run_io(
                storage_type="fs",
                size=f"{self.pvc_size - 1}G",
                runtime=30,
                fio_filename=file_name,
            )
        log.info("IO started on all pods")

        # Wait for IO completion
        for pod_obj in self.pods:
            pod_obj.get_fio_results()
            # Get md5sum of the file
            pod_obj.pvc.md5sum = cal_md5sum(pod_obj=pod_obj, file_name=file_name)
        log.info("IO completed on all pods")

        # Create snapshots
        log.info("Create snapshots of all PVCs")
        for pvc_obj in self.pvcs:
            log.info(f"Creating snapshot of PVC {pvc_obj.name}")
            snap_obj = snapshot_factory(pvc_obj, wait=False)
            snap_obj.md5sum = pvc_obj.md5sum
            snap_obj.interface = pvc_obj.interface
            snap_objs.append(snap_obj)
        log.info("Snapshots created")

        # Verify snapshots are Ready
        log.info("Verify snapshots are ready")
        for snap_obj in snap_objs:
            snap_obj.ocp.wait_for_resource(
                condition="true",
                resource_name=snap_obj.name,
                column=constants.STATUS_READYTOUSE,
                timeout=180,
            )

        # Create storage classes.
        sc_objs = {
            constants.CEPHBLOCKPOOL: [
                storageclass_factory(
                    interface=constants.CEPHBLOCKPOOL,
                ).name
            ],
            constants.CEPHFILESYSTEM: [
                storageclass_factory(interface=constants.CEPHFILESYSTEM).name
            ],
        }

        # If ODF >=4.9 create one more storage class that will use new pool
        # to verify the bug 1901954
        if (
            not storagecluster_independent_check()
            and version.get_semantic_ocs_version_from_config() >= version.VERSION_4_9
        ):
            sc_objs[constants.CEPHBLOCKPOOL].append(
                storageclass_factory(
                    interface=constants.CEPHBLOCKPOOL, new_rbd_pool=True
                ).name
            )

        # Create PVCs out of the snapshots
        restore_pvc_objs = []
        log.info("Creating new PVCs from snapshots")
        for snap_obj in snap_objs:
            for storageclass in sc_objs[snap_obj.interface]:
                log.info(f"Creating a PVC from snapshot {snap_obj.name}")
                restore_pvc_obj = snapshot_restore_factory(
                    snapshot_obj=snap_obj,
                    storageclass=storageclass,
                    size=f"{self.pvc_size}Gi",
                    volume_mode=snap_obj.parent_volume_mode,
                    access_mode=snap_obj.parent_access_mode,
                    status="",
                )

                log.info(
                    f"Created PVC {restore_pvc_obj.name} from snapshot {snap_obj.name}."
                    f"Used the storage class {storageclass}"
                )
                restore_pvc_obj.md5sum = snap_obj.md5sum
                restore_pvc_objs.append(restore_pvc_obj)
        log.info("Created new PVCs from all the snapshots")

        # Confirm that the restored PVCs are Bound
        log.info("Verify the restored PVCs are Bound")
        for pvc_obj in restore_pvc_objs:
            wait_for_resource_state(
                resource=pvc_obj, state=constants.STATUS_BOUND, timeout=180
            )
            pvc_obj.reload()
        log.info("Verified: Restored PVCs are Bound.")

        # Attach the restored PVCs to pods
        log.info("Attach the restored PVCs to pods")
        restore_pod_objs = []
        for restore_pvc_obj in restore_pvc_objs:
            restore_pod_obj = pod_factory(
                interface=restore_pvc_obj.snapshot.interface,
                pvc=restore_pvc_obj,
                status="",
            )
            log.info(
                f"Attached the PVC {restore_pvc_obj.name} to pod {restore_pod_obj.name}"
            )
            restore_pod_objs.append(restore_pod_obj)

        # Verify the new pods are running
        log.info("Verify the new pods are running")
        for pod_obj in restore_pod_objs:
            wait_for_resource_state(pod_obj, constants.STATUS_RUNNING)
        log.info("Verified: New pods are running")

        # Verify md5sum
        log.info("Verifying md5sum on new pods")
        for pod_obj in restore_pod_objs:
            log.info(f"Verifying md5sum on pod {pod_obj.name}")
            verify_data_integrity(
                pod_obj=pod_obj,
                file_name=file_name,
                original_md5sum=pod_obj.pvc.snapshot.md5sum,
            )
            log.info(f"Verified md5sum on pod {pod_obj.name}")
        log.info("Verified md5sum on all pods")

        # Run IO on new pods
        log.info("Starting IO on new pods")
        for pod_obj in restore_pod_objs:
            pod_obj.run_io(storage_type="fs", size="500M", runtime=15)

        # Wait for IO completion on new pods
        log.info("Waiting for IO completion on new pods")
        for pod_obj in restore_pod_objs:
            pod_obj.get_fio_results()
        log.info("IO completed on new pods.")
