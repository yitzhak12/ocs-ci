"""
Test to measure pvc scale creation time. Total pvc count would be 1500
"""
import logging
import csv
import random
import pytest
import threading

from tests import helpers
from ocs_ci.framework.testlib import scale, E2ETest, polarion_id, ignore_leftovers
from ocs_ci.ocs import constants
from concurrent.futures import ThreadPoolExecutor


log = logging.getLogger(__name__)


@scale
@ignore_leftovers
class TestPVCCreationScale(E2ETest):
    """
    Base class for PVC scale creation
    """
    @pytest.fixture()
    def namespace(self, project_factory):
        """
        Create a new project
        """
        proj_obj = project_factory()
        self.namespace = proj_obj.namespace

    @pytest.mark.parametrize(
        argnames=["access_mode", "interface"],
        argvalues=[
            pytest.param(
                *[constants.ACCESS_MODE_RWO, constants.CEPHBLOCKPOOL],
                marks=pytest.mark.polarion_id("OCS-1225")
            ),
            pytest.param(
                *[constants.ACCESS_MODE_RWX, constants.CEPHBLOCKPOOL],
                marks=pytest.mark.polarion_id("OCS-2010")
            ),
            pytest.param(
                *[constants.ACCESS_MODE_RWO, constants.CEPHFS_INTERFACE],
                marks=pytest.mark.polarion_id("OCS-2011")
            ),
            pytest.param(
                *[constants.ACCESS_MODE_RWX, constants.CEPHFS_INTERFACE],
                marks=pytest.mark.polarion_id("OCS-2008")
            ),
        ]
    )
    @pytest.mark.usefixtures(namespace.__name__)
    def test_1500_pvc_creation_scale(
        self, namespace, teardown_factory, access_mode, interface
    ):
        """
        Measuring PVC creation time while scaling PVC
        """
        number_of_pvc = 1500
        log.info(f"Start creating {access_mode}-{interface} {number_of_pvc} PVC")

        if interface == constants.CEPHBLOCKPOOL:
            self.sc_obj = constants.DEFAULT_STORAGECLASS_RBD
        elif interface == constants.CEPHFS_INTERFACE:
            self.sc_obj = constants.DEFAULT_STORAGECLASS_CEPHFS

        pvc_objs = helpers.create_multiple_pvcs(
            sc_name=self.sc_obj,
            namespace=self.namespace,
            number_of_pvc=number_of_pvc,
            size=f"{random.randrange(5, 105, 5)}Gi",
            access_mode=access_mode
        )
        for pvc_obj in pvc_objs:
            pvc_obj.reload()
            teardown_factory(pvc_obj)
        pvc_name = list()
        with ThreadPoolExecutor(max_workers=5) as executor:
            for pvc_obj in pvc_objs:
                executor.submit(
                    helpers.wait_for_resource_state, pvc_obj,
                    constants.STATUS_BOUND
                )

                executor.submit(pvc_obj.reload)
                pvc_name.append(pvc_obj.name)

        pvc_create_time = helpers.measure_pvc_creation_time_bulk(
            interface=interface, pvc_name_list=pvc_name
        )

        # TODO: Update below code with google API, to record value in spreadsheet
        # TODO: For now observing Google API limit to write more than 100 writes
        csv_obj = csv.writer(open(f"/tmp/{self.sc_obj}-{access_mode}.csv", "w"))
        for k, v in pvc_create_time.items():
            csv_obj.writerow([k, v])

    @polarion_id('OCS-1885')
    @pytest.mark.usefixtures(namespace.__name__)
    def test_all_4_type_pvc_creation_scale(self, namespace, teardown_factory):
        """
        Measuring PVC creation time while scaling PVC of all 4 types, Total 1500 PVCs
        will be created, i.e. 375 each pvc type
        """
        number_of_pvc = 375
        log.info(f"Start creating {number_of_pvc} PVC of all 4 types")

        cephfs_sc_obj = constants.DEFAULT_STORAGECLASS_CEPHFS
        rbd_sc_obj = constants.DEFAULT_STORAGECLASS_RBD

        fs_pvc_obj, rbd_pvc_obj = ([] for i in range(2))
        for mode in [constants.ACCESS_MODE_RWO, constants.ACCESS_MODE_RWX]:
            fs_pvc_obj.extend(helpers.create_multiple_pvcs(
                sc_name=cephfs_sc_obj, namespace=self.namespace, number_of_pvc=number_of_pvc,
                size=f"{random.randrange(5, 105, 5)}Gi", access_mode=mode)
            )
            rbd_pvc_obj.extend(helpers.create_multiple_pvcs(
                sc_name=rbd_sc_obj, namespace=self.namespace, number_of_pvc=number_of_pvc,
                size=f"{random.randrange(5, 105, 5)}Gi", access_mode=mode)
            )

        fs_pvc_name, rbd_pvc_name = ([] for i in range(2))
        for fs_obj, rbd_obj in zip(fs_pvc_obj, rbd_pvc_obj):
            fs_pvc_name.append(fs_obj.name)
            rbd_pvc_name.append(rbd_obj.name)

        # Check for PVC status using threads
        threads = list()
        for obj in fs_pvc_obj:
            process = threading.Thread(
                target=helpers.wait_for_resource_state,
                args=(obj, constants.STATUS_BOUND, )
            )
            process.start()
            threads.append(process)
        for obj in rbd_pvc_obj:
            process = threading.Thread(
                target=helpers.wait_for_resource_state,
                args=(obj, constants.STATUS_BOUND,)
            )
            process.start()
            threads.append(process)
        for process in threads:
            process.join()

        fs_pvc_create_time = helpers.measure_pvc_creation_time_bulk(
            interface=constants.CEPHFS_INTERFACE, pvc_name_list=fs_pvc_name
        )
        rbd_pvc_create_time = helpers.measure_pvc_creation_time_bulk(
            interface=constants.CEPHBLOCKPOOL, pvc_name_list=rbd_pvc_name
        )
        fs_pvc_create_time.update(rbd_pvc_create_time)

        # TODO: Update below code with google API, to record value in spreadsheet
        # TODO: For now observing Google API limit to write more than 100 writes
        csv_obj = csv.writer(open("/tmp/All-type-PVC-Creation-Scale.csv", "w"))
        for k, v in fs_pvc_create_time.items():
            csv_obj.writerow([k, v])

        for fs_obj, rbd_obj in zip(fs_pvc_obj, rbd_pvc_obj):
            teardown_factory(fs_obj)
            teardown_factory(rbd_obj)
