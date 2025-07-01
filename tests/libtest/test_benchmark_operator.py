import logging
from uuid import uuid4

from ocs_ci.framework.testlib import (
    ManageTest,
    ignore_leftovers,
    libtest,
)
from ocs_ci.ocs.cluster import get_percent_used_capacity

log = logging.getLogger(__name__)


@libtest
@ignore_leftovers
class TestBenchmarkOperator(ManageTest):
    """
    Test the Benchmark Operator FIO Class functionalities
    """

    def test_benchmark_workload_storageutilization_default_values(
        self, benchmark_workload_storageutilization
    ):
        """
        Create a new benchmark operator with the default values
        """
        benchmark_workload_storageutilization(target_percentage=25, is_completed=True)

    def test_benchmark_workload_storageutilization_picked_values(
        self, benchmark_workload_storageutilization
    ):
        """
        Create a new benchmark operator with picked values
        """
        benchmark_name = f"fio-benchmark{uuid4().hex[:4]}"
        benchmark_workload_storageutilization(
            target_percentage=20,
            bs="2048KiB",
            benchmark_name=benchmark_name,
            is_completed=True,
        )

    def test_benchmark_workload_storageutilization_fill_up_quickly(
        self, benchmark_workload_storageutilization
    ):
        """
        Run a benchmark with high-performance FIO settings to quickly fill up to target capacity.
        """
        benchmark_name = f"fio-benchmark{uuid4().hex[:4]}"
        log.info(f"Starting benchmark {benchmark_name} with fast fill settings")
        numjobs = 4

        benchmark_workload_storageutilization(
            target_percentage=30,
            bs="4096KiB",
            benchmark_name=benchmark_name,
            is_completed=True,
            numjobs=numjobs,
            iodepth=64,
        )
        used_capacity = get_percent_used_capacity()
        log.info(f"The current percent used capacity = {used_capacity}%")
