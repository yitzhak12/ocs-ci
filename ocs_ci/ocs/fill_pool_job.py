import logging

from ocs_ci.helpers.helpers import create_unique_resource_name
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs import constants
from ocs_ci.utility.retry import catch_exceptions
from ocs_ci.utility import templating
from ocs_ci.helpers import helpers
from ocs_ci.ocs.resources.pvc import PVC
from ocs_ci.ocs.resources.pod import Pod, get_pods_having_label
from ocs_ci.utility.utils import exec_cmd


log = logging.getLogger(__name__)

# RBD discards actual zero writes (discard_on_zeroed_write_same). /dev/urandom is
# CPU-bound and too slow under the default 500m limit. AES-CTR over /dev/zero is
# cheap to generate and is stored as real used capacity.
FILL_MODES = ("zero", "random", "incompressible")


class FillPoolJob(object):
    """
    Fill Pool Job operations (assumes a Job manifest).
    """

    def __init__(self):
        self.name = "<unknown>"
        self.job_obj = None
        self.pod_obj = None
        self.pvc_obj = None
        self.namespace = None

    def create(
        self,
        name=None,
        block_size="1M",
        cpu_request="100m",
        mem_request="128Mi",
        cpu_limit="500m",
        mem_limit="256Mi",
        fill_mode="zero",
        base_yaml_path=constants.FILL_POOL_JOB_YAML,
        pvc_name=None,
        sc_name=constants.DEFAULT_STORAGECLASS_RBD,
        storage="50Gi",
        pvc_base_yaml_path=constants.FILL_POOL_PVC_YAML,
        wait_for_resource=True,
    ):
        """
        Create a Job that fills up cluster storage by writing data to a PVC.
        Assumes manifest is a Job (pod spec under spec.template.spec).

        Args:
            fill_mode (str): How to generate write data:
                'zero' - dd from /dev/zero (does not increase Ceph used-raw on RBD).
                'random' - dd from /dev/urandom (slow; CPU-bound).
                'incompressible' - openssl AES-CTR over /dev/zero piped to dd
                (fast and stored as real used capacity). Prefer this for
                filling cluster used % on RBD.
        """
        self.name = name or create_unique_resource_name("fill-pool", "job")
        sc_name = sc_name or constants.DEFAULT_STORAGECLASS_RBD
        proj_obj = helpers.create_project()
        self.namespace = proj_obj.namespace

        if fill_mode not in FILL_MODES:
            raise ValueError(f"fill_mode must be one of {FILL_MODES}")

        log.info(
            f"Creating FillPoolJob {self.name} fill_mode={fill_mode} "
            f"storage={storage} block_size={block_size}"
        )

        # Load Job manifest and apply metadata
        job_data = templating.load_yaml(base_yaml_path)
        job_data.setdefault("metadata", {})
        job_data["metadata"]["name"] = self.name
        job_data["metadata"]["namespace"] = self.namespace

        # Assume Job: pod spec under spec.template.spec
        template = job_data["spec"]["template"]
        template.setdefault("metadata", {})
        template["metadata"]["namespace"] = self.namespace
        pod_spec = template["spec"]

        container = pod_spec["containers"][0]
        volume = pod_spec["volumes"][0]
        container["image"] = constants.FEDORA_FIO_IMAGE

        # Prepare PVC name and update volume claim
        pvc_name = pvc_name or create_unique_resource_name("fill-pool", "pvc")
        if "persistentVolumeClaim" in volume:
            volume["persistentVolumeClaim"]["claimName"] = pvc_name

        # Update BLOCK_SIZE env variable if present
        for env_var in container.get("env", []):
            if env_var.get("name") == "BLOCK_SIZE":
                env_var["value"] = block_size

        # Update resources
        container["resources"] = {
            "requests": {"cpu": cpu_request, "memory": mem_request},
            "limits": {"cpu": cpu_limit, "memory": mem_limit},
        }

        fill_cmd = self._build_fill_command(fill_mode, block_size)
        container["command"] = ["sh", "-c", fill_cmd]
        container.pop("args", None)

        # Prepare PVC manifest
        pvc_data = templating.load_yaml(pvc_base_yaml_path)
        pvc_data.setdefault("metadata", {})
        pvc_data["metadata"]["name"] = pvc_name
        pvc_data["metadata"]["namespace"] = self.namespace
        pvc_data["spec"]["storageClassName"] = sc_name
        pvc_data["spec"]["resources"]["requests"]["storage"] = storage

        # Create PVC resource
        ocs_obj = helpers.create_resource(**pvc_data)
        self.pvc_obj = PVC(**ocs_obj.data)

        # Create Job resource
        self.job_obj = helpers.create_resource(**job_data)
        # Get Pod created by the Job
        label = f"job-name={self.name}"
        pods = get_pods_having_label(label, namespace=self.namespace)
        if pods:
            self.pod_obj = Pod(**pods[0])

        # Wait for Pod to be Running if we wrapped it
        if wait_for_resource and self.pod_obj:
            self.pod_obj.ocp.wait_for_resource(
                condition=constants.STATUS_RUNNING,
                resource_name=self.pod_obj.name,
                timeout=180,
                sleep=10,
            )

    @staticmethod
    def _build_fill_command(fill_mode, block_size):
        """
        Build the container shell command for the given fill mode.

        'incompressible' uses AES-CTR over /dev/zero so RBD stores the writes
        (unlike raw /dev/zero, which kernel RBD discards) without the CPU cost
        of /dev/urandom. ENOSPC is treated as success for every mode.

        """
        bs = f"${{BLOCK_SIZE:-{block_size}}}"
        enospc_handler = (
            "EXIT_STATUS=$?; "
            "if [ $EXIT_STATUS -ne 0 ] && grep -q 'No space left on device' /tmp/dd_err; then "
            "  cat /tmp/dd_err; echo 'Capacity reached. Exiting successfully.'; exit 0; "
            "fi; "
            "cat /tmp/dd_err; exit $EXIT_STATUS"
        )
        if fill_mode == "incompressible":
            return (
                'echo "Filling PVC with incompressible AES-CTR data..."; '
                "openssl version >/dev/null || "
                "{ echo 'openssl is required for incompressible fill'; exit 1; }; "
                "openssl enc -aes-128-ctr -nosalt -pass pass:ocs-ci-fill "
                "-in /dev/zero 2>/dev/null "
                f"| dd of=/mnt/fill/testfile bs={bs} oflag=direct 2>/tmp/dd_err; "
                f"{enospc_handler}"
            )
        input_source = "/dev/zero" if fill_mode == "zero" else "/dev/urandom"
        return (
            f'echo "Filling PVC with {fill_mode} data..."; '
            f"dd if={input_source} of=/mnt/fill/testfile bs={bs} 2>/tmp/dd_err; "
            f"{enospc_handler}"
        )

    def wait_for_completion(self, timeout=3600, sleep=30):
        """
        Wait for the Fill Pool Job Pod to complete.
        """
        if not self.pod_obj:
            raise RuntimeError("Fill Pool Job Pod object is not available")

        log.info(f"Waiting for Fill Pool Job Pod {self.pod_obj.name} to complete...")
        self.pod_obj.ocp.wait_for_resource(
            condition=constants.STATUS_COMPLETED,
            resource_name=self.pod_obj.name,
            timeout=timeout,
            sleep=sleep,
        )
        log.info(f"Fill Pool Job Pod {self.pod_obj.name} has completed.")

    def cleanup(self):
        """
        Cleanup resources: Job, Pod, PVC, and Namespace.
        """
        log.info("Cleaning up Fill Pool Job resources...")

        if self.job_obj:
            job_name = getattr(self.job_obj, "name", "<unknown>")
            log.info(f"Deleting Job {job_name}")
            try:
                self.job_obj.delete()
            except Exception as e:
                log.warning(f"Failed to delete Job {job_name}: {e}")

        # Delete Pod if it still exists
        if self.pod_obj:
            pod_name = getattr(self.pod_obj, "name", "<unknown>")
            log.info(f"Deleting Pod {pod_name}")
            try:
                self.pod_obj.delete()
            except Exception as e:
                log.warning(f"Failed to delete Pod {pod_name}: {e}")

        if self.pvc_obj:
            pvc_name = getattr(self.pvc_obj, "name", "<unknown>")
            log.info(f"Deleting PVC {pvc_name}")
            try:
                self.pvc_obj.delete()
            except Exception as e:
                log.warning(f"Failed to delete PVC {pvc_name}: {e}")
        if self.namespace:
            log.info(f"Deleting Namespace {self.namespace}")
            catch_exceptions(CommandFailed)(exec_cmd)(
                f"oc delete project {self.namespace}"
            )
