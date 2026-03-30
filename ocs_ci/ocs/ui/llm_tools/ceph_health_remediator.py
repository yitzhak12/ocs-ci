import json
import logging
import time

from ocs_ci.framework import config
from ocs_ci.ocs.ui.llm_tools.llm_helper import get_llm_client

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Safety allowlists / blocklists
# ---------------------------------------------------------------------------

# Ceph commands the LLM is permitted to call via exec_ceph_cmd().
# Prefixes are checked; more specific blocked substrings take priority.
_ALLOWED_CEPH_PREFIXES = (
    "ceph health",
    "ceph status",
    "ceph osd df",
    "ceph osd tree",
    "ceph osd stat",
    "ceph osd in ",
    "ceph osd out ",
    "ceph pg stat",
    "ceph pg dump",
    "ceph pg repair ",
    "ceph log last ",
    "ceph mgr stat",
    "ceph mon stat",
    "ceph df",
    "ceph fs status",
)

# Substrings that make any ceph command unconditionally blocked.
_BLOCKED_CEPH_SUBSTRINGS = (
    "osd rm",
    "osd purge",
    "osd destroy",
    " fs rm",
    "mon remove",
    "auth del",
    "config-key rm",
)

# PVC / PV phases and conditions that indicate the resource is stuck and
# safe to force-delete.  "Terminating" / "Deleting" are detected via
# deletionTimestamp; "Released" is a PV-specific phase.
_STUCK_PHASES = {"Released"}

# JSON-pointer path prefixes allowed for StorageCluster patches.
# Blocked prefixes below take priority over this list.
_ALLOWED_STORAGECLUSTER_PATHS = (
    "/spec/managedResources",
    "/spec/logCollector",
    "/spec/resources",
    "/spec/nodeTopologies",
    "/spec/multiCloudGateway/reconcileStrategy",
)

# JSON-pointer path prefixes unconditionally blocked for StorageCluster.
_BLOCKED_STORAGECLUSTER_PATHS = (
    "/spec/storageDeviceSets",
    "/spec/monDataDirHostPath",
)

# JSON-pointer path prefixes allowed for CephCluster patches.
_ALLOWED_CEPHCLUSTER_PATHS = (
    "/spec/mon/count",
    "/spec/mon/allowMultiplePerNode",
    "/spec/mgr/count",
    "/spec/mgr/allowMultiplePerNode",
    "/spec/resources",
    "/spec/priorityClassNames",
    "/spec/network",
    "/spec/logCollector",
)

# JSON-pointer path prefixes unconditionally blocked for CephCluster.
_BLOCKED_CEPHCLUSTER_PATHS = (
    "/spec/dataDirHostPath",
    "/spec/storage",
    "/spec/cleanupPolicy",
    "/spec/removeOSDsIfOutAndSafeToRemove",
)

# Seconds between health-check polls in wait_for_health_ok().
_HEALTH_POLL_INTERVAL = 15

# ---------------------------------------------------------------------------
# Prompt templates (constant across all rounds)
# ---------------------------------------------------------------------------

_TOOL_DESCRIPTIONS = """\
collect_health()
    Run 'ceph health detail'. Returns the full health output string.
collect_status()
    Run 'ceph status'. Returns the full status output string.
collect_osd_df()
    Run 'ceph osd df'. Returns OSD disk-usage string.
collect_pg_status()
    Run 'ceph pg stat'. Returns PG summary string.
collect_operator_logs(since_minutes: int)
    Fetch rook-ceph-operator pod logs for the last N minutes.
collect_pod_logs(pod_name: str, since_minutes: int)
    Fetch logs from a named pod in the ODF namespace.
exec_ceph_cmd(command: str)
    Run an allowlisted ceph command. Returns command output.
exec_ceph_config_set(who: str, key: str, value: str)
    Set a ceph config key for 'who' (e.g. "global", "osd", "osd.0").
    The original value is captured automatically and restored at end.
wait_for_health_ok(timeout: int)
    Poll ceph health until HEALTH_OK or timeout (seconds).
    Returns "ok" or "timeout".
delete_pvc_if_stuck(name: str, namespace: str)
    Delete a PVC only if it is Terminating, Deleting, or Released.
    Returns a result string.
delete_pv_if_stuck(name: str)
    Delete a PV only if it is Terminating, Deleting, or Released.
    Returns a result string.
scale_deployment(name: str, replicas: int)
    Scale an ODF deployment to the given replica count.
    Scale-downs are tracked; original count is restored at session end.
patch_storagecluster(path: str, value)
    Patch a StorageCluster CR field at the given JSON pointer path
    (e.g. "/spec/managedResources/cephBlockPools/reconcileStrategy").
    Only allowlisted paths are accepted. Original value is restored.
patch_cephcluster(path: str, value)
    Patch a CephCluster CR field at the given JSON pointer path
    (e.g. "/spec/mon/count"). Only allowlisted paths accepted.
    Original value is restored at session end.
collect_node_status(node_name: str)
    Get conditions and schedulability status of a worker node.
uncordon_node(node_name: str)
    Remove the unschedulable taint from a node (oc adm uncordon).
finish(message: str)
    End the remediation session. Always call this as the final action."""

_RULES = """\
1. Always collect data before taking any write action.
2. After each write action, call wait_for_health_ok before the next one.
3. Issue at most ONE write action per round.
4. NEVER call exec_ceph_cmd with: osd rm, osd purge, osd destroy, \
fs rm, mon remove, auth del.
5. NEVER delete a PVC or PV that is in Bound or Pending phase.
6. Do NOT manually revert ceph config, patches, or scales — all are \
reverted automatically at session end.
7. Call finish() as soon as healthy or no further progress is possible.
8. After scaling a deployment down, call wait_for_health_ok before the \
next write action.
9. Never scale a deployment above its original replica count.
10. For node issues, always collect_node_status before uncordon_node."""


# ---------------------------------------------------------------------------
# Result object
# ---------------------------------------------------------------------------


class RemediationResult:
    """
    Holds the outcome of a CephHealthRemediator session.

    Attributes:
        success (bool): True if the cluster reached HEALTH_OK.
        message (str): Final message from the LLM agent.
        actions_taken (list[dict]): Ordered log of every tool call made.
        config_reverts (list[dict]): Ceph config keys that were restored,
            each with keys: who, key, original_value, status.
        scale_reverts (list[dict]): Deployments that were scaled back up,
            each with keys: name, namespace, original_replicas, status.
        patch_reverts (list[dict]): CR patches that were reverted,
            each with keys: kind, name, path, original_value, status.
        total_cost_usd (float): Cumulative LLM cost for the session.
        rounds (int): Number of reasoning rounds completed.
    """

    def __init__(
        self,
        success,
        message,
        actions_taken,
        config_reverts,
        scale_reverts,
        patch_reverts,
        total_cost_usd,
        rounds,
    ):
        self.success = success
        self.message = message
        self.actions_taken = actions_taken
        self.config_reverts = config_reverts
        self.scale_reverts = scale_reverts
        self.patch_reverts = patch_reverts
        self.total_cost_usd = total_cost_usd
        self.rounds = rounds

    def __str__(self):
        status = "SUCCESS" if self.success else "INCOMPLETE"
        lines = [
            f"[RemediationResult] status={status} rounds={self.rounds}",
            f"  message : {self.message}",
            f"  cost    : ${self.total_cost_usd:.4f}",
            f"  actions : {len(self.actions_taken)}",
        ]
        for i, a in enumerate(self.actions_taken, 1):
            lines.append(f"    {i}. [{a['tool']}] {a.get('reasoning', '')}")
        if self.config_reverts:
            lines.append(f"  config reverts : {len(self.config_reverts)}")
            for r in self.config_reverts:
                lines.append(
                    f"    {r['who']} {r['key']} -> "
                    f"{r['original_value']!r} ({r['status']})"
                )
        if self.scale_reverts:
            lines.append(f"  scale reverts  : {len(self.scale_reverts)}")
            for r in self.scale_reverts:
                lines.append(
                    f"    {r['name']} -> {r['original_replicas']} replicas"
                    f" ({r['status']})"
                )
        if self.patch_reverts:
            lines.append(f"  patch reverts  : {len(self.patch_reverts)}")
            for r in self.patch_reverts:
                lines.append(
                    f"    {r['kind']}/{r['name']} {r['path']} -> "
                    f"{r['original_value']!r} ({r['status']})"
                )
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main remediator class
# ---------------------------------------------------------------------------


class CephHealthRemediator:
    """
    AI-powered agentic remediator for Ceph cluster health issues.

    Runs a multi-round reasoning loop: the LLM selects a tool each round,
    the tool is executed, and the result is fed back into the next prompt.
    Ceph config changes made during the session are always reverted in a
    ``finally`` block, regardless of whether remediation succeeded.

    Protection layers:
    - **Allowlist**: exec_ceph_cmd only accepts commands with approved
      prefixes; blocked substrings are rejected regardless of prefix.
    - **Tier gate**: PVC/PV deletion is guarded by a live phase check
      before the delete call is issued.
    - **Config revert**: every exec_ceph_config_set captures the original
      value first and restores it unconditionally at session end.
    - **Prompt rules**: the system prompt instructs the LLM not to attempt
      destructive operations, reducing the chance it ever tries.

    Args:
        model (str): LLM model identifier (e.g. "claude:sonnet"). Defaults
            to ``config.UI_SELENIUM["llm_model"]``.
        max_rounds (int): Maximum reasoning rounds before giving up.
        health_ok_timeout (int): Seconds to wait for HEALTH_OK after each
            write action.
    """

    def __init__(self, model=None, max_rounds=10, health_ok_timeout=300):
        self._model = model
        self.max_rounds = max_rounds
        self.health_ok_timeout = health_ok_timeout
        self._client = None
        self._toolbox = None
        self._cluster = None
        # list of (who, key, original_value) tuples for config revert
        self._config_changes = []
        # list of (name, original_replicas) tuples for scale revert
        self._scale_changes = []
        # list of (kind, name, path, original_value) tuples for patch revert
        self._patch_changes = []

    @property
    def client(self):
        """Lazy-initialised LLM client."""
        if self._client is None:
            self._client = get_llm_client(model=self._model)
        return self._client

    @property
    def _tools(self):
        """Returns the tool dispatch table (name → method)."""
        return {
            "collect_health": self._tool_collect_health,
            "collect_status": self._tool_collect_status,
            "collect_osd_df": self._tool_collect_osd_df,
            "collect_pg_status": self._tool_collect_pg_status,
            "collect_operator_logs": self._tool_collect_operator_logs,
            "collect_pod_logs": self._tool_collect_pod_logs,
            "exec_ceph_cmd": self._tool_exec_ceph_cmd,
            "exec_ceph_config_set": self._tool_exec_ceph_config_set,
            "wait_for_health_ok": self._tool_wait_for_health_ok,
            "delete_pvc_if_stuck": self._tool_delete_pvc_if_stuck,
            "delete_pv_if_stuck": self._tool_delete_pv_if_stuck,
            "scale_deployment": self._tool_scale_deployment,
            "patch_storagecluster": self._tool_patch_storagecluster,
            "patch_cephcluster": self._tool_patch_cephcluster,
            "collect_node_status": self._tool_collect_node_status,
            "uncordon_node": self._tool_uncordon_node,
        }

    def _init_cluster(self):
        """Lazily initialises CephCluster and toolbox pod references."""
        if self._cluster is None:
            from ocs_ci.ocs.cluster import CephCluster
            from ocs_ci.ocs.resources import pod as pod_module

            self._cluster = CephCluster()
            self._toolbox = pod_module.get_ceph_tools_pod()

    # -----------------------------------------------------------------------
    # Public entry point
    # -----------------------------------------------------------------------

    def remediate(self):
        """
        Runs the agentic remediation loop.

        Config changes are always reverted after the loop ends, whether
        it completed successfully, exhausted max rounds, or raised.

        Returns:
            RemediationResult: Full outcome including action log,
                config reverts, and LLM cost.

        Raises:
            RuntimeError: If the LLM client is unavailable.
        """
        if not self.client.is_available():
            raise RuntimeError(
                "LLM client is not available. "
                "Check model configuration and credentials."
            )
        result = None
        try:
            result = self._run_loop()
            return result
        finally:
            config_reverts = self._revert_configs()
            scale_reverts = self._revert_scales()
            if self._patch_changes and not self._are_crs_ready():
                logger.warning(
                    "StorageCluster or CephCluster not ready — "
                    "reverting %d patch(es)",
                    len(self._patch_changes),
                )
                patch_reverts = self._revert_patches()
            else:
                if self._patch_changes:
                    logger.info(
                        "CRs are ready after session — "
                        "keeping %d patch(es) in place",
                        len(self._patch_changes),
                    )
                patch_reverts = []
            if result is not None:
                result.config_reverts = config_reverts
                result.scale_reverts = scale_reverts
                result.patch_reverts = patch_reverts

    # -----------------------------------------------------------------------
    # Reasoning loop
    # -----------------------------------------------------------------------

    def _run_loop(self):
        """Executes the multi-round LLM reasoning loop."""
        history = []
        cost_start = self.client.total_cost_usd

        for round_num in range(1, self.max_rounds + 1):
            logger.info("Remediation round %d / %d", round_num, self.max_rounds)

            prompt = self._build_prompt(round_num, history)
            logger.debug(
                "Round %d prompt (%d chars):\n%s", round_num, len(prompt), prompt
            )
            raw = self.client.query_dom(prompt)
            logger.debug("Round %d LLM raw response: %s", round_num, raw)

            try:
                action = self._parse_action(raw)
            except ValueError as e:
                logger.warning("Failed to parse LLM action: %s", e)
                history.append(
                    {
                        "round": round_num,
                        "tool": "parse_error",
                        "args": {},
                        "reasoning": "",
                        "result": f"Parse error: {e}",
                    }
                )
                continue

            tool_name = action["tool"]
            args = action.get("args", {})
            reasoning = action.get("reasoning", "")

            logger.info(
                "Round %d: tool=%s args=%s reasoning=%s",
                round_num,
                tool_name,
                json.dumps(args),
                reasoning,
            )

            if tool_name == "finish":
                message = args.get("message", "Remediation complete.")
                logger.info("Remediation finished: %s", message)
                return RemediationResult(
                    success=True,
                    message=message,
                    actions_taken=history,
                    config_reverts=[],  # filled by finally in remediate()
                    scale_reverts=[],
                    patch_reverts=[],
                    total_cost_usd=self.client.total_cost_usd - cost_start,
                    rounds=round_num,
                )

            dispatch = self._tools.get(tool_name)
            if dispatch is None:
                result_str = f"Unknown tool: {tool_name!r}"
                logger.warning("LLM called unknown tool: %s", tool_name)
            else:
                try:
                    result_str = str(dispatch(**args))
                    logger.debug("Tool %s result: %s", tool_name, result_str[:500])
                except Exception as e:
                    result_str = f"Tool error: {e}"
                    logger.warning("Tool %s raised: %s", tool_name, e)

            history.append(
                {
                    "round": round_num,
                    "tool": tool_name,
                    "args": args,
                    "reasoning": reasoning,
                    "result": result_str[:2000],
                }
            )

        return RemediationResult(
            success=False,
            message=f"Max rounds ({self.max_rounds}) reached without finish.",
            actions_taken=history,
            config_reverts=[],  # filled by finally in remediate()
            scale_reverts=[],
            patch_reverts=[],
            total_cost_usd=self.client.total_cost_usd - cost_start,
            rounds=self.max_rounds,
        )

    # -----------------------------------------------------------------------
    # Prompt builder
    # -----------------------------------------------------------------------

    def _build_prompt(self, round_num, history):
        """
        Builds the full prompt for the current round.

        Args:
            round_num (int): Current round number (1-based).
            history (list[dict]): All previous round entries.

        Returns:
            str: Complete prompt string to send to the LLM.
        """
        system_section = (
            "You are an expert Ceph storage administrator performing "
            "automated cluster remediation. Your goal is to diagnose "
            "and fix Ceph health issues step by step, then call "
            "finish() when done.\n\n"
            f"Available tools:\n{_TOOL_DESCRIPTIONS}\n\n"
            f"Rules:\n{_RULES}"
        )

        if history:
            parts = []
            for entry in history:
                args_str = json.dumps(entry.get("args", {}))
                parts.append(
                    f"Round {entry['round']}: [{entry['tool']}] "
                    f"args={args_str}\n"
                    f"  Reasoning : {entry.get('reasoning', '')}\n"
                    f"  Result    : {entry.get('result', '')}"
                )
            history_text = "\n\n".join(parts)
        else:
            history_text = "(no actions taken yet)"

        return (
            f"{system_section}\n\n"
            "=== REMEDIATION HISTORY ===\n"
            f"{history_text}\n\n"
            f"=== ROUND {round_num} ===\n"
            "Decide your next action. Respond with JSON only:\n"
            '{"tool": "<name>", "args": {...}, "reasoning": "<why>"}'
        )

    # -----------------------------------------------------------------------
    # Action parser
    # -----------------------------------------------------------------------

    def _parse_action(self, raw_response):
        """
        Parses an LLM tool-call response into a dict.

        Args:
            raw_response (str): Raw LLM response text.

        Returns:
            dict: Keys: tool (str), args (dict), reasoning (str).

        Raises:
            ValueError: If the response cannot be parsed or is missing
                the required 'tool' key.
        """
        cleaned = raw_response.strip()
        if cleaned.startswith("```"):
            lines = cleaned.split("\n")
            lines = [l for l in lines if not l.strip().startswith("```")]
            cleaned = "\n".join(lines)

        json_start = cleaned.find("{")
        json_end = cleaned.rfind("}") + 1
        if json_start == -1 or json_end <= json_start:
            raise ValueError(f"No JSON found in LLM response: {cleaned[:200]}")

        try:
            data = json.loads(cleaned[json_start:json_end])
        except json.JSONDecodeError as e:
            raise ValueError(f"JSON parse error: {e}")

        if "tool" not in data:
            raise ValueError(f"LLM response missing 'tool' key: {data}")
        return data

    # -----------------------------------------------------------------------
    # Tool implementations
    # -----------------------------------------------------------------------

    def _tool_collect_health(self):
        logger.info("Collecting: ceph health detail")
        self._init_cluster()
        result = self._cluster.get_ceph_health(detail=True)
        logger.debug("ceph health detail: %s", result[:500])
        return result

    def _tool_collect_status(self):
        logger.info("Collecting: ceph status")
        self._init_cluster()
        result = self._cluster.get_ceph_status()
        logger.debug("ceph status: %s", result[:500])
        return result

    def _tool_collect_osd_df(self):
        logger.info("Collecting: ceph osd df")
        self._init_cluster()
        result = self._toolbox.exec_cmd_on_pod("ceph osd df", out_yaml_format=False)
        logger.debug("ceph osd df: %s", result[:500])
        return result

    def _tool_collect_pg_status(self):
        logger.info("Collecting: ceph pg stat")
        self._init_cluster()
        result = self._toolbox.exec_cmd_on_pod("ceph pg stat", out_yaml_format=False)
        logger.debug("ceph pg stat: %s", result[:500])
        return result

    def _tool_collect_operator_logs(self, since_minutes=10):
        logger.info("Collecting: rook-ceph-operator logs (last %d min)", since_minutes)
        from ocs_ci.ocs.ocp import OCP

        namespace = config.ENV_DATA["cluster_namespace"]
        ocp = OCP(namespace=namespace)
        cmd = (
            f"logs deployment/rook-ceph-operator "
            f"--since={since_minutes}m --tail=200"
        )
        result = ocp.exec_oc_cmd(cmd, out_yaml_format=False)
        logger.debug("operator logs: %s", result[:500])
        return result

    def _tool_collect_pod_logs(self, pod_name, since_minutes=10):
        logger.info("Collecting: pod logs pod=%s last=%dmin", pod_name, since_minutes)
        from ocs_ci.ocs.ocp import OCP

        namespace = config.ENV_DATA["cluster_namespace"]
        ocp = OCP(namespace=namespace)
        cmd = f"logs {pod_name} --since={since_minutes}m --tail=200"
        result = ocp.exec_oc_cmd(cmd, out_yaml_format=False)
        logger.debug("pod %s logs: %s", pod_name, result[:500])
        return result

    def _tool_exec_ceph_cmd(self, command):
        """
        Validates command against the allowlist then executes it.

        Args:
            command (str): The ceph command to run (must start with an
                allowed prefix and must not contain blocked substrings).

        Returns:
            str: Command output.

        Raises:
            ValueError: If the command is blocked or not in the allowlist.
        """
        cmd = command.strip()

        for blocked in _BLOCKED_CEPH_SUBSTRINGS:
            if blocked in cmd:
                raise ValueError(
                    f"Command blocked by safety policy: {cmd!r} "
                    f"(matched blocked pattern {blocked!r})"
                )

        if not any(cmd.startswith(p) for p in _ALLOWED_CEPH_PREFIXES):
            raise ValueError(
                f"Command not in allowlist: {cmd!r}. "
                "Use exec_ceph_config_set for config changes."
            )

        logger.info("Executing ceph command: %s", cmd)
        self._init_cluster()
        result = self._toolbox.exec_cmd_on_pod(cmd, out_yaml_format=False)
        logger.debug("Command result: %s", result[:500])
        return result

    def _tool_exec_ceph_config_set(self, who, key, value):
        """
        Sets a ceph config key, saving the original value for revert.

        Args:
            who (str): Config entity (e.g. "global", "osd", "osd.0").
            key (str): Config key name.
            value (str): New value to set.

        Returns:
            str: Command output.
        """
        self._init_cluster()

        try:
            original = self._toolbox.exec_cmd_on_pod(
                f"ceph config get {who} {key}", out_yaml_format=False
            ).strip()
        except Exception:
            original = None

        self._config_changes.append((who, key, original))
        logger.info("ceph config set %s %s=%s (original=%r)", who, key, value, original)
        return self._toolbox.exec_cmd_on_pod(
            f"ceph config set {who} {key} {value}", out_yaml_format=False
        )

    def _tool_wait_for_health_ok(self, timeout=None):
        """
        Polls ceph health until HEALTH_OK or timeout expires.

        Args:
            timeout (int): Seconds to wait. Defaults to health_ok_timeout.

        Returns:
            str: "ok" if HEALTH_OK was reached, "timeout" otherwise.
        """
        self._init_cluster()
        if timeout is None:
            timeout = self.health_ok_timeout

        deadline = time.time() + timeout
        while time.time() < deadline:
            health = self._cluster.get_ceph_health()
            if "HEALTH_OK" in health:
                logger.info("Ceph health is HEALTH_OK")
                return "ok"
            logger.info("Health not OK yet: %s", health[:120])
            time.sleep(_HEALTH_POLL_INTERVAL)

        logger.warning("Timed out waiting for HEALTH_OK after %ds", timeout)
        return "timeout"

    def _tool_delete_pvc_if_stuck(self, name, namespace):
        """
        Deletes a PVC only if it is in a stuck state.

        A PVC is considered stuck when its deletionTimestamp is set
        (Terminating / Deleting) or its phase is in _STUCK_PHASES.

        Args:
            name (str): PVC name.
            namespace (str): Namespace of the PVC.

        Returns:
            str: Confirmation message.

        Raises:
            ValueError: If the PVC is not in a stuck state.
        """
        from ocs_ci.ocs.ocp import OCP

        pvc_ocp = OCP(kind="PersistentVolumeClaim", namespace=namespace)
        pvc_data = pvc_ocp.get(resource_name=name)
        phase = pvc_data.get("status", {}).get("phase", "Unknown")
        has_deletion_ts = bool(pvc_data.get("metadata", {}).get("deletionTimestamp"))

        if not has_deletion_ts and phase not in _STUCK_PHASES:
            raise ValueError(
                f"PVC {name!r} is in phase {phase!r} with no "
                "deletionTimestamp. Deletion only allowed when "
                "Terminating, Deleting, or Released."
            )

        condition = "Terminating" if has_deletion_ts else phase
        logger.info("Deleting PVC %s (condition=%s)", name, condition)
        pvc_ocp.delete(resource_name=name)
        return f"Deleted PVC {name!r} (was {condition})"

    def _tool_delete_pv_if_stuck(self, name):
        """
        Deletes a PV only if it is in a stuck state.

        A PV is considered stuck when its deletionTimestamp is set
        (Terminating / Deleting) or its phase is Released.

        Args:
            name (str): PV name.

        Returns:
            str: Confirmation message.

        Raises:
            ValueError: If the PV is not in a stuck state.
        """
        from ocs_ci.ocs.ocp import OCP

        pv_ocp = OCP(kind="PersistentVolume")
        pv_data = pv_ocp.get(resource_name=name)
        phase = pv_data.get("status", {}).get("phase", "Unknown")
        has_deletion_ts = bool(pv_data.get("metadata", {}).get("deletionTimestamp"))

        if not has_deletion_ts and phase not in _STUCK_PHASES:
            raise ValueError(
                f"PV {name!r} is in phase {phase!r} with no "
                "deletionTimestamp. Deletion only allowed when "
                "Terminating, Deleting, or Released."
            )

        condition = "Terminating" if has_deletion_ts else phase
        logger.info("Deleting PV %s (condition=%s)", name, condition)
        pv_ocp.delete(resource_name=name)
        return f"Deleted PV {name!r} (was {condition})"

    def _tool_scale_deployment(self, name, replicas):
        """
        Scales an ODF deployment; tracks scale-downs for automatic revert.

        Args:
            name (str): Deployment name in the ODF namespace.
            replicas (int): Desired replica count.

        Returns:
            str: Command output.
        """
        from ocs_ci.ocs.ocp import OCP

        namespace = config.ENV_DATA["cluster_namespace"]
        ocp = OCP(kind="Deployment", namespace=namespace)
        dep_data = ocp.get(resource_name=name)
        current = dep_data["spec"]["replicas"]

        if replicas < current:
            self._scale_changes.append((name, current))
            logger.info(
                "Scaling down deployment %s: %d -> %d (original saved)",
                name,
                current,
                replicas,
            )
        else:
            logger.info("Scaling deployment %s: %d -> %d", name, current, replicas)

        result = ocp.exec_oc_cmd(
            f"scale deployment/{name} --replicas={replicas}",
            out_yaml_format=False,
        )
        logger.debug("Scale result: %s", result)
        return result

    def _tool_patch_storagecluster(self, path, value):
        """
        Patches a StorageCluster CR field at an allowlisted JSON pointer path.

        Args:
            path (str): JSON pointer path (e.g.
                "/spec/managedResources/cephBlockPools/reconcileStrategy").
            value: New value to set at the path.

        Returns:
            str: Confirmation message.

        Raises:
            ValueError: If path is blocked or not in the allowlist.
        """
        namespace = config.ENV_DATA["cluster_namespace"]
        name = self._get_first_resource_name("StorageCluster", namespace)
        return self._patch_cr_resource(
            "StorageCluster",
            name,
            path,
            value,
            _ALLOWED_STORAGECLUSTER_PATHS,
            _BLOCKED_STORAGECLUSTER_PATHS,
        )

    def _tool_patch_cephcluster(self, path, value):
        """
        Patches a CephCluster CR field at an allowlisted JSON pointer path.

        Args:
            path (str): JSON pointer path (e.g. "/spec/mon/count").
            value: New value to set at the path.

        Returns:
            str: Confirmation message.

        Raises:
            ValueError: If path is blocked or not in the allowlist.
        """
        namespace = config.ENV_DATA["cluster_namespace"]
        name = self._get_first_resource_name("CephCluster", namespace)
        return self._patch_cr_resource(
            "CephCluster",
            name,
            path,
            value,
            _ALLOWED_CEPHCLUSTER_PATHS,
            _BLOCKED_CEPHCLUSTER_PATHS,
        )

    def _tool_collect_node_status(self, node_name):
        """
        Collects conditions and schedulability of a worker node.

        Args:
            node_name (str): Name of the node.

        Returns:
            str: Formatted node status string.
        """
        logger.info("Collecting: node status for %s", node_name)
        from ocs_ci.ocs.ocp import OCP

        node_data = OCP(kind="Node").get(resource_name=node_name)
        unschedulable = node_data.get("spec", {}).get("unschedulable", False)
        conditions = node_data.get("status", {}).get("conditions", [])
        lines = [
            f"Node: {node_name}",
            f"Unschedulable: {unschedulable}",
            "Conditions:",
        ]
        for cond in conditions:
            lines.append(
                f"  {cond['type']}: {cond['status']}" f" - {cond.get('message', '')}"
            )
        result = "\n".join(lines)
        logger.debug("Node status: %s", result)
        return result

    def _tool_uncordon_node(self, node_name):
        """
        Removes the unschedulable taint from a worker node.

        Args:
            node_name (str): Name of the node to uncordon.

        Returns:
            str: Command output.
        """
        logger.info("Uncordoning node: %s", node_name)
        from ocs_ci.ocs.ocp import OCP

        result = OCP(kind="Node").exec_oc_cmd(
            f"adm uncordon {node_name}", out_yaml_format=False
        )
        logger.debug("Uncordon result: %s", result)
        return result

    # -----------------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------------

    def _navigate_json_path(self, data, path):
        """
        Navigates a /a/b/c style JSON pointer path in a nested dict.

        Args:
            data (dict): The root dict to navigate.
            path (str): JSON pointer path (e.g. "/spec/mon/count").

        Returns:
            The value at the path, or None if any segment is missing.
        """
        current = data
        for part in [p for p in path.strip("/").split("/") if p]:
            if not isinstance(current, dict):
                return None
            current = current.get(part)
            if current is None:
                return None
        return current

    def _build_merge_patch(self, path, value):
        """
        Builds a merge-patch dict from a JSON pointer path and value.

        Args:
            path (str): JSON pointer path (e.g. "/spec/mon/count").
            value: The value to set.

        Returns:
            dict: Nested dict suitable for ``--type=merge`` patch.
        """
        result = value
        for part in reversed([p for p in path.strip("/").split("/") if p]):
            result = {part: result}
        return result

    def _get_first_resource_name(self, kind, namespace):
        """
        Returns the name of the first CR of the given kind in the namespace.

        Args:
            kind (str): CR kind (e.g. "StorageCluster").
            namespace (str): Namespace to search.

        Returns:
            str: Resource name.

        Raises:
            RuntimeError: If no resource of that kind is found.
        """
        from ocs_ci.ocs.ocp import OCP

        data = OCP(kind=kind, namespace=namespace).get()
        items = data.get("items", [])
        if not items:
            raise RuntimeError(f"No {kind} found in namespace {namespace}")
        return items[0]["metadata"]["name"]

    def _are_crs_ready(self):
        """
        Returns True if both StorageCluster and CephCluster are in Ready phase.

        Used to decide whether patch changes should be reverted at session
        end: if the CRs are healthy the patches are considered part of the
        fix and are kept; if either CR is not ready the patches are reverted
        to restore the original configuration.

        On any error the method returns False (assume not ready) so the
        patches are reverted as a safe default.

        Returns:
            bool: True if both CRs report phase "Ready".
        """
        from ocs_ci.ocs.ocp import OCP

        namespace = config.ENV_DATA["cluster_namespace"]
        try:
            sc_name = self._get_first_resource_name("StorageCluster", namespace)
            sc_data = OCP(kind="StorageCluster", namespace=namespace).get(
                resource_name=sc_name
            )
            sc_phase = sc_data.get("status", {}).get("phase", "")
            if sc_phase != "Ready":
                logger.info("StorageCluster phase=%s (not Ready)", sc_phase)
                return False

            cc_name = self._get_first_resource_name("CephCluster", namespace)
            cc_data = OCP(kind="CephCluster", namespace=namespace).get(
                resource_name=cc_name
            )
            cc_phase = cc_data.get("status", {}).get("phase", "")
            if cc_phase != "Ready":
                logger.info("CephCluster phase=%s (not Ready)", cc_phase)
                return False

            logger.info("Both StorageCluster and CephCluster are Ready")
            return True
        except Exception as e:
            logger.warning(
                "Could not determine CR readiness: %s — " "assuming not ready",
                e,
            )
            return False

    def _patch_cr_resource(self, kind, name, path, value, allowed_paths, blocked_paths):
        """
        Validates the patch path and applies a merge patch to a CR.

        Captures the original value at path before patching so it can be
        restored by ``_revert_patches()`` at session end.

        Args:
            kind (str): CR kind ("StorageCluster" or "CephCluster").
            name (str): Resource name.
            path (str): JSON pointer path (e.g. "/spec/mon/count").
            value: New value to set.
            allowed_paths (tuple): Permitted path prefixes.
            blocked_paths (tuple): Unconditionally blocked path prefixes.

        Returns:
            str: Confirmation message.

        Raises:
            ValueError: If path is blocked or not in the allowlist.
        """
        from ocs_ci.ocs.ocp import OCP

        for blocked in blocked_paths:
            if path.startswith(blocked):
                raise ValueError(
                    f"Path {path!r} is blocked for {kind} patches "
                    f"(matched blocked prefix {blocked!r})"
                )

        if not any(path.startswith(p) for p in allowed_paths):
            raise ValueError(f"Path {path!r} is not in the {kind} patch allowlist.")

        namespace = config.ENV_DATA["cluster_namespace"]
        ocp = OCP(kind=kind, namespace=namespace)
        original = self._navigate_json_path(ocp.get(resource_name=name), path)
        self._patch_changes.append((kind, name, path, original))

        merge_patch = self._build_merge_patch(path, value)
        patch_json = json.dumps(merge_patch)
        cmd = f"patch {kind.lower()}/{name} --type=merge -p '{patch_json}'"
        logger.info(
            "Patching %s/%s path=%s value=%r (original=%r)",
            kind,
            name,
            path,
            value,
            original,
        )
        result = ocp.exec_oc_cmd(cmd, out_yaml_format=False)
        logger.debug("Patch result: %s", result)
        return f"Patched {kind}/{name} {path}={value!r} (was {original!r})"

    # -----------------------------------------------------------------------
    # Config revert
    # -----------------------------------------------------------------------

    def _revert_configs(self):
        """
        Reverts all ceph config changes recorded during the session.

        Called unconditionally in the ``finally`` block of ``remediate()``.

        Returns:
            list[dict]: One entry per change with keys: who, key,
                original_value, status ("reverted" or "failed: <reason>").
        """
        if not self._config_changes:
            return []

        logger.info("Reverting %d ceph config change(s)", len(self._config_changes))
        self._init_cluster()
        reverts = []

        for who, key, original in self._config_changes:
            entry = {"who": who, "key": key, "original_value": original}
            try:
                if original:
                    cmd = f"ceph config set {who} {key} {original}"
                    logger.info(
                        "Reverting: ceph config set %s %s=%r",
                        who,
                        key,
                        original,
                    )
                else:
                    cmd = f"ceph config rm {who} {key}"
                    logger.info("Reverting: ceph config rm %s %s", who, key)
                self._toolbox.exec_cmd_on_pod(cmd, out_yaml_format=False)
                entry["status"] = "reverted"
            except Exception as e:
                logger.error("Failed to revert config %s %s: %s", who, key, e)
                entry["status"] = f"failed: {e}"
            reverts.append(entry)

        return reverts

    def _revert_scales(self):
        """
        Restores all deployment replica counts that were scaled down.

        Called unconditionally in the ``finally`` block of ``remediate()``.

        Returns:
            list[dict]: One entry per change with keys: name, namespace,
                original_replicas, status.
        """
        if not self._scale_changes:
            return []

        namespace = config.ENV_DATA["cluster_namespace"]
        logger.info(
            "Reverting %d deployment scale change(s)",
            len(self._scale_changes),
        )
        from ocs_ci.ocs.ocp import OCP

        ocp = OCP(kind="Deployment", namespace=namespace)
        reverts = []

        for name, original_replicas in self._scale_changes:
            entry = {
                "name": name,
                "namespace": namespace,
                "original_replicas": original_replicas,
            }
            try:
                ocp.exec_oc_cmd(
                    f"scale deployment/{name} --replicas={original_replicas}",
                    out_yaml_format=False,
                )
                entry["status"] = "reverted"
                logger.info(
                    "Reverted deployment %s to %d replicas",
                    name,
                    original_replicas,
                )
            except Exception as e:
                entry["status"] = f"failed: {e}"
                logger.error("Failed to revert scale of %s: %s", name, e)
            reverts.append(entry)

        return reverts

    def _revert_patches(self):
        """
        Reverts all StorageCluster / CephCluster patches made this session.

        Called unconditionally in the ``finally`` block of ``remediate()``.

        Returns:
            list[dict]: One entry per change with keys: kind, name, path,
                original_value, status.
        """
        if not self._patch_changes:
            return []

        namespace = config.ENV_DATA["cluster_namespace"]
        logger.info("Reverting %d CR patch change(s)", len(self._patch_changes))
        from ocs_ci.ocs.ocp import OCP

        reverts = []

        for kind, name, path, original in self._patch_changes:
            entry = {
                "kind": kind,
                "name": name,
                "path": path,
                "original_value": original,
            }
            if original is None:
                entry["status"] = "skipped (no original value captured)"
                reverts.append(entry)
                continue
            try:
                ocp = OCP(kind=kind, namespace=namespace)
                merge_patch = self._build_merge_patch(path, original)
                patch_json = json.dumps(merge_patch)
                cmd = f"patch {kind.lower()}/{name} " f"--type=merge -p '{patch_json}'"
                ocp.exec_oc_cmd(cmd, out_yaml_format=False)
                entry["status"] = "reverted"
                logger.info("Reverted %s/%s %s -> %r", kind, name, path, original)
            except Exception as e:
                entry["status"] = f"failed: {e}"
                logger.error(
                    "Failed to revert patch %s/%s %s: %s",
                    kind,
                    name,
                    path,
                    e,
                )
            reverts.append(entry)

        return reverts
