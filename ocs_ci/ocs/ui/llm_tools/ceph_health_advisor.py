import json
import logging

from ocs_ci.ocs.ui.llm_tools.llm_helper import get_llm_client

logger = logging.getLogger(__name__)


DIAGNOSIS_PROMPT = """\
You are a Ceph storage expert. Analyze the following Ceph cluster health \
data and provide a diagnosis.

=== CEPH HEALTH DETAIL ===
{health_detail}

=== CEPH STATUS ===
{ceph_status}

=== OSD DISK USAGE (ceph osd df) ===
{osd_df}

=== RECENT LOG EVENTS (ceph log last 20) ===
{ceph_log}

Based on this data, provide your diagnosis as a JSON object with these fields:
- "severity": one of "OK", "WARNING", "ERROR", "CRITICAL"
- "summary": a single sentence describing the overall health situation
- "root_cause": a detailed explanation of what is causing the issue(s)
- "affected_components": list of affected Ceph components (e.g. ["OSD", "PG"])
- "fix_steps": list of recommended remediation steps or commands

Respond ONLY with a valid JSON object. No markdown, no explanation, just JSON.
"""

# Maximum characters forwarded to the LLM for large command outputs
_OSD_DF_MAX_CHARS = 3000
_LOG_MAX_CHARS = 2000


class CephHealthDiagnosis:
    """
    Holds the LLM diagnosis of a Ceph cluster health issue.

    Attributes:
        severity (str): "OK", "WARNING", "ERROR", or "CRITICAL".
        summary (str): One-sentence description of the health situation.
        root_cause (str): Detailed explanation of the issue.
        affected_components (list): Ceph components involved (e.g. OSD, PG).
        fix_steps (list): Recommended remediation steps or commands.
        raw_response (str): Raw LLM text response.
        cost_usd (float): LLM cost for this diagnosis request.
    """

    def __init__(
        self,
        severity,
        summary,
        root_cause,
        affected_components,
        fix_steps,
        raw_response="",
        cost_usd=0.0,
    ):
        self.severity = severity
        self.summary = summary
        self.root_cause = root_cause
        self.affected_components = affected_components
        self.fix_steps = fix_steps
        self.raw_response = raw_response
        self.cost_usd = cost_usd

    def __str__(self):
        lines = [
            f"[CephHealthDiagnosis] severity={self.severity}",
            f"  summary    : {self.summary}",
            f"  root_cause : {self.root_cause}",
            f"  components : {', '.join(self.affected_components)}",
            "  fix_steps  :",
        ]
        for i, step in enumerate(self.fix_steps, 1):
            lines.append(f"    {i}. {step}")
        if self.cost_usd:
            lines.append(f"  cost       : ${self.cost_usd:.4f}")
        return "\n".join(lines)


class CephHealthAdvisor:
    """
    AI-powered advisor for diagnosing Ceph cluster health issues.

    Collects health data from the cluster via the Ceph toolbox pod,
    sends it to an LLM, and returns a structured diagnosis with root
    cause and remediation steps.

    Args:
        model (str): LLM model identifier (e.g. "claude:sonnet"). If None,
            reads from ``config.UI_SELENIUM["llm_model"]``.
    """

    def __init__(self, model=None):
        self._client = None
        self._model = model

    @property
    def client(self):
        """Lazy-initialised LLM client."""
        if self._client is None:
            self._client = get_llm_client(model=self._model)
        return self._client

    def _collect_health_data(self):
        """
        Collects Ceph health data from the cluster toolbox pod.

        Returns:
            dict: Keys: health_detail, ceph_status, osd_df, ceph_log.
        """
        from ocs_ci.ocs.cluster import CephCluster
        from ocs_ci.ocs.resources import pod

        cluster = CephCluster()
        toolbox = pod.get_ceph_tools_pod()

        logger.info("Collecting ceph health detail")
        health_detail = cluster.get_ceph_health(detail=True)

        logger.info("Collecting ceph status")
        ceph_status = cluster.get_ceph_status()

        logger.info("Collecting ceph osd df")
        try:
            osd_df = toolbox.exec_cmd_on_pod("ceph osd df", out_yaml_format=False)
            if len(osd_df) > _OSD_DF_MAX_CHARS:
                osd_df = osd_df[:_OSD_DF_MAX_CHARS] + "\n[truncated]"
        except Exception as e:
            logger.warning("Failed to collect ceph osd df: %s", e)
            osd_df = "(unavailable)"

        logger.info("Collecting ceph log last 20")
        try:
            ceph_log = toolbox.exec_cmd_on_pod(
                "ceph log last 20", out_yaml_format=False
            )
            if len(ceph_log) > _LOG_MAX_CHARS:
                ceph_log = ceph_log[:_LOG_MAX_CHARS] + "\n[truncated]"
        except Exception as e:
            logger.warning("Failed to collect ceph log: %s", e)
            ceph_log = "(unavailable)"

        return {
            "health_detail": health_detail,
            "ceph_status": ceph_status,
            "osd_df": osd_df,
            "ceph_log": ceph_log,
        }

    def _parse_diagnosis(self, raw_response, cost_usd):
        """
        Parses the LLM JSON response into a CephHealthDiagnosis.

        Args:
            raw_response (str): Raw LLM text response.
            cost_usd (float): LLM cost for this request.

        Returns:
            CephHealthDiagnosis: Parsed diagnosis object.

        Raises:
            ValueError: If the response cannot be parsed as JSON.
        """
        cleaned = raw_response.strip()
        if cleaned.startswith("```"):
            lines = cleaned.split("\n")
            lines = [l for l in lines if not l.strip().startswith("```")]
            cleaned = "\n".join(lines)

        try:
            data = json.loads(cleaned)
        except json.JSONDecodeError:
            json_start = cleaned.find("{")
            json_end = cleaned.rfind("}") + 1
            if json_start != -1 and json_end > json_start:
                try:
                    data = json.loads(cleaned[json_start:json_end])
                except json.JSONDecodeError:
                    raise ValueError(
                        "Could not parse LLM response as JSON. "
                        f"Raw response: {raw_response[:300]}"
                    )
            else:
                raise ValueError(
                    "Could not parse LLM response as JSON. "
                    f"Raw response: {raw_response[:300]}"
                )

        return CephHealthDiagnosis(
            severity=data.get("severity", "UNKNOWN"),
            summary=data.get("summary", ""),
            root_cause=data.get("root_cause", ""),
            affected_components=data.get("affected_components", []),
            fix_steps=data.get("fix_steps", []),
            raw_response=raw_response,
            cost_usd=cost_usd,
        )

    def diagnose(self):
        """
        Collects Ceph health data and queries the LLM for a diagnosis.

        Returns:
            CephHealthDiagnosis: Structured diagnosis with severity, root
                cause, affected components, and remediation steps.

        Raises:
            RuntimeError: If the LLM is unavailable or the query fails.
            ValueError: If the LLM response cannot be parsed.
        """
        if not self.client.is_available():
            raise RuntimeError(
                "LLM client is not available. "
                "Check model configuration and credentials."
            )

        data = self._collect_health_data()
        prompt = DIAGNOSIS_PROMPT.format(**data)

        logger.info("Querying LLM for Ceph health diagnosis")
        cost_before = self.client.total_cost_usd
        raw_response = self.client.query_dom(prompt)
        cost_usd = self.client.total_cost_usd - cost_before

        diagnosis = self._parse_diagnosis(raw_response, cost_usd)
        logger.info(
            "Ceph health diagnosis: severity=%s summary=%s cost=$%.4f",
            diagnosis.severity,
            diagnosis.summary,
            cost_usd,
        )
        return diagnosis
