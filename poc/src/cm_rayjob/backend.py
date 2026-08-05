"""RayJobK8sBackend — checkmaite JobBackend on RayJob CRs.

Design under test (Tier 3):
- submit  = create a RayJob CR; KubeRay provisions an ephemeral Ray cluster,
  runs the entrypoint, and tears everything down (shutdownAfterJobFinishes +
  TTL). No Ray connection from the driver at all.
- dedupe  = deterministic CR name from (scope, run_key); a duplicate submit
  gets 409 AlreadyExists and reattaches to the existing job. This replaces the
  current backend's registry-actor dedupe index.
- status  = the CR *is* the durable job record (outlives driver and cluster).
- cancel  = delete the CR.
- isolation = whatever the target namespace enforces: ResourceQuota, node
  pools, NetworkPolicy, RBAC. One namespace per group.
"""

from __future__ import annotations

import base64
import hashlib
import textwrap
import time
import uuid
from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Any

from kubernetes import client, config
from kubernetes.client.rest import ApiException

from checkmaite.jobs.protocol import BackpressureError, JobStatus
from cm_rayjob.job import (
    RAYJOB_GROUP,
    RAYJOB_PLURAL,
    RAYJOB_VERSION,
    RayJobHandle,
)
from cm_rayjob.payload import JobPayload

SCOPE_LABEL = "checkmaite.io/scope"
JOB_ID_LABEL = "checkmaite.io/job-id"

# Self-contained stub entrypoint: needs ONLY a python interpreter in the job
# image — no checkmaite, no cm_rayjob install. Level A runs on the stock Ray
# image with zero extras. sys.argv[1] is the base64 JobPayload.
_STUB_SCRIPT = textwrap.dedent(
    """
    import base64, json, sys, time
    p = json.loads(base64.b64decode(sys.argv[1]))
    time.sleep(float(p.get("stub_sleep_s", 0)))
    if p.get("stub_fail"):
        raise SystemExit("stub failure requested: boom")
    ref = {
        "run_uid": p["job_id"],
        "capability_id": "stub",
        "store_uri": None,
        "outputs_uri": None,
        "report": None,
    }
    print("CM_RAYJOB_RESULT:" + json.dumps(ref), flush=True)
    """
).strip()


def _load_kube() -> None:
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()


class RayJobK8sBackend:
    """Implements checkmaite.jobs.protocol.JobBackend (structurally)."""

    def __init__(
        self,
        *,
        namespace: str,
        scope: str,
        image: str = "rayproject/ray:2.43.0",
        head_cpu: str = "500m",
        head_memory: str = "2Gi",
        ttl_seconds_after_finished: int = 600,
        active_deadline_seconds: int = 6 * 3600,
        # pip requirements for capability-mode jobs (per-job env pinning);
        # e.g. ["checkmaite==0.3.0", "git+https://github.com/<fork>#subdirectory=poc"]
        capability_pip: Sequence[str] = (),
        analytics_store: dict[str, Any] | None = None,
    ) -> None:
        _load_kube()
        self._crd = client.CustomObjectsApi()
        self._core = client.CoreV1Api()
        self._namespace = namespace
        self._scope = scope
        self._image = image
        self._head_cpu = head_cpu
        self._head_memory = head_memory
        self._ttl = ttl_seconds_after_finished
        self._active_deadline = active_deadline_seconds
        self._capability_pip = list(capability_pip)
        self._analytics_store = analytics_store

    # ------------------------------------------------------------- name/dedupe

    def _cr_name(self, run_key: str) -> str:
        digest = hashlib.sha256(f"{self._scope}:{run_key}".encode()).hexdigest()[:16]
        return f"cm-{digest}"

    # ---------------------------------------------------------------- CR build

    def _cluster_spec(self, num_cpus: float, memory: str | None) -> dict[str, Any]:
        return {
            "rayVersion": self._image.split(":")[1].split("-")[0],
            "headGroupSpec": {
                "rayStartParams": {},
                "template": {
                    "spec": {
                        "containers": [
                            {
                                "name": "ray-head",
                                "image": self._image,
                                "resources": {
                                    "requests": {
                                        "cpu": self._head_cpu,
                                        "memory": memory or self._head_memory,
                                    },
                                    "limits": {
                                        "cpu": str(max(1, int(num_cpus))),
                                        "memory": memory or self._head_memory,
                                    },
                                },
                            }
                        ]
                    }
                },
            },
            # Head-only for the POC — entrypointNumCpus reserves capacity on
            # it. Real jobs add workerGroupSpecs (or spec.clusterSelector to
            # target a long-lived shared cluster with the same lifecycle).
        }

    def _build_cr(self, payload: JobPayload, resources: dict[str, Any]) -> dict[str, Any]:
        num_cpus = float(resources.get("num_cpus", 1.0))
        num_gpus = float(resources.get("num_gpus", 0.0))
        if payload.mode == "stub":
            script_b64 = base64.b64encode(_STUB_SCRIPT.encode()).decode()
            entrypoint = (
                f'python -c "import base64; exec(base64.b64decode(\'{script_b64}\'))" '
                f"{payload.to_b64()}"
            )
            runtime_env: str | None = None
        else:
            entrypoint = f"python -m cm_rayjob.entrypoint --payload-b64 {payload.to_b64()}"
            pip_lines = "\n".join(f'  - "{req}"' for req in self._capability_pip)
            runtime_env = f"pip:\n{pip_lines}\n" if pip_lines else None

        spec: dict[str, Any] = {
            "entrypoint": entrypoint,
            "entrypointNumCpus": num_cpus,
            "shutdownAfterJobFinishes": True,
            "ttlSecondsAfterFinished": self._ttl,
            "activeDeadlineSeconds": self._active_deadline,
            "rayClusterSpec": self._cluster_spec(num_cpus, resources.get("memory")),
        }
        if num_gpus:
            spec["entrypointNumGpus"] = num_gpus
        if runtime_env:
            spec["runtimeEnvYAML"] = runtime_env

        return {
            "apiVersion": f"{RAYJOB_GROUP}/{RAYJOB_VERSION}",
            "kind": "RayJob",
            "metadata": {
                "name": self._cr_name(payload.run_key),
                "labels": {SCOPE_LABEL: self._scope, JOB_ID_LABEL: payload.job_id},
            },
            "spec": spec,
        }

    # -------------------------------------------------------- JobBackend proto

    def submit_capability(self, capability: Any, **kwargs: Any) -> RayJobHandle:
        """Submit a capability run as a RayJob.

        POC calling conventions (see README "graduation path" for how the real
        implementation converges with the frontend adapter):
        - ``capability`` may be a ``JobPayload`` (fully explicit),
        - or a dotted-path string to a capability class, with declarative
          ``models=/datasets=/metrics=/config=`` spec dicts in kwargs.
        Live capability objects are intentionally NOT accepted: shipping
        objects instead of specs is exactly the version-parity coupling this
        design removes.
        """
        run_key = str(kwargs.pop("run_key", uuid.uuid4()))
        resources = dict(kwargs.pop("resources", None) or {})

        if isinstance(capability, JobPayload):
            payload = capability
        elif isinstance(capability, str):
            payload = JobPayload(
                job_id=str(uuid.uuid4()),
                scope=self._scope,
                run_key=run_key,
                mode="capability",
                capability_class=capability,
                task=kwargs.pop("task", "image_classification"),
                models=kwargs.pop("models", []),
                datasets=kwargs.pop("datasets", []),
                metrics=kwargs.pop("metrics", []),
                config=kwargs.pop("config", {}),
                report_threshold=float(kwargs.pop("report_threshold", 0.5)),
                analytics_store=self._analytics_store,
            )
        else:
            raise TypeError(
                "cm-rayjob POC submits declarative payloads (JobPayload or a "
                "dotted capability path + spec dicts), not live capability "
                "objects — see README."
            )

        cr = self._build_cr(payload, resources)
        name = cr["metadata"]["name"]
        try:
            created = self._crd.create_namespaced_custom_object(
                RAYJOB_GROUP, RAYJOB_VERSION, self._namespace, RAYJOB_PLURAL, cr
            )
        except ApiException as exc:
            if exc.status == 409:
                # Dedupe: same (scope, run_key) already submitted — reattach.
                return self._handle_from_name(name)
            if exc.status in (429, 403) and "exceeded quota" in (exc.body or ""):
                raise BackpressureError(str(exc.reason)) from exc
            raise
        return self._handle_from_cr(created)

    def list_jobs(
        self,
        limit: int | None = None,
        status_filter: JobStatus | Sequence[JobStatus] | None = None,
        submitted_before_ts: float | None = None,
    ) -> Sequence[RayJobHandle]:
        crs = self._crd.list_namespaced_custom_object(
            RAYJOB_GROUP,
            RAYJOB_VERSION,
            self._namespace,
            RAYJOB_PLURAL,
            label_selector=f"{SCOPE_LABEL}={self._scope}",
        )["items"]
        crs.sort(key=lambda c: c["metadata"]["creationTimestamp"], reverse=True)

        if status_filter is not None:
            wanted = (
                {status_filter}
                if isinstance(status_filter, JobStatus)
                else set(status_filter)
            )
        else:
            wanted = None

        out: list[RayJobHandle] = []
        for cr in crs:
            handle = self._handle_from_cr(cr)
            if submitted_before_ts is not None and (
                handle.created_at.timestamp() >= submitted_before_ts
            ):
                continue
            if wanted is not None and handle.status not in wanted:
                continue
            out.append(handle)
            if limit is not None and len(out) >= limit:
                break
        return out

    def get_job(self, job_id: str) -> RayJobHandle:
        crs = self._crd.list_namespaced_custom_object(
            RAYJOB_GROUP,
            RAYJOB_VERSION,
            self._namespace,
            RAYJOB_PLURAL,
            label_selector=f"{JOB_ID_LABEL}={job_id}",
        )["items"]
        if not crs:
            raise KeyError(job_id)
        return self._handle_from_cr(crs[0])

    def shutdown(self, wait: bool = True) -> None:
        """CRs are durable — nothing leaks if we just return (wait=False).

        wait=True politely blocks until tracked jobs reach terminal states,
        matching the protocol contract.
        """
        if not wait:
            return
        deadline = time.monotonic() + 300
        while time.monotonic() < deadline:
            active = [j for j in self.list_jobs() if not j.status.is_terminal]
            if not active:
                return
            time.sleep(2.0)

    # -------------------------------------------------------------- internals

    def _handle_from_cr(self, cr: dict[str, Any]) -> RayJobHandle:
        meta = cr["metadata"]
        created_raw = meta.get("creationTimestamp")
        created_at = (
            datetime.fromisoformat(created_raw.replace("Z", "+00:00"))
            if created_raw
            else datetime.now(timezone.utc)
        )
        return RayJobHandle(
            name=meta["name"],
            namespace=self._namespace,
            job_id=meta["labels"].get(JOB_ID_LABEL, meta["name"]),
            created_at=created_at,
            crd_api=self._crd,
            core_api=self._core,
        )

    def _handle_from_name(self, name: str) -> RayJobHandle:
        cr = self._crd.get_namespaced_custom_object(
            RAYJOB_GROUP, RAYJOB_VERSION, self._namespace, RAYJOB_PLURAL, name
        )
        return self._handle_from_cr(cr)
