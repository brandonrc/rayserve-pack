"""RayJobHandle — implements checkmaite's Job[CapabilityRunRef] protocol.

The handle is stateless apart from a terminal-status cache: every observation
reads the RayJob CR. If the driver process dies, a new handle on the same CR
name resumes exactly where things stand — the durability the current ray
backend gets from a detached registry actor, for free, from the k8s API.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from kubernetes import client
from kubernetes.client.rest import ApiException

from checkmaite.jobs.protocol import (
    CapabilityRunRef,
    JobCancelledError,
    JobFailedError,
    JobStatus,
    JobTimeoutError,
)
from cm_rayjob.status import map_status

if TYPE_CHECKING:
    from collections.abc import Callable

RESULT_SENTINEL = "CM_RAYJOB_RESULT:"
_POLL_BACKOFF_S = (0.05, 0.25, 1.0, 5.0)  # same ladder as the ray backend

RAYJOB_GROUP = "ray.io"
RAYJOB_VERSION = "v1"
RAYJOB_PLURAL = "rayjobs"


class RayJobHandle:
    """Job handle backed by one RayJob CR."""

    def __init__(
        self,
        *,
        name: str,
        namespace: str,
        job_id: str,
        created_at: datetime | None = None,
        crd_api: client.CustomObjectsApi,
        core_api: client.CoreV1Api,
    ) -> None:
        self._name = name
        self._namespace = namespace
        self._job_id = job_id
        self._created_at = created_at or datetime.now(timezone.utc)
        self._crd = crd_api
        self._core = core_api
        self._terminal_status: JobStatus | None = None
        self._failure_message: str | None = None

    # ------------------------------------------------------------------ CR IO

    def _fetch_cr(self) -> dict[str, Any] | None:
        try:
            return self._crd.get_namespaced_custom_object(
                RAYJOB_GROUP, RAYJOB_VERSION, self._namespace, RAYJOB_PLURAL, self._name
            )
        except ApiException as exc:
            if exc.status == 404:
                return None
            raise

    def _observe(self) -> JobStatus:
        if self._terminal_status is not None:
            return self._terminal_status  # terminals are immutable
        cr = self._fetch_cr()
        if cr is None:
            # Tracked CR gone before a terminal was observed: deleted out from
            # under us (cancel() or TTL) — cancellation semantics.
            self._terminal_status = JobStatus.CANCELLED
            return self._terminal_status
        status = cr.get("status") or {}
        mapped = map_status(
            str(status.get("jobDeploymentStatus") or ""),
            str(status.get("jobStatus") or ""),
        )
        if mapped.is_terminal:
            self._terminal_status = mapped
            if mapped is JobStatus.FAILED:
                self._failure_message = str(
                    status.get("message") or "RayJob reported FAILED"
                )
        return mapped

    # ------------------------------------------------------------ Job protocol

    @property
    def job_id(self) -> str:
        return self._job_id

    @property
    def status(self) -> JobStatus:
        return self._observe()

    @property
    def created_at(self) -> datetime:
        return self._created_at

    def wait(self, timeout: float | None = None) -> JobStatus:
        deadline = None if timeout is None else time.monotonic() + timeout
        delay_i = 0
        while True:
            observed = self._observe()
            if observed.is_terminal:
                return observed
            if deadline is not None and time.monotonic() >= deadline:
                return observed  # protocol: wait() does not raise on timeout
            time.sleep(_POLL_BACKOFF_S[min(delay_i, len(_POLL_BACKOFF_S) - 1)])
            delay_i += 1

    def result(self, timeout: float | None = None) -> CapabilityRunRef:
        observed = self.wait(timeout=timeout)
        if not observed.is_terminal:
            raise JobTimeoutError(self._job_id, timeout or 0.0)
        if observed is JobStatus.CANCELLED:
            raise JobCancelledError(self._job_id)
        if observed is JobStatus.FAILED:
            raise JobFailedError(
                self._job_id, self._failure_message or self._tail_logs() or "unknown"
            )
        payload = self._read_result_payload()
        if payload is None:
            raise JobFailedError(
                self._job_id,
                "job COMPLETED but no result sentinel found in submitter logs "
                "(POC result channel; production would read the analytics store)",
            )
        return CapabilityRunRef.model_validate(payload)

    def exception(self) -> BaseException | None:
        if self._observe() is JobStatus.FAILED:
            return JobFailedError(self._job_id, self._failure_message or "unknown")
        return None

    def cancel(self) -> bool:
        if self._observe().is_terminal:
            return False
        try:
            self._crd.delete_namespaced_custom_object(
                RAYJOB_GROUP, RAYJOB_VERSION, self._namespace, RAYJOB_PLURAL, self._name
            )
        except ApiException as exc:
            if exc.status == 404:
                return False
            raise
        self._terminal_status = JobStatus.CANCELLED
        return True

    # ----------------------------------------------------------- result channel

    def _submitter_logs(self) -> str | None:
        """Logs of the KubeRay submitter pod (a k8s Job named like the CR)."""
        pods = self._core.list_namespaced_pod(
            self._namespace, label_selector=f"job-name={self._name}"
        ).items
        if not pods:
            return None
        newest = max(pods, key=lambda p: p.metadata.creation_timestamp)
        try:
            return self._core.read_namespaced_pod_log(
                newest.metadata.name, self._namespace
            )
        except ApiException:
            return None

    def _read_result_payload(self) -> dict[str, Any] | None:
        logs = self._submitter_logs()
        if not logs:
            return None
        for line in reversed(logs.splitlines()):
            if RESULT_SENTINEL in line:
                return json.loads(line.split(RESULT_SENTINEL, 1)[1])
        return None

    def _tail_logs(self, lines: int = 5) -> str | None:
        logs = self._submitter_logs()
        return "\n".join(logs.splitlines()[-lines:]) if logs else None
