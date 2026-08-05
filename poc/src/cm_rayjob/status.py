"""Map RayJob CR status fields onto checkmaite's JobStatus lifecycle.

KubeRay (v1.x) exposes two fields on ``RayJob.status``:

- ``jobDeploymentStatus`` — the *infrastructure* lifecycle:
  "" | Initializing | Running | Complete | Failed | Suspended | Retrying | Waiting
- ``jobStatus`` — the *Ray job* lifecycle as reported by the cluster:
  "" | PENDING | RUNNING | SUCCEEDED | FAILED | STOPPED

checkmaite's JobStatus (protocol.py:20) is PENDING/RUNNING/COMPLETED/FAILED/
CANCELLED with immutable terminals. STOPPED is Ray's word for "stopped by
request" → CANCELLED.
"""

from __future__ import annotations

from checkmaite.jobs.protocol import JobStatus

_RAY_JOB_STATUS = {
    "SUCCEEDED": JobStatus.COMPLETED,
    "FAILED": JobStatus.FAILED,
    "STOPPED": JobStatus.CANCELLED,
    "RUNNING": JobStatus.RUNNING,
    "PENDING": JobStatus.PENDING,
    "": JobStatus.PENDING,
}


def map_status(job_deployment_status: str, job_status: str) -> JobStatus:
    """Combine the two CR fields into one checkmaite JobStatus."""
    # Terminal infrastructure states win, refined by the Ray-side verdict.
    if job_deployment_status == "Complete":
        return _RAY_JOB_STATUS.get(job_status, JobStatus.COMPLETED)
    if job_deployment_status == "Failed":
        # A Failed deployment with a STOPPED job is a cancellation race.
        return JobStatus.CANCELLED if job_status == "STOPPED" else JobStatus.FAILED
    if job_deployment_status == "Suspended":
        # Kueue's admission hook parks jobs here pre-admission.
        return JobStatus.PENDING
    # Initializing / Waiting / Retrying / Running / "" — defer to the Ray-side
    # status; a provisioning cluster with no Ray verdict yet is PENDING.
    return _RAY_JOB_STATUS.get(job_status, JobStatus.PENDING)
