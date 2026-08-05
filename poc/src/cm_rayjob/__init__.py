"""cm-rayjob — checkmaite JobBackend on KubeRay RayJob CRs (POC).

The CR *is* the durable job state: submit = create a RayJob, watch = read its
status, cancel = delete it, dedupe = deterministic CR names. KubeRay owns the
cluster lifecycle (spin up, run, tear down); this package is only CRUD+watch.
"""

from cm_rayjob.backend import RayJobK8sBackend
from cm_rayjob.job import RayJobHandle
from cm_rayjob.payload import JobPayload

__all__ = ["JobPayload", "RayJobHandle", "RayJobK8sBackend"]
