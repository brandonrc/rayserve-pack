"""Cluster-free unit tests: status mapping, payload round-trip, CR naming,
and structural protocol conformance."""

from __future__ import annotations

from checkmaite.jobs.protocol import JobStatus
from cm_rayjob.payload import JobPayload
from cm_rayjob.status import map_status


def test_status_mapping_terminals() -> None:
    assert map_status("Complete", "SUCCEEDED") is JobStatus.COMPLETED
    assert map_status("Complete", "FAILED") is JobStatus.FAILED
    assert map_status("Complete", "STOPPED") is JobStatus.CANCELLED
    assert map_status("Failed", "FAILED") is JobStatus.FAILED
    assert map_status("Failed", "") is JobStatus.FAILED
    assert map_status("Failed", "STOPPED") is JobStatus.CANCELLED


def test_status_mapping_nonterminals() -> None:
    assert map_status("", "") is JobStatus.PENDING
    assert map_status("Initializing", "") is JobStatus.PENDING
    assert map_status("Suspended", "") is JobStatus.PENDING  # Kueue parking
    assert map_status("Running", "PENDING") is JobStatus.PENDING
    assert map_status("Running", "RUNNING") is JobStatus.RUNNING
    assert map_status("Retrying", "RUNNING") is JobStatus.RUNNING


def test_terminals_are_terminal() -> None:
    for dep, ray_side in [("Complete", "SUCCEEDED"), ("Failed", ""), ("Complete", "STOPPED")]:
        assert map_status(dep, ray_side).is_terminal


def test_payload_roundtrip() -> None:
    p = JobPayload(
        job_id="j1",
        scope="team-a",
        run_key="rk",
        mode="capability",
        capability_class="pkg.mod.Cap",
        models=[{"id": "m", "resource_type": "torchvision", "weights": "DEFAULT"}],
        config={"batch_size": 4},
    )
    assert JobPayload.from_b64(p.to_b64()) == p


def test_cr_name_deterministic() -> None:
    from cm_rayjob.backend import RayJobK8sBackend

    name_fn = RayJobK8sBackend._cr_name
    class _Fake:  # avoid kube config loading in unit tests
        _scope = "team-a"
    a = name_fn(_Fake(), "run-1")  # type: ignore[arg-type]
    b = name_fn(_Fake(), "run-1")  # type: ignore[arg-type]
    c = name_fn(_Fake(), "run-2")  # type: ignore[arg-type]
    assert a == b != c
    assert a.startswith("cm-") and len(a) == 19


def test_protocol_conformance_structural() -> None:
    """mypy enforces this structurally; at runtime we assert the surface."""
    from cm_rayjob.backend import RayJobK8sBackend
    from cm_rayjob.job import RayJobHandle

    for method in ("submit_capability", "list_jobs", "get_job", "shutdown"):
        assert callable(getattr(RayJobK8sBackend, method))
    for member in ("job_id", "status", "created_at", "result", "wait", "exception", "cancel"):
        assert hasattr(RayJobHandle, member)
