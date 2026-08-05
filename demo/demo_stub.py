"""Level A demo: prove the RayJob lifecycle mechanics + measure latency.

Run from a machine with kubeconfig access to a cluster running
kuberay-operator (e.g. the local kind cluster):

    kubectl apply -f manifests/poc-namespace.yaml -f manifests/rbac.yaml
    python demo/demo_stub.py [--image rayproject/ray:2.43.0-aarch64]

Exercises: submit -> RUNNING -> COMPLETED timing, result() round-trip,
failure mapping, dedupe via run_key, cancel mid-flight, quota rejection.
"""

from __future__ import annotations

import argparse
import time
import uuid

from checkmaite.jobs.protocol import JobCancelledError, JobFailedError, JobStatus
from cm_rayjob import JobPayload, RayJobK8sBackend

NAMESPACE = "geraci-poc"
SCOPE = "geraci-demo"


def _payload(**kw) -> JobPayload:
    jid = str(uuid.uuid4())
    return JobPayload(job_id=jid, scope=SCOPE, run_key=kw.pop("run_key", jid), **kw)


def timed_lifecycle(backend: RayJobK8sBackend) -> None:
    print("\n=== 1. timed lifecycle (submit -> RUNNING -> COMPLETED) ===")
    t0 = time.monotonic()
    job = backend.submit_capability(_payload(stub_sleep_s=5.0))
    t_submit = time.monotonic() - t0

    t_running = None
    while True:
        s = job.status
        if s is JobStatus.RUNNING and t_running is None:
            t_running = time.monotonic() - t0
        if s.is_terminal:
            break
        time.sleep(0.5)
    t_terminal = time.monotonic() - t0

    ref = job.result(timeout=30)
    print(f"  submit (CR created):        {t_submit * 1000:8.0f} ms")
    print(f"  first RUNNING observed:     {t_running:8.1f} s   <- cluster spin-up + job start")
    print(f"  terminal (5s workload):     {t_terminal:8.1f} s")
    print(f"  result: run_uid={ref.run_uid} capability_id={ref.capability_id}")
    assert ref.run_uid == job.job_id


def failure_mapping(backend: RayJobK8sBackend) -> None:
    print("\n=== 2. failure maps to JobFailedError ===")
    job = backend.submit_capability(_payload(stub_fail=True))
    final = job.wait(timeout=600)
    assert final is JobStatus.FAILED, f"expected FAILED, got {final}"
    try:
        job.result(timeout=5)
        raise AssertionError("result() should have raised")
    except JobFailedError as exc:
        print(f"  ok: {exc}")
    assert job.exception() is not None


def dedupe(backend: RayJobK8sBackend) -> None:
    print("\n=== 3. dedupe: same run_key reattaches, no second cluster ===")
    key = f"dedupe-{uuid.uuid4()}"
    a = backend.submit_capability(_payload(stub_sleep_s=20.0, run_key=key))
    b = backend.submit_capability(_payload(stub_sleep_s=20.0, run_key=key))
    print(f"  first={a.job_id}  second={b.job_id}")
    assert a.job_id == b.job_id, "second submit must reattach to the first job"
    assert a.cancel() is True
    print("  ok (and cancelled the shared job)")


def cancel_midflight(backend: RayJobK8sBackend) -> None:
    print("\n=== 4. cancel mid-flight -> CANCELLED, result() raises ===")
    job = backend.submit_capability(_payload(stub_sleep_s=120.0))
    # wait until it is at least provisioning, then kill it
    time.sleep(5)
    assert job.cancel() is True
    final = job.wait(timeout=120)
    assert final is JobStatus.CANCELLED, f"expected CANCELLED, got {final}"
    try:
        job.result(timeout=5)
        raise AssertionError("result() should have raised")
    except JobCancelledError as exc:
        print(f"  ok: {exc}")
    assert job.cancel() is False, "cancel on terminal must return False"


def quota_rejection(backend: RayJobK8sBackend) -> None:
    print("\n=== 5. quota: oversized job never admits (stays PENDING) ===")
    job = backend.submit_capability(
        _payload(stub_sleep_s=1.0), resources={"num_cpus": 32.0, "memory": "64Gi"}
    )
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        assert job.status in (JobStatus.PENDING,), (
            f"oversized job must not admit under the namespace quota, got {job.status}"
        )
        time.sleep(3)
    print("  ok: stayed PENDING for 30s under quota; cancelling")
    job.cancel()


def listing(backend: RayJobK8sBackend) -> None:
    print("\n=== 6. list_jobs newest-first + status filter ===")
    jobs = backend.list_jobs(limit=10)
    print(f"  {len(jobs)} jobs in scope; newest first:")
    for j in jobs[:5]:
        print(f"    {j.created_at:%H:%M:%S}  {j.job_id[:8]}  {j.status.value}")
    ts = [j.created_at for j in jobs]
    assert ts == sorted(ts, reverse=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", default="rayproject/ray:2.43.0")
    args = parser.parse_args()

    backend = RayJobK8sBackend(namespace=NAMESPACE, scope=SCOPE, image=args.image)

    timed_lifecycle(backend)
    failure_mapping(backend)
    dedupe(backend)
    cancel_midflight(backend)
    quota_rejection(backend)
    listing(backend)
    print("\nALL LEVEL-A CHECKS PASSED")


if __name__ == "__main__":
    main()
