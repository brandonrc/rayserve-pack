"""Multi-group demo: Keycloak identity -> group claim -> namespace-isolated RayJobs.

The full Tier-3 tenancy line, live:

  Keycloak (alice:alice in team-a, bob:bob in team-b)
    -> OIDC ID token carries groups=[team-x]     (same claim the gateway's
                                                  IdToken cookie carries)
    -> group maps to namespace cm-team-x          (the api's policy point)
    -> RayJob submitted THERE: quota, RBAC, and the ephemeral cluster are
       all namespace-scoped                       (Kubernetes enforces)

Run:  python demo/demo_groups.py \
        --keycloak https://keycloak-demo.possum-fujita.ts.net/auth \
        [--image rayproject/ray:2.43.0]

Requires: manifests applied (cm-team-a/cm-team-b), kuberay-operator running.
"""

from __future__ import annotations

import argparse
import base64
import json
import ssl
import subprocess
import time
import urllib.parse
import urllib.request
import uuid

from checkmaite.jobs.protocol import JobStatus
from cm_rayjob import JobPayload, RayJobK8sBackend

GROUP_TO_NAMESPACE = {
    # mapping order = precedence (first mapping entry present in the user's
    # groups wins) — fruit groups outrank the original team groups
    "apple": "cm-apple",
    "banana": "cm-banana",
    "team-a": "cm-team-a",
    "team-b": "cm-team-b",
}


def login(keycloak_base: str, username: str, password: str) -> dict:
    """Password-grant login; returns decoded ID-token claims.

    The browser flow gets the same ID token via the gateway's OIDC filter
    (IdToken cookie); direct grant just skips the browser for a CLI demo.
    """
    data = urllib.parse.urlencode(
        {
            "client_id": "admin-cli",
            "grant_type": "password",
            "username": username,
            "password": password,
            "scope": "openid profile email",
        }
    ).encode()
    url = f"{keycloak_base}/realms/nebari/protocol/openid-connect/token"
    ctx = ssl.create_default_context()
    with urllib.request.urlopen(urllib.request.Request(url, data=data), context=ctx) as resp:
        body = json.load(resp)
    payload = body["id_token"].split(".")[1]
    claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
    return claims


def backend_for(claims: dict, image: str) -> tuple[str, RayJobK8sBackend]:
    """The api's policy point: group claim -> target namespace."""
    groups = claims.get("groups") or []
    namespace = next(
        (ns for g, ns in GROUP_TO_NAMESPACE.items() if g in groups), None
    )
    if namespace is None:
        raise SystemExit(f"user {claims.get('preferred_username')} has no mapped group: {groups}")
    return namespace, RayJobK8sBackend(
        namespace=namespace,
        scope=f"demo-{claims['preferred_username']}",
        image=image,
    )


def submit(user: str, backend: RayJobK8sBackend, ns: str, sleep_s: float = 8.0, **kw):
    jid = str(uuid.uuid4())
    job = backend.submit_capability(
        JobPayload(job_id=jid, scope=backend._scope, run_key=kw.pop("run_key", jid),
                   stub_sleep_s=sleep_s), **kw,
    )
    print(f"  [{user}] submitted {job.job_id[:8]} -> namespace {ns}")
    return job


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--keycloak", default="https://keycloak-demo.possum-fujita.ts.net/auth")
    parser.add_argument("--image", default="rayproject/ray:2.43.0")
    args = parser.parse_args()

    print("=== 1. identity: two users, two Keycloak groups ===")
    alice = login(args.keycloak, "alice", "alice")
    bob = login(args.keycloak, "bob", "bob")
    for c in (alice, bob):
        print(f"  {c['preferred_username']}: groups={c.get('groups')}  (iss: {c['iss']})")

    print("\n=== 2. group claim -> namespace (the api's policy point) ===")
    ns_a, be_a = backend_for(alice, args.image)
    ns_b, be_b = backend_for(bob, args.image)
    print(f"  alice -> {ns_a}   bob -> {ns_b}")

    print("\n=== 3. concurrent isolated clusters (one per user's namespace) ===")
    t0 = time.monotonic()
    ja = submit("alice", be_a, ns_a)
    jb = submit("bob", be_b, ns_b)
    ra = ja.result(timeout=900)
    rb = jb.result(timeout=900)
    print(f"  both completed in {time.monotonic()-t0:.1f}s "
          f"(alice run_uid={ra.run_uid[:8]}, bob run_uid={rb.run_uid[:8]})")

    print("\n=== 4. kubernetes enforces the line, not the app ===")
    out = subprocess.run(
        ["kubectl", "auth", "can-i", "create", "rayjobs.ray.io", "-n", ns_b,
         "--as", f"system:serviceaccount:{ns_a}:cm-rayjob-submitter"],
        capture_output=True, text=True,
    ).stdout.strip()
    print(f"  {ns_a}'s submitter creating RayJobs in {ns_b}: {out!r}")
    assert out == "no", "RBAC must deny cross-namespace submission"
    out = subprocess.run(
        ["kubectl", "auth", "can-i", "create", "rayjobs.ray.io", "-n", ns_a,
         "--as", f"system:serviceaccount:{ns_a}:cm-rayjob-submitter"],
        capture_output=True, text=True,
    ).stdout.strip()
    print(f"  ...and in its own namespace ({ns_a}): {out!r}")
    assert out == "yes"

    print("\n=== 5. blast radius: alice exceeds HER quota; bob unaffected ===")
    big = submit("alice", be_a, ns_a, sleep_s=1.0,
                 resources={"num_cpus": 64.0, "memory": "128Gi"})
    ok_b = submit("bob", be_b, ns_b, sleep_s=2.0)
    rb2 = ok_b.result(timeout=900)
    assert big.status is JobStatus.PENDING, f"oversized job should pend, got {big.status}"
    print(f"  alice's oversized job: {big.status.value} (held by their namespace quota)")
    print(f"  bob's normal job:      completed (run_uid={rb2.run_uid[:8]})")
    big.cancel()

    print("\n=== 6. visibility: each scope lists only its own jobs ===")
    for user, be in (("alice", be_a), ("bob", be_b)):
        jobs = be.list_jobs(limit=5)
        print(f"  {user}: {[(j.job_id[:8], j.status.value) for j in jobs]}")

    print("\nALL GROUP-ISOLATION CHECKS PASSED")


if __name__ == "__main__":
    main()
