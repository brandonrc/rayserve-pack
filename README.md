# cm-rayjob — Tier-3 job isolation POC for CheckMAITE

**What this fork is:** a proof-of-concept implementing CheckMAITE's
`JobBackend` protocol on **KubeRay RayJob CRs** — per-job (or per-group) Ray
clusters instead of one shared, long-lived, unauthenticated cluster. The
upstream [rayserve-pack](https://github.com/nebari-dev/rayserve-pack) chart is
kept unmodified under `chart/` for reference; everything POC lives in
[`poc/`](poc/), [`manifests/`](manifests/), and [`demo/`](demo/).

**What this is NOT:** a new microservice. The orchestrator already exists —
the KubeRay operator (the same one rayserve-pack already depends on) watches
RayJob CRs and owns cluster provisioning, submission, retries, deadlines, and
teardown. This POC is a **thin adapter library** (~400 lines, no process of
its own) that the existing checkmaite api imports: protocol methods →
Kubernetes API calls. No reconcile loop, no queue, no state store of ours.

## Why (the tier ladder)

CheckMAITE's current `ray` backend connects one api process to one shared Ray
cluster over `ray://…:10001` — a port with **no authentication**, where any
in-cluster client is fully trusted, job code is cloudpickled by reference
(api and workers must run the *exact same* checkmaite build), and job state
lives in detached Ray actors that die with the cluster.

| Tier | Mechanism | Isolation |
|------|-----------|-----------|
| 0 | shared cluster + NetworkPolicy/quota ([nebari-dev/rayserve-pack#28](https://github.com/nebari-dev/rayserve-pack/pull/28)) | cluster-level allow-listing |
| 1 | per-group Ray namespaces/resources in one cluster | visibility + placement only, **not security** |
| 2 | one cluster per group namespace | hard, but static |
| **3 (this POC)** | **RayJob CR per submission** | hard, dynamic, per-job env pinning |

## Design under test

| Concern | Current ray backend | This POC |
|---------|--------------------|----------|
| Job state | detached `JobRegistryActor` (dies with the cluster, `max_restarts=0`) | **the RayJob CR** — durable in etcd, outlives driver and cluster |
| Dedupe | registry actor index | **deterministic CR name** from `(scope, run_key)`; duplicate create → 409 → reattach |
| Code shipping | cloudpickle by reference → strict api↔worker version parity | **declarative spec payload** (the frontend catalog shape); job pins its own checkmaite via `runtimeEnvYAML` |
| Cancel | actor calls + `ray.cancel` | delete the CR |
| Cleanup | submit-triggered sweeps + retention knobs | `shutdownAfterJobFinishes` + TTL (nothing of ours to GC) |
| AuthZ | anyone reaching `:10001` is root on the cluster | **namespaced RBAC**: the api may create RayJobs only in group namespaces it holds a RoleBinding for |
| Fairness/queueing | none | deferred: [Kueue](https://kueue.sigs.k8s.io/) admits via `spec.suspend`; one LocalQueue per group namespace |
| Driver deps | Ray client (version-matched to cluster) | **kubernetes client only** — no Ray on the driver at all |

Execution semantics are identical where it matters: the in-job entrypoint
reuses checkmaite's own `_execute_capability_ref`, so capability runs,
analytics-store writes, and `CapabilityRunRef` construction are the same code
path as the existing backend.

## Measured results

**Level A (stub lifecycle)** — identical on kind (M-series Mac) and a real
x86 cluster (rocky, 88 CPU / 156Gi):

```
submit (CR created):        15 ms
first RUNNING observed:     15.8 s   <- ephemeral cluster spin-up + job start
terminal (5 s workload):    25.0 s   (26.4 s on rocky)
```

**Level B (real capability)** — checkmaite `DataevalCleaning` end-to-end on
rocky, environment pinned per-job via `runtimeEnvYAML`
(`checkmaite==0.3.0`, driver running a different env entirely):

```
COMPLETED in 3.2 min       <- includes in-job pip resolve + torch imports
  capability_id: ...dataeval_cleaning_capability.DataevalCleaning
  store_uri:     .../dataeval_cleaning/<ts>_<id>.parquet   (analytics write)
  report:        <present>                                 (typed report)
```

**Multi-group demo (`demo/demo_groups.py`)** — the full tenancy line, live on
rocky against a real Keycloak: alice/bob group claims → namespace mapping →
concurrent isolated clusters (37.8 s for both) → RBAC cross-namespace deny →
quota blast-radius containment → per-scope visibility. See `DEMO.md` for the
review walkthrough.

All protocol-contract checks pass (`demo/demo_stub.py`): failure →
`JobFailedError` carrying the real error, cancel mid-flight → `CANCELLED`
(`result()` raises `JobCancelledError`, `cancel()` on terminal returns
`False`), dedupe reattaches to the running job, an oversized job stays
`PENDING` under the namespace `ResourceQuota` with clean events, and
`list_jobs` honors newest-first/limit/status-filter.

**Read these numbers as a floor**: no node autoscaling and a warm local image
cache. Cloud worst case (node scale-up + cold multi-GB image pull) is
minutes, not seconds — mitigations below.

### Latency levers

1. **Prepull** job images via DaemonSet (ATEP's in-cluster Harbor makes pulls LAN-speed).
2. **Hybrid routing**: small runs → a warm shared cluster, big/GPU/isolated runs → ephemeral RayJob.
3. **Autoscaler headroom** (balloon pods) on the GPU pool.
4. **`spec.clusterSelector`**: a RayJob can target an *existing* RayCluster — same CR lifecycle/status/Kueue machinery, zero spin-up. A per-group long-lived cluster managed through the same submit/watch code; flip individual jobs to ephemeral only when they need the isolation.

### Operational findings (all hit for real during the POC)

- `ttlSecondsAfterFinished` delays **cluster teardown** (the CR persists
  regardless). A finished-but-lingering cluster holds real namespace quota —
  three lingering head pods starved the next submission entirely. Keep TTL
  short; size `ResourceQuota` for `max concurrent jobs × footprint × TTL window`.
- **Many-core hosts inflate Ray's memory**: Ray sizes thread pools / malloc
  arenas / object store from the *machine* (88 cores, 156Gi), not the cgroup —
  heads OOMKilled at 2Gi that ran fine on kind. Fix: explicit
  `object-store-memory`, 4Gi head default, `MALLOC_ARENA_MAX=2`. Budget ~2.5Gi
  for Ray's control processes before workload memory.
- **Runtime pip envs inherit the image's baked packages**
  (`--system-site-packages`): the image's old pydantic satisfied checkmaite's
  `>=2.0` floor and broke at import; loose pins install nothing. Workable for
  a POC with hard pins — production should **bake job images per checkmaite
  release** (the graduation plan), which also removes the ~2 min pip-resolve
  from the job path.
- **Spec contracts version with the pinned checkmaite** (0.3.0 wants
  `dataset_type`, main wants `dataset_format`): the payload schema must be
  versioned alongside the job image. Per-job pinning makes this explicit
  instead of a silent api↔worker skew — that's the feature working as
  intended.

## Layout

```
poc/src/cm_rayjob/
  payload.py      # declarative JSON job payload (the version-parity fix)
  backend.py      # RayJobK8sBackend — implements JobBackend
  job.py          # RayJobHandle — implements Job[CapabilityRunRef]
  status.py       # (jobDeploymentStatus, jobStatus) -> JobStatus
  entrypoint.py   # in-job: specs -> loaders -> _execute_capability_ref
manifests/        # group namespace + quota + submitter Role/RoleBinding
demo/
  demo_stub.py        # Level A: lifecycle/timing/dedupe/cancel/quota
  demo_capability.py  # Level B: real DataevalCleaning run, per-job pip env
```

Run it (any cluster with kuberay-operator ≥ 1.1):

```bash
kubectl apply -f manifests/poc-namespace.yaml -f manifests/rbac.yaml
uv venv && uv pip install -e './poc[dev]'
.venv/bin/python demo/demo_stub.py            # Level A
.venv/bin/python demo/demo_capability.py      # Level B (py311 image)
# Apple Silicon + kind: add --image rayproject/ray:2.43.0-aarch64 (A) /
#                          --image rayproject/ray:2.43.0-py311-aarch64 (B)
```

## POC shortcuts (would change before graduation)

- **Result channel** is a sentinel line parsed from the submitter pod's log
  tail. Production: the entrypoint writes the `CapabilityRunRef` payload to
  the analytics store keyed by `job_id`, and `result()` reads it there.
- `payload.bootstrap_b64` embedded demo dataset — real deployments point
  dataset specs at shared storage.
- Polling instead of a watch/informer feeding Postgres.

## Graduation path

1. Move `cm_rayjob` into `checkmaite/jobs/backends/rayjob/`; teach
   `configure_job_backend` to accept it (the factory is currently a closed
   `"ray" | "ray-simple"` map — an instance-accepting overload or entry-point
   plugin is needed).
2. checkmaite-frontend adapter change: submit **specs, not loaded objects**
   (the adapter already has the specs in hand from the catalog; it currently
   loads models/datasets/metrics api-side then pickles them). Also fixes the
   existing bug that the adapter never passes `idempotency_scope`, so
   `--job-backend ray` cannot start today.
3. Per-group namespaces provisioned by the platform (quota + node pools +
   NetworkPolicy per nebari-dev/rayserve-pack#28's pattern + RoleBinding for
   the api SA); api maps the IdToken group claim → target namespace.
4. Kueue for cross-group fairness (LocalQueue per namespace, `suspend: true`
   on submit).
