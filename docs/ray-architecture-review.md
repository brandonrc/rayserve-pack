# Review: proposed Ray architecture (rev c9044c211a88)

Comments keyed to the proposal's section numbers, from building and running the
RayJob-per-execution design it lists as "an optional future backend" (§12).
Rendered version: `ray-architecture-review-comments.html`.

## Headline

The designs are complementary. §12 rejects Ray Jobs because users "would need to
package and stage inputs rather than pass live objects" — true for notebooks,
untrue for the web API, which never holds live objects. It holds catalog spec
dicts and constructs the objects itself, so declarative submission costs it
nothing and removes the version-parity coupling. `spec.clusterSelector` lets
CR-tracked jobs run on the per-group clusters §5 already provisions, which
removes the startup-time objection as well.

Recommendation: notebooks keep Ray Client; UI/API submissions create a RayJob in
the caller's group namespace. One deployment, two paths, selected by config.

## Section comments

| § | Topic | Position |
|---|-------|----------|
| 4 | Shared model-serving | Agrees; the access control it specifies exists as deployable IaC (NetworkPolicy + gateway claim authz) |
| 5 | Interactive environments | Sizing gap: the driver runs on the head (OOMKilled at the illustrative 4 GiB); the minimum warm worker receives no work today |
| 6 | Execution lifecycle | Risk understated: a Ray Client session cannot be re-established after a head restart, and there is no reconnect logic |
| 8 | Isolation and authorization | Agrees; add entitlement-vs-tenancy groups, and refuse (don't default) callers matching no group |
| 9 | Scaling and scheduling | Agrees; document that a finished cluster holds quota for its TTL |
| 10 | Failure and upgrade | Add three real startup failures: wget-less probes, login-shell PATH, machine-vs-cgroup memory sizing |
| 11 | Visibility | Mechanism decides the goal: Ray Client yields one driver with tasks beneath, not per-run job entries |
| 12 | Ray Jobs as alternative | Principal comment: appropriate backend for non-interactive clients, deployable today, no startup cost with clusterSelector |
| 13 | Tradeoffs | Add: version parity as operational coupling; ~1,800 lines of control plane to maintain; shared storage as a precondition |
| 14 | Key decisions | Add: backend chosen per client; entitlement group separate from tenancy group |

## Measured

| Measurement | Result |
|---|---|
| RayJob CR created | 15 ms |
| Per-job cluster provisioned → RUNNING | 25–60 s |
| DataEval Cleaning end to end | ~100 s |
| MAITE evaluation (AlexNet) end to end | ~120 s |
| Autoscaler idle → serving | ~15 s |
| Autoscaler drain to zero | 120 s idle |
| Warm head idle cost per group | 350m CPU / 2.25 GiB |
| Same at 50 groups | 17.5 CPU / 112 GiB |
| Cross-group dashboard (non-member) | 403 |
| Cross-group run by ID (non-member) | 404 |
| Submission without entitlement group | 403 |

Single-node cluster with a warm image cache: treat provisioning times as a floor.

## Six findings

1. Ray Client sessions are unrecoverable after a head restart (§6, §10).
2. The job driver runs on the head, so the head needs the workload's memory (§5).
3. A minimum warm worker receives no work today — one run is one task (§5, §9).
4. Finished clusters keep holding quota for their TTL (§9).
5. Slim images break KubeRay's defaults twice: probe `wget`, login-shell `PATH` (§10).
6. Group isolation forces the shared-storage decision (§8, §13).
