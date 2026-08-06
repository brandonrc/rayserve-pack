# Demo flow — Tier-3 RayJob isolation, live on the rocky cluster

A ~10-minute review script. Everything below runs against a real k8s cluster
(rocky, 88 CPU / 156Gi, CPU-only) with the nebari stack installed: Envoy
Gateway, cert-manager, Keycloak (realm `nebari`), nebari-operator,
kuberay-operator, and two group namespaces.

**Cast** (test creds, tailnet-only):

| User | Password | Keycloak group | Namespace | Quota |
|------|----------|----------------|-----------|-------|
| alice | alice | team-a | cm-team-a | 8 CPU / 16Gi |
| bob   | bob   | team-b | cm-team-b | 8 CPU / 16Gi |
| admin | nebari-admin | (realm admin) | — | — |

**URLs** (tailnet): `https://keycloak-demo.possum-fujita.ts.net/auth`
(Keycloak), `https://checkmaite-demo.possum-fujita.ts.net`,
`https://ray-demo.possum-fujita.ts.net` (apps behind the gateway).

## Act 0 — setup recap (say, don't run)

> "Ray has no auth on its ports. Anyone who can reach a shared Ray cluster
> can run code on it, as any user, with the cluster's credentials. Instead of
> policing one shared cluster, each *submission* gets its own ephemeral
> cluster inside its *group's* namespace — and Kubernetes primitives (RBAC,
> quota, NetworkPolicy) do the enforcement. The adapter is ~400 lines; the
> orchestrator is stock KubeRay."

## Act 1 — the whole tenancy line, one command

```bash
.venv/bin/python demo/demo_groups.py   # add --keycloak <url> if not on the tailnet
```

Narrate as it goes:

1. **Identity** — alice and bob log into Keycloak; their ID tokens carry
   `groups: [team-a]` / `groups: [team-b]`. *Same claim the gateway's
   IdToken cookie carries in production — Keycloak is the single place
   membership lives.*
2. **Policy point** — the group claim maps to a namespace
   (`team-a → cm-team-a`). *This mapping is the only "app logic" in the
   whole tenancy story.*
3. **Concurrent isolated clusters** — both users' jobs run at the same time,
   each in its own ephemeral Ray cluster in its own namespace. ~25–30s
   end-to-end each, including cluster spin-up.
4. **Kubernetes enforces, not the app** — `kubectl auth can-i` shows team-a's
   submitter identity is *denied* creating RayJobs in team-b's namespace.
   *Even a compromised api can't cross the line.*
5. **Blast radius** — alice submits a job requesting 64 CPUs; it pends
   against *her* quota while bob's job runs untouched.
6. **Visibility** — each scope lists only its own jobs.

Live color commands in a second terminal while Act 1 runs:

```bash
watch kubectl get rayjobs,pods -n cm-team-a
kubectl get resourcequota -n cm-team-a   # watch the quota fill and drain
```

## Act 2 — the browser story (tailscale required)

1. Open `https://keycloak-demo.possum-fujita.ts.net/auth` → admin console
   (admin / nebari-admin) → realm `nebari` → Users / Groups. *One place to
   add someone to team-a.*
2. Open a gateway-fronted app (e.g. the Ray dashboard NebariApp at
   `https://ray-demo.possum-fujita.ts.net`) in a private window → redirected
   to Keycloak → log in as **bob:bob** → back through the gateway with the
   IdToken cookie. *This is the identical claim path the CLI demo used.*

## Act 3 — durability + cleanup (30 seconds)

```bash
kubectl get rayjobs -n cm-team-a          # CRs persist: the durable job record
kubectl get pods -n cm-team-a             # clusters are GONE (TTL teardown)
```

> "Job state lives in etcd, not in actors inside the cluster it describes.
> The driver can die, the cluster is torn down, and the record survives."

## Numbers to quote

| Metric | Value | Where |
|--------|-------|-------|
| Submit (CR create) | ~15 ms | kind + rocky |
| Cluster spin-up → RUNNING | ~16 s | warm image |
| End-to-end, 5s workload | ~25–26 s | **identical on kind and rocky** |
| Adapter size | ~400 lines | replaces ~1,800-line actor machinery |

## Ops findings worth repeating in review

- `ttlSecondsAfterFinished` delays **cluster teardown** (CR persists) — a
  lingering cluster holds quota; keep TTL short, size quota × TTL window.
- On many-core hosts Ray sizes memory off the **machine**, not the cgroup —
  cap `object-store-memory`, give heads 4Gi, `MALLOC_ARENA_MAX=2`.
- Keycloak `start-dev` is ephemeral — PVC the data dir or lose the realm on
  every restart.

## Teardown

```bash
kubectl delete ns cm-team-a cm-team-b tailscale-ingress
helm -n kuberay-system uninstall kuberay-operator
# nebari stack: helm -n keycloak uninstall keycloak; helm -n cert-manager uninstall cert-manager;
# helm -n envoy-gateway-system uninstall eg; kubectl delete -f <operator install.yaml>
```
