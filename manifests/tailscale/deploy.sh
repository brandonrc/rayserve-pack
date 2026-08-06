#!/usr/bin/env bash
# Deploy tailscale ingress proxies for the demo hostnames.
# Usage: TS_AUTHKEY=tskey-auth-... ./deploy.sh
set -euo pipefail
cd "$(dirname "$0")"

: "${TS_AUTHKEY:?set TS_AUTHKEY to a reusable tailscale auth key}"

kubectl create namespace tailscale-ingress --dry-run=client -o yaml | kubectl apply -f -
kubectl create secret generic ts-authkey -n tailscale-ingress \
  --from-literal=authkey="$TS_AUTHKEY" --dry-run=client -o yaml | kubectl apply -f -

# SA needs to manage its own state secrets (tailscale k8s pattern)
kubectl apply -f - <<'EOF'
apiVersion: v1
kind: ServiceAccount
metadata:
  name: tailscale-ingress
  namespace: tailscale-ingress
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: tailscale-ingress
  namespace: tailscale-ingress
rules:
  - apiGroups: [""]
    resources: ["secrets"]
    verbs: ["create", "get", "update", "patch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: tailscale-ingress
  namespace: tailscale-ingress
subjects:
  - kind: ServiceAccount
    name: tailscale-ingress
    namespace: tailscale-ingress
roleRef:
  kind: Role
  name: tailscale-ingress
  apiGroup: rbac.authorization.k8s.io
EOF

ENVOY_SVC=$(kubectl get svc -n envoy-gateway-system \
  -l gateway.envoyproxy.io/owning-gateway-name=nebari-gateway \
  -o jsonpath='{.items[0].metadata.name}')
echo "envoy service: ${ENVOY_SVC}"

render() { # host backend
  TS_HOST="$1" TS_BACKEND="$2" envsubst '${TS_HOST} ${TS_BACKEND}' < proxy.yaml.tmpl | kubectl apply -f -
}

render keycloak-demo  "http://keycloak-keycloakx-http.keycloak.svc.cluster.local:80"
render checkmaite-demo "https+insecure://${ENVOY_SVC}.envoy-gateway-system.svc.cluster.local:443"
render ray-demo        "https+insecure://${ENVOY_SVC}.envoy-gateway-system.svc.cluster.local:443"

kubectl rollout status -n tailscale-ingress deploy/ts-keycloak-demo deploy/ts-checkmaite-demo deploy/ts-ray-demo --timeout=180s
echo "Proxies up. Hostnames:"
echo "  https://keycloak-demo.possum-fujita.ts.net/auth"
echo "  https://checkmaite-demo.possum-fujita.ts.net"
echo "  https://ray-demo.possum-fujita.ts.net"
