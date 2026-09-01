#!/usr/bin/env bash

set -euo pipefail

cluster_name="argo-sfx-smoke"
context="kind-${cluster_name}"
fake_image="signalfx-fake:local"
node_image="kindest/node:v1.31.4"
release_base="https://github.com/argoproj-labs/rollouts-plugin-metric-signalfx/releases/download/v0.2.0"
cluster_created=0

cleanup() {
	if [[ "$cluster_created" == "1" ]]; then
		kind delete cluster --name "$cluster_name" >/dev/null 2>&1 || true
	fi
}

fail() {
	printf 'kind smoke test failed: %s\n' "$1" >&2
	exit 1
}

trap cleanup EXIT

for command_name in docker kind kubectl; do
	command -v "$command_name" >/dev/null 2>&1 || fail "missing command: $command_name"
done

docker info >/dev/null 2>&1 || fail "Docker daemon is not running"

if kind get clusters | grep -Fxq "$cluster_name"; then
	fail "cluster $cluster_name already exists; refusing to touch it"
fi

docker build -f test/fake-signalflow/Dockerfile -t "$fake_image" .

cluster_created=1
kind create cluster --name "$cluster_name" --image "$node_image" --wait 5m
kind load docker-image "$fake_image" --name "$cluster_name"

kubectl_cmd=(kubectl --context "$context")

"${kubectl_cmd[@]}" apply -f - <<'YAML'
apiVersion: v1
kind: Namespace
metadata:
  name: signalfx-test
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fake-signalflow
  namespace: signalfx-test
spec:
  selector:
    matchLabels:
      app: fake-signalflow
  template:
    metadata:
      labels:
        app: fake-signalflow
    spec:
      containers:
        - name: fake-signalflow
          image: signalfx-fake:local
          imagePullPolicy: Never
          ports:
            - name: websocket
              containerPort: 8080
          readinessProbe:
            httpGet:
              path: /healthz
              port: websocket
---
apiVersion: v1
kind: Service
metadata:
  name: fake-signalflow
  namespace: signalfx-test
spec:
  selector:
    app: fake-signalflow
  ports:
    - name: websocket
      port: 8080
      targetPort: websocket
YAML

"${kubectl_cmd[@]}" -n signalfx-test rollout status deployment/fake-signalflow --timeout=120s
"${kubectl_cmd[@]}" create namespace argo-rollouts --dry-run=client -o yaml | "${kubectl_cmd[@]}" apply -f -
"${kubectl_cmd[@]}" apply --server-side --force-conflicts -n argo-rollouts \
	-f https://github.com/argoproj/argo-rollouts/releases/download/v1.10.0/install.yaml
"${kubectl_cmd[@]}" -n argo-rollouts rollout status deployment/argo-rollouts --timeout=180s

node_arch="$("${kubectl_cmd[@]}" get nodes -o jsonpath='{.items[0].status.nodeInfo.architecture}')"
case "$node_arch" in
	amd64)
		plugin_asset="metric-plugin-linux-amd64"
		plugin_sha="68384494927b6f0ad6f7daa11476d62ce7ab4e599b76336624eb67d47f55e04c"
		;;
	arm64)
		plugin_asset="metric-plugin-linux-arm64"
		plugin_sha="59d08fcb6052167f0196fec2f301059b3f79c4f1dcc4ee6967c2c351dd6acf83"
		;;
	*)
		fail "unsupported kind node architecture: $node_arch"
		;;
esac

plugin_url="${release_base}/${plugin_asset}"

"${kubectl_cmd[@]}" apply -f - <<YAML
apiVersion: v1
kind: ConfigMap
metadata:
  name: argo-rollouts-config
  namespace: argo-rollouts
data:
  metricProviderPlugins: |-
    - name: "argoproj-labs/rollouts-plugin-metric-signalfx"
      location: "$plugin_url"
      sha256: "$plugin_sha"
YAML

"${kubectl_cmd[@]}" apply -f - <<'YAML'
apiVersion: v1
kind: Secret
metadata:
  name: signalfx
  namespace: argo-rollouts
type: Opaque
stringData:
  token: abcd
YAML

"${kubectl_cmd[@]}" -n argo-rollouts patch deployment/argo-rollouts --type=strategic \
	-p '{"spec":{"template":{"spec":{"containers":[{"name":"argo-rollouts","env":[{"name":"SIGNALFX_ACCESS_TOKEN","valueFrom":{"secretKeyRef":{"name":"signalfx","key":"token"}}}]}]}}}}'
"${kubectl_cmd[@]}" -n argo-rollouts rollout restart deployment/argo-rollouts
"${kubectl_cmd[@]}" -n argo-rollouts rollout status deployment/argo-rollouts --timeout=180s

"${kubectl_cmd[@]}" apply -f - <<'YAML'
apiVersion: argoproj.io/v1alpha1
kind: AnalysisRun
metadata:
  name: signalfx-smoke
  namespace: default
spec:
  metrics:
    - name: signalfx-smoke
      count: 1
      failureLimit: 0
      successCondition: result >= 42
      provider:
        plugin:
          argoproj-labs/rollouts-plugin-metric-signalfx:
            streamURL: ws://fake-signalflow.signalfx-test.svc.cluster.local:8080
            query: data('demo').publish()
            duration: 2
            aggregator: latest
YAML

deadline=$((SECONDS + 180))
phase=""
while ((SECONDS < deadline)); do
	phase="$("${kubectl_cmd[@]}" -n default get analysisrun signalfx-smoke -o jsonpath='{.status.phase}' 2>/dev/null || true)"
	case "$phase" in
		Successful)
			break
			;;
		Failed|Error|Inconclusive)
			"${kubectl_cmd[@]}" -n default get analysisrun signalfx-smoke -o yaml >&2 || true
			fail "AnalysisRun finished in phase $phase"
			;;
	esac
	sleep 2
done

[[ "$phase" == "Successful" ]] || fail "AnalysisRun did not become Successful before timeout (phase: ${phase:-unset})"

measurement_value="$("${kubectl_cmd[@]}" -n default get analysisrun signalfx-smoke -o jsonpath='{.status.metricResults[0].measurements[0].value}')"
[[ "$measurement_value" == "42" ]] || fail "measurement value was $measurement_value, want 42"

controller_logs="$("${kubectl_cmd[@]}" -n argo-rollouts logs deployment/argo-rollouts --since=10m 2>/dev/null || true)"
if printf '%s' "$controller_logs" | grep -Fq 'abcd'; then
	fail "fake token appeared in controller logs"
fi

"${kubectl_cmd[@]}" -n default get analysisrun signalfx-smoke -o yaml
printf 'kind smoke test passed: architecture=%s phase=%s value=%s\n' "$node_arch" "$phase" "$measurement_value"
