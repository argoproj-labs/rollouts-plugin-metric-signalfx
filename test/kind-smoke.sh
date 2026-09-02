#!/usr/bin/env bash

set -euo pipefail

cluster_name="argo-sfx-smoke"
context="kind-${cluster_name}"
fake_image="signalfx-fake:local"
node_image="kindest/node:v1.31.4"
release_base="https://github.com/argoproj-labs/rollouts-plugin-metric-signalfx/releases/download/v0.2.0"
cluster_created=0
tls_dir=""

cleanup() {
	if [[ "$cluster_created" == "1" ]]; then
		kind delete cluster --name "$cluster_name" >/dev/null 2>&1 || true
	fi
	if [[ -n "$tls_dir" ]]; then
		rm -rf "$tls_dir"
	fi
}

fail() {
	printf 'kind smoke test failed: %s\n' "$1" >&2
	exit 1
}

trap cleanup EXIT

for command_name in docker kind kubectl openssl; do
	command -v "$command_name" >/dev/null 2>&1 || fail "missing command: $command_name"
done

docker info >/dev/null 2>&1 || fail "Docker daemon is not running"

if kind get clusters | grep -Fxq "$cluster_name"; then
	fail "cluster $cluster_name already exists; refusing to touch it"
fi

tls_dir="$(mktemp -d)"
openssl req -x509 -newkey rsa:2048 -nodes \
	-keyout "$tls_dir/tls.key" -out "$tls_dir/tls.crt" -days 1 \
	-subj "/CN=stream.us0.signalfx.com" \
	-addext "basicConstraints=critical,CA:TRUE" \
	-addext "subjectAltName=DNS:stream.us0.signalfx.com" >/dev/null 2>&1

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
YAML

"${kubectl_cmd[@]}" -n signalfx-test create secret tls fake-signalflow-tls \
	--cert="$tls_dir/tls.crt" --key="$tls_dir/tls.key"

"${kubectl_cmd[@]}" apply -f - <<'YAML'
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
          env:
            - name: TLS_CERT_FILE
              value: /tls/tls.crt
            - name: TLS_KEY_FILE
              value: /tls/tls.key
          ports:
            - name: websocket
              containerPort: 8080
          readinessProbe:
            tcpSocket:
              port: websocket
          volumeMounts:
            - name: tls
              mountPath: /tls
              readOnly: true
      volumes:
        - name: tls
          secret:
            secretName: fake-signalflow-tls
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
      port: 443
      targetPort: websocket
YAML

"${kubectl_cmd[@]}" -n signalfx-test rollout status deployment/fake-signalflow --timeout=120s

fake_service_ip="$("${kubectl_cmd[@]}" -n signalfx-test get service fake-signalflow -o jsonpath='{.spec.clusterIP}')"
[[ "$fake_service_ip" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] || fail "invalid fake SignalFlow service IP: $fake_service_ip"

"${kubectl_cmd[@]}" create namespace argo-rollouts --dry-run=client -o yaml | "${kubectl_cmd[@]}" apply -f -
"${kubectl_cmd[@]}" apply --server-side --force-conflicts -n argo-rollouts \
	-f https://github.com/argoproj/argo-rollouts/releases/download/v1.10.0/install.yaml
"${kubectl_cmd[@]}" -n argo-rollouts rollout status deployment/argo-rollouts --timeout=180s

"${kubectl_cmd[@]}" -n argo-rollouts create configmap fake-signalflow-ca \
	--from-file=ca.crt="$tls_dir/tls.crt"

node_arch="$("${kubectl_cmd[@]}" get nodes -o jsonpath='{.items[0].status.nodeInfo.architecture}')"
case "$node_arch" in
	amd64)
		plugin_asset="metric-plugin-linux-amd64"
		plugin_sha="bcafd5eff3c686181a4baa5237eeaddd0833de9760c4046f4246d8d07fcd0d17"
		;;
	arm64)
		plugin_asset="metric-plugin-linux-arm64"
		plugin_sha="1b8f926af2239abd69fb18b129aecf06ca9b18bfaf8a097645ddc7c6f2f4b011"
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

controller_patch="$(printf '{\"spec\":{\"template\":{\"spec\":{\"hostAliases\":[{\"ip\":\"%s\",\"hostnames\":[\"stream.us0.signalfx.com\"]}],\"volumes\":[{\"name\":\"fake-signalflow-ca\",\"configMap\":{\"name\":\"fake-signalflow-ca\"}}],\"containers\":[{\"name\":\"argo-rollouts\",\"env\":[{\"name\":\"SIGNALFX_ACCESS_TOKEN\",\"valueFrom\":{\"secretKeyRef\":{\"name\":\"signalfx\",\"key\":\"token\"}}},{\"name\":\"SSL_CERT_FILE\",\"value\":\"/etc/ssl/certs/fake-signalflow-ca.crt\"}],\"volumeMounts\":[{\"name\":\"fake-signalflow-ca\",\"mountPath\":\"/etc/ssl/certs/fake-signalflow-ca.crt\",\"subPath\":\"ca.crt\",\"readOnly\":true}]}]}}}}' "$fake_service_ip")"
"${kubectl_cmd[@]}" -n argo-rollouts patch deployment/argo-rollouts --type=strategic -p "$controller_patch"
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
            realm: us0
            streamURL: wss://stream.us0.signalfx.com/v2/signalflow
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
