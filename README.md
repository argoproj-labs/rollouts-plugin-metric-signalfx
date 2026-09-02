# SignalFx Metric Plugin for Argo Rollouts

An out-of-tree RPC metric provider that evaluates bounded SignalFlow programs against Splunk Observability Cloud (formerly SignalFx).

- **Status:** v0.2.0 initial `argoproj-labs` release. Metric plugins are still alpha in Argo Rollouts.
- **Compatibility:** Argo Rollouts v1.10.0. Pin the dependency exactly for the first release.
- **Provider key:** `argoproj-labs/rollouts-plugin-metric-signalfx`
- **Module:** `github.com/argoproj-labs/rollouts-plugin-metric-signalfx`

## Behavior

- Each `Run` parses one metric configuration, creates one SignalFlow client, executes the configured program, consumes `Computation.Data()` for the configured `duration` in seconds while incrementally reducing numeric payloads to one value using `aggregator`, stops the computation, and drains all computation channels for up to 2 seconds before evaluating the accumulated value through Argo Rollouts `evaluate.EvaluateResult`. A `CHANNEL_ABORT` with `sf_job_abortState` `STOPPED` is normal completion only when its reason matches the plugin's successfully issued stop; genuine and premature channel aborts remain errors. If the cleanup bound is reached, the run returns an error while client shutdown closes the channels and lets the cleanup owner finish.
- Empty data, unsupported value types, non-finite values, stream errors, and deadline expiry return `AnalysisPhaseError`.
- A failed SignalFlow computation propagates its error without transparent retries. The controller's analysis retry policy remains authoritative.
- `Resume`, `Terminate`, and `GarbageCollect` are idempotent no-ops. Measurements are finite and not persisted by the plugin.
- `Type()` returns `RPCPlugin`.

## Configuration

The JSON object under `metric.provider.plugin["argoproj-labs/rollouts-plugin-metric-signalfx"]`:

| Field | Required | Description |
| --- | --- | --- |
| `query` | yes | SignalFlow program containing the published result to measure |
| `realm` | yes unless `streamURL` is set with an explicit inline `accessToken` | Splunk Observability realm, e.g. `us0`. Use up to 63 lowercase letters, digits, or hyphens, without a leading or trailing hyphen. Required with Secret-backed authentication, including custom endpoints. |
| `accessToken` | yes unless `SIGNALFX_ACCESS_TOKEN` is set | Access token for the realm |
| `duration` | yes | Positive integer seconds for the measurement window; the value must leave room for the 10-second timeout margin |
| `aggregator` | yes | One of `max`, `min`, `avg`, `sum`, `count`, `latest` |
| `streamURL` | no | Full WebSocket endpoint override for a tested non-default deployment. It must use `ws://` or `wss://`, include a host, and omit user info. With Secret-backed authentication, it must use `wss://` and its host must be the SignalFlow host derived from `realm`; an explicit inline token may use a local test endpoint. |

`aggregator` semantics across the window:

- `max`, `min`, `avg`, `sum` over all numeric payloads
- `count` is the number of numeric payloads
- `latest` is the last numeric payload received

All payload value types `double`, `long` (`int64`), and `int` (`int32`) are converted to `float64`. Non-finite values and unknown aggregators return an error.

## Installation

Argo Rollouts loads metric provider plugins through `argo-rollouts-config`.

### HTTPS location (release)

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: argo-rollouts-config
data:
  metricProviderPlugins: |-
    - name: "argoproj-labs/rollouts-plugin-metric-signalfx"
      location: "https://github.com/argoproj-labs/rollouts-plugin-metric-signalfx/releases/download/v0.2.0/metric-plugin-linux-amd64"
      sha256: "bcafd5eff3c686181a4baa5237eeaddd0833de9760c4046f4246d8d07fcd0d17"
```

For arm64 nodes, use `metric-plugin-linux-arm64` with checksum `1b8f926af2239abd69fb18b129aecf06ca9b18bfaf8a097645ddc7c6f2f4b011`. For unreleased or development use, omit `sha256`.

### File location

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: argo-rollouts-config
data:
  metricProviderPlugins: |-
    - name: "argoproj-labs/rollouts-plugin-metric-signalfx"
      location: "file:///opt/argo/rollouts/plugins/metric-plugin-signalfx"
```

`file://` requires the binary to be mounted into the controller container (init container, shared volume, or baked image). The controller does not start if the plugin is not available at the configured location.

## Authentication

Production use must inject `SIGNALFX_ACCESS_TOKEN` from a Kubernetes Secret into the Rollouts controller. The plugin subprocess inherits the controller's environment.

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: signalfx
stringData:
  token: <splunk-access-token>
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: argo-rollouts
spec:
  template:
    spec:
      containers:
        - name: argo-rollouts
          env:
            - name: SIGNALFX_ACCESS_TOKEN
              valueFrom:
                secretKeyRef:
                  name: signalfx
                  key: token
```

Inline `accessToken` in an `AnalysisTemplate` is supported for local testing only. Do not commit tokens to Git. The inline value, when present, takes precedence over the environment variable. The plugin never logs the token or returns it in metadata. `GetMetadata` only returns `ResolvedSignalFlowQuery`.

## Example

See `examples/analysis-template.yaml` for a complete `AnalysisTemplate`:

```yaml
apiVersion: argoproj.io/v1alpha1
kind: AnalysisTemplate
metadata:
  name: signalfx-latency
spec:
  metrics:
    - name: p95-latency
      interval: 5m
      successCondition: result < 200
      failureLimit: 1
      provider:
        plugin:
          argoproj-labs/rollouts-plugin-metric-signalfx:
            realm: us0
            query: |
              data('demo.trans.latency').max().publish()
            duration: 60
            aggregator: latest
```

The example query and thresholds are illustrative. Substitute a SignalFlow program that publishes one result and SLO thresholds that match the service under analysis. The provider key, field names, duration units, and `aggregator` values are part of the contract.

## Testing without Splunk

The automated suite does not contact Splunk Observability Cloud. It uses the SignalFlow client's `FakeBackend` to exercise data collection, aggregation, provider phase mapping, and executable RPC behavior. Protocol-valid computation errors are covered by a test-only WebSocket handler, not `FakeBackend`.

```bash
make test
make vet
```

`TestBuiltPluginBinaryUsesFakeSignalFlow` also builds the current executable, launches it as a separate go-plugin process, connects it to a fake SignalFlow WebSocket server, and verifies an Argo-compatible measurement of `42`:

```bash
go test ./... -run TestBuiltPluginBinaryUsesFakeSignalFlow -count=1 -v
```

The repository carries a small Apache-licensed source copy of SignalFlow client v2.3.0 because the upstream client does not expose computation termination reasons needed to correlate a `STOPPED` abort with the plugin's stop request and reject other channel aborts. Its regression tests are included by `make test` and can also be run directly as a nested module:

```bash
(cd third_party/signalflow-client-go && go test -race ./...)
```

For a controller-level smoke test, install Docker, kind, kubectl, and OpenSSL, start a local Docker runtime, and run:

```bash
bash test/kind-smoke.sh
```

The script builds a test-only fake SignalFlow service, creates the disposable `argo-sfx-smoke` cluster, installs Argo Rollouts v1.10.0, downloads the public v0.2.0 plugin binary with its architecture-specific checksum, injects the fake token through a Kubernetes Secret, and runs a one-shot `AnalysisRun` that must finish `Successful` with value `42`. It deletes the cluster on exit. This validates plugin download, RPC startup, Secret environment propagation, TLS WebSocket connectivity, and Argo wiring; it does not validate live Splunk authentication or metric ingestion.

## Building and verifying locally

```bash
make fmt
make test
make vet
make build
```

Artifacts:

- `dist/metric-plugin-linux-amd64`
- `dist/metric-plugin-linux-arm64`

Release binaries are not committed. Each release publishes SHA-256 checksums alongside the binaries.

## References

- Issue: https://github.com/argoproj/argo-rollouts/issues/1046
- Plugin docs: https://argo-rollouts.readthedocs.io/en/stable/analysis/plugins/
- Sample plugin: https://github.com/argoproj-labs/rollouts-plugin-metric-sample-prometheus
- SignalFlow client: https://github.com/signalfx/signalflow-client-go
