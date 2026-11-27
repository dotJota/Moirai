# Kubernetes replica benchmark launcher

Run these commands from the Moirai root. You need Python 3.10+, kubectl, a
reachable Kubernetes cluster, and access to build and distribute an image.
The launcher uses the current kubectl context. It needs permissions to create
and delete its temporary namespace, create, read and delete Pods, create
ConfigMaps and a Service, and use Pod logs and port forwarding.

```sh
docker build -f kubernetes-launcher/Dockerfile \
  --build-arg REPLICA_EXAMPLE=benchmark -t moirai-replica-benchmark:dev .
```
```sh
docker build -f kubernetes-launcher/benchmark-replica/Dockerfile.network-tools \
  -t moirai-network-tools:dev kubernetes-launcher/benchmark-replica
```
```sh
# For kind, make the image available to every worker before launching.
kind load docker-image moirai-replica-benchmark:dev moirai-network-tools:dev
```
```sh
bash kubernetes-launcher/benchmark-replica/benchmark_launch.sh \
  kubernetes-launcher/benchmark-replica/experiment_sample.json
```

For a remote cluster, publish the image to a registry reachable by the
workers and put its tag or digest in `replica_image`. Publish the network-tools
image as well and set `network_limit.tools_image` to its registry reference.

The launcher creates a fresh namespace with a headless Service, one Pod
and one ConfigMap per replica. The replicas use stable Pod DNS names and
retry DNS during startup. Once every replica has connected to all peers,
the launcher sends start requests concurrently over loopback kubectl port
forwards. It polls status until all surviving replicas have processed
their local queue, then allows `grace_s` for replication and commitment
(10 seconds in the sample). It requests a final measurement, waits for the replica admin acknowledgments,
and validates the captured logs before requesting Pod deletion. A missing or
partial stream is recovered through bounded, sequential `kubectl logs`
requests. The collector joins overlapping output without duplicating samples.
A gap left by container log rotation causes recovery to fail explicitly. The Pod manifest
allows Kubernetes one second of termination grace after deletion. The
launcher submits namespace cleanup asynchronously. Start requests
can arrive at slightly different times. Every replica records its own
monotonic origin and a Unix start timestamp.

Set `fault_after_s` to an integer to crash replica 0 that many seconds
after start fan-out. Its replica process exits and the Pod is configured
with `restartPolicy: Never`. The launcher verifies the exit and waits
for the surviving replicas. It retains the crashed replica's records
through its last periodic sample. The configured commitment quorum or
membership still applies. A run can finish with pending updates, such as
after an timestamp-stability participant fails.

The experiment JSON controls the replica count, protocol, image, TCP
pull and idle timing, event-oracle timeout, workload composition and
rate, sample interval, measured network interface, start and drain
deadlines, grace period and optional crash time. Change the interface
if your cluster uses a Pod interface other than `eth0`. Interface
byte counters include background traffic on that interface, including
readiness probes and protocol traffic during the measured interval.
The headless Service publishes TCP port 19001 for causal replication and,
in Creek runs, TCP port 19002 for consensus. Creek derives the consensus
port by adding one to each replica's causal port. The admin server
has no authentication and is intended for disposable benchmark Pods on
a trusted cluster.

Pass `--output DIRECTORY` for a specific new result directory. The
default is under `kubernetes-benchmark/results`. Every result directory
contains the experiment snapshot, rendered manifest, run status, and
`replicas/<id>.log` files. The console and `run.json` report separate
durations for commitment grace, seal acknowledgments, final log delivery,
optional crash verification, deletion requests and tunnel cleanup. Final
log delivery includes recovery and validation. The namespace name in `run.json` identifies the resources
for checking asynchronous cleanup. The launcher follows logs during the run
to retain measurements across
container log rotations. Raw streams and stderr are saved as
`replicas/<id>.stream.log` and `replicas/<id>.stderr.log`. Recovered snapshots
are retained as `replicas/<id>.snapshot.log`. Only validated output becomes
`replicas/<id>.log`. The per-replica recovery result and follower exit code
are in `log_collection.json`.

On failure the launcher returns nonzero and retains the namespace and Pods
for diagnosis or manual log retrieval. The printed namespace can be removed
with `kubectl delete namespace NAME` after recovery. Incomplete runs are
skipped by the viewers. See [the replica benchmark](../../moirai-network-node/examples/benchmark/README.md)
for the sample schema and [the experiment suite](../../kubernetes-benchmark/README.md)
for generation and plotting.

| File | Purpose |
|---|---|
| `benchmark_launch.sh` | Shell entry point for one experiment. |
| `launch_benchmark.py` | Coordinates Kubernetes startup, control calls, crash injection, log collection and cleanup. |
| `benchmark_config.py` | Validates settings and renders Pod, Service, ConfigMap and replica JSON configuration. |
| `replica_logs.py` | Captures stream diagnostics, recovers snapshots with overlap checks, and validates records before deletion. |
| `test_replica_logs.py` | Regression checks for interrupted streams, rotated snapshots, and incomplete measurements. |
| `experiment_sample.json` | A four-replica example with a mixed local workload and ten seconds of drain grace. |

Run the log-recovery checks from the project root with
`python3 -m unittest discover -s kubernetes-launcher/benchmark-replica -p 'test_replica_logs.py'`.

## Per-replica bandwidth limits

The sample enables a 10 Mbit/s aggregate egress budget on each replica's
`network_interface`. Set `network_limit` to `null` to disable shaping;
older files without this field also remain unlimited.

| `network_limit` field | Meaning |
|---|---|
| `rate_kbit_s` | Positive integer sustained rate in decimal kilobits per second (10000 = 10 Mbit/s). |
| `burst_bytes` | Positive integer token-bucket burst size in bytes (sample: 32768). |
| `latency_ms` | Positive integer TBF maximum queueing-time parameter in milliseconds (sample: 100). |
| `tools_image` | Image containing `tc`, built with `Dockerfile.network-tools`. |

Queue latency determines the buffer size; it does not inject a fixed network
delay. Burst size must suit the MTU and requested rate; very small bursts can
prevent packets from being sent or reduce achievable throughput. Calibrate
these settings on the target nodes before collecting measurements.

Each Pod gets a `configure-bandwidth` init container that installs the TBF
qdisc before the replica starts. Only this container runs as root with
`NET_ADMIN`; the application retains its existing non-root security settings.
The cluster must permit that capability and the nodes must support TBF.
A failed init prevents the replica from starting, and the launcher's startup
timeout fails the experiment instead of silently running it without limits.
Inspect failures with `kubectl -n NAMESPACE logs POD -c configure-bandwidth`
and `kubectl -n NAMESPACE describe pod POD`.

All egress on the selected interface shares one budget, including causal
replication on 19001, Creek consensus on 19002, and administrative responses
sent over that interface. This does not cap aggregate ingress or allocate a
separate budget per peer. No CNI annotations are used.
The launcher records the settings in `experiment.json` and the generated rules
in `manifest.json`. Generated experiment suites inherit the settings from their
base configuration. Result grouping separates different network limits.

### Saturated-transfer calibration

Before paper measurements, test on disposable Pods using the same init rules
and node placement as the benchmark. The tools image includes `iperf3` for this
purpose. For example, render two probe Pods from the sample (run from the repo
root), replacing only their application containers:

```sh
python3 - <<'PYPROBE' > /tmp/moirai-bandwidth-probe.json
import json, sys
sys.path.insert(0, 'kubernetes-launcher/benchmark-replica')
from benchmark_config import manifest, validate
with open('kubernetes-launcher/benchmark-replica/experiment_sample.json') as f:
    config = validate(json.load(f))
config['n_replicas'] = 2
pods = [obj for obj in manifest(config)['items'] if obj['kind'] == 'Pod']
for pod in pods:
    spec = pod['spec']
    spec.pop('volumes')
    spec.pop('subdomain')
    spec['containers'] = [{
        'name': 'probe', 'image': config['network_limit']['tools_image'],
        'command': ['iperf3'], 'args': ['-s'],
        'securityContext': {'allowPrivilegeEscalation': False,
                            'capabilities': {'drop': ['ALL']}}
    }]
print(json.dumps({'apiVersion': 'v1', 'kind': 'List', 'items': pods}))
PYPROBE
kubectl create namespace moirai-bandwidth-probe
kubectl -n moirai-bandwidth-probe create -f /tmp/moirai-bandwidth-probe.json
kubectl -n moirai-bandwidth-probe wait --for=condition=Ready pod --all --timeout=180s
peer_ip=$(kubectl -n moirai-bandwidth-probe get pod benchmark-replica-1 -o jsonpath='{.status.podIP}')
kubectl -n moirai-bandwidth-probe exec benchmark-replica-0 -c probe -- tc -s qdisc show dev eth0
kubectl -n moirai-bandwidth-probe exec benchmark-replica-0 -c probe -- iperf3 -c "$peer_ip" -t 30 -O 3 -P 4
kubectl -n moirai-bandwidth-probe exec benchmark-replica-0 -c probe -- iperf3 -c "$peer_ip" -t 30 -O 3 -P 4 -R
kubectl delete namespace moirai-bandwidth-probe
```

Use the configured interface instead of `eth0` if changed. Inspect the qdisc
and the aggregate throughput across streams in both directions: payload
throughput should be near, but below, the configured rate due to protocol
overhead. Compare with an unlimited baseline to establish that the underlying
network can exceed the requested rate. Repeat across the node placements used
in experiments and retain the outputs alongside measurement results. This
calibration runs separately so its traffic does not contaminate benchmarks.
