# Kubernetes deployment (kind / minikube)

Manifests for the whole pipeline. Structured to be applied to a local cluster;
on an 8 GB laptop the *full* stack is tight (same RAM story as docker-compose),
so treat this as the "genuinely deployable to a cluster" artifact and run it on
a node with enough memory.

## Build + load images (kind)

The manifests use locally-built images with `imagePullPolicy: IfNotPresent`, so
build them and load into the kind cluster:

```bash
# build
docker build -t fleet:latest ./services
docker build -t log-shipper:latest -f log-shipper/Dockerfile .
docker build -t flink-job:latest  -f flink-job/Dockerfile .
docker build -t control-api:latest -f control-api/Dockerfile .

# create cluster + load images
kind create cluster --name logging
for img in fleet log-shipper flink-job control-api; do kind load docker-image $img:latest --name logging; done
```

## Apply

```bash
kubectl apply -f k8s/            # applies 00→60 in order
kubectl -n logging get pods -w
```

Reach the UIs (NodePort): control-api on `:30890`, Grafana on `:30300`
(with kind, `kubectl -n logging port-forward svc/control-api 8090:8090`).

## How it scales (the interview story)

- **Kafka** scales by **partitions**: a partition is consumed by at most one
  consumer subtask, so partitions cap consumer parallelism. `logs` has 6.
- **Flink** scales by **task slots**: total slots = `taskmanager replicas ×
  numberOfTaskSlots`. Bump `replicas` in [50-flink.yaml](50-flink.yaml)
  (`kubectl -n logging scale deploy/flink-taskmanager --replicas=4`) to add slots;
  keep `parallelism.default` ≤ total slots and ≤ Kafka partitions.
- **Stateless tiers** (mock services, log-shipper, control-api) scale by
  Deployment `replicas` behind their Services (add an HPA on CPU for autoscaling).
- **Log collection**: here each service pod carries a **Fluent Bit sidecar**
  (see [30-fleet.yaml](30-fleet.yaml)). The common alternative is a Fluent Bit
  **DaemonSet** (one collector per node) tailing `/var/log/containers/*.log` —
  fewer moving parts at cluster scale, at the cost of node-level coupling.

## Notes
- Kafka runs as a single-broker StatefulSet (KRaft). For HA: raise `replicas`,
  list all voters in `KAFKA_CONTROLLER_QUORUM_VOTERS`, and set topic RF ≥ 3.
- Flink checkpoints go to MinIO (`s3://flink-checkpoints`) so any TaskManager/
  JobManager can recover — the same reason you use S3/HDFS in production.
- Grafana provisioning (datasource/dashboards) is mounted via ConfigMap in a
  real deploy; omitted inline here for brevity.
