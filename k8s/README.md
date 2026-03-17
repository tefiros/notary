# Kubernetes Deployment Configuration

This directory contains the Kubernetes manifests converted from `docker-compose.yml`.

## Quick Start

Apply all manifests in order:

```bash
kubectl apply -f k8s/
```

Or apply individually:

```bash
kubectl apply -f k8s/00-namespace.yaml
kubectl apply -f k8s/01-configmap.yaml
kubectl apply -f k8s/02-storage.yaml
kubectl apply -f k8s/03-statefulsets.yaml
kubectl apply -f k8s/04-deployments.yaml
kubectl apply -f k8s/05-services.yaml
kubectl apply -f k8s/06-ingress.yaml
```

## Files Overview

| File | Description |
|------|-------------|
| `00-namespace.yaml` | Namespace `it6g` |
| `01-configmap.yaml` | Environment variables for all services |
| `02-storage.yaml` | Longhorn PVCs |
| `03-statefulsets.yaml` | StatefulSets: ZooKeeper, Kafka, PostgreSQL |
| `04-deployments.yaml` | Deployments: Schema Registry, Kafka Connect, ksqldb, PostgREST |
| `05-services.yaml` | ClusterIP/NodePort services |
| `06-ingress.yaml` | Ingress for external access |
| `07-postgres-init-example.yaml` | Optional example: SQL bootstrap ConfigMap + Job for PostgreSQL |

## Required User Adjustments

This `k8s` directory is intended as a fresh deployment baseline. Before using it, review these environment-specific settings:

1. Kafka external access: update `KAFKA_ADVERTISED_LISTENERS` in `01-configmap.yaml` to the real host/port your clients use.
2. Ingress DNS: update the host in `06-ingress.yaml`.
3. Storage class and size: confirm `storageClassName` and PVC sizes in `02-storage.yaml` for your cluster.
4. Credentials: replace PostgreSQL credentials in `01-configmap.yaml` with a Secret.
5. Optional service aliases: if existing tools still use Docker Compose names (`broker`, `schema-registry`, etc.), add alias Services in `05-services.yaml`.
6. PostgreSQL init data/scripts: the repository currently does not include a `data/postgres` directory. If you need compose-like SQL bootstrap files, create `data/postgres` and use the optional example in `07-postgres-init-example.yaml`.

## Customization Guide

### 1. Namespace

Change `it6g` to your desired namespace in all files.

### 2. Resource Names & Prefixing

All resources use the `ter-` prefix (Transparent Evidence Repository). To change:

- Update `metadata.name` in each file
- Update `selector.app` labels to match
- Update `configMapRef.name` references
- Update `persistentVolumeClaim.claimName` references

### 3. Storage (Longhorn)

This setup assumes Longhorn is already installed and provides a `StorageClass` named `longhorn`.

If your class name is different, update each PVC in `02-storage.yaml`:

```yaml
# In 02-storage.yaml, change:
storageClassName: <your-storage-class>
```

Adjust PVC sizes as needed:
- `ter-kafka-connect-connectors`: 1Gi
- `ter-postgres-data`: 5Gi
- `ter-kafka-data`: 10Gi

All PVCs are currently `ReadWriteOnce`, which is correct for the current single-replica workloads.

### 4. Image Versions

Update images in StatefulSet/Deployment specs:
- ZooKeeper: `confluentinc/cp-zookeeper:6.1.0`
- Kafka: `confluentinc/cp-kafka:6.1.0`
- Schema Registry: `confluentinc/cp-schema-registry:6.1.0`
- Kafka Connect: `confluentinc/cp-kafka-connect:6.1.0`
- ksqldb: `confluentinc/ksqldb-server:0.15.0`
- PostgreSQL: `debezium/postgres:12`
- PostgREST: `postgrest/postgrest:v7.0.2`

### 5. Resource Limits

Adjust CPU/memory requests and limits in each container spec as needed for your cluster capacity.

### 6. Ingress Host

Update the ingress host in `06-ingress.yaml`:

```yaml
rules:
  - host: ter.k8s.itrust6g.pdmfc.com  # Change this
```

### 7. External Kafka Access

Kafka is exposed via NodePort on port 30092. Update `05-services.yaml` if you need a different port or LoadBalancer:

```yaml
spec:
  type: NodePort  # or LoadBalancer
  ports:
    - nodePort: 30092  # Change port number
```

  `KAFKA_ADVERTISED_LISTENERS` in `01-configmap.yaml` must match the way clients reach Kafka externally.

  Current default is tuned for local Docker Desktop-style setups:

  ```yaml
  KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://ter-kafka:29092,PLAINTEXT_HOST://kubernetes.docker.internal:30092"
  ```

  Use one of these alternatives when needed:

  - Bare metal / VM Kubernetes: replace `kubernetes.docker.internal` with a reachable node IP or DNS name.
  - Cloud Kubernetes: switch `ter-kafka-external` service to `LoadBalancer` and advertise the load balancer DNS/IP.
  - Local port-forward workflow: advertise `localhost:9092` and use `kubectl port-forward` instead of NodePort.

  If advertised host/port and actual entry point differ, Kafka clients will connect but fail on metadata refresh.

### 8. Database Credentials

PostgreSQL credentials are in `01-configmap.yaml`. For production, use Secrets:

```yaml
envFrom:
  - secretRef:
      name: ter-postgres-secret
```

### 9. Kafka Connect Connectors

The Kubernetes deployment now includes plugin bootstrapping behavior equivalent to compose via an initContainer in `04-deployments.yaml`.

Plugins are installed into the PVC-mounted `/connectors` directory, which is included in `CONNECT_PLUGIN_PATH`.

## Dependencies & Startup Order

Services start in this order (enforced by initContainers):

1. ZooKeeper → Kafka
2. Kafka → Schema Registry, Kafka Connect
3. Kafka + Kafka Connect → ksqldb
4. PostgreSQL → PostgREST

You can apply all manifests in one shot; startup ordering is handled by pod init steps.

## Optional: PostgreSQL SQL Bootstrap

Use `07-postgres-init-example.yaml` when you want compose-like SQL initialization from script files.

Suggested workflow:

1. Create the folder `data/postgres` in the repository and add one or more `.sql` files.
2. Create/update the SQL ConfigMap from that folder:

```bash
kubectl -n it6g create configmap ter-postgres-init-sql --from-file=data/postgres --dry-run=client -o yaml | kubectl apply -f -
```

3. Apply the optional bootstrap job:

```bash
kubectl apply -f k8s/07-postgres-init-example.yaml
```

4. Re-run the job after SQL changes:

```bash
kubectl delete job -n it6g ter-postgres-init-job --ignore-not-found
kubectl apply -f k8s/07-postgres-init-example.yaml
```

## Service Name Reference

All service names are prefixed with `ter-`:

| Docker Compose | Kubernetes Service | Notes |
|-----------------|--------------------|--------|
| `zookeeper` | `ter-zookeeper:2181` | Internal DNS |
| `broker` | `ter-kafka:29092` (internal), `:30092` (external NodePort) | Use internal for pod-to-pod |
| `schema-registry` | `ter-schema-registry:8081` | |
| `kafka-connect` | `ter-kafka-connect:8083` | |
| `ksqldb` | `ter-ksqldb:8088` | |
| `postgres` | `ter-postgres:5432` | |
| `postgrest` | `ter-postgrest:3000` | |

## Impact on Existing Project Files

The `ter-` prefix changes affects these external files:

### Kafka Connect Connector Configs

Files: `kafka-avro-producer/connect/*.json`

These JSON files reference internal service names. When deploying to K8s:

1. Change `broker:29092` → `ter-kafka:29092` for internal access
2. Or use external NodePort: `<node-hostname-or-ip>:30092`

Example fix:
```json
"confluent.topic.bootstrap.servers": "ter-kafka:29092"
```

### Commands & Scripts

Commands expecting `localhost:9092` only work when using `kubectl port-forward`.

However, scripts using Docker network names (e.g., `schema-registry:8081`) need updating:
- `schema-registry:8081` → `ter-schema-registry:8081`
- `kafka-connect:8083` → `ter-kafka-connect:8083`
- `ksqldb:8088` → `ter-ksqldb:8088`

### Workaround Without Changing External Files

Keep original service names by adding aliases services:

```yaml
# In 05-services.yaml, add alias services:
---
apiVersion: v1
kind: Service
metadata:
  name: broker
  namespace: it6g
spec:
  type: ClusterIP
  selector:
    app: ter-kafka
  ports:
    - port: 29092
      targetPort: 29092
---
apiVersion: v1
kind: Service
metadata:
  name: schema-registry
  namespace: it6g
spec:
  type: ClusterIP
  selector:
    app: ter-schema-registry
  ports:
    - port: 8081
      targetPort: 8081
```

Add similar aliases for `kafka-connect`, `ksqldb`, `postgres`, `postgrest` if needed.

## Troubleshooting

### Check Pod Status
```bash
kubectl get pods -n it6g
```

### Check Logs
```bash
kubectl logs -n it6g <pod-name>
```

### Check Services
```bash
kubectl get svc -n it6g
```

### Port Forward for Local Testing
```bash
# Kafka
kubectl port-forward -n it6g svc/ter-kafka 9092:9092

# PostgreSQL
kubectl port-forward -n it6g svc/ter-postgres 5432:5432

# PostgREST
kubectl port-forward -n it6g svc/ter-postgrest 3000:3000
```