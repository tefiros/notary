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
| `02-storage.yaml` | Longhorn StorageClass + PVCs |
| `03-statefulsets.yaml` | StatefulSets: ZooKeeper, Kafka, PostgreSQL |
| `04-deployments.yaml` | Deployments: Schema Registry, Kafka Connect, ksqldb, PostgREST |
| `05-services.yaml` | ClusterIP/NodePort services |
| `06-ingress.yaml` | Ingress for external access |

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

The `StorageClass` expects Longhorn to be installed. To use a different storage:

```yaml
# In 02-storage.yaml, change:
storageClassName: <your-storage-class>
```

Adjust PVC sizes as needed:
- `ter-kafka-connect-connectors`: 1Gi
- `ter-postgres-data`: 5Gi
- `ter-kafka-data`: 10Gi

### 4. Image Versions

Update images in StatefulSet/Deployment specs:
- ZooKeeper: `confluentinc/cp-zookeeper:6.1.0`
- Kafka: `confluentinc/cp-kafka:6.1.0`
- Schema Registry: `confluentinc/cp-schema-registry:6.1.0`
- Kafka Connect: `confluentinc/cp-kafka-connect:6.1.0`
- ksqldb: `confluentinc/ksqldb-server:0.15.0`
- PostgreSQL: `postgres:12`
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

### 8. Database Credentials

PostgreSQL credentials are in `01-configmap.yaml`. For production, use Secrets:

```yaml
envFrom:
  - secretRef:
      name: ter-postgres-secret
```

### 9. Kafka Connect Connectors

The connectors plugins initialization command runs on startup. To customize, modify the container `command` in `04-deployments.yaml` for `ter-kafka-connect`.

## Dependencies & Startup Order

Services start in this order (due to implicit dependencies):

1. ZooKeeper → Kafka
2. Kafka → Schema Registry, Kafka Connect
3. Kafka + Kafka Connect → ksqldb
4. PostgreSQL → PostgREST

Wait ~30s between applying each layer for proper initialization.

## Service Name Reference

All service names are prefixed with `ter-`:

| Docker Compose | Kubernetes Service | Notes |
|-----------------|--------------------|--------|
| `zookeeper` | `ter-zookeeper:2181` | Internal DNS |
| `broker` | `ter-kafka:29092` (internal), `:9092` (external) | Use internal for pod-to-pod |
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
2. Or use external: `localhost:9092` (via NodePort 30092)

Example fix:
```json
"confluent.topic.bootstrap.servers": "ter-kafka:29092"
```

### Commands & Scripts

Commands expecting `localhost:9092` still work via the NodePort service (port 30092).

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