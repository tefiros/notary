# Notary


# Deployment Guide
 
Full event-driven stack deployed on Kubernetes. Includes Kafka, Schema Registry, Kafka Connect, PostgreSQL, PostgREST, and RabbitMQ.
 
---
 
## Architecture
 
| Service | Internal DNS | External Port |
|---|---|---|
| Zookeeper | `zookeeper:2181` | — |
| Kafka Broker | `broker:29092` (internal) | `localhost:30092` |
| Schema Registry | `schema-registry:8081` | — |
| Kafka Connect | `kafka-connect:8083` | — |
| PostgreSQL | `postgres:5432` | — |
| PostgREST | `postgrest:3000` | `localhost:30003` |
| RabbitMQ AMQP | `rabbitmq:5672` | `localhost:30672` |
| RabbitMQ UI | `rabbitmq:15672` | `localhost:30673` |
 
---

 
---
 
## 1. Deploy the stack
 
Apply manifests in order:
 
```bash
kubectl apply -f 00-namespace.yaml //if it6g is not already created
kubectl apply -f 01-zookeeper.yaml
kubectl apply -f 02-broker.yaml
kubectl apply -f 03-schema-registry.yaml
kubectl apply -f 04-kafka-connect.yaml
kubectl apply -f 05-postgres.yaml
kubectl apply -f 06-postgrest.yaml
kubectl apply -f 08-rabbitmq.yaml
```
 
Or apply the whole folder at once:
```bash
kubectl apply -f k8s/
```
 
Wait for all pods to be `Running`:
```bash
kubectl get pods -n it6g -w
```
 
> **Note:** `kafka-connect` takes 3–5 minutes on first start: the init container installs plugins (kafka-connect-jdbc, kafka-connect-transform-common, kafka-connect-rabbitmq).
 
---

 
## 2. Signer Service
 
The signer service consumes from `test-topic`, signs each message and produces Avro records to `test-signed-topic`.
 
Build and push the image - only for developer new settings:
```bash
docker build -t anamp26/signer-service-it6g:1.0.0 .
docker push anamp26/signer-service-it6g:1.0.0
```
 
Deploy:
```bash
kubectl apply -f signer-service.yaml
kubectl logs -n it6g deployment/signer-service -f
```
 
---
 
## 3. Post-deployment configuration
 
### 3.1 PostgreSQL — create `anon` role for PostgREST
 
PostgREST requires an `anon` role to exist in PostgreSQL. Run once after first deploy:
 
```bash
kubectl exec -it -n kafka deployment/postgres -- \
  psql -U postgres -c "CREATE ROLE anon NOLOGIN;"
 
kubectl exec -it -n kafka deployment/postgres -- \
  psql -U postgres -c "GRANT USAGE ON SCHEMA public TO anon; GRANT SELECT ON ALL TABLES IN SCHEMA public TO anon;"
```

 
---
 
### 3.2 RabbitMQ — create input queue
 
The RabbitMQ source connector expects the queue `q.input.test` to exist:
 
```bash
kubectl exec -it -n kafka deployment/rabbitmq -- \
  rabbitmqadmin declare queue name=q.input.test durable=true \
  --username=admin --password=admin
```
 
Or create it from the RabbitMQ UI at `http://localhost:30673` (admin/admin) → **Queues → Add a new queue**.

The produced messages through the RabbitMQ queue must be in JSON format.
 
---
 
### 3.3 Kafka Connect — register RabbitMQ source connector
 
Find in folder k8s-connectors/  `rabbitproducer.json`:
```json
{
  "name": "SOURCERABBIT",
  "config": {
    "connector.class": "io.confluent.connect.rabbitmq.RabbitMQSourceConnector",
    "tasks.max": "1",
    "kafka.topic": "test-topic",
    "rabbitmq.host": "rabbitmq",
    "rabbitmq.port": "5672",
    "rabbitmq.username": "admin",
    "rabbitmq.password": "admin",
    "rabbitmq.virtual.host": "/",
    "rabbitmq.queue": "q.input.test",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
    "confluent.topic.bootstrap.servers": "broker:29092",
    "confluent.topic.replication.factor": "1",
    "confluent.topic.partitions": "1",
    "confluent.license.topic.replication.factor": "1"
  }
}
```
 
Post the connector:
```bash
kubectl exec -it -n kafka deployment/kafka-connect -- \
  curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @/dev/stdin < rabbitproducer.json
```
 
---
 
### 3.4 Kafka Connect — register JDBC sink connector
 
Find in folder k8s-connectors/  `sink-receipt.json`:
```json
{
  "name": "SINK_RECEIPT",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "topics": "test-signed-topic",
    "connection.url": "jdbc:postgresql://postgres:5432/postgres",
    "connection.user": "postgres",
    "connection.password": "postgres",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "auto.create": "true",
    "auto.evolve": "true",
    "transforms": "InsertTS,CopyProducerId,InsertMeta",
    "transforms.InsertTS.type": "org.apache.kafka.connect.transforms.InsertField$Value",
    "transforms.InsertTS.timestamp.field": "ingest_ts",
    "transforms.CopyProducerId.type": "com.github.jcustenborder.kafka.connect.transform.common.HeaderToField$Value",
    "transforms.CopyProducerId.header.mappings": "producer_id:STRING",
    "transforms.InsertMeta.type": "org.apache.kafka.connect.transforms.InsertField$Value",
    "transforms.InsertMeta.offset.field": "kafka_offset",
    "transforms.InsertMeta.partition.field": "kafka_partition",
    "transforms.InsertMeta.topic.field": "kafka_topic"
  }
}
```
 
Post the connector:
```bash
kubectl exec -it -n kafka deployment/kafka-connect -- \
  curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @/dev/stdin < sink-receipt.json
```
 
---
 
## 4. Verify everything is working
 
### Check all pods
```bash
kubectl get pods -n it6g
```
 
### Check connector status
```bash
# RabbitMQ source
kubectl exec -it -n kafka deployment/kafka-connect -- \
  curl http://localhost:8083/connectors/SOURCERABBIT/status | python3 -m json.tool
 
# JDBC sink
kubectl exec -it -n kafka deployment/kafka-connect -- \
  curl http://localhost:8083/connectors/SINK_RECEIPT/status | python3 -m json.tool
```
 
### Consume from a topic
```bash
kubectl exec -it -n kafka deployment/broker -- \
  kafka-console-consumer \
  --topic test-signed-topic \
  --bootstrap-server localhost:29092 \
  --from-beginning
```
 
### Check PostgreSQL table
```bash
kubectl exec -it -n kafka deployment/postgres -- \
  psql -U postgres -c "SELECT * FROM \"test-signed-topic\";"
```
 
### Query via PostgREST
```bash
curl http://localhost:30003/test-signed-topic
```
 
---

 
## 5. Message flow
 
```
RabbitMQ (q.input.test)
    → [SOURCERABBIT connector]
    → Kafka topic: test-topic
    → [Signer Service]
    → Kafka topic: test-signed-topic (Avro)
    → [SINK_RECEIPT connector]
    → PostgreSQL table: test-signed-topic
    → [PostgREST]
    → HTTP API: localhost:30003/test-signed-topic
```

## Authors and acknowledgment
Telefónica - Ana Méndez

## License
This project is licensed under the Apache License, Version 2.0.
Copyright 2025 TID
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at:

http://www.apache.org/licenses/LICENSE-2.0


Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

## Project status
The code has been tested on local Kubernetes cluster. Contact developer for integration adjustments.
