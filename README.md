# Real-Time Leaderboard

A Go leaderboard system split into three independently runnable applications:

- **API** (`cmd/api`): HTTP registration, login, score submission, and leaderboard endpoints. It writes score history to PostgreSQL and publishes `score.submitted` events to Kafka. Leaderboard reads use the Query Service through gRPC; the gRPC client connects lazily.
- **Query Service** (`cmd/query`): gRPC leaderboard reads from Redis, with PostgreSQL display-name lookup. It exposes gRPC health and reflection.
- **Worker** (`cmd/worker`): consumes Kafka score events in the existing `leaderboard-writer` group and updates the Redis projection.

Compose also runs PostgreSQL 17, Redis 7, and Apache Kafka 4.3.1 in a single-node KRaft configuration. Compose service names provide internal DNS.

## Prerequisites

- Docker Desktop with WSL integration enabled for this distribution, or a working Docker Engine.
- Docker Compose v2 or later.
- `curl` for the API health request; `grpcurl` is optional for a Query Service health request.

## Local environment

From the repository root, create the ignored local environment file:

```sh
cp .env.example .env
```

Replace the placeholder `POSTGRES_PASSWORD`, `REDIS_PASSWORD`, and `JWT_SECRET` values in `.env` with local secrets before starting the services. One way to generate a JWT secret is `openssl rand -hex 32`. Do not commit `.env`. `.env.example` contains placeholders only.

The `.env` values also select host ports. Defaults are API `8080`, Query gRPC `50053`, PostgreSQL `5432`, Redis `6379`, and Kafka external `9092`. Published ports bind to `127.0.0.1`. PostgreSQL, Redis, and Kafka are available on the host for local debugging; Redis requires its configured password.

## Build and run

Build the three independent app images, then start the stack:

```sh
docker compose build api query-service worker
docker compose up -d
```

Inspect health and logs:

```sh
docker compose ps
docker compose logs --follow
```

API health: `curl -fsS http://localhost:8080/health`

Optional Query gRPC health check: `grpcurl -plaintext localhost:50053 grpc.health.v1.Health/Check`

Build targets are `api`, `query`, and `worker`, producing `leaderboard-api:dev`, `leaderboard-query:dev`, and `leaderboard-worker:dev` respectively.

## Container network addresses

Containers connect to each other using Compose DNS, never host `localhost`:

| Caller | Dependency | Address |
|---|---|---|
| API | PostgreSQL | `postgres:5432` |
| API | Kafka | `kafka:29092` |
| API | Query Service | `query-service:50051` |
| Query Service | PostgreSQL | `postgres:5432` |
| Query Service | Redis | `redis:6379` |
| Worker | Kafka | `kafka:29092` |
| Worker | Redis | `redis:6379` |

The Query Service listens on `0.0.0.0:50051` inside its container and is published to the host at `localhost:50053` by default. The API does not wait for Query Service health: gRPC connection establishment is lazy, and API startup only requires PostgreSQL and Kafka readiness.

## Kafka listeners and topic

Kafka runs as one combined KRaft broker/controller with a stable node ID and cluster ID. It uses PLAINTEXT listeners for this isolated local development network:

- Containers: `INTERNAL://kafka:29092`.
- WSL host tools: `EXTERNAL://localhost:9092` by default (the configured host port is advertised).
- KRaft controller: `CONTROLLER://kafka:9093`, internal only and not published.

The `kafka-init` one-shot service waits for Kafka health, creates `score.submitted` with `--create --if-not-exists`, 2 partitions, and replication factor 1, then describes and validates the existing topic. If an existing topic has a different partition count or replication factor, initialization exits with an error; it does not delete or reset the topic. API and Worker wait for successful initialization.

## PostgreSQL schema initialization

`db/schema.sql` is mounted read-only as `/docker-entrypoint-initdb.d/001-schema.sql`. It creates `users`, `scores`, and their indexes with `IF NOT EXISTS`; its foreign key references `users`, which the same script creates first. The official PostgreSQL image runs initialization scripts only when initializing an empty data directory. Changing the schema file does not rerun it against an existing database volume. Apply later schema changes through a deliberate migration process; do not remove the data volume to force initialization.

## Persistence and safe shutdown

PostgreSQL, Redis (AOF enabled), and Kafka data use project-scoped named volumes: `leaderboard-dev_postgres_data`, `leaderboard-dev_redis_data`, and `leaderboard-dev_kafka_data`. They remain after containers stop or are removed.

Stop the stack while retaining data:

```sh
docker compose stop
```

Or remove only this Compose project’s containers and network while keeping its named volumes:

```sh
docker compose down
```

Do not add `-v` when shutting down if you want to preserve database, Redis, and Kafka data.

## Troubleshooting

- **Docker permission/socket error:** confirm Docker Desktop is running and WSL integration is enabled for this distribution; check `docker version` and `docker info`.
- **Port already allocated:** change the corresponding `*_HOST_PORT` in `.env`, then run `docker compose config` to review the effective mappings. Kafka's external advertised port follows `KAFKA_HOST_PORT`.
- **App waits or exits during startup:** inspect `docker compose ps` and `docker compose logs postgres redis kafka kafka-init api query-service worker`. API requires PostgreSQL and Kafka/topic initialization; Query requires PostgreSQL and Redis; Worker requires Redis and Kafka/topic initialization.
- **Kafka topic initialization fails:** inspect the `kafka-init` logs. An existing incompatible topic must be reconciled deliberately; initialization will not alter its partition count or replication factor.
- **Schema change not reflected:** PostgreSQL initialization scripts only run for a new empty database volume. Use a migration or apply an intentional SQL change to the existing database; never reset the volume as a troubleshooting shortcut.

## Kubernetes deployment on Kind

The Kustomize manifests under `k8s/` target the existing Kind cluster and namespace `leaderboard`. They use the already-loaded local images `leaderboard-api:dev`, `leaderboard-query:dev`, and `leaderboard-worker:dev` with `IfNotPresent`; they do not build or load images. They do not use or mount any Docker Compose volume. The Compose stack remains independent.

### Prerequisites and cluster selection

- Kind and kubectl installed.
- Existing Kind context `kind-leaderboard` is healthy and selected explicitly in each command.
- Docker images listed above are already loaded into that Kind cluster.
- `.env` exists with local development credentials. To create it for the first time, run `cp .env.example .env` and replace the credential placeholders before continuing.

Confirm the intended cluster and its node before deployment:

```sh
kubectl config get-contexts kind-leaderboard
kubectl --context kind-leaderboard get nodes -o wide
kubectl --context kind-leaderboard get storageclass
```

The manifests request `standard` StorageClass (Kind's `rancher.io/local-path` provisioner). Each infrastructure StatefulSet has a separate ReadWriteOnce volume claim template for PostgreSQL (5 Gi), Redis (2 Gi), and Kafka (8 Gi). Kind local-path volumes are suitable for single-node development; they are not replicated or production HA storage. Kubernetes data is independent of the Compose named volumes.

### Secret creation

The Kubernetes Secret is created from the ignored `.env` file and is not stored in Kustomize. Run this after the namespace exists. It filters only the five required keys into a mode-600 temporary file, pipes generated Secret YAML directly to `kubectl apply`, and removes the temporary file on exit. Secret values are not printed:

```sh
set -eu
secret_env="$(mktemp)"
chmod 600 "$secret_env"
trap 'rm -f "$secret_env"' EXIT
grep -E '^(POSTGRES_USER|POSTGRES_PASSWORD|POSTGRES_DB|REDIS_PASSWORD|JWT_SECRET)=' .env > "$secret_env"
test "$(wc -l < "$secret_env")" -eq 5
kubectl create secret generic leaderboard-secrets \
  --context kind-leaderboard \
  --namespace leaderboard \
  --from-env-file="$secret_env" \
  --dry-run=client -o yaml \
  | kubectl apply --context kind-leaderboard -f -
```

The Secret contains only `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `REDIS_PASSWORD`, and `JWT_SECRET`. Do not print Secret YAML or commit a Secret manifest.

### Staged deployment

Kubernetes has no Compose-style `depends_on`; use these stages and wait for each before continuing. First create only the namespace, then the Secret, then the infrastructure:

```sh
kubectl apply --context kind-leaderboard -f k8s/base/infra/namespace.yaml
# Create leaderboard-secrets using the command above.
kubectl apply --context kind-leaderboard -k k8s/overlays/kind/infra
kubectl --context kind-leaderboard -n leaderboard rollout status statefulset/postgres --timeout=5m
kubectl --context kind-leaderboard -n leaderboard rollout status statefulset/redis --timeout=5m
kubectl --context kind-leaderboard -n leaderboard rollout status statefulset/kafka --timeout=8m
```

Then initialize/verify the Kafka topic, deploy Query Service and wait for its gRPC health probe, and finally deploy API and Worker:

```sh
kubectl apply --context kind-leaderboard -k k8s/overlays/kind/topic
kubectl --context kind-leaderboard -n leaderboard wait --for=condition=complete job/kafka-topic-init --timeout=5m
kubectl --context kind-leaderboard -n leaderboard logs job/kafka-topic-init
kubectl --context kind-leaderboard -n leaderboard exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic score.submitted

kubectl apply --context kind-leaderboard -k k8s/overlays/kind/query
kubectl --context kind-leaderboard -n leaderboard rollout status deployment/query-service --timeout=5m

kubectl apply --context kind-leaderboard -k k8s/overlays/kind/apps
kubectl --context kind-leaderboard -n leaderboard rollout status deployment/api --timeout=5m
kubectl --context kind-leaderboard -n leaderboard rollout status deployment/worker --timeout=5m
```

The root overlay renders all stages for inspection with `kubectl kustomize k8s/overlays/kind`; deployment should still follow the staged order above. API startup is not gated on Query Service availability because its gRPC connection is lazy and its PostgreSQL leaderboard fallback remains available. Query and Worker use narrowly scoped init containers to wait for their required stores/broker when their Pods start.

### Access and verification

In separate terminals, forward API and Query Service to ports that do not collide with Compose:

```sh
kubectl --context kind-leaderboard -n leaderboard port-forward service/api 18080:8080
kubectl --context kind-leaderboard -n leaderboard port-forward service/query-service 15053:50051
```

Check resources, probes, topic configuration, and consumer group:

```sh
kubectl --context kind-leaderboard get pods,deployments,statefulsets,services,jobs,pvc -n leaderboard -o wide
kubectl --context kind-leaderboard -n leaderboard exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic score.submitted
kubectl --context kind-leaderboard -n leaderboard exec kafka-0 -- /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:29092 --describe --group leaderboard-writer
grpcurl -plaintext -d '{"service":"leaderboard.v1.LeaderboardQuery"}' localhost:15053 grpc.health.v1.Health/Check
curl -fsS http://localhost:18080/health
```

Query Service uses the standard gRPC health service name `leaderboard.v1.LeaderboardQuery` in its Kubernetes startup, readiness, and liveness probes. PostgreSQL and Redis have authenticated/credential-aware probes; Kafka probes its listener socket, while the topic-init Job checks broker API availability and the topic configuration. Worker intentionally has no invented HTTP health endpoint.

PostgreSQL mounts the schema ConfigMap read-only at `/docker-entrypoint-initdb.d/001-schema.sql`. The ConfigMap content mirrors `db/schema.sql`; keep the two synchronized when schema changes. Official PostgreSQL initialization scripts run only when the data directory is empty. Apply schema changes through a migration or deliberate SQL operation; never delete the PVC to rerun initialization.

### Safe shutdown and troubleshooting

To pause workloads while retaining persistent claims, scale the application Deployments and infrastructure StatefulSets to zero; do not delete the namespace, PVCs, or PVs:

```sh
kubectl --context kind-leaderboard -n leaderboard scale deployment/api deployment/query-service deployment/worker --replicas=0
kubectl --context kind-leaderboard -n leaderboard scale statefulset/postgres statefulset/redis statefulset/kafka --replicas=0
```

To resume, scale infrastructure back to one and wait for readiness, verify or re-run the idempotent topic Job, then start Query Service followed by API and Worker:

```sh
kubectl --context kind-leaderboard -n leaderboard scale statefulset/postgres statefulset/redis statefulset/kafka --replicas=1
kubectl --context kind-leaderboard -n leaderboard rollout status statefulset/postgres --timeout=5m
kubectl --context kind-leaderboard -n leaderboard rollout status statefulset/redis --timeout=5m
kubectl --context kind-leaderboard -n leaderboard rollout status statefulset/kafka --timeout=8m
kubectl --context kind-leaderboard -n leaderboard exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic score.submitted
kubectl --context kind-leaderboard -n leaderboard scale deployment/query-service --replicas=1
kubectl --context kind-leaderboard -n leaderboard rollout status deployment/query-service --timeout=5m
kubectl --context kind-leaderboard -n leaderboard scale deployment/api deployment/worker --replicas=1
kubectl --context kind-leaderboard -n leaderboard rollout status deployment/api --timeout=5m
kubectl --context kind-leaderboard -n leaderboard rollout status deployment/worker --timeout=5m
```

The normal development cleanup path must preserve claims and database data.

- **Pods cannot pull a `:dev` image:** verify the named image is loaded into the existing cluster with `kind load docker-image ... --name leaderboard`; do not rebuild or change clusters without checking first.
- **PVC Pending:** inspect `kubectl --context kind-leaderboard -n leaderboard describe pvc` and `kubectl --context kind-leaderboard get storageclass`; do not remove/recreate claims.
- **PostgreSQL tables missing:** check PostgreSQL logs and whether the data directory was already initialized. The schema script is first-volume-only; use a migration for existing data.
- **Topic Job fails:** inspect its logs and Kafka readiness. The job is idempotent and fails on incompatible existing topic settings; it never deletes the topic.
- **Application waits in an init container:** inspect that init container's status/logs and the relevant infrastructure readiness probe before restarting anything.
- **Port-forward conflict:** retain the distinct host ports `18080` and `15053`; these do not overlap the Compose API/query ports.

The Kind stack is a development deployment: one replica each, plaintext Kafka inside the cluster, and local-path storage without HA or cross-node recovery guarantees.

### K3 runtime verification (2026-11-19)

Runtime checks completed against Kind context `kind-leaderboard` in namespace `leaderboard`. The single node was Ready; API, Query Service, and Worker Deployments were each 1/1 Available; PostgreSQL, Redis, and Kafka StatefulSets were each 1/1 Ready; the topic Job completed; all three PVCs remained Bound. The topic had 2 partitions and replication factor 1. The verification user was a disposable local test account (user ID 1); no credentials or token are recorded here.

The end-to-end flow returned HTTP 201 for registration and score submission, HTTP 200 for login, leaderboard, and rank, and gRPC health `SERVING`. A score of 812347 (score ID 1) was present in PostgreSQL and in Kafka partition 0 offset 0; the Worker committed offset 1 with lag 0, Redis held 812347, and leaderboard/rank both reported rank 1 with the expected display name. During Query Service scale-to-zero, its Service had no ready endpoints, API health remained HTTP 200, and two score requests of 812357 (score IDs 2 and 3) returned HTTP 201 and persisted/processed; PostgreSQL fallback returned HTTP 200 including that score. The rank route returned its existing HTTP 500 behavior while Query Service was absent. After restoring Query Service, the first rank request briefly returned HTTP 500 while the gRPC connection was reconnecting; a retry returned HTTP 200 with rank 1, and gRPC health returned `SERVING` without restarting API. This observed transient is expected to be considered when manually testing recovery.

Deleting only the API Pod caused its Deployment to create a Ready replacement; its health and leaderboard requests then returned HTTP 200. Deleting only the Worker Pod caused a Ready replacement to rejoin `leaderboard-writer`; the previously committed offset was preserved. A subsequent score submission returned HTTP 201 (score ID 4, score 812367), appeared in PostgreSQL and Kafka partition 0 offset 3, advanced the committed offset to 4 with lag 0, and set the Redis best score to 812367. The final leaderboard and rank responses returned HTTP 200 with the expected display name, score 812367, and rank 1. These checks demonstrate recovery for this single-node development run only; they do not establish high availability or production reliability.

Useful inspection commands (all Kubernetes commands explicitly select the Kind context):

```sh
kubectl --context kind-leaderboard get nodes
kubectl --context kind-leaderboard get pods -n leaderboard -o wide
kubectl --context kind-leaderboard get deployments,statefulsets,jobs -n leaderboard
kubectl --context kind-leaderboard get services,pvc -n leaderboard
kubectl --context kind-leaderboard logs deployment/api -n leaderboard
kubectl --context kind-leaderboard logs deployment/query-service -n leaderboard
kubectl --context kind-leaderboard logs deployment/worker -n leaderboard
kubectl --context kind-leaderboard exec -n leaderboard kafka-0 -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic score.submitted
kubectl --context kind-leaderboard exec -n leaderboard kafka-0 -- /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:29092 --describe --group leaderboard-writer
```

For a safe recovery check, delete only an application Pod selected by its Deployment label (for example, `kubectl --context kind-leaderboard delete pod -n leaderboard -l app.kubernetes.io/name=api --wait=true`) and wait for the Deployment to become Available. Do not delete StatefulSet Pods, PVCs, or PVs as part of ordinary application recovery checks. Query fallback covers leaderboard reads while Query Service is unavailable; user-rank requests depend on Query Service and return an error until its gRPC connection recovers.
