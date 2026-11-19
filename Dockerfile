# Build all services from the same dependency and source context, then select
# one small runtime target with `docker build --target <name> ...`.
FROM golang:1.25-bookworm AS build-base
WORKDIR /src

# Cache module downloads independently of application source changes.
COPY go.mod go.sum ./
RUN go mod download
COPY . .

FROM build-base AS api-build
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/api ./cmd/api

FROM build-base AS query-build
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/query ./cmd/query

FROM build-base AS worker-build
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/worker ./cmd/worker

# Distroless static includes CA certificates and the unprivileged `nonroot`
# account (65532:65532). No shell, package manager, source, or Go toolchain is
# present in these runtime targets.
FROM gcr.io/distroless/static-debian12:nonroot AS api
COPY --from=api-build --chown=65532:65532 /out/api /api
USER 65532:65532
ENTRYPOINT ["/api"]

FROM gcr.io/distroless/static-debian12:nonroot AS query
COPY --from=query-build --chown=65532:65532 /out/query /query
USER 65532:65532
ENTRYPOINT ["/query"]

FROM gcr.io/distroless/static-debian12:nonroot AS worker
COPY --from=worker-build --chown=65532:65532 /out/worker /worker
USER 65532:65532
ENTRYPOINT ["/worker"]
