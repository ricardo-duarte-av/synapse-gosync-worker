FROM golang:1.25-alpine AS build
WORKDIR /src

# Download dependencies first so they cache independently of source changes.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# .dockerignore strips .git, so build info is passed in rather than derived.
ARG TAG=dev
ARG COMMIT=unknown
ARG BUILD_TIME=unknown

# CGO_ENABLED=0 for the distroless static base below: every dependency here is
# pure Go, pgx included, so there is nothing to link against.
#
# Unlike gopro-worker this needs no GOEXPERIMENT: that worker depends on
# mautrix's federation/pdu package for per-room-version redaction, while this
# one implements the client-facing serialisation itself.
RUN CGO_ENABLED=0 go build -trimpath \
      -ldflags="-s -w \
        -X main.tag=${TAG} \
        -X main.commit=${COMMIT} \
        -X main.buildTime=${BUILD_TIME}" \
      -o /out/gosync-worker ./cmd/gosync-worker

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/gosync-worker /gosync-worker
ENTRYPOINT ["/gosync-worker"]
CMD ["-config", "/data/gosync-worker.yaml"]
