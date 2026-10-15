# ─── Stage 1: Build ───────────────────────────────────────────────────────────
# Use a full Go image so the race detector (CGO) is available if needed.
FROM golang:1.23-alpine AS builder

WORKDIR /app

# Cache module downloads before copying source.
COPY go.mod ./
RUN go mod download

# Copy the rest of the source.
COPY . .

# Build a statically linked binary for each command.
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" \
      -o /bin/server      ./cmd/server && \
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" \
      -o /bin/mockserver  ./cmd/mockserver

# ─── Stage 2: Runtime ─────────────────────────────────────────────────────────
# Minimal scratch image — no shell, no libc, just the binary.
FROM scratch

# Copy CA certificates so TLS works if needed in future.
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Copy both binaries from the builder stage.
COPY --from=builder /bin/server     /server
COPY --from=builder /bin/mockserver /mockserver

# Default environment (overridable at runtime).
ENV LISTEN_ADDR=:8080
ENV DOWNSTREAM_ADDR=:9090
ENV POOL_SIZE=5

EXPOSE 8080

ENTRYPOINT ["/server"]
