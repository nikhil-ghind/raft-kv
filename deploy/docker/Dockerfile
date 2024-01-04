# Build stage
FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /raft-kv-server ./cmd/server
RUN CGO_ENABLED=0 go build -o /raft-kv-client ./cmd/client

# Runtime stage
FROM alpine:3.19

RUN apk add --no-cache ca-certificates

COPY --from=builder /raft-kv-server /usr/local/bin/raft-kv-server
COPY --from=builder /raft-kv-client /usr/local/bin/raft-kv-client

EXPOSE 50051

ENTRYPOINT ["raft-kv-server"]
