# examples/queue/Dockerfile
FROM 84codes/crystal:1.19.0-alpine AS builder
WORKDIR /app
COPY . .
RUN crystal build examples/queue/src/main.cr -o queue_node -Dpreview_mt -Dexecution_context -Draft_debug --release --static

FROM alpine:3.19
COPY --from=builder /app/queue_node /usr/local/bin/queue_node
RUN mkdir -p /data/raft
CMD ["/usr/local/bin/queue_node"]
