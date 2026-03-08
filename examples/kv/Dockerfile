# examples/kv/Dockerfile
FROM crystallang/crystal:1.15.1-alpine AS builder
WORKDIR /app
COPY . .
RUN crystal build examples/kv/src/main.cr -o kv_node -Dpreview_mt -Dexecution_context --release --static

FROM alpine:3.19
COPY --from=builder /app/kv_node /usr/local/bin/kv_node
RUN mkdir -p /data/raft
CMD ["/usr/local/bin/kv_node"]
