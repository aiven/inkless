FROM docker.io/rust:1.73 as builder
WORKDIR /app
RUN apt-get update && apt-get install -y cmake
COPY src /app/src
COPY Cargo.toml /app/Cargo.toml
COPY Cargo.lock /app/Cargo.lock
RUN cargo install --locked --path .

FROM docker.io/debian:bookworm-slim
COPY --from=builder /usr/local/cargo/bin/workload /app/workload
COPY --from=builder /usr/local/cargo/bin/validation /app/validation
COPY --from=builder /usr/local/cargo/bin/load /app/load
COPY scripts/docker-entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh
RUN mkdir -p /app/logs
WORKDIR /app
CMD ["/app/entrypoint.sh"]