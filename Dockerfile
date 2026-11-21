# 第一阶段包含编译工具，Cargo.lock 固定所有依赖版本。
FROM rust:1.95-bookworm AS build
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY examples ./examples
RUN cargo build --release --locked --bin recall-relay --example mock_upstream

# 两个程序都使用 Rustls，运行镜像只需二进制文件和健康检查工具。
FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*
USER 10001:10001

FROM runtime AS mock-upstream
COPY --from=build /app/target/release/examples/mock_upstream /usr/local/bin/mock-upstream
ENV MOCK_BIND=0.0.0.0:8091
EXPOSE 8091
ENTRYPOINT ["/usr/local/bin/mock-upstream"]

FROM runtime AS relay
COPY --from=build /app/target/release/recall-relay /usr/local/bin/recall-relay
ENV RELAY_BIND=0.0.0.0:8000
EXPOSE 8000
ENTRYPOINT ["/usr/local/bin/recall-relay"]
