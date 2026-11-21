# 第一阶段包含编译工具，Cargo.lock 固定所有依赖版本。
FROM rust:1.95-bookworm AS build
# llama.cpp 随 Rust 包从源代码构建；各架构使用镜像内的本机工具链。
RUN apt-get update && apt-get install -y --no-install-recommends \
        clang cmake g++ git libclang-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY examples ./examples
RUN cargo build --release --locked --bin recall-relay --example mock_upstream

# 原生 CPU 推理依赖 C++ 标准库。libgomp 支持使用 OpenMP 的 Linux 构建。
FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates libstdc++6 libgomp1 \
    && rm -rf /var/lib/apt/lists/*
USER 10001:10001

FROM runtime AS model-init
COPY deploy/compose/prepare-model.sh /usr/local/bin/prepare-model.sh
COPY src/embeddinggemma.sha256 /usr/local/share/recall-relay/embeddinggemma.sha256
ENTRYPOINT ["/bin/sh", "/usr/local/bin/prepare-model.sh"]

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
