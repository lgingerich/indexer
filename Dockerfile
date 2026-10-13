# The production image: the indexer binary with the `postgres` and `delta` stores, the
# per-chain settings files, and the protocol manifests. One image serves every chain;
# the settings file is the container's argument:
#
#   infisical run -- bash -c 'docker run --init --env-file <(env | grep -E "^(INDEXER_|S3_)") \
#     ghcr.io/lgingerich/indexer:prod-bsc indexer.bsc.toml'
#
# Secrets are never baked in: the settings files name environment variables, which the
# host passes at run time.

FROM rust:1.95-bookworm AS build
WORKDIR /src
COPY . .
# The cache mounts only speed up local rebuilds; the binary is copied out of the cached
# target directory in the same step, since a mount does not outlive its step.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked --bin indexer --features postgres,delta \
    && cp target/release/indexer /usr/local/bin/indexer

FROM debian:bookworm-slim
# native-tls links the platform's OpenSSL; the CA bundle verifies RPC, R2, and Postgres.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/*
RUN useradd --system --no-create-home indexer
WORKDIR /app
COPY --from=build /usr/local/bin/indexer /app/indexer
COPY indexer.*.toml /app/
COPY protocols /app/protocols
USER indexer
# The Prometheus endpoint each settings file's `[metrics]` names.
EXPOSE 9100
ENV RUST_LOG=warn,indexer=info
ENTRYPOINT ["/app/indexer"]
