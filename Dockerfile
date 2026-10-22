# The production image: the indexer binary with the `postgres` and `delta` stores, the
# per-chain settings files, and the protocol manifests. One image serves every chain;
# the settings file is the container's argument:
#
#   infisical run -- bash -c 'docker run --init --env-file <(env | grep -E "^(INDEXER_|S3_)") \
#     ghcr.io/lgingerich/indexer:prod-bsc indexer.bsc.toml'
#
# Secrets are never baked in: the settings files name environment variables, which the
# host passes at run time.

# The dependencies are built in a layer of their own with cargo-chef, so a commit that
# leaves `Cargo.lock` and the manifests alone reuses it from the registry build cache and
# compiles only this crate. Everything stays in layers rather than cache mounts: the
# build cache exports layers, and a mount starts empty on every CI runner.
#
# The toolchain file comes first, so every stage compiles with the pinned toolchain and
# the dependency layer matches the final build's compiler.
FROM rust:1.95-bookworm AS chef
WORKDIR /src
COPY rust-toolchain.toml .
RUN cargo install cargo-chef --locked --version 0.1.78

# The recipe is the manifests and lockfile with the sources stripped out, so it changes
# only when the dependencies do.
FROM chef AS plan
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS build
# `Cargo.toml` builds release incrementally, which pays off in a checkout that keeps
# `target/`. Here nothing keeps the incremental cache past the build, so it would only
# split the crate into more codegen units and optimize it less.
ENV CARGO_PROFILE_RELEASE_INCREMENTAL=false
COPY --from=plan /src/recipe.json recipe.json
RUN cargo chef cook --release --locked --bin indexer --features postgres,delta \
    --recipe-path recipe.json
COPY . .
RUN cargo build --release --locked --bin indexer --features postgres,delta \
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
# No transparent huge pages for the process (mimalloc reads this once, at start). mimalloc
# asks for them on its arenas, and once it frees part of one the kernel keeps charging
# the container for the whole 2 MiB page until it splits it: about 0.6 GB of Ethereum's
# 2 GB under its cap was such pages, with a few percent less CPU per commit their only
# gain.
ENV MIMALLOC_ALLOW_THP=0
ENTRYPOINT ["/app/indexer"]
