# Masstree server (mtd) container
# Based on https://github.com/kohler/masstree-beta

FROM debian:bookworm-slim AS builder

# Pin to specific commit for reproducibility (2023-10-12)
# https://github.com/kohler/masstree-beta/commit/11198427a1170654ca646dd20d96c8f349bca2bd
ARG MASSTREE_COMMIT=11198427a1170654ca646dd20d96c8f349bca2bd

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    autoconf \
    automake \
    libtool \
    git \
    ca-certificates \
    libjemalloc-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

RUN git clone https://github.com/kohler/masstree-beta.git . \
    && git checkout ${MASSTREE_COMMIT} \
    && git rev-parse HEAD | grep -q "^${MASSTREE_COMMIT}" \
    || (echo "Commit verification failed" && exit 1)

RUN ./bootstrap.sh \
    && ./configure --disable-assertions --with-malloc=jemalloc \
    && make -j$(nproc)

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libjemalloc2 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /build/mtd /app/mtd

EXPOSE 2117

# Run mtd without logging (-n) for testing
# Default port is 2117
CMD ["/app/mtd", "-n"]
