# syntax=docker/dockerfile:1

# ---- builder: LDC + dub, produces the release binary --------------------------
# Debian 12 so the binary's glibc matches the distroless/*-debian12 runtime.
FROM debian:12-slim AS builder
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates curl xz-utils git build-essential \
        libjemalloc-dev libsodium-dev \
    && rm -rf /var/lib/apt/lists/*

# LDC pinned to the dev toolchain (bumpable). install.sh drops it under ~/dlang.
ARG LDC_VERSION=1.42.0
RUN curl -fsS https://dlang.org/install.sh | bash -s "ldc-${LDC_VERSION}"
ENV PATH=/root/dlang/ldc-${LDC_VERSION}/bin:${PATH}

WORKDIR /src
COPY . .
# The release build runs vendor/lua/build.sh (downloads the pristine Lua 5.4.8
# tarball, applies our read-only patch, builds a static liblua.a) and links
# jemalloc + libsodium. Needs network during build (Docker build has it).
RUN dub build -b release --compiler=ldc2 \
    && strip bin/dreads \
    && mkdir /libs \
    && cp -L /usr/lib/x86_64-linux-gnu/libjemalloc.so.2 /usr/lib/x86_64-linux-gnu/libsodium.so.* /libs/

# ---- runtime: distroless (glibc + libstdc++ + libgcc, no shell/apt) -----------
# ~20 MB base; we add only the two libs distroless lacks (jemalloc, sodium).
FROM gcr.io/distroless/cc-debian12:nonroot AS runtime
COPY --from=builder /libs/ /usr/lib/x86_64-linux-gnu/
COPY --from=builder /src/bin/dreads /usr/local/bin/dreads

# Redis-compatible default port (drop-in). Mount a volume at /data for AOF.
WORKDIR /data
EXPOSE 6379
# `dreads [conf-file] [port] [--appendonly[=path]] [--lockfile=path]`
ENTRYPOINT ["dreads"]
CMD ["6379"]
