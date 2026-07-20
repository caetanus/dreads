# syntax=docker/dockerfile:1

# ---- builder: LDC + dub, produces the release binary --------------------------
# Debian 12 so the binary's glibc/libstdc++/libgcc_s match the distroless/cc-debian12
# runtime EXACTLY (same debian12 provenance) — no symbol-version skew.
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
# The release build runs vendor/lua/build.sh + vendor/lz4/build.sh (download the
# pristine tarballs, verify sha256, build static libs) and links jemalloc +
# libsodium. Needs network during build (Docker build has it).
#
# distroless/cc-debian12 already ships glibc + libstdc++ + libgcc_s + ca-certs
# (all debian12, matching this builder), so we copy ONLY the two libs it lacks:
# jemalloc + sodium. lz4/lua are statically linked (vendored).
RUN dub build -b release --compiler=ldc2 \
    && strip bin/dreads \
    && mkdir /libs \
    && cp -L /usr/lib/x86_64-linux-gnu/libjemalloc.so.2 \
             /usr/lib/x86_64-linux-gnu/libsodium.so.* \
             /libs/

# A shell for the otherwise-shell-less distroless runtime: a static (musl) busybox
# — vendored INLINE at build time (not in the repo), sha256-pinned like lua/lz4.
# Being fully static it runs under distroless' glibc unchanged. We stage a /rootfs
# with busybox + every applet symlinked to the FINAL /bin/busybox path, and a
# /data owned by distroless' nonroot uid — because the runtime stage has no shell
# to mkdir/chown/ln itself.
ARG BUSYBOX_VERSION=1.35.0
ARG BUSYBOX_SHA256=6e123e7f3202a8c1e9b1f94d8941580a25135382b99e8d3e34fb858bba311348
RUN curl -fsSL -o /tmp/busybox \
      "https://busybox.net/downloads/binaries/${BUSYBOX_VERSION}-x86_64-linux-musl/busybox" \
    && echo "${BUSYBOX_SHA256}  /tmp/busybox" | sha256sum -c - \
    && chmod +x /tmp/busybox \
    && mkdir -p /rootfs/bin /rootfs/data \
    && cp /tmp/busybox /rootfs/bin/busybox \
    && for applet in $(/rootfs/bin/busybox --list); do \
           ln -sf /bin/busybox "/rootfs/bin/$applet"; \
       done \
    && chown -R 65532:65532 /rootfs/data

# ---- runtime: distroless/cc-debian12 (nonroot) + a static busybox shell --------
# distroless/cc gives the exact debian12 glibc/libstdc++/libgcc_s/ca-certs the
# binary needs, with no package manager and no attack surface; the vendored
# busybox adds `sh` + coreutils so a sysadmin can `docker exec -it … sh`. Both
# images (this and the alpine one) run the SAME docker-entrypoint.sh, so they
# share ONE redis/valkey-style config interface (file + DREADS_* env + args).
FROM gcr.io/distroless/cc-debian12:nonroot AS runtime
COPY --from=builder /libs/ /usr/lib/x86_64-linux-gnu/
COPY --from=builder /rootfs/bin/ /bin/
COPY --from=builder --chown=65532:65532 /rootfs/data /data
COPY --from=builder /src/bin/dreads /usr/local/bin/dreads
COPY docker/dreads.conf /etc/dreads/dreads.conf
COPY --chmod=0755 docker/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

WORKDIR /data
EXPOSE 6379
# redis/valkey-compatible config: mounted/COPYed file, DREADS_* env (k8s), or
# `--<directive> value` args — all via the entrypoint. See docker/dreads.conf.
ENTRYPOINT ["docker-entrypoint.sh"]
CMD []
