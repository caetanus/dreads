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
# jemalloc + sodium. lz4/lua are statically linked (vendored). The libs go to an
# arch-independent dir (on LD_LIBRARY_PATH in the runtime) so ONE Dockerfile
# builds both amd64 and arm64 — `gcc -dumpmachine` yields the right multiarch
# triplet (x86_64-linux-gnu / aarch64-linux-gnu) on whichever arch buildx targets.
RUN dub build -b release --compiler=ldc2 \
    && strip bin/dreads \
    && LIBDIR="/usr/lib/$(gcc -dumpmachine)" \
    && mkdir -p /dist/lib \
    && cp -L "$LIBDIR"/libjemalloc.so.2 "$LIBDIR"/libsodium.so.* /dist/lib/

# A shell for the otherwise-shell-less distroless runtime: a static (musl) busybox.
# COPY --from=busybox:musl is multi-arch-native — buildx pulls the busybox matching
# the target platform (x86_64 or aarch64), fully static so it runs under distroless'
# glibc unchanged. Stage /rootfs here (the runtime has no shell to ln/chown itself):
# busybox + every applet symlinked to the final /bin/busybox, and /data owned by
# distroless' nonroot uid.
COPY --from=busybox:musl /bin/busybox /rootfs/bin/busybox
RUN mkdir -p /rootfs/data \
    && for applet in $(/rootfs/bin/busybox --list); do \
           ln -sf /bin/busybox "/rootfs/bin/$applet"; \
       done \
    && chown -R 65532:65532 /rootfs/data

# ---- runtime: distroless/cc-debian12 (nonroot) + a static busybox shell --------
# distroless/cc gives the exact debian12 glibc/libstdc++/libgcc_s/ca-certs the
# binary needs (for the matching arch), with no package manager and no attack
# surface; the busybox adds `sh` + coreutils so a sysadmin can `docker exec -it … sh`.
# Both images (this and the alpine one) run the SAME docker-entrypoint.sh, so they
# share ONE redis/valkey-style config interface (file + DREADS_* env + args).
# Multi-arch: amd64 + arm64 (AWS Graviton) from this one Dockerfile.
FROM gcr.io/distroless/cc-debian12:nonroot AS runtime
ENV LD_LIBRARY_PATH=/opt/dreads/lib
COPY --from=builder /dist/lib /opt/dreads/lib
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
