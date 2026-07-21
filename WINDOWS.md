# dreads on Windows

The Windows build is a **standalone, single-node** dreads: the RESP server, every
data type, scripting, pub/sub and AOF — a fully static `dreads.exe` (no DLLs of
ours to ship). It exists for local development, testing and desktop use.

## No Raft on Windows

**There is no Raft consensus — and therefore no clustering or replication — on
Windows.** Raft is a Linux/macOS feature; a Windows node is always standalone
(node id 0). Run your replicated, highly-available deployment on Linux (or macOS);
use the Windows binary to develop and test against a real dreads on your desktop.

Why: the durability and I/O model Raft relies on (group-commit `fdatasync`, the
io_uring fast path, advisory port locking) is POSIX-shaped. The Windows build
keeps compiling by mapping the durability syscalls to their Win32 equivalents
(`_commit` for `fsync`/`fdatasync`, `_chsize_s` for `ftruncate`), but that is
**best-effort durability**, not the Raft-strict guarantee — so replication is not
offered here rather than offered with a weaker promise.

Consequences on Windows:

- **AOF is best-effort.** Persistence works, but the fine-grained durability
  contract that gates Raft acks is relaxed. Don't rely on it for a replicated
  source of truth.
- **No `SO_REUSEPORT` / no advisory port lock.** Windows has no `SO_REUSEPORT`
  (plain `SO_REUSEADDR` already permits rebind), and the one-live-instance-per-port
  lock is skipped. Don't co-bind two servers to the same port.
- **io_uring is Linux-only** and simply absent; fsync uses the blocking path.

## Build

```powershell
# needs LDC, MSVC (cl/lib), and libsodium (static). See .github/workflows/windows.yml
dub build --compiler=ldc2
```

The build is fully static: `-mscrtlib=libcmt` (static CRT), static libsodium
(vcpkg `x64-windows-static`), and the vendored Lua + lz4 compiled `/MT`.

## Run

```powershell
dreads.exe 6379
```

Then connect with any Redis client: `redis-cli -p 6379 ping`. The startup banner
uses ANSI colour + UTF-8 — dreads turns on the console's virtual-terminal
processing and UTF-8 codepage automatically (Windows 10+), the same way npm does.

CI publishes a versioned `dreads-<version>.exe` artifact on every run
(see the **Windows build** badge in the top-level README).
