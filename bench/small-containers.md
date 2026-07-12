# Small containers benchmark — SmallSet vs old Dict-only set

Methodology: single server pinned to core 2, redis-benchmark on core 3, LDC
release + jemalloc. `OLD` = master (set is always `Dict!Unit`); `NEW` = the
SmallSet (contiguous linear-scan array below the Redis set-max-listpack/intset
thresholds, spilling one-way to a Dict). Memory is the server process VmRSS
delta after loading, measured from `/proc/<pid>/status`.

## Memory — 100 000 sets × 5 members each (the many-tiny-sets case)

| members | OLD (hashtable) | NEW | reduction |
|---|---|---|---|
| 5 short strings | 104.8 MB | **56.8 MB** (listpack) | **−46% (1.85×)** |
| 5 integers      | 104.8 MB | **56.8 MB** (intset)   | **−46% (1.85×)** |

The headline: roughly **half the memory** for small sets. OLD pays a full open-
addressing hash table + per-entry overhead per set; NEW is one contiguous array
of member slices — one allocation, no per-element nodes.

## Throughput

| op | OLD | NEW small | NEW big |
|---|---|---|---|
| insertion (load 100k×5) | 0.13 s | **0.11 s** | — |
| SMEMBERS (iterate 100-member set) | 157k rps | **174k rps** (+11%) | — |
| SISMEMBER (lookup, 100-member set) | **1.65M rps** | 1.36M rps (−18%) | — |
| SISMEMBER (lookup, 2000-member set) | 1.63M rps | — | 1.65M rps (=) |

## Reading the result (honest tradeoff)

This is the **Redis listpack tradeoff**, and NEW behaves exactly like it:

- **Memory**: big win (~half) for small collections — the whole point.
- **Insertion / iteration**: NEW is faster — a contiguous array is cheaper to
  append to and to walk than a hash table (cache-friendly, prefetchable).
- **Point lookup (SISMEMBER) on a small set**: NEW is ~18% slower — a linear
  scan of up to 128 members loses to an O(1) hash probe at that size, even
  cache-resident. This is the price of the memory win, and matches Redis.
- **Large sets**: identical (both are a Dict/hashtable) — zero regression once
  spilled.

The lookup cost is bounded by the small threshold (128 entries). Lowering it
trades a little memory back for faster small-set lookups; the current values
mirror Redis defaults (`set-max-listpack-entries` 128, `set-max-intset-entries`
512). The array is sized to stay in L1 so the scan never leaves cache.
