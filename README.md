# Shopping Sessions — native C++ pipeline

A Databricks Shopping Sessions Buildathon submission. It turns the messy `events/`, `clicks/` and
`links/` CSVs into `user_daily` and `campaign_daily` Parquet tables, following `rules.md` exactly.

The submission is one static C++17 binary (`src/ss.cpp`) in a `FROM scratch` image (~1.6 MB). There
are no runtime dependencies and no network access. It reads `$DATA_DIR`, writes `$OUTPUT_DIR`, and
uses `NCPU` threads. It never writes to `$SCRATCH_DIR`, because everything stays in memory.

## Correctness

| Check | Result |
| --- | --- |
| 36 official fixtures, image run with the scorer's flags (`tools/fixtures_docker.py`) | 36/36 |
| 36 fixtures, native binary (`make fixtures-native`) | 36/36 |
| Official 1M dev set (`--seed 1`) vs `dev-answer/` (`starter-duckdb/scripts/compare.py`) | MATCH (117,038 + 27,400 rows) |
| Fresh seeds 7, 42, 2027 (3M events each): native vs independent DuckDB reference | MATCH |
| 20M events (seed 20): native vs DuckDB reference | MATCH (2,247,046 + 28,014 rows) |
| Robustness variants: IDs > 15 bytes (with shared 15-byte prefixes), no `links/`, files without a final newline, 0-byte files | MATCH vs DuckDB |
| Race stress: 11 runs at 3–64 threads | all MATCH |
| Timestamp parser fuzz vs a Python implementation of the rules (`make fuzz-ts`) | 300,000 cases, 0 mismatches |

`reference/duckdb_pipeline.py` is a separate, deliberately simple DuckDB implementation of the rules.
It also matches the dev answer, and it is the oracle for seeds that have no published answer.
`reference/ss_v1.cpp` and `reference/ss_v2.cpp` are earlier, simpler native versions, kept for fast
differential testing.

## Architecture

```
manifest, products, campaigns           tiny; scalar parse
links/*.csv   -- parallel parse --> key pairs --> global ID table (lock-free) --> parallel union-find
events/*.csv  -- parallel AVX2 parse --> event copies, partitioned by hash(event_id)
clicks/*.csv  -- parallel AVX2 parse --> valid clicks, partitioned by person
per event partition:  dedup (smallest ingest_ts wins, before validation) --> survivors by person
per person partition: counting sort -> sort by ts -> sessions -> user_daily rows
                      sliding 7-day click window -> credit + assists -> campaign_daily counters
Parquet writer (hand-written, uncompressed PLAIN): one user_daily file per thread + campaign_daily
```

Key techniques:

- **CSV scanning:** each 64-byte block produces AVX2 bitmasks for `\n`, `,` and `"`. Fields are cut at
  the bits that are set, and a popcount of quotes decides whether each field is valid. A field is
  valid if it holds no quote, or is exactly `"…"`. Wrong field counts, stray quotes and empty lines
  are dropped as `rules.md` says. Input is `mmap`ed and split into 2 MB chunks aligned to newlines,
  and each file's header line is skipped. `madvise(WILLNEED)` reads ahead the current chunk and the
  one a round later, which helps when the page cache is cold.
- **Compact 8-byte event/click IDs:** IDs of up to 9 characters from `[-0-9A-Z_a-z]` are packed 7 bits per
  character in byte order (exact; integer order = string order). Anything else gets an exact id from a
  side table. Event copies shrink to 28 bytes and clicks to 20.
- **Exact 16-byte keys:** an ID of up to 15 bytes is packed big-endian with its length in the last
  byte. Keys are then exact, and comparing them as integers gives byte-wise string order, which the
  names and the `click_id` tie-break need. A longer ID gets a 120-bit hash plus a side table of
  strings for ordering and output.
- **Global ID table:** user IDs from links, events and clicks are inserted lock-free (16-byte CAS
  plus atomic 16-byte loads). A linked ID's person id is its union-find root slot; an unlinked ID's
  person id is its own slot, so person ids are dense integers. Union-find is lock-free and parallel:
  a root is linked under a smaller root by CAS, so one forward pass finds all roots. The person name
  is an atomic minimum over each component's keys. The table is sized from the link count. If an
  estimate is ever badly wrong, the process re-execs itself with a bigger table: slower, still
  correct.
- **Timestamps:** the F1/F2 forms are validated with SWAR (all digit and separator checks in a few
  64-bit operations); validated dates are cached per thread, and F3 digits are converted 8 at a time. F3 accepts any
  number of leading zeros, and values too big for int64 fall outside the window.
- **Memory:** a huge-page arena feeds per-thread slabs and append-only block streams, so buffers are
  never reallocated. Consumed event blocks are reused for the survivor streams. Lookups into the ID
  and product tables are software-prefetched 16 lines ahead.
- **Dedup:** each event partition has about 20k events, so its open-addressing table fits in L2. The
  table holds the current winner's payload: records are read once, and the output is a sequential
  scan of the table.
- **Sessions and attribution:** survivors are counting-sorted by dense person id, and big persons
  (whales) use a radix sort. Credit is the newest click in `[start − 7d, start]`, with ties broken by
  the smallest `click_id`. Assists are counted as campaign *activation intervals* over the sorted
  sessions, which costs O(clicks) even for the huge shared-device people.

## Performance (measured locally)

These numbers come from an i7-12700H laptop running Docker Desktop (WSL2), with `--cpus 8`. That
machine also had an Android emulator running in the background, so the numbers are noisy. The
scorer's Xeon will differ; only the leaderboard's times count.

| Dataset | In-process (min / median) | Container wall, scorer flags |
| --- | --- | --- |
| 1M events (dev) | ~0.1 s | 0.62–0.68 s (≈ Docker Desktop overhead, ~0.6 s for an empty run) |
| 20M events / 5M clicks | 0.82 / 0.88 s | 1.43–1.63 s |
| 40M events / 10M clicks | 2.41 / 2.83 s | 3.3–4.0 s |

At 20M, the first correct native version (v1) ran in ~2.6–3.0 s in-process, and the DuckDB reference
in ~10 s at **1M**. With a cold page cache, the 20M run adds ~0.8–1.0 s of I/O on this machine.

Peak memory at 20M is ~3.3 GB RSS (half of it the mapped input). At 40M it is ~6.9 GB. Expect about
3× the 20M figure at 60M, well inside the 24 GB limit.

## Layout

| Path | What |
| --- | --- |
| `Dockerfile` | Two-stage build: `gcc:14` compiles a static binary, `scratch` runs it as uid 1000 |
| `src/ss.cpp` | The pipeline |
| `reference/duckdb_pipeline.py` | Independent DuckDB implementation (correctness oracle) |
| `reference/ss_v1.cpp`, `ss_v2.cpp` | Earlier native versions for differential tests |
| `tools/` | Scorer-style runners, cross-checks, A/B timing, fuzzing, variant generator, dev images |
| `starter-duckdb/` | The official starter: fixtures, `run_fixtures.py`, `compare.py`, `docker_run.sh` |
| `generator/` | The official dataset generator (Rust) |
| `rules.md` | The official rules |

## Reproduce

```sh
make build              # docker image (linux/amd64)
make fixtures           # 36 fixtures through the image with the scorer's flags (Linux/macOS host)
make dev run-dev        # needs `generate` on PATH: cd generator && cargo build --release
make check-dev          # needs dev-answer/ (dev-answer.zip unzipped at the repo root)
make fixtures-native    # native build + fixtures, no Docker
make fuzz-ts            # timestamp parser fuzz
```

On Windows, `tools/fixtures_docker.py` runs the fixtures through the image with the same flags, and
`tools/scorer_run.sh` times a dataset held in a Docker volume.
