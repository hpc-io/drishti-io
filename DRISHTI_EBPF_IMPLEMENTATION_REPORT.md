# Drishti + eBPF Integration — Implementation Report

Datacrumbs `.pfw` trace support for `hpc-io/drishti-io`, with the anti-pattern
checks rebuilt to reason about **cost** rather than **counts**.

---

## 1. Deliverables

| File | Change | Size |
|---|---|---|
| `drishti/includes/parser_ebpf.py` | **new** | 336 lines |
| `drishti/handlers/handle_ebpf.py` | **new** | 516 lines |
| `drishti/includes/module.py` | modified (appended) | +2003 lines |
| `drishti/includes/parser.py` | modified (2 CLI flags) | +17 lines |
| `drishti/reporter.py` | modified (dispatch branch) | +9 lines |
| `drishti/handlers/handle_recorder.py` | **untouched** — verified empty `git diff` | — |
| `drishti/handlers/handle_darshan.py` | **untouched** | — |

All existing `check_*` functions in `module.py` are unmodified, so the Darshan
and Recorder paths behave exactly as before. Everything new is appended in
clearly marked sections.

### Usage

```bash
drishti trace.pfw \
    --fs-type lustre \          # lustre|gpfs|beegfs|nfs|local — gates collective advice
    --redundant-details \       # every redundant region, instead of aggregate stats
    --size 200 \                # console width; without it long findings are truncated
    --html --svg \              # styled report files (colour preserved as CSS classes)
    --export_dir ./reports
```

---

## 2. Parser (`parser_ebpf.py`)

Reads Chrome/Perfetto Trace Event Format (`ph:"X"` complete events, JSON Lines)
and emits the canonical frames the check pipeline consumes.

| Output | Columns |
|---|---|
| `df_intervals` | `file_id, rank, tid, api, function, start, end, duration, size, offset` |
| `df_posix_records` | `fname, rank, tid, function, start, end, duration` |
| `df_layers` | `rank, tid, cat, function, start, end, duration` |
| `file_map` | `{file_id: filename}` |

**Mechanics**

- **fd → filename** resolved by a stateful, time-ordered pass: `open`/`openat`
  registers `(pid, fd) → args.buffer`, `close` invalidates. Fds opened before
  capture began get an `UNRESOLVED_FD::pid<N>::fd<M>` placeholder — counted and
  reported, never silently dropped.
- **rank** = enumerated sorted unique `pid` → `0..N-1`.
- **Unit conversion.** `.pfw` `ts`/`dur` are microseconds; Drishti's thresholds
  assume seconds. Converted at parse time so no check needs to know the trace
  format.
- **`df_layers`** retains every non-syscall event (HDF5, VOL, MPI). These are the
  enclosing intervals used for layer attribution and self-time accounting. This
  is also where `MPI_Wait`/`Waitall`/`Test` live.
- **Robustness for fixed-size eBPF buffers** (see §5.2): binary read with
  per-line UTF-8 replacement, `json.loads(strict=False)` for control bytes,
  `clean_path()` truncation at the first NUL, JSON-array wrapping tolerated, and
  parse failures counted and reported rather than swallowed.

---

## 3. Checks implemented

21 insight codes across 14 check functions. Codes use an `E` prefix so
`config.py` stays untouched.

### 3.1 Reworked from existing Darshan-era checks

| Code | Check | What changed |
|---|---|---|
| `E01`/`E02`/`E03` | `check_small_operation_cost` | Cost not count; cache/deferred classification; layer attribution; trace-derived "small" |
| `E04`–`E07` | `check_mpi_collective_operation_cost` | Four gates replace the unconditional "use collectives" |
| `E12`/`E12b` | `check_misaligned_cost` | Latency-measured cost replaces offset counting |
| `E13`/`E13b` | `check_nonblocking_overlap` | Achieved overlap replaces "was a non-blocking call used" |
| `E14` | `check_mpi_aggregators` | Aggregators identified from the trace; check was never called before |
| `E15`/`E17` | `check_traffic_redundancy` | Exact interval-union redundancy replaces an inverted heuristic |
| `E16`/`E16b` | `check_metadata_categories` | Five categories + contention detection replace a summed-time count |

### 3.2 New — no counter-based equivalent exists

| Code | Check | Measures |
|---|---|---|
| `E08`/`E08b` | `check_io_compute_serialization` | Storage idle while compute runs; recoverable overlap |
| `E09` | `check_file_reopen_churn` | Same file reopened per iteration |
| `E10` | `check_sync_overkill` | Durability paid per record |
| `E11` | `check_software_stack_overhead` | Self time inside library code vs syscalls |

### 3.3 Method notes worth keeping

**Collectives (E04–E07).** Gates in order of decisiveness:

1. **Filesystem** — `FILESYSTEM_COLLECTIVE_POLICY` + `--fs-type`. NFS/local →
   "not worth converting" plus `romio_cb_read = disable`; the rest is skipped.
2. **Pattern** — `interleave_ratio()`: fraction of **offset-adjacent** request
   pairs owned by different ranks. This is the metric the existing
   random/sequential classifier cannot express, because it sorts merged-by-time
   and labels both "one rank jumping around" and "each rank sequential but
   globally strided" as random. The second is the canonical collective win.
3. **Data volume** — total bytes vs `collective_min_bytes`.
4. **Sync cost** — **arrival skew** (per-rank non-I/O time = compute) is charged;
   **service skew** (slow I/O) is not, because aggregation is meant to remove it.
   Also rejects when arrival skew dominates the phase (≥50%) or relative gain <10%.

Benefit is modeled pessimistically — a *single* aggregator at the peak per-stream
throughput measured elsewhere in the same trace. Real collective I/O uses several
aggregators and is faster, so when the pessimistic model still wins the case is
solid. The issue text says so.

**Misalignment (E12).** Within each log₂ size bucket, compares median latency of
aligned vs misaligned requests. That difference *is* the measured cost — the
trace-only proxy for page-cache tracing. Reports "misaligned but not measurably
slower, most absorbed by the page cache" when there is no penalty, and says so
explicitly when no bucket has enough of both to compare. Also counts requests
crossing one more boundary than their size requires.

**Non-blocking (E13).** Pairs issues to completions FIFO per thread (`waitall`
drains all outstanding; `_begin`/`_end` pair by name), then
`effectiveness = overlap / (overlap + wait)`. Flags the poll-only-progress
possibility, since no wait placement helps there.

**Redundancy (E15/E17).** `byte_union()` gives distinct bytes; `redundancy =
total / covered`. Decomposed into intra-rank (application caching problem) and
cross-rank (read-once + `MPI_Bcast` opportunity), because the fix differs.
`redundant_regions()` sweeps sorted boundaries accumulating touch depth and merges
neighbouring equal-depth ranges — one pass, ranked by *wasted* bytes
(`span × (times−1)`), so a small region touched 50× outranks a large one touched
twice. Write side gives the overwrite factor.

**Metadata (E16).** Categories: `namespace` (incl. `stat`/`fstat`/`access`),
`sync`, `lock`, `seek`, `close` — data transfers excluded. Burst detection via
`merge_intervals` with a gap. Contention detection compares median latency **per
depth level**, lowest vs highest.

**Stack overhead (E11).** `compute_self_time()` walks layers and syscalls merged
per thread, sorted parents-first, charging each call against its parent. Verified
against hand-computed ground truth on a 3-deep nest: self + syscalls = inclusive
exactly. Reports **exclusive** time because Datacrumbs instruments HDF5 internals,
not just the API boundary — inclusive time would charge callers for callees. All
totals labelled `thread-seconds cumulative across N threads` with wall-clock span
alongside.

### 3.4 Thresholds added (23)

All registered into the shared `thresholds` dict so `init_console()` resets their
triggered flags. They do not appear in the thresholds panel, because
`display_thresholds()` uses a hardcoded key list.

```
small_time_fraction 0.2      cache_latency_cutoff 100µs   knee_throughput_fraction 0.5
knee_min_bucket_ops 5        collective_interleave 0.3    collective_min_bytes 16MB
collective_small_fraction .5 collective_min_gain 0.1      collective_sync_dominance 0.5
serialization_idle_fraction  serialization_min_io 0.1     serialization_burst_gap 10ms
  0.5
reopen_count 10              sync_min_bytes 1MB           sync_time_fraction 0.1
stack_overhead_fraction 0.5  alignment_boundary 4KB       misalign_time_fraction 0.05
overlap_effective 0.5        nonblocking_min_ops 5        redundancy_factor 1.5
metadata_burst_gap 10ms      metadata_contention_ratio 3.0
```

---