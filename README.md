# S3 / MinIO Bulk Downloader

A small, resource-aware tool that **concurrently downloads every object under one
or more S3 prefixes** and mirrors the remote directory layout onto your local
machine. Works with **AWS S3** and **self-hosted MinIO**.

It is built to be safe on a laptop: the worker pool is sized from your available
CPU and RAM (so it won't hang or OOM), downloads are **resumable**, and an
interrupt or crash never leaves a corrupted file behind. Objects sitting in
**Glacier / Deep Archive** are detected and can be restored with a single flag.

---

## Setup

Requires [`uv`](https://docs.astral.sh/uv/).

```bash
uv sync                 # install dependencies into .venv
cp .env.example .env    # then fill in your credentials + bucket
```

### Configure `.env`

```ini
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_REGION=ap-southeast-1
S3_BUCKET=your-bucket-name

# Local directory to download into (structure is preserved under it)
LOCAL_DIR=./downloads

# MinIO only — leave blank for AWS S3.
# e.g. http://localhost:9000
S3_ENDPOINT_URL=
```

For **MinIO**, set `S3_ENDPOINT_URL` to your server URL; the tool automatically
switches to path-style addressing. For **AWS S3**, leave it blank.

---

## Usage

```bash
# one or more prefixes inline
uv run download.py uploads/reports/2026/01 uploads/reports/2026/02

# or read prefixes from a file (one per line)
uv run download.py --prefixes-file prefixes.txt

# override bucket / destination / worker count
uv run download.py uploads/reports/2026/01 \
    --bucket my-bucket --dest ./downloads --workers 8

# force re-download even if a local copy already exists
uv run download.py --prefixes-file prefixes.txt --overwrite

# only download PDFs (omit --ext to download every file type)
uv run download.py --prefixes-file prefixes.txt --ext pdf

# only download multiple types
uv run download.py --prefixes-file prefixes.txt --ext pdf,jpg

# request a restore for any GLACIER / DEEP_ARCHIVE objects, then re-run later
uv run download.py --prefixes-file prefixes.txt --restore

# restore via the cheaper Bulk tier, kept available for 14 days
uv run download.py --prefixes-file prefixes.txt --restore --restore-tier Bulk --restore-days 14
```

### `prefixes.txt`

One S3 prefix per line. Blank lines and `#` comments are ignored:

```text
# 2026 reports + exports
uploads/reports/2026/01
uploads/reports/2026/02
exports/invoices/q1
backups/database/daily
```

You can list as many prefixes as you need — see [Scaling](#scaling-how-many-prefixes--files).

### CLI options

| Option | Description |
|---|---|
| `prefixes` | One or more S3 key prefixes (positional). |
| `--prefixes-file PATH` | Read prefixes from a file, one per line (`#` comments ok). |
| `--bucket NAME` | Bucket name. Defaults to `S3_BUCKET` from `.env`. |
| `--dest DIR` | Local download root. Defaults to `LOCAL_DIR` or `./downloads`. |
| `--workers N` | Override the auto-picked concurrency (clamped to 32). |
| `--overwrite` | Re-download every file even if a same-size local copy exists. |
| `--ext EXTS` | Only download files with these extensions, comma-separated (e.g. `pdf` or `pdf,jpg`). **Omit to download everything.** |
| `--restore` | For GLACIER / DEEP_ARCHIVE objects, initiate a restore so they can be downloaded on a later run. Without it, archived objects are reported and skipped. |
| `--restore-days N` | How many days a restored copy stays available (default: `7`). |
| `--restore-tier TIER` | Glacier retrieval tier: `Standard` (default), `Bulk`, or `Expedited`. **`Expedited` is not supported for DEEP_ARCHIVE.** |

---

## How it works

### 1. Directory structure is mirrored

Each object's full key is recreated under your destination directory. Given:

```
PATH: uploads/test_folder/2026/05/13/report.csv
```

the file is saved to:

```
<LOCAL_DIR>/uploads/test_folder/2026/05/13/report.csv
```

### 2. Listing

All prefixes are listed **one at a time** using a paginator
(`list_objects_v2`), so listing memory stays flat no matter how many objects
exist. "Folder marker" keys (ending in `/`) and duplicates across overlapping
prefixes are skipped. The result is a flat work-list of `(key, size)` pairs.

### 3. Filtering by file type (`--ext`)

By default **every file** under the prefixes is downloaded. Pass `--ext` to
restrict to specific extensions:

```bash
uv run download.py --prefixes-file prefixes.txt --ext pdf       # PDFs only
uv run download.py --prefixes-file prefixes.txt --ext pdf,jpg   # PDFs and JPGs
```

- Input is forgiving: `pdf`, `.pdf`, and `.PDF` all work.
- Matching is **case-insensitive**, so `REPORT.PDF` is still caught.
- Filtering happens **during listing**, so non-matching files are excluded
  before anything is queued — the file count, total size, and progress bar all
  reflect only the matched files.
- The active filter is shown in the run header (`Filter : .pdf`, or
  `Filter : all files` when `--ext` is omitted).

### 4. Concurrent download

All files are merged into a single queue and downloaded by a
`ThreadPoolExecutor`. Downloads are I/O-bound (threads spend their time waiting
on the network), so threads — not processes — are the right tool. Each worker
thread gets its **own boto3 client** (boto3 clients are not thread-safe).

Progress is printed live as `[done/total]` with a per-file status:

```
[12/240] ↓ uploads/reports/2026/01/report-01.pdf (1,234,567 B)
[13/240] = uploads/reports/2026/01/report-02.pdf (already present, skipped)
```

### 5. Automatic worker sizing (won't hang / OOM)

The worker count is chosen from your machine at startup
(`auto_workers`), taking the **minimum** of three limits:

| Limit | Formula | Purpose |
|---|---|---|
| CPU-based | `logical_cpus × 4` | I/O-bound, so more threads than cores is fine. |
| RAM-based | `(free_RAM × 0.5) ÷ ~32 MiB` | Leaves **half your free RAM untouched**. |
| Hard cap | `32` | Absolute ceiling regardless of machine size. |

It prints exactly what it chose, e.g.:

```
Resources: 16 logical CPUs, 3.6 GiB RAM available (total 15.0 GiB)
  cpu-based=64  ram-based=56  hard-cap=32  -> auto=32
```

**Memory is bounded.** By default boto3's `download_file` spins up its own
internal thread pool (~10 threads × 8 MB each) *per file* — nested inside our
worker pool that could use gigabytes and OOM the machine. We disable it with
`TransferConfig(use_threads=False)`, so each download streams single-threaded.
Total RAM is roughly `workers × ~8 MB` (≈ 1 GB worst case at 32 workers, far
less in practice), not `workers × 10 × 8 MB`.

Use `--workers N` to override (still clamped to the hard cap of 32).

### 6. Resumable — already-downloaded files are skipped

Before downloading, each file is skipped if a local copy already exists **with
the same byte size** as the remote object. So if a run is interrupted, just
re-run the same command — it only fetches what's missing. Use `--overwrite` to
force a fresh download of everything.

> Note: the skip check compares **size**, not content. If a remote file is
> replaced by a different file of the exact same byte count, it will be skipped.
> This is rare; use `--overwrite` if your archive mutates files in place.

### 7. Crash-safe — no corrupt files

Each file downloads to a temporary `<name>.part` and is **atomically renamed**
into place only after the full transfer succeeds. Therefore:

- A crash or Ctrl+C mid-download leaves a `.part` stub, **never** a corrupt file
  at the real path.
- A file present at its real path is *always* a complete download — which is
  what makes the size-based resume check safe.
- Stale `.part` files are simply overwritten on the next run.

### 8. Archive-aware (Glacier / Deep Archive)

Objects in the `GLACIER` or `DEEP_ARCHIVE` storage classes can't be downloaded
directly — S3 must **restore** them to a temporary staging copy first.
(`GLACIER_IR` / Instant Retrieval *is* directly downloadable, so it's treated as
a normal object.)

For each archived object the tool checks its restore state and reports it:

```
[5/12] 🧊 backups/database/daily/2026-01.dump (archived — pass --restore to retrieve)
[6/12] ⏳ backups/database/daily/2026-02.dump (restore in progress — re-run later)
```

- Without `--restore`, archived objects are **reported and skipped** (the run
  still succeeds — they're counted separately, not as failures).
- With `--restore`, the tool issues a restore request for any object that
  doesn't have one in progress, then prints `restore requested — re-run later`.
- Restores take **hours** (Standard ~12h, Bulk up to ~48h for Deep Archive), so
  this is a two-step flow: run once with `--restore`, then re-run the same
  command after the copies are staged to actually download them.
- `--restore-days` controls how long the staged copy lives; `--restore-tier`
  selects the retrieval speed/cost (`Standard`, `Bulk`, `Expedited` — the last
  not valid for Deep Archive).

### 9. Graceful interrupts & error handling

- **Ctrl+C** prints a clean summary, cancels queued downloads (doesn't wait for
  the long tail), and exits `130`. It never prints a raw traceback. Re-run to
  resume.
- **Per-file errors** (network, permissions, disk) are logged and the run
  **continues** — one bad object won't sink the whole batch. Failures are listed
  in the final summary (capped at 20), and the process exits `1` if any failed.

Example summary:

```
Done. 238 downloaded, 0 skipped, 0 archived, 2 failed of 240 total. 512.3 MiB transferred into /home/dexter/.../downloads

2 file(s) failed:
  - uploads/.../broken.pdf: An error occurred (AccessDenied) ...
```

### Exit codes

| Code | Meaning |
|---|---|
| `0` | All files downloaded/skipped successfully. |
| `1` | Completed, but one or more files failed. |
| `130` | Interrupted with Ctrl+C. |

---

## Scaling: how many prefixes / files?

- **Prefixes**: list as many as you want. They're processed sequentially during
  listing, so prefix count has no memory cost and doesn't increase concurrency.
- **Concurrency**: always capped by the worker pool (≤ 32), independent of how
  many prefixes or files you queue.
- **The in-memory work-list** holds one small `(key, size)` tuple per *file*:
  ~30 MB at 100k files, ~300 MB at 1M files — fine on a typical machine. Only
  multi-million-file runs would need a streaming approach.

---

## Tuning

All knobs are constants at the top of [`download.py`](download.py):

| Constant | Default | Effect |
|---|---|---|
| `HARD_CAP` | `32` | Maximum worker threads. |
| `CHUNK_MB` | `8` | Streamed chunk size; dominant per-download RAM cost. |
| `PER_WORKER_MB` | `~32` | RAM budgeted per worker for auto-sizing. |
