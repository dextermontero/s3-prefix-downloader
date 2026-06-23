"""Concurrently download all objects under one or more S3 prefixes,
preserving the directory structure locally.

The worker pool is sized automatically from the machine's available CPU and
RAM so the laptop stays responsive (won't hang) during large transfers.

Usage:
    # one or more prefixes inline
    uv run download.py uploads/archive/Tabloid/2026/06 uploads/archive/Magazine/2026/06

    # or read prefixes from a file (one per line)
    uv run download.py --prefixes-file prefixes.txt

    # override the auto-picked worker count / destination / bucket
    uv run download.py uploads/archive/Tabloid/2026/06 --workers 8 --dest ./downloads --bucket my-bucket

Each object key is mirrored under LOCAL_DIR, e.g.
    uploads/archive/Tabloid/2026/06/file.pdf
  -> <LOCAL_DIR>/uploads/archive/Tabloid/2026/06/file.pdf
"""

from __future__ import annotations

import argparse
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import boto3
import psutil
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv

load_dotenv()

# Never spawn more than this many threads regardless of how big the machine is.
HARD_CAP = 32

# Size of each streamed read/multipart chunk. This — NOT the file size — is the
# dominant RAM cost of one in-flight download.
CHUNK_MB = 8

# CRITICAL for bounding memory: by default boto3's download_file launches its
# OWN pool (~10 threads × CHUNK_MB each) per file. Nested inside our worker
# pool that multiplies memory and threads and can OOM the machine. use_threads
# =False makes each download stream single-threaded, so total RAM is roughly
# workers × CHUNK_MB instead of workers × 10 × CHUNK_MB.
TRANSFER = TransferConfig(use_threads=False, multipart_chunksize=CHUNK_MB * 1024 * 1024)

# RAM we budget per worker: one streamed chunk plus generous overhead (SSL
# buffers, Python objects). Used to cap the worker count against free memory.
PER_WORKER_MB = CHUNK_MB * 2 + 16  # ~32 MiB

# Storage classes whose objects are NOT directly downloadable — they must be
# restored first. (GLACIER_IR / Instant Retrieval is downloadable directly, so
# it is intentionally excluded.)
ARCHIVED_CLASSES = {"GLACIER", "DEEP_ARCHIVE"}

# boto3 clients are not thread-safe, so give each worker thread its own client.
_local = threading.local()


def make_client():
    """Build an S3 client. Works for AWS S3 and MinIO (via S3_ENDPOINT_URL)."""
    endpoint = os.getenv("S3_ENDPOINT_URL") or None
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION"),
        config=Config(
            max_pool_connections=HARD_CAP + 4,
            s3={"addressing_style": "path"} if endpoint else {},
        ),
    )


def get_client():
    """Return this thread's S3 client, creating it on first use."""
    client = getattr(_local, "client", None)
    if client is None:
        client = _local.client = make_client()
    return client


def auto_workers(requested: int | None) -> int:
    """Pick a safe worker count from available CPU and RAM.

    Downloads are I/O-bound, so we allow more threads than CPU cores, but cap
    by free RAM (keeping ~half of available memory untouched) and a hard limit.
    """
    cpus = psutil.cpu_count(logical=True) or os.cpu_count() or 2
    avail_mb = psutil.virtual_memory().available / (1024 * 1024)

    cpu_based = cpus * 4  # I/O-bound: threads spend most time waiting on the network
    ram_based = int((avail_mb * 0.5) // PER_WORKER_MB)  # leave half of free RAM alone

    auto = max(1, min(cpu_based, ram_based, HARD_CAP))

    print(
        f"Resources: {cpus} logical CPUs, {avail_mb/1024:.1f} GiB RAM available "
        f"(total {psutil.virtual_memory().total/1024**3:.1f} GiB)"
    )
    print(f"  cpu-based={cpu_based}  ram-based={ram_based}  hard-cap={HARD_CAP}  -> auto={auto}")

    if requested:
        chosen = max(1, min(requested, HARD_CAP))
        print(f"  using requested workers={requested} (clamped to {chosen})")
        return chosen
    return auto


def list_objects(
    prefixes: list[str], bucket: str, extensions: tuple[str, ...] = ()
) -> list[tuple[str, int, str]]:
    """List every (key, size, storage_class) under the prefixes (paginated).

    If `extensions` is given (e.g. (".pdf",)), only keys ending in one of those
    extensions are returned. Matching is case-insensitive.
    """
    client = make_client()
    paginator = client.get_paginator("list_objects_v2")
    found: list[tuple[str, int, str]] = []
    seen: set[str] = set()
    for prefix in prefixes:
        n_before = len(found)
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith("/") or key in seen:  # skip folder markers / dups
                    continue
                if extensions and not key.lower().endswith(extensions):
                    continue  # filtered out by --ext
                seen.add(key)
                found.append((key, obj["Size"], obj.get("StorageClass", "STANDARD")))
        print(f"  {prefix}: {len(found) - n_before} object(s)")
    return found


def restore_status(client, bucket: str, key: str) -> str:
    """Return the restore state of an archived object: 'available',
    'in-progress', or 'not-started'."""
    header = client.head_object(Bucket=bucket, Key=key).get("Restore")
    if header is None:
        return "not-started"
    # Header looks like: ongoing-request="false", expiry-date="..."
    return "in-progress" if 'ongoing-request="true"' in header else "available"


def download_one(
    key: str,
    size: int,
    storage_class: str,
    bucket: str,
    dest: Path,
    overwrite: bool,
    restore: bool,
    restore_days: int,
    restore_tier: str,
) -> tuple[str, int]:
    """Download a single object. Resumable + crash-safe + archive-aware.

    Returns (status, bytes). Status is one of:
      downloaded | skipped | restoring | restore-requested | archived

    - Skips the file if it already exists locally with the same size as the
      remote object (so re-running after a crash won't re-pull what's done).
    - For GLACIER / DEEP_ARCHIVE objects, an object must be restored before it
      can be downloaded. We check restore state and, with --restore, kick off a
      restore request. Once restored, a later run downloads it normally.
    - Downloads to a temporary "<name>.part" file and atomically renames it
      into place only after the full transfer succeeds. An interrupted run
      therefore never leaves a corrupt file at the real path: the stale .part
      is simply overwritten next time.
    """
    target = dest / key
    if not overwrite and target.exists() and target.stat().st_size == size:
        return "skipped", target.stat().st_size

    client = get_client()

    if storage_class in ARCHIVED_CLASSES:
        state = restore_status(client, bucket, key)
        if state == "in-progress":
            return "restoring", 0
        if state == "not-started":
            if not restore:
                return "archived", 0
            client.restore_object(
                Bucket=bucket,
                Key=key,
                RestoreRequest={
                    "Days": restore_days,
                    "GlacierJobParameters": {"Tier": restore_tier},
                },
            )
            return "restore-requested", 0
        # state == "available": fall through and download the staged copy.

    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(target.name + ".part")
    try:
        get_client().download_file(bucket, key, str(tmp), Config=TRANSFER)
        os.replace(tmp, target)  # atomic on the same filesystem
    except BaseException:
        # Clean up the partial file so it can't be mistaken for valid later.
        tmp.unlink(missing_ok=True)
        raise
    return "downloaded", target.stat().st_size


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Concurrently download all files under one or more S3 prefixes."
    )
    parser.add_argument("prefixes", nargs="*", help="One or more S3 key prefixes.")
    parser.add_argument("--prefixes-file", help="Path to a file with one prefix per line.")
    parser.add_argument("--bucket", default=os.getenv("S3_BUCKET"), help="Bucket name (or set S3_BUCKET).")
    parser.add_argument(
        "--dest",
        default=os.getenv("LOCAL_DIR", "./downloads"),
        help="Local directory to download into (default: ./downloads or LOCAL_DIR).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Override the auto-picked worker count (clamped to the hard cap).",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Re-download every file even if a same-size copy already exists locally.",
    )
    parser.add_argument(
        "--ext",
        default="",
        help="Only download files with these extensions, comma-separated "
        "(e.g. 'pdf' or 'pdf,jpg'). Omit to download everything.",
    )
    parser.add_argument(
        "--restore",
        action="store_true",
        help="For GLACIER/DEEP_ARCHIVE objects, initiate a restore so they can "
        "be downloaded later. Re-run once restore completes (hours).",
    )
    parser.add_argument(
        "--restore-days",
        type=int,
        default=7,
        help="How many days the restored copy stays available (default: 7).",
    )
    parser.add_argument(
        "--restore-tier",
        default="Standard",
        choices=["Standard", "Bulk", "Expedited"],
        help="Glacier retrieval tier (default: Standard). Expedited is NOT "
        "supported for DEEP_ARCHIVE.",
    )
    args = parser.parse_args()

    # Normalize "pdf, .JPG" -> (".pdf", ".jpg"); empty means "all files".
    extensions = tuple(
        ("." + e.strip().lower().lstrip(".")) for e in args.ext.split(",") if e.strip()
    )

    prefixes = list(args.prefixes)
    if args.prefixes_file:
        text = Path(args.prefixes_file).read_text()
        prefixes += [ln.strip() for ln in text.splitlines() if ln.strip() and not ln.startswith("#")]

    if not prefixes:
        parser.error("No prefixes given. Pass them as arguments or via --prefixes-file.")
    if not args.bucket:
        parser.error("No bucket given. Pass --bucket or set S3_BUCKET in your .env")

    dest = Path(args.dest).expanduser().resolve()
    print(f"Bucket : {args.bucket}")
    print(f"Dest   : {dest}")
    print(f"Filter : {', '.join(extensions) if extensions else 'all files'}")
    print(f"Prefixes ({len(prefixes)}):")
    for p in prefixes:
        print(f"  - {p}")
    print()

    workers = auto_workers(args.workers)
    print()

    try:
        print("Listing objects...")
        objects = list_objects(prefixes, args.bucket, extensions)
    except (BotoCoreError, ClientError) as exc:
        print(f"\nError listing objects: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("\nInterrupted while listing objects. Nothing was downloaded yet.")
        return 130

    total = len(objects)
    if total == 0:
        suffix = f" matching {', '.join(extensions)}" if extensions else ""
        print(f"\nNo objects found under those prefixes{suffix}.")
        return 0

    total_bytes = sum(size for _, size, _ in objects)
    print(f"\nDownloading {total} file(s), ~{total_bytes/1024**2:.1f} MiB, with {workers} workers...\n")

    done = 0
    downloaded = 0
    skipped = 0
    archived = 0  # archived objects not yet restored (or restore just requested)
    bytes_done = 0
    errors: list[tuple[str, str]] = []
    interrupted = False
    lock = threading.Lock()

    pool = ThreadPoolExecutor(max_workers=workers)
    futures = {
        pool.submit(
            download_one,
            key,
            size,
            storage_class,
            args.bucket,
            dest,
            args.overwrite,
            args.restore,
            args.restore_days,
            args.restore_tier,
        ): key
        for key, size, storage_class in objects
    }
    try:
        for fut in as_completed(futures):
            key = futures[fut]
            try:
                status, size = fut.result()
                with lock:
                    done += 1
                    if status == "skipped":
                        skipped += 1
                        print(f"[{done}/{total}] = {key} (already present, skipped)")
                    elif status == "downloaded":
                        downloaded += 1
                        bytes_done += size
                        print(f"[{done}/{total}] ↓ {key} ({size:,} B)")
                    elif status == "restore-requested":
                        archived += 1
                        print(f"[{done}/{total}] ⏳ {key} (restore requested — re-run later)")
                    elif status == "restoring":
                        archived += 1
                        print(f"[{done}/{total}] ⏳ {key} (restore in progress — re-run later)")
                    else:  # archived, no --restore
                        archived += 1
                        print(f"[{done}/{total}] 🧊 {key} (archived — pass --restore to retrieve)")
            except (BotoCoreError, ClientError, OSError) as exc:
                # A single file failed (network, permissions, disk). Record it
                # and keep going so one bad object doesn't sink the whole run.
                with lock:
                    done += 1
                    errors.append((key, str(exc)))
                    print(f"[{done}/{total}] ✗ {key}  FAILED: {exc}", file=sys.stderr)
            except Exception as exc:  # noqa: BLE001 - unexpected, but don't crash the run
                with lock:
                    done += 1
                    errors.append((key, f"unexpected: {exc!r}"))
                    print(f"[{done}/{total}] ✗ {key}  UNEXPECTED: {exc!r}", file=sys.stderr)
    except KeyboardInterrupt:
        interrupted = True
        print("\n\nInterrupted (Ctrl+C). Stopping — cancelling queued downloads...")
        # Don't wait for the long tail; drop anything not yet started. The
        # in-flight files clean up their own .part temp files, so no corrupt
        # files are left behind and a re-run resumes where this stopped.
        pool.shutdown(wait=False, cancel_futures=True)
    else:
        pool.shutdown(wait=True)

    print(
        f"\n{'Stopped' if interrupted else 'Done'}. "
        f"{downloaded} downloaded, {skipped} skipped, {archived} archived, "
        f"{len(errors)} failed of {total} total. "
        f"{bytes_done/1024**2:.1f} MiB transferred into {dest}"
    )
    if archived:
        print(
            f"\n{archived} object(s) are in Glacier/Deep Archive. "
            + (
                "Restore was requested where needed — re-run this command after "
                f"restore completes (Standard ~12h, Bulk up to ~48h for Deep Archive)."
                if args.restore
                else "Re-run with --restore to retrieve them."
            )
        )
    if errors:
        print(f"\n{len(errors)} file(s) failed:", file=sys.stderr)
        for key, msg in errors[:20]:
            print(f"  - {key}: {msg}", file=sys.stderr)
        if len(errors) > 20:
            print(f"  ... and {len(errors) - 20} more", file=sys.stderr)

    if interrupted:
        print("Re-run the same command to resume — already-downloaded files are skipped.")
        return 130
    return 1 if errors else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        # Catch-all so Ctrl+C never prints a raw traceback.
        print("\nInterrupted.", file=sys.stderr)
        raise SystemExit(130)
