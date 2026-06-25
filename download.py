"""Concurrently download all objects under one or more S3 prefixes,
preserving the directory structure locally or on a NAS (SMB/CIFS).

The worker pool is sized automatically from the machine's available CPU and
RAM so the laptop stays responsive (won't hang) during large transfers.

Usage:
    # one or more prefixes inline
    uv run download.py uploads/archive/Tabloid/2026/06 uploads/archive/Magazine/2026/06

    # or read prefixes from a file (one per line)
    uv run download.py --prefixes-file prefixes.txt

    # transfer a single specific file (or a few) by exact key
    uv run download.py --file uploads/archive/Broadsheet/2026/06/01/paper/file.pdf --minio

    # write directly to NAS (fill NAS_* vars in .env first)
    uv run download.py --prefixes-file prefixes.txt --nas

    # mirror directly into MinIO (fill MINIO_* vars in .env first)
    uv run download.py --prefixes-file prefixes.txt --minio

    # preview what would transfer to MinIO without touching anything
    uv run download.py --prefixes-file prefixes.txt --minio --dry-run

    # override the auto-picked worker count / destination / bucket
    uv run download.py uploads/archive/Tabloid/2026/06 --workers 8 --dest ./downloads --bucket my-bucket

Each object key is mirrored under the destination, e.g.
    uploads/archive/Tabloid/2026/06/file.pdf
  -> <dest>/uploads/archive/Tabloid/2026/06/file.pdf
"""

from __future__ import annotations

import argparse
import io
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
from smbprotocol.connection import Connection
from smbprotocol.session import Session
from smbprotocol.tree import TreeConnect
from smbprotocol import open as smb_open
from smbprotocol.open import (
    CreateDisposition,
    CreateOptions,
    FileAttributes,
    FilePipePrinterAccessMask,
    ImpersonationLevel,
    Open,
    ShareAccess,
)
import uuid

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

# ---------------------------------------------------------------------------
# NAS (SMB/CIFS) support
# ---------------------------------------------------------------------------

class NASWriter:
    """Write files directly to an SMB share without mounting.

    One instance is shared across all threads; smbprotocol's Open objects are
    created per-write so there is no shared mutable state.
    """

    def __init__(self, host: str, user: str, password: str, share: str, nas_path: str = ""):
        self.host = host
        self.share = share
        self.base = nas_path.strip("/")

        self._conn = Connection(uuid.uuid4(), host, 445)
        self._conn.connect()
        self._session = Session(self._conn, user, password)
        self._session.connect()
        self._tree = TreeConnect(self._session, f"\\\\{host}\\{share}")
        self._tree.connect()

    def close(self):
        try:
            self._tree.disconnect()
            self._session.disconnect()
            self._conn.disconnect()
        except Exception:
            pass

    def _smb_path(self, key: str) -> str:
        parts = [self.base, key] if self.base else [key]
        return "\\".join("/".join(parts).replace("/", "\\").split("\\"))

    def exists_with_size(self, key: str, expected_size: int) -> bool:
        path = self._smb_path(key)
        try:
            f = Open(self._tree, path)
            info = f.query_info(
                info_type=1,  # FILE_INFO
                file_info_class=5,  # FileStandardInformation
            )
            f.close(False)
            # FileStandardInformation: EndOfFile is at offset 8, 8 bytes (int64)
            size = int.from_bytes(info[8:16], "little")
            return size == expected_size
        except Exception:
            return False

    def _make_dirs(self, smb_dir: str):
        parts = smb_dir.split("\\")
        for i in range(1, len(parts) + 1):
            partial = "\\".join(parts[:i])
            if not partial:
                continue
            try:
                f = Open(
                    self._tree,
                    partial,
                    desired_access=FilePipePrinterAccessMask.GENERIC_READ,
                    file_attributes=FileAttributes.FILE_ATTRIBUTE_DIRECTORY,
                    share_access=ShareAccess.FILE_SHARE_READ | ShareAccess.FILE_SHARE_WRITE,
                    create_disposition=CreateDisposition.FILE_OPEN_IF,
                    create_options=CreateOptions.FILE_DIRECTORY_FILE,
                    impersonation_level=ImpersonationLevel.Impersonation,
                )
                f.create(self._session, self._tree)
                f.close(False)
            except Exception:
                pass  # already exists or not a dir error — proceed

    def write(self, key: str, data: bytes):
        path = self._smb_path(key)
        smb_dir = "\\".join(path.split("\\")[:-1])
        if smb_dir:
            self._make_dirs(smb_dir)
        f = Open(
            self._tree,
            path,
            desired_access=(
                FilePipePrinterAccessMask.GENERIC_WRITE
                | FilePipePrinterAccessMask.GENERIC_READ
            ),
            file_attributes=FileAttributes.FILE_ATTRIBUTE_NORMAL,
            share_access=ShareAccess.FILE_SHARE_READ,
            create_disposition=CreateDisposition.FILE_SUPERSEDE,
            create_options=CreateOptions.FILE_NON_DIRECTORY_FILE,
            impersonation_level=ImpersonationLevel.Impersonation,
        )
        f.create(self._session, self._tree)
        f.write(data, 0)
        f.close(False)


def make_nas_writer() -> NASWriter:
    host = os.getenv("NAS_HOST", "")
    user = os.getenv("NAS_USER", "")
    password = os.getenv("NAS_PASSWORD", "")
    share = os.getenv("NAS_SHARE", "")
    nas_path = os.getenv("NAS_PATH", "")
    missing = [k for k, v in [("NAS_HOST", host), ("NAS_USER", user), ("NAS_PASSWORD", password), ("NAS_SHARE", share)] if not v]
    if missing:
        raise SystemExit(f"--nas requires these .env vars to be set: {', '.join(missing)}")
    return NASWriter(host, user, password, share, nas_path)


def make_client():
    """Build an S3 source client. Works for AWS S3 and MinIO (via S3_ENDPOINT_URL)."""
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
    """Return this thread's S3 source client, creating it on first use."""
    client = getattr(_local, "client", None)
    if client is None:
        client = _local.client = make_client()
    return client


# ---------------------------------------------------------------------------
# MinIO destination support
# ---------------------------------------------------------------------------

_minio_local = threading.local()


def make_minio_client():
    """Build a boto3 client pointed at the MinIO destination."""
    endpoint = os.getenv("MINIO_ENDPOINT_URL", "")
    if not endpoint:
        raise SystemExit("--minio requires MINIO_ENDPOINT_URL in .env")
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_ACCESS_KEY"),
        config=Config(
            max_pool_connections=HARD_CAP + 4,
            s3={"addressing_style": "path"},
        ),
    )


def get_minio_client():
    """Return this thread's MinIO client, creating it on first use."""
    client = getattr(_minio_local, "client", None)
    if client is None:
        client = _minio_local.client = make_minio_client()
    return client


def minio_exists_with_size(key: str, bucket: str, expected_size: int) -> bool:
    """Return True if the object already exists in MinIO with the same size."""
    try:
        resp = get_minio_client().head_object(Bucket=bucket, Key=key)
        return resp["ContentLength"] == expected_size
    except ClientError:
        return False


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
    nas: NASWriter | None = None,
    minio_bucket: str | None = None,
) -> tuple[str, int]:
    """Download a single object. Resumable + crash-safe + archive-aware.

    Returns (status, bytes). Status is one of:
      downloaded | skipped | restoring | restore-requested | archived

    Destination priority (mutually exclusive flags):
      --minio  → stream source S3 object directly into MinIO; no local file.
      --nas    → stream into memory and write to SMB share; no local file.
      default  → download to local disk with atomic .part-file rename.

    Skip detection queries the actual destination (MinIO head_object, NAS file
    size, or local stat) so re-runs are safe and idempotent everywhere.
    """
    # --- skip check ---
    if minio_bucket is not None:
        if not overwrite and minio_exists_with_size(key, minio_bucket, size):
            return "skipped", size
    elif nas is not None:
        if not overwrite and nas.exists_with_size(key, size):
            return "skipped", size
    else:
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

    # --- transfer ---
    if minio_bucket is not None:
        # Stream source → in-memory buffer → MinIO. No local file touched.
        buf = io.BytesIO()
        get_client().download_fileobj(bucket, key, buf, Config=TRANSFER)
        buf.seek(0)
        get_minio_client().upload_fileobj(buf, minio_bucket, key, Config=TRANSFER)
        return "downloaded", size

    if nas is not None:
        buf = io.BytesIO()
        get_client().download_fileobj(bucket, key, buf, Config=TRANSFER)
        data = buf.getvalue()
        nas.write(key, data)
        return "downloaded", len(data)

    target = dest / key
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(target.name + ".part")
    try:
        get_client().download_file(bucket, key, str(tmp), Config=TRANSFER)
        os.replace(tmp, target)  # atomic on the same filesystem
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    return "downloaded", target.stat().st_size


def dry_run_one(
    key: str,
    size: int,
    overwrite: bool,
    dest: Path,
    nas: NASWriter | None,
    minio_bucket: str | None,
) -> tuple[str, int, str, bool]:
    """Check whether the object already exists at the destination.

    Returns (key, size, dest_url, would_skip). Nothing is downloaded or uploaded.
    dest_url is the fully-resolved path / URL the object would land at.
    """
    if minio_bucket is not None:
        endpoint = os.getenv("MINIO_ENDPOINT_URL", "").rstrip("/")
        dest_url = f"{endpoint}/{minio_bucket}/{key}"
        would_skip = not overwrite and minio_exists_with_size(key, minio_bucket, size)
    elif nas is not None:
        dest_url = "\\\\" + os.getenv("NAS_HOST", "") + "\\" + os.getenv("NAS_SHARE", "") + "\\" + nas._smb_path(key)
        would_skip = not overwrite and nas.exists_with_size(key, size)
    else:
        target = dest / key
        dest_url = str(target)
        would_skip = not overwrite and target.exists() and target.stat().st_size == size
    return key, size, dest_url, would_skip


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Concurrently download all files under one or more S3 prefixes."
    )
    parser.add_argument("prefixes", nargs="*", help="One or more S3 key prefixes.")
    parser.add_argument("--prefixes-file", help="Path to a file with one prefix per line.")
    parser.add_argument(
        "--file",
        dest="files",
        metavar="KEY",
        action="append",
        default=[],
        help="Transfer a single object by its exact S3 key. "
        "Can be repeated: --file key1 --file key2. "
        "Mutually exclusive with prefix arguments.",
    )
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
    parser.add_argument(
        "--nas",
        action="store_true",
        help="Write files directly to the NAS (SMB/CIFS). "
        "Requires NAS_HOST, NAS_USER, NAS_PASSWORD, NAS_SHARE in .env.",
    )
    parser.add_argument(
        "--minio",
        action="store_true",
        help="Mirror objects directly into MinIO (no local copy). "
        "Requires MINIO_ENDPOINT_URL, MINIO_ACCESS_KEY_ID, MINIO_SECRET_ACCESS_KEY, "
        "MINIO_BUCKET in .env.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List what would be transferred and print each destination URL without "
        "downloading or uploading anything. Works with --minio, --nas, and local mode.",
    )
    args = parser.parse_args()

    if args.nas and args.minio:
        parser.error("--nas and --minio are mutually exclusive. Pick one destination.")

    # Normalize "pdf, .JPG" -> (".pdf", ".jpg"); empty means "all files".
    extensions = tuple(
        ("." + e.strip().lower().lstrip(".")) for e in args.ext.split(",") if e.strip()
    )

    prefixes = list(args.prefixes)
    if args.prefixes_file:
        text = Path(args.prefixes_file).read_text()
        prefixes += [ln.strip() for ln in text.splitlines() if ln.strip() and not ln.startswith("#")]

    if args.files and prefixes:
        parser.error("--file and prefix arguments are mutually exclusive. Use one or the other.")
    if not prefixes and not args.files:
        parser.error("No prefixes given. Pass them as arguments, via --prefixes-file, or use --file KEY.")
    if not args.bucket:
        parser.error("No bucket given. Pass --bucket or set S3_BUCKET in your .env")

    nas_writer: NASWriter | None = None
    if args.nas:
        print("Connecting to NAS...")
        nas_writer = make_nas_writer()
        nas_dest = f"\\\\{os.getenv('NAS_HOST')}\\{os.getenv('NAS_SHARE')}"
        if os.getenv("NAS_PATH"):
            nas_dest += f"\\{os.getenv('NAS_PATH').strip('/')}"
        print(f"NAS    : {nas_dest}")

    minio_bucket: str | None = None
    if args.minio:
        minio_bucket = os.getenv("MINIO_BUCKET", "")
        if not minio_bucket:
            parser.error("--minio requires MINIO_BUCKET in .env")
        # Validate credentials by attempting a connection now, not mid-transfer.
        try:
            make_minio_client().list_buckets()
        except Exception as exc:
            raise SystemExit(f"Cannot connect to MinIO: {exc}") from exc
        print(f"MinIO  : {os.getenv('MINIO_ENDPOINT_URL')}  bucket={minio_bucket}")

    dest = Path(args.dest).expanduser().resolve()
    print(f"Source : {args.bucket}")
    if args.minio:
        print(f"Dest   : MinIO → {minio_bucket}")
    elif args.nas:
        print(f"Dest   : NAS (see above)")
    else:
        print(f"Dest   : {dest}")
    print(f"Filter : {', '.join(extensions) if extensions else 'all files'}")
    print(f"Prefixes ({len(prefixes)}):")
    for p in prefixes:
        print(f"  - {p}")
    print()

    workers = auto_workers(args.workers)
    print()

    try:
        if args.files:
            print(f"Resolving {len(args.files)} explicit file(s)...")
            client = make_client()
            objects: list[tuple[str, int, str]] = []
            for key in args.files:
                try:
                    head = client.head_object(Bucket=args.bucket, Key=key)
                    size = head["ContentLength"]
                    storage_class = head.get("StorageClass", "STANDARD")
                    objects.append((key, size, storage_class))
                    print(f"  found: {key} ({size:,} B, {storage_class})")
                except ClientError as exc:
                    print(f"  not found: {key}  ({exc})", file=sys.stderr)
        else:
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

    if args.dry_run:
        print(f"\nDry run — {total} object(s) found (~{total_bytes/1024**2:.1f} MiB). "
              f"Checking destinations with {workers} workers...\n")
        would_transfer = 0
        would_skip = 0
        transfer_bytes = 0
        dr_lock = threading.Lock()
        dr_done = 0

        dr_pool = ThreadPoolExecutor(max_workers=workers)
        dr_futures = {
            dr_pool.submit(dry_run_one, key, size, args.overwrite, dest, nas_writer, minio_bucket): key
            for key, size, _ in objects
        }
        try:
            for fut in as_completed(dr_futures):
                key, size, dest_url, skipping = fut.result()
                with dr_lock:
                    dr_done += 1
                    if skipping:
                        would_skip += 1
                        print(f"[{dr_done}/{total}] = {key} ({size:,} B, already present — skip)")
                    else:
                        would_transfer += 1
                        transfer_bytes += size
                        print(f"[{dr_done}/{total}] → {key} ({size:,} B)")
                    print(f"             {dest_url}")
        except KeyboardInterrupt:
            dr_pool.shutdown(wait=False, cancel_futures=True)
            print("\nDry run interrupted.")
            if nas_writer is not None:
                nas_writer.close()
            return 130
        else:
            dr_pool.shutdown(wait=True)

        print(
            f"\nDry run complete. "
            f"{would_transfer} would transfer (~{transfer_bytes/1024**2:.1f} MiB), "
            f"{would_skip} already present (would skip), "
            f"{total} total."
        )
        if nas_writer is not None:
            nas_writer.close()
        return 0

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
            nas_writer,
            minio_bucket,
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

    dest_label = (
        f"MinIO:{minio_bucket}" if minio_bucket
        else f"NAS:{nas_dest}" if nas_writer
        else str(dest)
    )
    print(
        f"\n{'Stopped' if interrupted else 'Done'}. "
        f"{downloaded} downloaded, {skipped} skipped, {archived} archived, "
        f"{len(errors)} failed of {total} total. "
        f"{bytes_done/1024**2:.1f} MiB transferred into {dest_label}"
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

    if nas_writer is not None:
        nas_writer.close()

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
