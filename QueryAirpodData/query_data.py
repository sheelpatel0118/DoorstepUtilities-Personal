#!/usr/bin/env python3
"""Find iOS sessions whose GCS data contains headphoneMotion (AirPod IMU) data.

Fully self-contained — depends only on pip packages (google-cloud-storage,
pymongo, and optionally python-dotenv). It does NOT import from or run inside the
data-processing repo.

This is a lightweight presence check. For each session it reads the small
main_session.json to find the capture id, lists the iOS chunk blobs, samples a
handful of chunks spread across the session timeline, and checks each for the raw
``headphone_internal_sensor`` stream — stopping at the first hit. (It deliberately
avoids the heavy full retrieve/preprocess/interpolate path.)

UUIDs are enumerated from MongoDB. Matching sessions are written to
headphoneMotion_uuid_list.csv (uuid,deliveryType), and a small report (headphone
sessions vs. total iOS sessions) is written to README.md.

Configuration (CLI flag > environment variable > local .env in this folder):
    GCS bucket   : --gcs-bucket / GCS_BUCKET
    GCS key file : --gcs-key   / GCS_CREDENTIALS_PATH / GOOGLE_APPLICATION_CREDENTIALS
                   (omit to use Application Default Credentials)
    Mongo URI    : --mongo-uri / MONGO_URI / MONGO_MAIN_URI
    Mongo db     : --mongo-db  / MONGO_DB  / MONGO_MAIN_DB            (default: main)
    Collection   : --collection / MONGO_COLLECTION / MONGO_TRACKING_COLLECTION
                   (default: tracking_sessions_v3)

Setup & run:
    python3 -m venv .venv && . .venv/bin/activate
    pip install -r requirements.txt
    python query_data.py --limit 300        # smoke test; drop --limit for a full sweep
"""

import argparse
import csv
import gzip
import json
import os
import random
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from io import BytesIO

from google.cloud import storage
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound
from pymongo import MongoClient

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Optional: load a local .env in this folder (python-dotenv is optional).
try:
    from dotenv import load_dotenv

    load_dotenv(os.path.join(SCRIPT_DIR, ".env"))
except Exception:
    pass


# --- GCS access primitives (self-contained; mirror the data-processing repo) ---
# Offset between the UUIDv1 epoch (1582-10-15) and the Unix epoch, in 100ns ticks.
_UUID_V1_GREGORIAN_OFFSET = 0x01B21DD213814000


def make_bucket(bucket_name: str, key_path):
    """Return a GCS bucket handle, with a generous HTTP connection pool.

    Uses an explicit service-account key file when key_path is given, otherwise
    falls back to Application Default Credentials.
    """
    if key_path:
        creds = service_account.Credentials.from_service_account_file(key_path)
        client = storage.Client(credentials=creds)
    else:
        client = storage.Client()
    try:
        from requests.adapters import HTTPAdapter

        adapter = HTTPAdapter(pool_connections=64, pool_maxsize=64, max_retries=3)
        client._http.mount("https://", adapter)
        client._http.mount("http://", adapter)
    except Exception:
        pass
    return client.bucket(bucket_name)


def gcs_read_json(bucket, blob_name):
    """Download and parse a JSON blob."""
    return json.loads(bucket.blob(blob_name).download_as_bytes().decode("utf-8"))


def gcs_list_chunks(bucket, capture_session_id, platform):
    """Sorted list of chunk blob names for a session's platform (metadata-only)."""
    prefix = f"platform_sessions/captureSessionId={capture_session_id}/{platform}/"
    out = [b.name for b in bucket.list_blobs(prefix=prefix) if b.name.endswith("/chunk.jsonl.gz")]
    out.sort()
    return out


def gcs_read_chunk_json(bucket, blob_name):
    """Download + gunzip + parse the single JSON line in a chunk blob."""
    gz_bytes = bucket.blob(blob_name).download_as_bytes()
    with gzip.GzipFile(fileobj=BytesIO(gz_bytes), mode="rb") as f:
        line = f.readline()
    return json.loads(line.decode("utf-8"))


def uuid_v1_epoch(blob_name):
    """Epoch seconds embedded in a chunk blob's UUIDv1 directory name, or None.

    Lexical blob-name order is NOT time order (v1 puts time-low first), so this is
    the only way to time-order chunks without downloading them.
    """
    try:
        u = uuid.UUID(blob_name.rsplit("/", 2)[-2])
    except (ValueError, IndexError):
        return None
    if u.version != 1:
        return None
    return (u.time - _UUID_V1_GREGORIAN_OFFSET) / 1e7


# --- Headphone detection -------------------------------------------------------
# The raw per-chunk key that carries headphone (AirPod) IMU rows.
HEADPHONE_KEY = "headphone_internal_sensor"
# Keys tried (in order) to find the capture id inside main_session.json.
CAPTURE_ID_KEYS = ("capturesessionid", "capture_session_id", "session_id", "captureSessionId")

# Per-session classification statuses (persisted in the checked log).
ST_HEADPHONE = "ios_headphone"      # iOS session WITH headphoneMotion data
ST_IOS_NONE = "ios_no_headphone"    # iOS session WITHOUT headphoneMotion data
ST_NOT_IOS = "not_ios"              # session has no iOS chunks (android / empty)
ST_NODATA = "nodata"               # no main_session.json found
ST_ERROR = "error"                 # transient failure (NOT persisted -> retried)

# Module-level GCS handle, initialised once in main().
_BUCKET = None


def _chunk_has_headphone(chunk) -> bool:
    """True iff a parsed chunk dict carries a non-empty headphone stream."""
    if not isinstance(chunk, dict):
        return False
    rows = chunk.get(HEADPHONE_KEY)
    if isinstance(rows, list) and rows:
        return True
    # Case-insensitive fallback in case a chunk uses a different key casing.
    for key, value in chunk.items():
        if key.lower() == HEADPHONE_KEY and isinstance(value, list) and value:
            return True
    return False


def _resolve_capture_id(uuid_str: str):
    """Return capture_session_id for a session UUID, or None if no main session."""
    blob = f"main_sessions/main_session_id={uuid_str}/main_session.json"
    try:
        main_data = gcs_read_json(_BUCKET, blob)
    except NotFound:
        return None
    if not isinstance(main_data, dict):
        return None
    for key in CAPTURE_ID_KEYS:
        if key in main_data and main_data[key]:
            return str(main_data[key])
    return uuid_str  # main session exists but no explicit capture id


def _stratified_sample(blobs, n_samples: int, rng: random.Random):
    """Pick n_samples chunk blobs spread across the time-ordered session.

    Chunks are time-ordered via their UUIDv1 directory epoch (undecodable ones
    sort last). The ordered list is split into n contiguous segments and one chunk
    is drawn at random from each segment, so the sample covers start/middle/end.
    """
    if len(blobs) <= n_samples:
        return list(blobs)
    ordered = sorted(blobs, key=lambda b: (uuid_v1_epoch(b) is None, uuid_v1_epoch(b) or 0.0, b))
    n = len(ordered)
    picked = []
    for i in range(n_samples):
        lo = (i * n) // n_samples
        hi = ((i + 1) * n) // n_samples  # exclusive
        if hi <= lo:
            hi = lo + 1
        picked.append(ordered[rng.randrange(lo, hi)])
    return picked


def classify_session(uuid_str: str, n_samples: int, seed: int) -> str:
    """Classify one session into one of the ST_* statuses."""
    capture_id = _resolve_capture_id(uuid_str)
    if capture_id is None:
        return ST_NODATA

    ios_chunks = gcs_list_chunks(_BUCKET, capture_id, "ios")
    if not ios_chunks:
        return ST_NOT_IOS

    rng = random.Random(f"{seed}:{uuid_str}")
    sample = _stratified_sample(ios_chunks, n_samples, rng)

    # Download the sampled chunks in parallel; early-exit on the first hit.
    with ThreadPoolExecutor(max_workers=min(len(sample), 8)) as ex:
        futures = {ex.submit(gcs_read_chunk_json, _BUCKET, b): b for b in sample}
        try:
            for fut in as_completed(futures):
                try:
                    chunk = fut.result()
                except Exception:
                    continue  # a single unreadable chunk shouldn't fail the session
                if _chunk_has_headphone(chunk):
                    return ST_HEADPHONE
        finally:
            for fut in futures:
                fut.cancel()
    return ST_IOS_NONE


# --- Mongo enumeration ---------------------------------------------------------
def iter_mongo_sessions(uri, db_name: str, collection: str, limit):
    """Yield distinct (uuid, deliveryType) from a Mongo collection (streamed)."""
    if not uri:
        raise SystemExit(
            "No Mongo URI configured. Set --mongo-uri / MONGO_URI / MONGO_MAIN_URI "
            "(env or a local .env)."
        )
    db = MongoClient(uri)[db_name]
    cursor = db[collection].find(
        {"uuid": {"$exists": True, "$ne": None}}, {"uuid": 1, "deliveryType": 1, "_id": 0}
    )
    if limit:
        cursor = cursor.limit(limit)
    seen = set()
    for doc in cursor:
        u = doc.get("uuid")
        if u is None:
            continue
        u = str(u)
        if u and u not in seen:
            seen.add(u)
            delivery = doc.get("deliveryType")
            yield u, ("" if delivery is None else str(delivery))


# --- IO helpers ----------------------------------------------------------------
def _load_checked(path):
    """Set of UUIDs already recorded (first CSV column), skipping a 'uuid' header."""
    if not os.path.exists(path):
        return set()
    out = set()
    with open(path, newline="") as f:
        for row in csv.reader(f):
            if not row:
                continue
            uid = row[0].strip()
            if uid and uid != "uuid":
                out.add(uid)
    return out


def write_report(checked_log_path, readme_path, meta):
    """Tally the checked log and write a small markdown report (resume-safe)."""
    tally = {ST_HEADPHONE: 0, ST_IOS_NONE: 0, ST_NOT_IOS: 0, ST_NODATA: 0}
    if os.path.exists(checked_log_path):
        with open(checked_log_path, newline="") as f:
            for row in csv.reader(f):
                if len(row) < 2 or row[0] == "uuid":
                    continue
                status = row[1].strip()
                if status in tally:
                    tally[status] += 1

    ios_total = tally[ST_HEADPHONE] + tally[ST_IOS_NONE]
    headphone = tally[ST_HEADPHONE]
    pct = (headphone / ios_total * 100.0) if ios_total else 0.0
    total_classified = sum(tally.values())

    lines = [
        "# headphoneMotion (AirPod) session scan",
        "",
        f"_Generated {meta['generated']}_",
        "",
        "## Result (iOS only)",
        "",
        f"**{headphone:,} of {ios_total:,} iOS sessions ({pct:.2f}%) contained "
        f"headphoneMotion data.**",
        "",
        "| Metric | Count |",
        "| --- | ---: |",
        f"| iOS sessions with headphoneMotion | {headphone:,} |",
        f"| iOS sessions without headphoneMotion | {tally[ST_IOS_NONE]:,} |",
        f"| **Total iOS sessions** | **{ios_total:,}** |",
        "",
        "## All sessions examined",
        "",
        "| Category | Count |",
        "| --- | ---: |",
        f"| iOS (with headphoneMotion) | {tally[ST_HEADPHONE]:,} |",
        f"| iOS (without headphoneMotion) | {tally[ST_IOS_NONE]:,} |",
        f"| Non-iOS (android / no iOS chunks) | {tally[ST_NOT_IOS]:,} |",
        f"| No data (no main session) | {tally[ST_NODATA]:,} |",
        f"| **Total classified** | **{total_classified:,}** |",
        "",
        "## How this was produced",
        "",
        f"- Source: MongoDB collection `{meta['collection']}` (field `uuid`).",
        f"- Per session: read `main_session.json` for the capture id, list iOS "
        f"chunks, then sample **{meta['samples']}** chunks at intervals across the "
        f"session timeline (seed `{meta['seed']}`) and check each for the raw "
        f"`{HEADPHONE_KEY}` stream, stopping at the first hit.",
        "- Matching sessions (with `deliveryType`) are listed in "
        "[`headphoneMotion_uuid_list.csv`](./headphoneMotion_uuid_list.csv); every "
        "examined UUID + status is in `headphoneMotion_checked.log`.",
        "- Sampling (not full scan) means a session that used AirPods only briefly, "
        "outside the sampled chunks, could be missed; raise `--samples` to reduce this.",
        "",
    ]
    with open(readme_path, "w") as f:
        f.write("\n".join(lines))


def _resolve(cli_value, *env_keys, default=None):
    """First non-empty of: CLI value, then each env var, then default."""
    if cli_value:
        return cli_value
    for k in env_keys:
        v = os.getenv(k)
        if v:
            return v
    return default


def main():
    global _BUCKET

    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    # Connection / config (all overridable; otherwise read from env / local .env).
    ap.add_argument("--gcs-bucket", default=None, help="GCS bucket (else GCS_BUCKET)")
    ap.add_argument("--gcs-key", default=None, help="path to GCS service-account JSON "
                    "(else GCS_CREDENTIALS_PATH / GOOGLE_APPLICATION_CREDENTIALS; omit for ADC)")
    ap.add_argument("--mongo-uri", default=None, help="Mongo URI (else MONGO_URI / MONGO_MAIN_URI)")
    ap.add_argument("--mongo-db", default=None, help="Mongo db (else MONGO_DB / MONGO_MAIN_DB, default main)")
    ap.add_argument("--collection", default=None, help="Mongo collection of candidate UUIDs "
                    "(else MONGO_COLLECTION / MONGO_TRACKING_COLLECTION, default tracking_sessions_v3)")
    # Scan / output knobs.
    ap.add_argument("--limit", type=int, default=None, help="cap the number of UUIDs pulled from Mongo")
    ap.add_argument("--workers", type=int, default=16, help="concurrent sessions to check (default: 16)")
    ap.add_argument("--samples", type=int, default=12, help="chunks sampled per session (default: 12)")
    ap.add_argument("--seed", type=int, default=0, help="seed for stratified sampling (default: 0)")
    ap.add_argument("--out", default=os.path.join(SCRIPT_DIR, "headphoneMotion_uuid_list.csv"), help="output CSV of matching sessions (uuid,deliveryType)")
    ap.add_argument("--checked-log", default=os.path.join(SCRIPT_DIR, "headphoneMotion_checked.log"), help="log of every UUID examined (uuid,status; for resume + report)")
    ap.add_argument("--readme", default=os.path.join(SCRIPT_DIR, "README.md"), help="markdown report path")
    ap.add_argument("--no-resume", dest="resume", action="store_false", help="re-check UUIDs even if already in the checked log")
    args = ap.parse_args()

    bucket_name = _resolve(args.gcs_bucket, "GCS_BUCKET")
    key_path = _resolve(args.gcs_key, "GCS_CREDENTIALS_PATH", "GOOGLE_APPLICATION_CREDENTIALS")
    mongo_uri = _resolve(args.mongo_uri, "MONGO_URI", "MONGO_MAIN_URI")
    mongo_db = _resolve(args.mongo_db, "MONGO_DB", "MONGO_MAIN_DB", default="main")
    collection = _resolve(args.collection, "MONGO_COLLECTION", "MONGO_TRACKING_COLLECTION",
                          default="tracking_sessions_v3")
    if not bucket_name:
        raise SystemExit("No GCS bucket configured. Set --gcs-bucket / GCS_BUCKET (env or local .env).")
    if key_path and not os.path.isabs(key_path):
        key_path = os.path.join(SCRIPT_DIR, key_path)  # resolve relative to this folder

    _BUCKET = make_bucket(bucket_name, key_path)

    found = _load_checked(args.out)        # already-matched UUIDs (CSV col0; avoid dup rows)
    checked = _load_checked(args.checked_log) if args.resume else set()
    print(f"[query] bucket={bucket_name} mongo_db={mongo_db} collection={collection}", flush=True)
    print(f"[query] resume: {len(found)} already found, {len(checked)} already checked", flush=True)

    delivery_by_uuid = {}
    pending = []
    for u, delivery in iter_mongo_sessions(mongo_uri, mongo_db, collection, args.limit):
        delivery_by_uuid[u] = delivery
        if u not in checked:
            pending.append(u)
    print(f"[query] {len(pending)} session(s) to check "
          f"(workers={args.workers}, samples/session={args.samples})", flush=True)

    out_lock = threading.Lock()
    out_new = not os.path.exists(args.out) or os.path.getsize(args.out) == 0
    out_f = open(args.out, "a", newline="")
    out_writer = csv.writer(out_f)
    if out_new:
        out_writer.writerow(["uuid", "deliveryType"])
        out_f.flush()
    log_new = not os.path.exists(args.checked_log) or os.path.getsize(args.checked_log) == 0
    log_f = open(args.checked_log, "a", newline="")
    if log_new:
        log_f.write("uuid,status\n")
        log_f.flush()

    counts = {ST_HEADPHONE: 0, ST_IOS_NONE: 0, ST_NOT_IOS: 0, ST_NODATA: 0, ST_ERROR: 0}
    first_error = None
    start = time.time()

    def handle(uuid_str):
        try:
            return uuid_str, classify_session(uuid_str, args.samples, args.seed), None
        except Exception as exc:  # network blips etc.: log & keep going
            return uuid_str, ST_ERROR, exc

    try:
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            for uuid_str, status, exc in _as_completed_map(ex, handle, pending):
                counts[status] += 1

                if status == ST_ERROR:
                    if first_error is None:
                        first_error = f"{uuid_str}: {exc!r}"
                        print(f"[query] first error — {first_error}", flush=True)
                    continue  # not persisted -> retried on the next run

                if status == ST_HEADPHONE:
                    with out_lock:
                        if uuid_str not in found:
                            found.add(uuid_str)
                            out_writer.writerow([uuid_str, delivery_by_uuid.get(uuid_str, "")])
                            out_f.flush()

                with out_lock:
                    log_f.write(f"{uuid_str},{status}\n")
                    log_f.flush()

                done = sum(counts.values())
                if done % 100 == 0:
                    ios_total = counts[ST_HEADPHONE] + counts[ST_IOS_NONE]
                    rate = done / max(time.time() - start, 1e-9)
                    print(f"[query] checked={done} headphone={counts[ST_HEADPHONE]} "
                          f"ios={ios_total} not_ios={counts[ST_NOT_IOS]} "
                          f"nodata={counts[ST_NODATA]} error={counts[ST_ERROR]} "
                          f"({rate:.1f}/s)", flush=True)
    finally:
        out_f.close()
        log_f.close()

    write_report(
        args.checked_log,
        args.readme,
        {
            "generated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "collection": collection,
            "samples": args.samples,
            "seed": args.seed,
        },
    )

    elapsed = time.time() - start
    ios_total = counts[ST_HEADPHONE] + counts[ST_IOS_NONE]
    print(f"\n[query] DONE in {elapsed:.1f}s (this run) — "
          f"headphone={counts[ST_HEADPHONE]} ios_total={ios_total} "
          f"not_ios={counts[ST_NOT_IOS]} nodata={counts[ST_NODATA]} error={counts[ST_ERROR]}",
          flush=True)
    print(f"[query] matches -> {args.out}", flush=True)
    print(f"[query] report  -> {args.readme}", flush=True)


def _as_completed_map(ex, fn, items):
    """Submit fn over items and yield results as they complete."""
    futures = [ex.submit(fn, it) for it in items]
    for fut in as_completed(futures):
        yield fut.result()


if __name__ == "__main__":
    main()
