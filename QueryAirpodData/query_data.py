#!/usr/bin/env python3
"""Find iOS sessions whose GCS data contains headphoneMotion (AirPod IMU) data.

This is a lightweight presence check: instead of calling retrieve_session() (which
downloads every chunk, decompresses, preprocesses, dedupes, unit-converts, and
interpolates), it reuses only the GCS access primitives, reads a stratified sample
of chunks across each session's timeline, and short-circuits the moment it finds a
chunk carrying the raw ``headphone_internal_sensor`` stream.

UUIDs are enumerated from MongoDB (same connection pattern as generate_csv.py).
Matching sessions are written to headphoneMotion_uuid_list.csv (uuid,deliveryType),
and a small report (headphone sessions vs. total iOS sessions) is written to README.md.

Run with the data-processing repo's virtualenv, e.g.:

    cd <data-processing>
    .venv/bin/python <this-repo>/QueryAirpodData/query_data.py --limit 200
"""

# --- Bootstrap into the data-processing repo BEFORE importing packages.* ------
import os
import sys

DP_ROOT = "/Users/sheelpatel/Documents/doorstepai/doorstepai-track/DataProcessing/reclone/data-processing"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

sys.path.insert(0, DP_ROOT)
os.chdir(DP_ROOT)  # so the relative GCS_CREDENTIALS_PATH / .env in helper.py resolve

from dotenv import load_dotenv  # noqa: E402

load_dotenv(os.path.join(DP_ROOT, ".env"))

# helper.py calls load_dotenv() at import; cwd is now DP_ROOT so it finds .env.
from packages.db.gcs.helper import (  # noqa: E402
    _gcs_client,
    _gcs_read_json,
    _gcs_list_chunks,
    _gcs_read_chunk_json,
    _uuid_v1_epoch,
)
import google.api_core.exceptions  # noqa: E402
from pymongo import MongoClient  # noqa: E402

# --- Stdlib --------------------------------------------------------------------
import argparse  # noqa: E402
import csv  # noqa: E402
import random  # noqa: E402
import threading  # noqa: E402
import time  # noqa: E402
from datetime import datetime  # noqa: E402
from concurrent.futures import ThreadPoolExecutor, as_completed  # noqa: E402


# The raw per-chunk key that carries headphone (AirPod) IMU rows. See
# packages/db/gcs/retrieve.py: _STREAM_OUTPUT_KEY maps it to 'headphone_motion'
# and _IMU_STREAM_KEYS lists it. Keys tried (in order) to find the capture id.
HEADPHONE_KEY = "headphone_internal_sensor"
CAPTURE_ID_KEYS = ("capturesessionid", "capture_session_id", "session_id", "captureSessionId")

# Per-session classification statuses (persisted in the checked log).
ST_HEADPHONE = "ios_headphone"      # iOS session WITH headphoneMotion data
ST_IOS_NONE = "ios_no_headphone"    # iOS session WITHOUT headphoneMotion data
ST_NOT_IOS = "not_ios"              # session has no iOS chunks (android / empty)
ST_NODATA = "nodata"               # no main_session.json found
ST_ERROR = "error"                 # transient failure (NOT persisted -> retried)

# Module-level GCS handle, initialised once in main() (the helper caches it too).
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
    """Return capture_session_id for a session UUID, or None if no main session.

    Reads main_sessions/main_session_id={uuid}/main_session.json (small) and
    extracts the capture id, falling back to the uuid itself when present but
    unlabelled.
    """
    blob = f"main_sessions/main_session_id={uuid_str}/main_session.json"
    try:
        main_data = _gcs_read_json(_BUCKET, blob)
    except google.api_core.exceptions.NotFound:
        return None
    if not isinstance(main_data, dict):
        return None
    for key in CAPTURE_ID_KEYS:
        if key in main_data and main_data[key]:
            return str(main_data[key])
    return uuid_str  # main session exists but no explicit capture id


def _stratified_sample(blobs, n_samples: int, rng: random.Random):
    """Pick n_samples chunk blobs spread across the time-ordered session.

    Chunks are time-ordered via their UUIDv1 directory epoch (lexical order is
    NOT time order); undecodable ones sort last. The ordered list is split into
    n contiguous segments and one chunk is drawn at random from each segment, so
    the sample covers the start, middle, and end of the session.
    """
    if len(blobs) <= n_samples:
        return list(blobs)
    ordered = sorted(blobs, key=lambda b: (_uuid_v1_epoch(b) is None, _uuid_v1_epoch(b) or 0.0, b))
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

    ios_chunks = _gcs_list_chunks(_BUCKET, capture_id, "ios")
    if not ios_chunks:
        return ST_NOT_IOS

    rng = random.Random(f"{seed}:{uuid_str}")
    sample = _stratified_sample(ios_chunks, n_samples, rng)

    # Download the sampled chunks in parallel; early-exit on the first hit.
    with ThreadPoolExecutor(max_workers=min(len(sample), 8)) as ex:
        futures = {ex.submit(_gcs_read_chunk_json, _BUCKET, b): b for b in sample}
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


def iter_mongo_sessions(collection: str, limit):
    """Yield distinct (uuid, deliveryType) from a Mongo collection (streamed)."""
    uri = os.getenv("MONGO_MAIN_URI")
    if not uri:
        raise SystemExit("MONGO_MAIN_URI is not set in the data-processing .env")
    db = MongoClient(uri)[os.getenv("MONGO_MAIN_DB", "main")]
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


def _load_checked(path):
    """Return the set of UUIDs already classified (first CSV column), skipping header."""
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
    """Tally the checked log and write a small markdown report.

    Aggregates the persisted per-session statuses (resume-safe across runs), then
    reports headphone sessions vs. total iOS sessions.
    """
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


def main():
    global _BUCKET

    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--collection", default=os.getenv("MONGO_TRACKING_COLLECTION", "tracking_sessions_v3"),
                    help="Mongo collection of candidate UUIDs (default: MONGO_TRACKING_COLLECTION / tracking_sessions_v3)")
    ap.add_argument("--limit", type=int, default=None, help="cap the number of UUIDs pulled from Mongo")
    ap.add_argument("--workers", type=int, default=16, help="concurrent sessions to check (default: 16)")
    ap.add_argument("--samples", type=int, default=12, help="chunks sampled per session (default: 12)")
    ap.add_argument("--seed", type=int, default=0, help="seed for stratified sampling (default: 0)")
    ap.add_argument("--out", default=os.path.join(SCRIPT_DIR, "headphoneMotion_uuid_list.csv"), help="output CSV of matching sessions (uuid,deliveryType)")
    ap.add_argument("--checked-log", default=os.path.join(SCRIPT_DIR, "headphoneMotion_checked.log"), help="log of every UUID examined (uuid,status; for resume + report)")
    ap.add_argument("--readme", default=os.path.join(SCRIPT_DIR, "README.md"), help="markdown report path")
    ap.add_argument("--no-resume", dest="resume", action="store_false", help="re-check UUIDs even if already in the checked log")
    args = ap.parse_args()

    _BUCKET = _gcs_client()[1]

    found = _load_checked(args.out)        # already-matched UUIDs (CSV col0; avoid dup rows)
    checked = _load_checked(args.checked_log) if args.resume else set()
    print(f"[query] resume: {len(found)} already found, {len(checked)} already checked", flush=True)

    delivery_by_uuid = {}
    pending = []
    for u, delivery in iter_mongo_sessions(args.collection, args.limit):
        delivery_by_uuid[u] = delivery
        if u not in checked:
            pending.append(u)
    print(f"[query] {len(pending)} session(s) to check from '{args.collection}' "
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
            "collection": args.collection,
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
