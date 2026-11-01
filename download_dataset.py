"""Download Telecom Italia Milan data, retaining grid 5060 by default.

Usage: python download_dataset.py [out_dir] [--workers N] [--full]
Default: data/grid5060_rows.tsv, with verified daily subsets cached for resume.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime as dt
import hashlib
import json
from pathlib import Path
import tempfile
import threading
import time
import urllib.request

API = "https://dataverse.harvard.edu/api"
DOI = "doi:10.7910/DVN/EGZHFV"
HEADERS = {"User-Agent": "milano5g-coursework/1.0", "Content-Type": "application/json"}
START = dt.date(2013, 11, 1)


class DownloadCancelled(Exception):
    """Stop incomplete workers without publishing partial daily subsets."""


def call(url, data=None):
    with urllib.request.urlopen(urllib.request.Request(url, data=data, headers=HEADERS), timeout=60) as response:
        return json.load(response)["data"]


def digest_file(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def write_json(path, data):
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, delete=False) as target:
            temporary = Path(target.name)
            json.dump(data, target, indent=2)
            target.write("\n")
        temporary.replace(path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def filter_stream(source, target, expected_md5, expected_size=None, chunk_size=2**20,
                  stop_event=None, grid_only=True):
    """Hash every source byte and retain grid 5060, or the original full stream.

    expected_md5=None is used only to extract already-present legacy full files;
    remote downloads always require the checksum from the source manifest.
    """
    checksum = hashlib.md5()
    pending = b""
    count = size = 0

    def keep(line):
        nonlocal count
        if line.startswith(b"5060\t"):
            if len(line.rstrip(b"\r\n").split(b"\t")) != 8:
                raise ValueError("Unexpected field count")
            target.write(line if line.endswith(b"\n") else line + b"\n")
            count += 1

    while True:
        if stop_event is not None and stop_event.is_set():
            raise DownloadCancelled()
        chunk = source.read(chunk_size)
        if not chunk:
            break
        checksum.update(chunk)
        size += len(chunk)
        if not grid_only:
            target.write(chunk)
            continue
        lines = (pending + chunk).split(b"\n")
        pending = lines.pop()
        for line in lines:
            keep(line + b"\n")
    if pending:
        keep(pending)
    if expected_md5 is not None and checksum.hexdigest() != expected_md5:
        raise ValueError("Source checksum mismatch")
    if expected_size is not None and size != expected_size:
        raise ValueError("Source byte count differs from manifest")
    if grid_only and count == 0:
        raise ValueError("No grid 5060 rows")
    return {"source_bytes": size, "rows": count if grid_only else None}


def fetch_day(meta, cache, stop_event=None, full=False):
    name = meta["filename"]
    if not isinstance(meta.get("md5"), str) or len(meta["md5"]) != 32:
        raise ValueError(f"Missing source MD5 for {name}")
    subset = cache / (name if full else name + ".5060.tsv")
    record_path = cache / (name + ".json")
    if full and subset.exists():
        with subset.open("rb") as source:
            checksum = hashlib.file_digest(source, "md5").hexdigest()
        size = subset.stat().st_size
        if checksum == meta["md5"] and (meta.get("filesize") is None or size == meta["filesize"]):
            print(f"Already verified: {name}", flush=True)
            return subset, {"filename": name, "source_id": meta["id"], "source_md5": checksum,
                            "source_bytes": size, "rows": None}
    if not full and subset.exists() and record_path.exists():
        try:
            record = json.loads(record_path.read_text())
            if (record["source_id"] == meta["id"] and record["source_md5"] == meta["md5"]
                    and record["subset_sha256"] == digest_file(subset)):
                print(f"Already verified: {name}", flush=True)
                return subset, record
        except (ValueError, KeyError):
            pass

    for attempt in range(1, 6):
        if stop_event is not None and stop_event.is_set():
            raise DownloadCancelled()
        temporary = None
        try:
            # Retain the project's existing Dataverse guestbook/access method.
            access = call(f"{API}/access/datafile/{meta['id']}?gbrecs=true", data=b"{}")
            request = urllib.request.Request(access["signedUrl"], headers=HEADERS)
            with urllib.request.urlopen(request, timeout=60) as source:
                with tempfile.NamedTemporaryFile(dir=cache, delete=False) as target:
                    temporary = Path(target.name)
                    info = filter_stream(source, target, meta["md5"], meta.get("filesize"),
                                         stop_event=stop_event, grid_only=not full)
            record = {"filename": name, "source_id": meta["id"], "source_md5": meta["md5"],
                      **info}
            if not full:
                record["subset_sha256"] = digest_file(temporary)
            temporary.replace(subset)
            if not full:
                write_json(record_path, record)
            detail = "kept full source" if full else f"kept {record['rows']:,} rows"
            print(f"Verified {name}: {detail}", flush=True)
            return subset, record
        except (OSError, ValueError, KeyError) as error:
            if stop_event is not None and stop_event.is_set():
                raise DownloadCancelled() from None
            # Do not print signed access URLs or response bodies.
            print(f"{name}: attempt {attempt} failed ({type(error).__name__})", flush=True)
            if attempt == 5:
                raise RuntimeError(f"Could not verify {name}; completed days remain cached") from None
            delay = min(2**attempt, 8)
            if stop_event is None:
                time.sleep(delay)
            elif stop_event.wait(delay):
                raise DownloadCancelled() from None
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)


def download_days(metadata, cache, workers=8, full=False):
    """Fetch concurrently and return subsets in chronological manifest order."""
    stop_event = threading.Event()
    pool = ThreadPoolExecutor(max_workers=workers)
    futures = []
    results = {}
    try:
        futures = [pool.submit(fetch_day, meta, cache, stop_event, full=full) for meta in metadata]
        for future in as_completed(futures):
            subset, record = future.result()
            results[record["filename"]] = (subset, record)
            print(f"Completed {len(results)} of {len(metadata)} daily files", flush=True)
        return [results[meta["filename"]] for meta in metadata]
    finally:
        stop_event.set()
        for future in futures:
            future.cancel()
        pool.shutdown(wait=True, cancel_futures=True)


def assemble(subsets, destination):
    """Replace the notebook input only after all verified subsets are available."""
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=destination.parent, delete=False) as target:
            temporary = Path(target.name)
            for subset in subsets:
                with subset.open("rb") as source:
                    while chunk := source.read(2**20):
                        target.write(chunk)
        temporary.replace(destination)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def daily_names():
    return [f"sms-call-internet-mi-{START + dt.timedelta(days=i)}.txt" for i in range(62)]


def prepare_grid_input(data_dir=Path("data")):
    """Reuse filtered input, or extract complete legacy data/full downloads offline."""
    data_dir = Path(data_dir)
    destination = data_dir / "grid5060_rows.tsv"
    manifest_path = data_dir / "grid5060-manifest.json"
    if destination.exists():
        if not destination.stat().st_size:
            raise ValueError("Grid 5060 input is empty; rerun python download_dataset.py")
        if manifest_path.exists():
            manifest = json.loads(manifest_path.read_text())
            if digest_file(destination) != manifest["sha256"]:
                raise ValueError("Grid 5060 input differs from its manifest; rerun python download_dataset.py")
        return destination
    sources = [data_dir / "full" / name for name in daily_names()]
    if not all(path.is_file() for path in sources):
        raise FileNotFoundError("Grid input is missing. Run python download_dataset.py first.")
    data_dir.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=data_dir, delete=False) as target:
            temporary = Path(target.name)
            for path in sources:
                with path.open("rb") as source:
                    filter_stream(source, target, expected_md5=None)
        temporary.replace(destination)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return destination


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("out_dir", nargs="?", type=Path, help="output directory (default: data; --full: data/full)")
    parser.add_argument("--workers", type=int, default=8, help="parallel daily downloads (default: 8)")
    parser.add_argument("--full", action="store_true", help="retain complete daily city files instead of grid 5060 only")
    args = parser.parse_args(argv)
    if args.workers < 1:
        parser.error("--workers must be at least 1")
    out = args.out_dir or Path("data/full" if args.full else "data")
    out.mkdir(parents=True, exist_ok=True)
    cache = out if args.full else out / ".grid5060-cache"
    cache.mkdir(exist_ok=True)

    import fcntl
    with (cache / "download.lock").open("w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            parser.exit(1, "Another download is already using this directory.\n")
        dataset = call(f"{API}/datasets/:persistentId/?persistentId={DOI}")
        version = dataset["latestVersion"]
        files = {f["dataFile"]["filename"]: f["dataFile"] for f in version["files"]}
        names = daily_names()
        missing = set(names) - files.keys()
        if missing:
            raise RuntimeError("Expected daily files missing from the remote manifest")
        retention = "full daily files" if args.full else "only grid 5060"
        print(f"Reading 62 daily source streams with {args.workers} parallel downloads; retaining {retention}.", flush=True)
        print("Approximately 21 GB network transfer for a fresh download; verified files are reused.", flush=True)
        completed = download_days([files[name] for name in names], cache, args.workers, full=args.full)
        parts = [subset for subset, record in completed]
        records = [record for subset, record in completed]
        manifest = {"dataset": DOI, "version": f"{version.get('versionNumber')}.{version.get('versionMinorNumber')}",
                    "source_checksums_verified": True, "files": records}
        if args.full:
            write_json(out / "download-manifest.json", manifest)
            print(f"Ready: 62 verified daily files in {out}", flush=True)
            return
        destination = out / "grid5060_rows.tsv"
        assemble(parts, destination)
        manifest.update(grid=5060, rows=sum(r["rows"] for r in records), sha256=digest_file(destination))
        write_json(out / "grid5060-manifest.json", manifest)
        print(f"Ready: {destination} ({destination.stat().st_size / 2**20:.1f} MiB)", flush=True)
        print(f"Rows: {manifest['rows']:,}; notebook previously reported 108,653.")
        print("Next check: expected ten-minute timestamps, date range and missingness.")
        print("Ready for notebook.ipynb; its first cell reuses and verifies this TSV.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        raise SystemExit("Stopped. Verified days are preserved; rerun to resume.") from None
    except (OSError, ValueError, RuntimeError, KeyError) as error:
        raise SystemExit(f"Preparation stopped ({type(error).__name__}). Check network/access and rerun; completed days are cached.") from None
