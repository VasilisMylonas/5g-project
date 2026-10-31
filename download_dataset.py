"""Download the 62 daily files of the Telecom Italia Milano dataset (~21 GB) from Harvard Dataverse.
Usage: python download_dataset.py [out_dir] [--workers N]
       (default: data/full, 8 parallel downloads; re-run to skip finished files)"""
import argparse, hashlib, json, os, shutil, sys, time, urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

API = "https://dataverse.harvard.edu/api"
UA = {"User-Agent": "milano5g-coursework/1.0", "Content-Type": "application/json"}  # default agent gets 403
TIMEOUT = 60      # seconds without data before a connection counts as failed
RETRIES = 5


def call(url, data=None):
    with urllib.request.urlopen(urllib.request.Request(url, data=data, headers=UA), timeout=TIMEOUT) as r:
        return json.load(r)["data"]


def download(file_id, md5, dest):
    url = call(f"{API}/access/datafile/{file_id}?gbrecs=true", data=b"{}")["signedUrl"]  # guestbook -> link
    part = dest.with_name(f"{dest.name}.part{os.getpid()}")    # per-process name: parallel runs cannot collide
    with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=TIMEOUT) as r, open(part, "wb") as w:
        shutil.copyfileobj(r, w)
    with open(part, "rb") as fh:
        ok = hashlib.file_digest(fh, "md5").hexdigest() == md5
    if not ok:
        part.unlink()
        raise OSError("MD5 mismatch")
    part.rename(dest)    # only complete, verified files get the final name


def fetch(name, file_id, md5, out):
    """Download one file with retries; returns True on success."""
    for attempt in range(1, RETRIES + 1):
        try:
            download(file_id, md5, out / name)
            print(name, flush=True)
            return True
        except OSError as e:     # DNS failures, timeouts, dropped connections, HTTP errors
            if attempt == RETRIES:
                print(f"{name}: giving up after {RETRIES} attempts ({e})", flush=True)
                return False
            print(f"{name}: attempt {attempt} failed ({e}), retrying in {10 * attempt} s", flush=True)
            time.sleep(10 * attempt)


ap = argparse.ArgumentParser()
ap.add_argument("out_dir", nargs="?", default="data/full")
ap.add_argument("--workers", type=int, default=8, help="parallel downloads (default 8)")
args = ap.parse_args()

out = Path(args.out_dir)
out.mkdir(parents=True, exist_ok=True)
files = call(f"{API}/datasets/:persistentId/?persistentId=doi:10.7910/DVN/EGZHFV")["latestVersion"]["files"]
todo = [(f["dataFile"]["filename"], f["dataFile"]["id"], f["dataFile"]["md5"]) for f in files
        if not (out / f["dataFile"]["filename"]).exists()]
print(f"{len(files) - len(todo)} of {len(files)} files already done, downloading {len(todo)} "
      f"with {args.workers} connections", flush=True)
with ThreadPoolExecutor(args.workers) as pool:
    ok = list(pool.map(lambda f: fetch(*f, out), todo))
if not all(ok):
    sys.exit(f"{ok.count(False)} file(s) failed; re-run to retry them")
print("all files downloaded")
