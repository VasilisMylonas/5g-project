"""Offline regression checks: make check (no network or scientific packages needed)."""
import contextlib
import hashlib
import io
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

import download_dataset as fetch

ROW = b"5060\t1383260400000\t39\t1\t2\t3\t4\t5\n"
PAYLOAD = b"50601\tignore\n" + ROW + b"5061\tignore\n" + ROW.rstrip(b"\n")


class StreamTests(unittest.TestCase):
    def test_four_concurrent_downloads_preserve_manifest_order(self):
        barrier = threading.Barrier(4, timeout=3)
        lock = threading.Lock()
        active = peak = 0
        metadata = [{"filename": f"day-{i}"} for i in range(8)]
        def fetch_stub(meta, cache, stop_event, full=False):
            nonlocal active, peak
            with lock:
                active += 1
                peak = max(peak, active)
            barrier.wait()
            with lock:
                active -= 1
            return Path(meta["filename"]), dict(meta)
        with patch.object(fetch, "fetch_day", side_effect=fetch_stub), contextlib.redirect_stdout(io.StringIO()):
            output = fetch.download_days(metadata, Path("unused"), workers=4)
        self.assertEqual(peak, 4)
        self.assertEqual([record["filename"] for subset, record in output], [m["filename"] for m in metadata])

    def test_failed_parallel_job_cancels_incomplete_workers(self):
        stopped = threading.Event()
        def fetch_stub(meta, cache, stop_event, full=False):
            if meta["filename"] == "bad":
                raise RuntimeError("Unverified source")
            if stop_event.wait(3):
                stopped.set()
                raise fetch.DownloadCancelled()
            self.fail("Other workers were not cancelled")
        metadata = [{"filename": name} for name in ["slow", "bad", "slow2", "queued"]]
        with patch.object(fetch, "fetch_day", side_effect=fetch_stub), contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaisesRegex(RuntimeError, "Unverified source"):
                fetch.download_days(metadata, Path("unused"), workers=2)
        self.assertTrue(stopped.is_set())

    def test_cancelled_stream_does_not_write(self):
        stop = threading.Event()
        stop.set()
        target = io.BytesIO()
        with self.assertRaises(fetch.DownloadCancelled):
            fetch.filter_stream(io.BytesIO(ROW), target, hashlib.md5(ROW).hexdigest(), stop_event=stop)
        self.assertEqual(target.getvalue(), b"")

    def test_chunk_boundaries_and_exact_grid_match(self):
        for chunk in (1, 7, 19, 1024):
            target = io.BytesIO()
            info = fetch.filter_stream(io.BytesIO(PAYLOAD), target, hashlib.md5(PAYLOAD).hexdigest(),
                                       len(PAYLOAD), chunk_size=chunk)
            self.assertEqual(target.getvalue(), ROW * 2)
            self.assertEqual(info, {"rows": 2, "source_bytes": len(PAYLOAD)})

    def test_rejects_bad_checksum_size_or_empty_selection(self):
        cases = [(PAYLOAD, "0" * 32, len(PAYLOAD)),
                 (PAYLOAD, hashlib.md5(PAYLOAD).hexdigest(), len(PAYLOAD) + 1),
                 (b"5061\tother\n", hashlib.md5(b"5061\tother\n").hexdigest(), None)]
        for payload, md5, size in cases:
            with self.assertRaises(ValueError):
                fetch.filter_stream(io.BytesIO(payload), io.BytesIO(), md5, size)

    def test_verified_cache_resumes_and_corruption_is_refetched(self):
        meta = {"filename": "day.txt", "id": 1, "md5": hashlib.md5(PAYLOAD).hexdigest(), "filesize": len(PAYLOAD)}
        with tempfile.TemporaryDirectory() as tmp, contextlib.redirect_stdout(io.StringIO()):
            cache = Path(tmp)
            with patch.object(fetch, "call", return_value={"signedUrl": "https://example.invalid/file"}), \
                 patch.object(fetch.urllib.request, "urlopen", side_effect=lambda *a, **k: io.BytesIO(PAYLOAD)):
                subset, record = fetch.fetch_day(meta, cache)
            with patch.object(fetch, "call", side_effect=AssertionError("Unexpected network request")):
                same, cached = fetch.fetch_day(meta, cache)
            self.assertEqual(same, subset)
            self.assertEqual(cached, record)
            subset.write_bytes(b"tampered")
            with patch.object(fetch, "call", return_value={"signedUrl": "https://example.invalid/file"}) as access, \
                 patch.object(fetch.urllib.request, "urlopen", side_effect=lambda *a, **k: io.BytesIO(PAYLOAD)):
                fetch.fetch_day(meta, cache)
                access.assert_called_once()
            self.assertEqual(subset.read_bytes(), ROW * 2)

    def test_failed_download_is_not_published(self):
        meta = {"filename": "day.txt", "id": 1, "md5": "0" * 32}
        with tempfile.TemporaryDirectory() as tmp, contextlib.redirect_stdout(io.StringIO()), \
             patch.object(fetch, "call", return_value={"signedUrl": "https://example.invalid/file"}), \
             patch.object(fetch.urllib.request, "urlopen", side_effect=lambda *a, **k: io.BytesIO(PAYLOAD)), \
             patch.object(fetch.time, "sleep"):
            with self.assertRaises(RuntimeError):
                fetch.fetch_day(meta, Path(tmp))
            self.assertEqual(list(Path(tmp).iterdir()), [])

    def test_failed_assembly_preserves_existing_input(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            destination = root / "grid5060_rows.tsv"
            destination.write_bytes(b"existing")
            with self.assertRaises(FileNotFoundError):
                fetch.assemble([root / "missing"], destination)
            self.assertEqual(destination.read_bytes(), b"existing")
            self.assertEqual(list(root.iterdir()), [destination])

    def test_complete_manifest_and_assembly_without_full_city_files(self):
        files = [{"dataFile": {"filename": f"sms-call-internet-mi-{fetch.START + fetch.dt.timedelta(days=i)}.txt",
                  "id": i + 1, "md5": hashlib.md5(ROW).hexdigest(), "filesize": len(ROW)}} for i in range(62)]
        metadata = {"latestVersion": {"versionNumber": 1, "versionMinorNumber": 0, "files": files}}
        def response(url, data=None):
            return metadata if "/datasets/" in url else {"signedUrl": "https://example.invalid/file"}
        with tempfile.TemporaryDirectory() as tmp, contextlib.redirect_stdout(io.StringIO()), \
             patch.object(fetch, "call", side_effect=response), \
             patch.object(fetch.urllib.request, "urlopen", side_effect=lambda *a, **k: io.BytesIO(ROW)), \
             patch("sys.argv", ["download_dataset.py", tmp]):
            fetch.main()
            root = Path(tmp)
            self.assertEqual((root / "grid5060_rows.tsv").read_bytes(), ROW * 62)
            manifest = json.loads((root / "grid5060-manifest.json").read_text())
            self.assertEqual(manifest["rows"], 62)
            self.assertEqual(len(manifest["files"]), 62)
            self.assertTrue(manifest["source_checksums_verified"])
            self.assertFalse((root / "full").exists())

    def test_full_mode_retains_original_bytes_and_verifies_existing_files(self):
        meta = {"filename": "day.txt", "id": 1, "md5": hashlib.md5(PAYLOAD).hexdigest(), "filesize": len(PAYLOAD)}
        with tempfile.TemporaryDirectory() as tmp, contextlib.redirect_stdout(io.StringIO()):
            root = Path(tmp)
            with patch.object(fetch, "call", return_value={"signedUrl": "https://example.invalid/file"}), \
                 patch.object(fetch.urllib.request, "urlopen", side_effect=lambda *a, **k: io.BytesIO(PAYLOAD)):
                path, record = fetch.fetch_day(meta, root, full=True)
            self.assertEqual(path, root / "day.txt")
            self.assertEqual(path.read_bytes(), PAYLOAD)
            with patch.object(fetch, "call", side_effect=AssertionError("Unexpected network request")):
                fetch.fetch_day(meta, root, full=True)
            path.write_bytes(b"corrupted")
            with patch.object(fetch, "call", return_value={"signedUrl": "https://example.invalid/file"}) as access, \
                 patch.object(fetch.urllib.request, "urlopen", side_effect=lambda *a, **k: io.BytesIO(PAYLOAD)):
                fetch.fetch_day(meta, root, full=True)
                access.assert_called_once()
            self.assertEqual(path.read_bytes(), PAYLOAD)

    def test_full_cli_keeps_all_daily_files_without_creating_filtered_input(self):
        files = [{"dataFile": {"filename": name, "id": i+1,
                  "md5": hashlib.md5(PAYLOAD).hexdigest(), "filesize": len(PAYLOAD)}}
                 for i, name in enumerate(fetch.daily_names())]
        metadata = {"latestVersion": {"versionNumber": 1, "versionMinorNumber": 0, "files": files}}
        def response(url, data=None):
            return metadata if "/datasets/" in url else {"signedUrl": "https://example.invalid/file"}
        with tempfile.TemporaryDirectory() as tmp, contextlib.redirect_stdout(io.StringIO()), \
             patch.object(fetch, "call", side_effect=response), \
             patch.object(fetch.urllib.request, "urlopen", side_effect=lambda *a, **k: io.BytesIO(PAYLOAD)):
            fetch.main([tmp, "--full", "--workers", "4"])
            root = Path(tmp)
            for name in fetch.daily_names():
                self.assertEqual((root/name).read_bytes(), PAYLOAD)
            self.assertFalse((root/"grid5060_rows.tsv").exists())
            manifest = json.loads((root/"download-manifest.json").read_text())
            self.assertEqual(len(manifest["files"]), 62)
            self.assertTrue(manifest["source_checksums_verified"])

    def test_missing_remote_checksum_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(fetch, "call") as access:
            with self.assertRaisesRegex(ValueError, "Missing source MD5"):
                fetch.fetch_day({"filename": "day.txt", "id": 1, "md5": None}, Path(tmp))
            access.assert_not_called()

    def test_notebook_reuses_existing_input_without_reading_full_city_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); path = root / "grid5060_rows.tsv"; path.write_bytes(ROW)
            (root / "grid5060-manifest.json").write_text(json.dumps({"sha256": hashlib.sha256(ROW).hexdigest()}))
            with patch.object(fetch, "filter_stream", side_effect=AssertionError("Unexpected extraction")):
                self.assertEqual(fetch.prepare_grid_input(root), path)
            self.assertEqual(path.read_bytes(), ROW)
            path.write_bytes(b"changed")
            with self.assertRaisesRegex(ValueError, "differs from its manifest"):
                fetch.prepare_grid_input(root)
            self.assertEqual(path.read_bytes(), b"changed")

    def test_notebook_missing_or_empty_input_stops_without_overwriting(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self.assertRaisesRegex(FileNotFoundError, "Run python download_dataset.py"):
                fetch.prepare_grid_input(root)
            path = root / "grid5060_rows.tsv"
            self.assertFalse(path.exists())
            path.touch()
            with self.assertRaisesRegex(ValueError, "empty"):
                fetch.prepare_grid_input(root)

    def test_notebook_extracts_legacy_full_files_in_date_order(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); full = root / "full"; full.mkdir()
            expected = []
            for i, name in enumerate(fetch.daily_names()):
                row = ROW.replace(b"1383260400000", str(1383260400000+i*600000).encode())
                (full / name).write_bytes(b"50601\tignore\n"+row)
                expected.append(row)
            path = fetch.prepare_grid_input(root)
            self.assertEqual(path.read_bytes(), b"".join(expected))

    def test_nonpositive_workers_fail_before_network(self):
        with contextlib.redirect_stderr(io.StringIO()), patch.object(fetch, "call") as access:
            with self.assertRaises(SystemExit) as error:
                fetch.main(["--workers", "0"])
            self.assertEqual(error.exception.code, 2)
            access.assert_not_called()


if __name__ == "__main__":
    unittest.main()
