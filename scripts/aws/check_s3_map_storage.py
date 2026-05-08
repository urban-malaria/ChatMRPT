#!/usr/bin/env python3
"""Upload and fetch a public test map object from S3.

Run on an EC2 instance or shell with AWS credentials:

    S3_UPLOADS_BUCKET=chatmrpt-uploads python3 scripts/aws/check_s3_map_storage.py
"""

from __future__ import annotations

import importlib.util
import tempfile
import time
import urllib.request
from pathlib import Path


def _load_s3_map_storage():
    repo_root = Path(__file__).resolve().parents[2]
    module_path = repo_root / "app" / "utils" / "s3_map_storage.py"
    spec = importlib.util.spec_from_file_location("s3_map_storage", module_path)
    module = importlib.util.module_from_spec(spec)
    if spec.loader is None:
        raise RuntimeError(f"Could not load {module_path}")
    spec.loader.exec_module(module)
    return module


def main() -> int:
    s3_map_storage = _load_s3_map_storage()
    key = f"maps/test/public-map-check-{int(time.time())}.html"

    with tempfile.TemporaryDirectory() as tmpdir:
        local_path = Path(tmpdir) / "public map check.html"
        local_path.write_text("<html><body>ChatMRPT public map check</body></html>", encoding="utf-8")

        url = s3_map_storage.upload_public(str(local_path), key)
        if not url:
            print("Upload failed or S3_UPLOADS_BUCKET is not configured.")
            return 1

        print(url)
        with urllib.request.urlopen(url, timeout=10) as response:
            body = response.read().decode("utf-8")
            if response.status != 200 or "ChatMRPT public map check" not in body:
                print(f"Unexpected response from S3: status={response.status}")
                return 1

    print("S3 public map check passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
