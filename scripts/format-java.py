#!/usr/bin/env python3
"""Download the pinned Google Java Format or run it with AOSP style (JDK 21)."""
import argparse
from datetime import timezone
from email.utils import parsedate_to_datetime
import hashlib
import http.client
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

VERSION = "1.24.0"
SHA256 = "812f805f58112460edf01bf202a8e61d0fd1f35c0d4fabd54220640776ec57a1"
ROOT = Path(__file__).resolve().parents[1]
RETRY_DELAYS = (10, 20, 40)
RETRY_STATUSES = {429, 500, 502, 503, 504}
TRANSIENT_ERRORS = (TimeoutError, socket.timeout, ConnectionError, http.client.IncompleteRead)


def retry_delay(value, fallback):
    if value is None or not value.isascii():
        return fallback
    value = value.strip()
    if value.isdecimal():
        seconds = value.lstrip("0") or "0"
        if len(seconds) > 2 or (len(seconds) == 2 and seconds > "60"):
            raise ValueError("Retry-After exceeds 60 seconds")
        delay = int(seconds)
    else:
        try:
            retry_at = parsedate_to_datetime(value)
            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=timezone.utc)
            delay = max(0, retry_at.timestamp() - time.time())
        except (ValueError, TypeError, OverflowError):
            return fallback
    if delay > 60:
        raise ValueError("Retry-After exceeds 60 seconds")
    return max(fallback, delay)


def download_formatter(url):
    for attempt in range(1, 5):
        response = None
        retry_after = None
        try:
            response = urllib.request.urlopen(url, timeout=60)
            return response.read()
        except urllib.error.HTTPError as error:
            failure = error
            retryable = error.code in RETRY_STATUSES
            try:
                retry_after = error.headers.get("Retry-After") if error.headers is not None else None
            finally:
                error.close()
        except urllib.error.URLError as error:
            failure = error
            retryable = isinstance(error.reason, TRANSIENT_ERRORS)
        except (OSError, http.client.HTTPException) as error:
            failure = error
            retryable = isinstance(error, TRANSIENT_ERRORS)
        finally:
            if response is not None:
                response.close()
        message = f"Google Java Format download failed on attempt {attempt}/4: {failure}"
        if not retryable or attempt == 4:
            raise RuntimeError(message) from failure
        try:
            delay = retry_delay(retry_after, RETRY_DELAYS[attempt - 1])
        except ValueError as error:
            raise RuntimeError(f"{message}; {error}") from failure
        print(f"{message}; retrying in {delay:g} seconds", file=sys.stderr)
        time.sleep(delay)


def ensure_formatter():
    jar = ROOT / ".tools" / ("google-java-format-" + VERSION + ".jar")
    if not jar.exists():
        url = ("https://repo.maven.apache.org/maven2/com/google/googlejavaformat/"
               f"google-java-format/{VERSION}/google-java-format-{VERSION}-all-deps.jar")
        data = download_formatter(url)
        if hashlib.sha256(data).hexdigest() != SHA256:
            raise RuntimeError("Formatter checksum mismatch")
        jar.parent.mkdir(exist_ok=True)
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(mode="wb", dir=jar.parent,
                                             prefix=".google-java-format-", suffix=".tmp", delete=False) as output:
                temporary = Path(output.name)
                output.write(data)
            os.replace(temporary, jar)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
    if hashlib.sha256(jar.read_bytes()).hexdigest() != SHA256:
        raise RuntimeError("Cached formatter checksum mismatch")
    return jar


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--check", action="store_true")
    modes.add_argument("--download-only", action="store_true",
                       help="Download the formatter if needed, verify it, and print its repository-relative path without running Java")
    args = parser.parse_args()
    try:
        jar = ensure_formatter()
    except (OSError, RuntimeError) as error:
        parser.exit(1, f"Formatter setup failed: {error}\n")
    if args.download_only:
        print(jar.relative_to(ROOT).as_posix())
        return
    sources = sorted(str(p) for folder in ["processors/src", "demo/src"]
                     for p in (ROOT / folder).rglob("*.java"))
    flags = ["--dry-run", "--set-exit-if-changed"] if args.check else ["--replace"]
    subprocess.run(["java", "-jar", str(jar), "--aosp"] + flags + sources, check=True)


if __name__ == "__main__":
    main()
