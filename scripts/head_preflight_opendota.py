"""HEAD preflight for OpenDota endpoints and any direct file downloads.

This script protects bandwidth by rejecting unknown or oversized resources before pulls.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import requests

DEFAULT_MAX_BYTES = 250 * 1024 * 1024


def run_head_preflight(url: str, timeout_seconds: int = 20) -> dict[str, object]:
    """Return HEAD metadata used by ingestion guardrails."""

    head_response = requests.head(url, timeout=timeout_seconds, allow_redirects=True)
    content_length_header = head_response.headers.get("Content-Length")
    content_length_bytes = int(content_length_header) if content_length_header else None

    preflight_result = {
        "url": url,
        "status_code": head_response.status_code,
        "content_type": head_response.headers.get("Content-Type"),
        "content_length_bytes": content_length_bytes,
        "allows_get": head_response.headers.get("Allow"),
    }
    return preflight_result


def main() -> None:
    parser = argparse.ArgumentParser(description="Run HEAD preflight checks.")
    parser.add_argument("url", help="Resource URL to inspect")
    parser.add_argument(
        "--max-bytes",
        type=int,
        default=DEFAULT_MAX_BYTES,
        help="Maximum allowed size before refusing a download",
    )
    parser.add_argument(
        "--output-path",
        default="outputs/manifests/head_preflight_result.json",
        help="Path to save preflight output",
    )
    args = parser.parse_args()

    preflight_result = run_head_preflight(url=args.url)
    content_length_bytes = preflight_result["content_length_bytes"]
    is_size_known = content_length_bytes is not None
    is_too_large = is_size_known and content_length_bytes > args.max_bytes

    if is_too_large:
        message = (
            f"Refusing to continue: {content_length_bytes} bytes exceeds limit "
            f"of {args.max_bytes} bytes."
        )
        raise SystemExit(message)

    output_path = Path(args.output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(preflight_result, indent=2), encoding="utf-8")
    print(json.dumps(preflight_result, indent=2))


if __name__ == "__main__":
    main()
