import argparse
import concurrent.futures
import functools
import json
import pathlib

import boto3
import botocore
import botocore.config
import botocore.exceptions

# This cache is first-in-chain: it has no upstream `sourcedata` and instead pulls its inputs
# directly from the public DANDI archive S3 bucket. Every Dandiset has a `draft` version, whose
# `dandiset.jsonld` manifest carries the current title (the `name` field) and is updated the
# moment an owner edits it -- published versions are frozen snapshots taken at publish time, so
# `draft` alone is the source of the current title.
_BUCKET = "dandiarchive"
_REGION = "us-east-2"
_DANDISETS_PREFIX = "dandisets/"
_DRAFT_MANIFEST_SUFFIX = "/draft/dandiset.jsonld"

# Testing mode processes only this many Dandisets and writes to its own designated file
# (`derivatives/testing.jsonl`), leaving the real cache untouched.
_TESTING_LIMIT = 10
_CACHE_FILE_NAME = "dandiset_id_to_title.jsonl"
_TESTING_FILE_NAME = "testing.jsonl"


def _build_s3_client(max_pool_connections: int = 10) -> "botocore.client.BaseClient":
    # `dandiarchive` is a public bucket, so requests are sent unsigned (anonymous). The
    # connection pool must hold one connection per download worker, or the surplus workers
    # redo the TCP/TLS handshake on every request.
    config = botocore.config.Config(
        signature_version=botocore.UNSIGNED,
        max_pool_connections=max_pool_connections,
        retries={"mode": "standard"},
    )
    return boto3.client("s3", region_name=_REGION, config=config)


def _iter_dandiset_ids(s3_client: "botocore.client.BaseClient"):
    """Yield every Dandiset ID (its folder name) under `dandisets/`, in lexicographic order."""
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=_BUCKET, Prefix=_DANDISETS_PREFIX, Delimiter="/"):
        for entry in page.get("CommonPrefixes", []):
            yield entry["Prefix"].removeprefix(_DANDISETS_PREFIX).rstrip("/")


def _get_title(s3_client: "botocore.client.BaseClient", dandiset_id: str) -> tuple[str, str] | None:
    key = f"{_DANDISETS_PREFIX}{dandiset_id}{_DRAFT_MANIFEST_SUFFIX}"
    try:
        response = s3_client.get_object(Bucket=_BUCKET, Key=key)
    except botocore.exceptions.ClientError as error:
        error_code = error.response.get("Error", {}).get("Code", "")
        # Embargoed Dandisets list their `draft` manifest publicly but deny anonymous reads
        # (AccessDenied); a manifest can also be deleted between listing and fetching
        # (NoSuchKey). Both are expected upstream states, not pipeline failures, so skip.
        if error_code in ("AccessDenied", "NoSuchKey"):
            print(f"Skipping inaccessible manifest `{key}` ({error_code}).", flush=True)
            return None
        raise
    body = response["Body"].read()
    title = json.loads(body).get("name")
    return (dandiset_id, title) if title is not None else None


def _collect_titles(s3_client: "botocore.client.BaseClient", max_workers: int, testing: bool) -> dict[str, str]:
    if testing:
        # Testing run: fetch manifests one at a time and stop as soon as `_TESTING_LIMIT`
        # titles have been collected, so the run is fast and does not enumerate the entire
        # `dandisets/` prefix.
        titles: dict[str, str] = {}
        for dandiset_id in _iter_dandiset_ids(s3_client):
            if result := _get_title(s3_client, dandiset_id):
                titles[result[0]] = result[1]
            if len(titles) >= _TESTING_LIMIT:
                break
        return titles

    # Full run: fetch every Dandiset's draft manifest concurrently.
    titles = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        get_title = functools.partial(_get_title, s3_client)
        for result in executor.map(get_title, _iter_dandiset_ids(s3_client)):
            if result:
                titles[result[0]] = result[1]
    return titles


def _load_previous_cache(cache_file_path: pathlib.Path) -> dict[str, str]:
    """Read the previous run's cache back into memory (empty on a bootstrap run)."""
    previous_cache: dict[str, str] = {}
    if not cache_file_path.exists():
        return previous_cache

    with cache_file_path.open() as file_stream:
        for line in file_stream:
            if stripped_line := line.strip():
                previous_cache.update(json.loads(stripped_line))
    return previous_cache


def _run(base_directory: pathlib.Path, max_workers: int, testing: bool) -> None:
    s3_client = _build_s3_client(max_pool_connections=max_workers)

    fresh_titles = _collect_titles(s3_client, max_workers=max_workers, testing=testing)
    if len(fresh_titles) == 0:
        message = (
            f"\nNo Dandiset titles found under `s3://{_BUCKET}/{_DANDISETS_PREFIX}`.\n"
            "The DANDI archive bucket may be unreachable or its layout may have changed.\n"
        )
        raise RuntimeError(message)

    derivatives_directory = base_directory / "derivatives"
    derivatives_directory.mkdir(parents=True, exist_ok=True)
    # Testing runs read from and write to their own designated file, so the real cache is
    # never touched.
    output_file_path = derivatives_directory / (_TESTING_FILE_NAME if testing else _CACHE_FILE_NAME)

    # The cache is accumulative: a Dandiset's title is refreshed whenever its draft manifest is
    # readable, and its last known title is retained if the Dandiset later becomes embargoed or
    # otherwise unreadable, rather than being dropped from the map.
    dandiset_id_to_title = _load_previous_cache(output_file_path)
    dandiset_id_to_title.update(fresh_titles)

    # One JSON value per line: `{"<dandiset_id>": "<title>"}`.
    with output_file_path.open(mode="w") as file_stream:
        for dandiset_id in sorted(dandiset_id_to_title):
            record = {dandiset_id: dandiset_id_to_title[dandiset_id]}
            file_stream.write(f"{json.dumps(record)}\n")


if __name__ == "__main__":
    default_base_directory = pathlib.Path(__file__).parent.parent

    parser = argparse.ArgumentParser(description="Update the dandiset-id-to-title DANDI cache.")
    parser.add_argument(
        "--base-directory",
        type=pathlib.Path,
        default=default_base_directory,
        help=(
            "The directory containing the `derivatives` directory. "
            "Set to the mounted dataset path when run inside the pipeline container; "
            "defaults to the repository root."
        ),
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=16,
        help="Number of concurrent S3 download workers used to fetch the draft manifests.",
    )
    parser.add_argument(
        "--testing",
        action="store_true",
        help=(
            f"Run in testing mode: process only the first {_TESTING_LIMIT} Dandisets from S3 "
            f"and read/write `derivatives/{_TESTING_FILE_NAME}` instead of the real cache, "
            "leaving it untouched. Omit for a complete update."
        ),
    )
    args = parser.parse_args()

    _run(base_directory=args.base_directory, max_workers=args.max_workers, testing=args.testing)
