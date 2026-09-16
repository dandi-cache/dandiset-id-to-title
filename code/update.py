"""The title of every Dandiset, from its draft metadata.

Each Dandiset's `dandiset.jsonld` carries a `name`, which is the title shown on the archive. The
metadata is read straight from the public bucket: no DANDI API, no download.

The cache is accumulative rather than a rebuild, which is the detail that matters here. A title is
refreshed whenever the draft metadata is readable, and the last known title is kept when the
Dandiset later becomes embargoed or is otherwise unreadable. Seeding `build` from what is already
published is what preserves that; returning only the fresh titles would silently drop every
Dandiset the archive has stopped exposing.

Everything shared -- the argument parsing, the logging, the output paths, testing mode, the JSON
Lines writing, the unsigned S3 client, the listing and the metadata reader -- comes from
`dandi_cache_utils`, which the runtime image carries.
"""

import dandi_cache_utils as dandi_cache

#: One connection per worker; a smaller pool makes the surplus workers redo the TLS handshake.
WORKERS = 16


def main() -> None:
    dataset, arguments = dandi_cache.open_dataset()
    client = dandi_cache.s3.anonymous_client(max_pool_connections=WORKERS)

    def get_title(dandiset_id: str, /) -> tuple[str, str] | None:
        """The Dandiset's title, or `None` when its metadata is unreadable or carries no name."""
        metadata = dandi_cache.s3.dandiset_metadata(client, dandiset_id)
        title = None if metadata is None else metadata.get("name")
        return None if title is None else (dandiset_id, title)

    def build() -> list[dict]:
        dandiset_ids = list(dandi_cache.s3.dandiset_ids(client))
        if dataset.testing:
            dandiset_ids = dandiset_ids[: dandi_cache.TESTING_LIMIT]

        results = dandi_cache.s3.concurrent_map(get_title, dandiset_ids, max_workers=WORKERS)
        titles = dict(result for result in results if result)
        if not titles:
            message = (
                f"No Dandiset titles were found under `s3://{dandi_cache.s3.BUCKET}/"
                f"{dandi_cache.s3.DANDISETS_PREFIX}`. The archive bucket may be unreachable or its "
                "layout may have changed."
            )
            raise RuntimeError(message)

        # Seeded with what is already published, so a Dandiset that has since become unreadable
        # keeps its last known title rather than disappearing from the cache.
        records = dataset.read_output_lookup()
        records.update(titles)
        return [{dandiset_id: records[dandiset_id]} for dandiset_id in sorted(records)]

    dandi_cache.run_full_rebuild(dataset, build=build, limit=arguments.limit)


if __name__ == "__main__":
    main()
