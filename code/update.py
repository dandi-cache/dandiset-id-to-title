"""The title of every Dandiset, from its draft metadata.

Each Dandiset's `dandiset.jsonld` carries a `name`, which is the title shown on the archive. The
metadata is read straight from the public bucket: no DANDI API, no download.

The cache is accumulative rather than a rebuild, which is the detail that matters here. A title is
refreshed whenever the draft metadata is readable, and the last known title is kept when the
Dandiset later becomes embargoed or is otherwise unreadable. Seeding the published records from
what is already there is what preserves that; returning only the fresh titles would silently drop
every Dandiset the archive has stopped exposing.

The work a run does is one metadata read per Dandiset, and `limit` is how many of those it does.
It does not bind at the archive's present size -- a run reads every Dandiset -- and exists so that
an archive several times larger is spread across crons rather than outgrowing one. What decides
the order is `<stem>_checked_at`, a side output recording when each Dandiset was last attempted:
never-attempted first, then oldest-attempt first, so a bounded run still cycles through all of
them. Whatever a run reads, it publishes the complete cache.

Everything shared -- the argument parsing, the logging, the output paths, testing mode, the JSON
Lines writing, the unsigned S3 client, the listing and the metadata reader -- comes from
`dandi_cache_utils`, which the runtime image carries.
"""

import datetime

import dandi_cache_utils as dandi_cache

#: The side output: when each Dandiset was last *attempted*, read or not, which is what
#: orders the batch once there are more Dandisets than one run reads. Stamping the
#: unreadable ones too is what stops an embargoed Dandiset occupying the batch every run.
CHECKED_AT = "dandiset_id_to_title_checked_at.jsonl"

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

    records = dataset.read_output_lookup()
    checked_at = dataset.read_output_lookup(CHECKED_AT)

    def build() -> list[dict]:
        dandiset_ids = list(dandi_cache.s3.dandiset_ids(client))

        # The limit bounds the reads, not the results. A Dandiset never attempted comes first, so
        # a new one is picked up promptly; the rest follow oldest-attempt first, so a run that
        # cannot reach all of them still cycles through every one over successive runs.
        limit = dataset.limit(arguments.limit)
        batch = dandi_cache.select_new(dandiset_ids, checked_at, limit=limit)
        if limit is None or len(batch) < limit:
            batch += dandi_cache.select_stale(
                [dandiset_id for dandiset_id in dandiset_ids if dandiset_id in checked_at],
                checked_at,
                limit=None if limit is None else limit - len(batch),
            )
        dandi_cache.logger.info("Reading %d of %d Dandisets this run.", len(batch), len(dandiset_ids))

        results = dandi_cache.s3.concurrent_map(get_title, batch, max_workers=WORKERS)
        titles = dict(result for result in results if result)
        if not titles:
            message = (
                f"No Dandiset titles were found under `s3://{dandi_cache.s3.BUCKET}/"
                f"{dandi_cache.s3.DANDISETS_PREFIX}`. The archive bucket may be unreachable or its "
                "layout may have changed."
            )
            raise RuntimeError(message)

        # Everything already published, updated with what this run read, so a Dandiset that has
        # become unreadable -- or that this run simply did not reach -- keeps its last known
        # title rather than disappearing from the cache.
        today = datetime.datetime.now(tz=datetime.UTC).date().isoformat()
        records.update(titles)
        # Every Dandiset this run attempted, not only the ones that answered: an unreadable one is
        # still done for now, and stamping it is what lets the batch move past it next run.
        checked_at.update(dict.fromkeys(batch, today))
        return [{dandiset_id: records[dandiset_id]} for dandiset_id in sorted(records)]

    dandi_cache.run_full_rebuild(dataset, build=build)
    dataset.write_output_lookup(checked_at, CHECKED_AT)


if __name__ == "__main__":
    main()
