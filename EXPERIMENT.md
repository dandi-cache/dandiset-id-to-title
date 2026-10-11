# Experiment: Zarr byte-stream store for derivatives

This branch is an experiment and is not meant to merge.
It prototypes keeping this cache's `derivatives` output as a Zarr v3 byte stream instead of a plain `.jsonl`.

## What was built

- `code/zarr_store.py` holds `append_lines`, `iter_lines` and `read_lookup`.
- The store is a Zarr v3 group holding one 1-D `uint8` array, `stream`, with `compressors=None` (codecs are `bytes` only, no sharding), `fill_value=10` (newline) and `chunks=(90_000_000,)`.
- The content is the JSONL lines concatenated.
- Appending resizes the array and writes the tail only, so chunk files `stream/c/0`, `stream/c/1`, ... are raw UTF-8 slices and earlier chunks stay byte-identical.
- The unfilled tail of the last chunk reads as blank lines, never NULs.
- A record is never split across chunks: when it does not fit in what is left of the current chunk, the rest is padded with newlines and the record starts the next one (a record longer than a chunk is refused).
  Every chunk file is therefore a self-contained set of whole lines, readable and parseable alone.
- The group's `zarr.json` carries consolidated metadata (the array's shape and chunking), refreshed after every append.
  Zarr-python warns that consolidated metadata is not yet in the version 3 specification; the warning is silenced in `_consolidate`.
- `code/update.py` reads prior titles from `derivatives/dandiset_id_to_title.zarr` (falling back to the JSONL on the first run) and appends only new or changed titles.
  The rest of `update.py` is unchanged, including the normal JSONL write, so `dist` publishing still works.
  The `checked_at` side output stays JSONL, because it changes for every attempted Dandiset on every run and would just grow the log.
- `tests/test_zarr_store.py` runs offline.
  It checks text chunks without NUL bytes, earlier chunks unchanged, rollover at the chunk size, last-wins, no record straddling a chunk boundary, an oversize record refused, consolidated metadata tracking the length after each append, and a real throwaway `git init` repo where `git diff --cached --numstat` shows text line counts and a second append changes only the last and new chunks.
- `update.py` itself was not run end to end, because that needs S3 and the shared pipeline.

## Measured on real data

Input is the current `origin/derivatives` file `dandiset_id_to_title.jsonl`: 973 lines, 78,277 bytes.
Converted with `chunks=(90_000_000,)`, then a second append of 3 lines (two changed, one new), in a throwaway git repo with `git gc --aggressive`.

| | zarr store (90 MB chunks) | plain JSONL |
| --- | --- | --- |
| Chunk files | 1 (`c/0`, 90,000,000 bytes) plus `zarr.json` (450 bytes) | 1 file, 78,277 bytes |
| Working tree size | about 90 MB | 78 KB |
| Pack after seed | 113.36 KiB | 26.29 KiB |
| Pack after append | 116.28 KiB | 26.66 KiB |
| Pack growth from append | 2.9 KiB | 0.4 KiB |
| `numstat` of the append | `c/0`: 3 added, 84 deleted | 3 added, 0 deleted |

With a 1 MB chunk the numbers are the same shape: one chunk file of 1,000,000 bytes, pack 28.88 KiB then 29.60 KiB.
The cache is far below one chunk, so rollover was only exercised in the unit tests, not by real data.

The padding is the main cost.
Because the last chunk is always written at full size, the first write creates a 90 MB file that is almost all newlines.
Git compresses that well inside a pack, but a checkout holds all 90 MB, and the `numstat` for a tail append is noisy (blank-line runs shift, so deletions are overcounted).
The 90 MB figure was chosen to stay under GitHub's 100 MB file limit.
Building and gc-ing the 90 MB case took about 40 seconds, mostly git.

## Tradeoffs

- Records no longer straddle chunks (the padding rule above), so each chunk is parseable alone and readers can stream a chunk at a time.
  The cost is a little more padding at each chunk's end, and a hard limit that a record fits in one chunk.
- Lookup by key needs the whole stream, or a small key to offset index.
  That index was not built here.
- A replaced or retried record is a new appended line and the last one wins.
  The log grows without bound unless it is compacted, and compaction rewrites every chunk, which defeats the stable-history benefit.
  Compaction was not implemented.
- Chunk files are not valid JSONL on their own and the store is a directory, so the `dist` compression step and downstream readers (`dandi_cache_utils` expects `.jsonl`) would all need to learn it.
- The sorted whole-file rewrite that the plain JSONL uses gives a diff of only the lines that changed.
  The append log gives the same small delta without sorting, but loses sorted order.

## Recommendation

Do not adopt this for this cache. (Written before the aligned-record and consolidated-metadata changes; those fix the straddling and layout-discovery points above but not the size argument below.)
Its data is 78 KB, so the plain sorted JSONL already gives line-level diffs and a 0.4 KiB pack delta per change.
The Zarr store costs a 90 MB working file, about 4x the pack size and about a 7x larger delta, and needs changes in the shared library and `dist`.
It only pays off for an output that is hundreds of MB and append-mostly, where rewriting the whole file each run is the problem.
If a large append-only cache appears, revisit it there, with a key to offset index and a compaction policy, and put it in `dandi-cache-utils`.

## Found while publishing the example

The repository's own `pre-commit.ci` hooks (`end-of-file-fixer`, `trailing-whitespace`) treated the chunk files as ordinary text and "fixed" them: they stripped the newline padding from the end of `c/3` and the trailing newline from `c/0`–`c/2` and `zarr.json`. That shortens the chunks below their declared size, which corrupts the store. Anything that normalizes whitespace in text files — these hooks, an editor's save settings, `.gitattributes` text conversion — would do the same, so a repository adopting this layout must exclude the store from them (here, `example/` is excluded in `.pre-commit-config.yaml`). This is one more cost of presenting binary-framed data as text.

## Revision: one appendable store instead of split files

The example under `example/` uses the aligned, consolidated layout: [`stream/c/0`–`c/3`](example/derivatives/dandiset_id_to_title.zarr/stream) each end on a whole record, and the group's [`zarr.json`](example/derivatives/dandiset_id_to_title.zarr/zarr.json) lists the array's shape and chunking.
Appending the last 73 titles changes only `stream/c/3` and the two small `zarr.json` files.
What this would replace in `dandi-cache-utils` is the prefix split and its repack: one store grows by chunk files as needed, so no `split_digits` to bump.
What it would add is a last-wins reader, a key index, and a compaction rule for retried or refreshed records.
