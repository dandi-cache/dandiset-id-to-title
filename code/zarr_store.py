"""Experiment: a JSON Lines log kept as a Zarr v3 byte stream.

The store is a Zarr group holding one 1-D `uint8` array, `stream`, with no compressor and no
sharding, so every chunk file `stream/c/0`, `stream/c/1`, ... is a raw slice of UTF-8 text. Unwritten
bytes read as `fill_value=10` (a newline), so the unfilled tail of the last chunk is blank lines and
never NULs.

Appending resizes the array and writes the tail only, so earlier chunks stay byte-identical. A record
is never split across two chunks: when it does not fit in what is left of the current chunk, the rest
of that chunk is padded with newlines and the record starts the next one. Every chunk file is
therefore a self-contained set of whole lines, and a record can be at most one chunk long.

The group's `zarr.json` carries consolidated metadata (the array's shape and chunking), refreshed
after every append, so a reader learns the layout from one small file.
"""

import json
import pathlib
import typing
import warnings

import numpy
import zarr

#: Production chunk size. Tests pass something small.
N_BYTES = 90_000_000

NEWLINE = 10


ARRAY_NAME = "stream"


def _open_or_create(store_path: pathlib.Path, /, *, chunk_bytes: int) -> zarr.Array:
    if (store_path / "zarr.json").exists():
        return zarr.open_group(store=store_path, mode="r+")[ARRAY_NAME]
    store_path.parent.mkdir(parents=True, exist_ok=True)
    root = zarr.open_group(store=store_path, mode="w", zarr_format=3)
    return root.create_array(
        ARRAY_NAME,
        shape=(0,),
        chunks=(chunk_bytes,),
        dtype="uint8",
        compressors=None,
        fill_value=NEWLINE,
    )


def _consolidate(store_path: pathlib.Path, /) -> None:
    # Zarr-python warns that consolidated metadata is not yet in the version 3 specification.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        zarr.consolidate_metadata(store_path)


def _aligned_payload(lines: typing.Iterable[str], position: int, chunk_bytes: int, /) -> bytes:
    """The bytes to write at `position`: each record whole, padding to the next chunk when it will not fit."""
    payload = bytearray()
    for line in lines:
        record = (line if line.endswith("\n") else f"{line}\n").encode("utf-8")
        if len(record) > chunk_bytes:
            raise ValueError(f"A record of {len(record)} bytes does not fit in a chunk of {chunk_bytes}.")
        used = (position + len(payload)) % chunk_bytes
        if used and len(record) > chunk_bytes - used:
            payload += b"\n" * (chunk_bytes - used)
        payload += record
    return bytes(payload)


def append_lines(store_path: pathlib.Path, lines: typing.Iterable[str], /, *, chunk_bytes: int = N_BYTES) -> int:
    """Append lines (newline added when missing) to the stream; return the number of bytes added."""
    array = _open_or_create(store_path, chunk_bytes=chunk_bytes)
    (chunk_bytes,) = array.chunks
    start = array.shape[0]
    payload = _aligned_payload(lines, start, chunk_bytes)
    if not payload:
        return 0
    array.resize((start + len(payload),))
    array[start:] = numpy.frombuffer(payload, dtype="uint8")
    _consolidate(store_path)
    return len(payload)


def iter_lines(store_path: pathlib.Path, /) -> typing.Iterator[str]:
    """Yield the non-blank lines in order, one chunk at a time."""
    if not (store_path / "zarr.json").exists():
        return
    array = zarr.open_group(store=store_path, mode="r", use_consolidated=True)[ARRAY_NAME]
    (length,) = array.shape
    (chunk_bytes,) = array.chunks
    for start in range(0, length, chunk_bytes):
        for raw in array[start : min(start + chunk_bytes, length)].tobytes().split(b"\n"):
            if raw.strip():
                yield raw.decode("utf-8")


def read_lookup(store_path: pathlib.Path, /) -> dict:
    """The `{key: value}` mapping of single-key records, the last record for a key winning."""
    records: dict = {}
    for line in iter_lines(store_path):
        records.update(json.loads(line))
    return records
