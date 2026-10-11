"""Experiment: a JSON Lines log kept as a Zarr v3 byte stream.

The store is one 1-D `uint8` array with no compressor and no sharding, so every chunk file
`c/0`, `c/1`, ... is a raw slice of the UTF-8 text. Unwritten bytes read as `fill_value=10`
(a newline), so the unfilled tail of the last chunk is blank lines and never NULs. Appending
resizes the array and writes the tail only, so earlier chunks stay byte-identical.

A record may straddle a chunk boundary, so a line is only meaningful once the chunks are joined.
"""

import json
import pathlib
import typing

import numpy
import zarr

#: Production chunk size. Tests pass something small.
N_BYTES = 90_000_000

NEWLINE = 10


def _open_or_create(store_path: pathlib.Path, /, *, chunk_bytes: int) -> zarr.Array:
    if (store_path / "zarr.json").exists():
        return zarr.open_array(store=store_path, mode="r+")
    store_path.parent.mkdir(parents=True, exist_ok=True)
    return zarr.create_array(
        store=store_path,
        shape=(0,),
        chunks=(chunk_bytes,),
        dtype="uint8",
        compressors=None,
        fill_value=NEWLINE,
        zarr_format=3,
    )


def append_lines(store_path: pathlib.Path, lines: typing.Iterable[str], /, *, chunk_bytes: int = N_BYTES) -> int:
    """Append lines (newline added when missing) to the stream; return the number of bytes added."""
    payload = "".join(line if line.endswith("\n") else f"{line}\n" for line in lines).encode("utf-8")
    if not payload:
        return 0
    array = _open_or_create(store_path, chunk_bytes=chunk_bytes)
    start = array.shape[0]
    array.resize((start + len(payload),))
    array[start:] = numpy.frombuffer(payload, dtype="uint8")
    return len(payload)


def iter_lines(store_path: pathlib.Path, /) -> typing.Iterator[str]:
    """Yield the non-blank lines in order, joining a line that straddles a chunk boundary."""
    if not (store_path / "zarr.json").exists():
        return
    array = zarr.open_array(store=store_path, mode="r")
    (length,) = array.shape
    (chunk_bytes,) = array.chunks
    carry = b""
    for start in range(0, length, chunk_bytes):
        carry += array[start : min(start + chunk_bytes, length)].tobytes()
        *complete, carry = carry.split(b"\n")
        for raw in complete:
            if raw.strip():
                yield raw.decode("utf-8")
    if carry.strip():
        yield carry.decode("utf-8")


def read_lookup(store_path: pathlib.Path, /) -> dict:
    """The `{key: value}` mapping of single-key records, the last record for a key winning."""
    records: dict = {}
    for line in iter_lines(store_path):
        records.update(json.loads(line))
    return records
