import json
import pathlib
import subprocess

import pytest
import zarr
import zarr_store

CHUNK = 1_000


def make_lines(start: int, count: int) -> list[str]:
    return [json.dumps({f"{index:06d}": f"A title number {index}"}) for index in range(start, start + count)]


def chunk_files(store: pathlib.Path) -> list[pathlib.Path]:
    return sorted((store / "stream" / "c").iterdir(), key=lambda path: int(path.name))


@pytest.mark.ai_generated
def test_append_twice_is_text_without_nul_and_earlier_chunks_unchanged(tmp_path):
    store = tmp_path / "out.zarr"
    zarr_store.append_lines(store, make_lines(0, 100), chunk_bytes=CHUNK)
    first = {path.name: path.read_bytes() for path in chunk_files(store)}
    zarr_store.append_lines(store, make_lines(100, 100), chunk_bytes=CHUNK)
    second = {path.name: path.read_bytes() for path in chunk_files(store)}

    assert len(second) > len(first)
    assert all(b"\x00" not in data for data in second.values())
    assert all(data.decode("utf-8") is not None for data in second.values())
    last_of_first = max(first, key=int)
    for name in first:
        if name != last_of_first:
            assert second[name] == first[name]
    assert [json.loads(line) for line in zarr_store.iter_lines(store)] == [json.loads(x) for x in make_lines(0, 200)]


@pytest.mark.ai_generated
def test_rollover_at_chunk_size(tmp_path):
    store = tmp_path / "out.zarr"
    added = zarr_store.append_lines(store, make_lines(0, 100), chunk_bytes=CHUNK)
    names = [path.name for path in chunk_files(store)]
    assert names == [str(index) for index in range((added + CHUNK - 1) // CHUNK)]
    assert all(path.stat().st_size == CHUNK for path in chunk_files(store))


@pytest.mark.ai_generated
def test_last_wins_and_blank_lines_skipped(tmp_path):
    store = tmp_path / "out.zarr"
    zarr_store.append_lines(store, ['{"a": 1}', '{"b": 2}'], chunk_bytes=CHUNK)
    zarr_store.append_lines(store, ['{"a": 3}'], chunk_bytes=CHUNK)
    assert zarr_store.read_lookup(store) == {"a": 3, "b": 2}
    assert list(zarr_store.iter_lines(store)) == ['{"a": 1}', '{"b": 2}', '{"a": 3}']


@pytest.mark.ai_generated
def test_missing_store_reads_empty(tmp_path):
    assert zarr_store.read_lookup(tmp_path / "none.zarr") == {}


@pytest.mark.ai_generated
def test_no_record_straddles_a_chunk_boundary(tmp_path):
    store = tmp_path / "out.zarr"
    lines = [json.dumps({f"{index:03d}": "x" * (40 + index % 50)}) for index in range(60)]
    zarr_store.append_lines(store, lines[:30], chunk_bytes=300)
    zarr_store.append_lines(store, lines[30:], chunk_bytes=300)

    assert len(chunk_files(store)) > 3
    seen = []
    for path in chunk_files(store):
        for line in path.read_text().splitlines():
            if line.strip():
                seen.append(json.loads(line))
    assert seen == [json.loads(line) for line in lines]
    assert list(zarr_store.iter_lines(store)) == lines


@pytest.mark.ai_generated
def test_a_record_longer_than_a_chunk_is_refused(tmp_path):
    with pytest.raises(ValueError, match="does not fit"):
        zarr_store.append_lines(tmp_path / "out.zarr", [json.dumps({"k": "x" * 200})], chunk_bytes=100)


@pytest.mark.ai_generated
def test_consolidated_metadata_tracks_the_length_after_each_append(tmp_path):
    store = tmp_path / "out.zarr"
    for start in (0, 50):
        zarr_store.append_lines(store, make_lines(start, 50), chunk_bytes=CHUNK)
        consolidated = json.loads((store / "zarr.json").read_text())["consolidated_metadata"]["metadata"]
        assert consolidated["stream"]["shape"] == [zarr.open_group(store, mode="r")["stream"].shape[0]]


def git(repo: pathlib.Path, *args: str) -> str:
    result = subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True, text=True)
    return result.stdout


@pytest.mark.ai_generated
def test_git_sees_chunks_as_text_and_second_append_changes_only_tail(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    git(repo, "init", "-q")
    git(repo, "config", "user.email", "t@example.com")
    git(repo, "config", "user.name", "t")
    store = repo / "out.zarr"
    zarr_store.append_lines(store, make_lines(0, 100), chunk_bytes=CHUNK)
    git(repo, "add", "-A")
    git(repo, "commit", "-q", "-m", "first")
    first_count = len(chunk_files(store))

    zarr_store.append_lines(store, make_lines(100, 100), chunk_bytes=CHUNK)
    git(repo, "add", "-A")
    numstat = git(repo, "diff", "--cached", "--numstat").splitlines()
    rows = {row.split("\t")[2]: row.split("\t")[:2] for row in numstat}

    chunk_rows = {name: counts for name, counts in rows.items() if "/c/" in name}
    assert all(counts[0].isdigit() and counts[1].isdigit() for counts in chunk_rows.values())
    assert all(counts[0] != "-" for counts in chunk_rows.values())
    changed = sorted(int(name.rsplit("/", 1)[1]) for name in chunk_rows)
    old_last = first_count - 1
    assert changed[0] == old_last
    assert changed == list(range(old_last, len(chunk_files(store))))
