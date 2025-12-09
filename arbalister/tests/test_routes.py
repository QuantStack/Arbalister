import json
import pathlib
from typing import Awaitable, Callable

import pyarrow as pa
import pytest
import tornado

import arbalister as arb


@pytest.fixture(params=list(arb.arrow.FileFormat))
def file_format(request: pytest.FixtureRequest) -> arb.arrow.FileFormat:
    """Parametrize the file format used in the test."""
    out: arb.arrow.FileFormat = request.param
    return out


@pytest.fixture
def dummy_table() -> pa.Table:
    """Generate a table with fake data."""
    data = {
        "letter": list("abcdefghij"),
        "number": list(range(10)),
    }
    return pa.table(data)


@pytest.fixture
def dummy_table_file(
    jp_root_dir: pathlib.Path, dummy_table: pa.Table, file_format: arb.arrow.FileFormat
) -> pathlib.Path:
    """Write the dummy table to file."""
    write_table = arb.arrow.get_table_writer(file_format)
    table_path = jp_root_dir / f"test.{str(file_format).lower()}"
    write_table(dummy_table, table_path)
    return table_path.relative_to(jp_root_dir)


JpFetch = Callable[..., Awaitable[tornado.httpclient.HTTPResponse]]


async def test_ipc_route(jp_fetch: JpFetch, dummy_table: pa.Table, dummy_table_file: pathlib.Path) -> None:
    """Test fetching a file returns the correct data in IPC."""
    response = await jp_fetch("arrow/stream/", str(dummy_table_file))

    assert response.code == 200
    assert response.headers["Content-Type"] == "application/vnd.apache.arrow.stream"

    payload = pa.ipc.open_stream(response.body).read_all()
    assert dummy_table.num_rows == payload.num_rows
    assert dummy_table.cast(payload.schema) == payload


async def test_ipc_route_limit(
    jp_fetch: JpFetch, dummy_table: pa.Table, dummy_table_file: pathlib.Path
) -> None:
    """Test fetching a file returns the limited data in IPC."""
    num_rows = 2
    chunk = 1
    response = await jp_fetch(
        "arrow/stream",
        str(dummy_table_file),
        params={"per_chunk": num_rows, "chunk": chunk},
    )

    assert response.code == 200
    assert response.headers["Content-Type"] == "application/vnd.apache.arrow.stream"

    payload = pa.ipc.open_stream(response.body).read_all()
    assert payload.num_rows == num_rows
    assert dummy_table.slice(chunk * num_rows, num_rows).cast(payload.schema) == payload


async def test_stats_route(jp_fetch: JpFetch, dummy_table: pa.Table, dummy_table_file: pathlib.Path) -> None:
    """Test fetching a file returns the correct metadata in Json."""
    response = await jp_fetch("arrow/stats/", str(dummy_table_file))

    assert response.code == 200
    assert response.headers["Content-Type"] == "application/json; charset=UTF-8"

    payload = json.loads(response.body)
    assert payload["num_cols"] == len(dummy_table.schema)
    assert payload["num_rows"] == dummy_table.num_rows
