import json
import pathlib
import random
import string
from typing import Awaitable, Callable

import pyarrow as pa
import pytest
import tornado

import arbalister as arb


@pytest.fixture(params=list(arb.arrow.FileFormat), scope="session")
def file_format(request: pytest.FixtureRequest) -> arb.arrow.FileFormat:
    """Parametrize the file format used in the test."""
    out: arb.arrow.FileFormat = request.param
    return out


DUMMY_TABLE_ROW_COUNT = 10


@pytest.fixture(scope="module")
def dummy_table() -> pa.Table:
    """Generate a table with fake data."""
    data = {
        "lower": random.choices(string.ascii_lowercase, k=DUMMY_TABLE_ROW_COUNT),
        "sequence": list(range(DUMMY_TABLE_ROW_COUNT)),
        "upper": random.choices(string.ascii_uppercase, k=DUMMY_TABLE_ROW_COUNT),
        "number": [random.random() for _ in range(DUMMY_TABLE_ROW_COUNT)],
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


@pytest.mark.parametrize(
    "params",
    [
        arb.routes.IpcParams(row_chunk=0, row_chunk_size=3),
        arb.routes.IpcParams(row_chunk=1, row_chunk_size=2),
        arb.routes.IpcParams(row_chunk=0, row_chunk_size=DUMMY_TABLE_ROW_COUNT),
        arb.routes.IpcParams(row_chunk=1, row_chunk_size=DUMMY_TABLE_ROW_COUNT // 2 + 1),
    ],
)
async def test_ipc_route_limit_row(
    jp_fetch: JpFetch,
    dummy_table: pa.Table,
    dummy_table_file: pathlib.Path,
    params: arb.routes.IpcParams,
) -> None:
    """Test fetching a file returns the limited rows in IPC."""
    response = await jp_fetch(
        "arrow/stream",
        str(dummy_table_file),
        params={"row_chunk_size": params.row_chunk_size, "row_chunk": params.row_chunk},
    )

    assert response.code == 200
    assert response.headers["Content-Type"] == "application/vnd.apache.arrow.stream"
    payload = pa.ipc.open_stream(response.body).read_all()

    if (size := params.row_chunk_size) is not None and (cidx := params.row_chunk) is not None:
        expected_num_rows = min((size * (cidx + 1)), dummy_table.num_rows) - (size * cidx)
        assert payload.num_rows == expected_num_rows
        expected = dummy_table.slice(cidx * size, size)
        assert expected.cast(payload.schema) == payload


async def test_stats_route(jp_fetch: JpFetch, dummy_table: pa.Table, dummy_table_file: pathlib.Path) -> None:
    """Test fetching a file returns the correct metadata in Json."""
    response = await jp_fetch("arrow/stats/", str(dummy_table_file))

    assert response.code == 200
    assert response.headers["Content-Type"] == "application/json; charset=UTF-8"

    payload = json.loads(response.body)
    assert payload["num_cols"] == len(dummy_table.schema)
    assert payload["num_rows"] == dummy_table.num_rows
