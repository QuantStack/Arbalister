import dataclasses
import json
import pathlib
import random
import string
from typing import Awaitable, Callable, Final

import pyarrow as pa
import pytest
import tornado

import arbalister as arb


@pytest.fixture(params=list(arb.arrow.FileFormat), scope="session")
def file_format(request: pytest.FixtureRequest) -> arb.arrow.FileFormat:
    """Parametrize the file format used in the test."""
    out: arb.arrow.FileFormat = request.param
    return out


DUMMY_TABLE_ROW_COUNT: Final = 10
DUMMY_TABLE_COL_COUNT: Final = 4


@pytest.fixture(scope="module")
def dummy_table() -> pa.Table:
    """Generate a table with fake data."""
    data = {
        "lower": random.choices(string.ascii_lowercase, k=DUMMY_TABLE_ROW_COUNT),
        "sequence": list(range(DUMMY_TABLE_ROW_COUNT)),
        "upper": random.choices(string.ascii_uppercase, k=DUMMY_TABLE_ROW_COUNT),
        "number": [random.random() for _ in range(DUMMY_TABLE_ROW_COUNT)],
    }
    table = pa.table(data)
    assert len(table.schema) == DUMMY_TABLE_COL_COUNT
    return table


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


@pytest.mark.parametrize(
    "params",
    [
        arb.routes.IpcParams(),
        # Limit only number of rows
        arb.routes.IpcParams(row_chunk=0, row_chunk_size=3),
        arb.routes.IpcParams(row_chunk=1, row_chunk_size=2),
        arb.routes.IpcParams(row_chunk=0, row_chunk_size=DUMMY_TABLE_ROW_COUNT),
        arb.routes.IpcParams(row_chunk=1, row_chunk_size=DUMMY_TABLE_ROW_COUNT // 2 + 1),
        # Limit only number of cols
        arb.routes.IpcParams(col_chunk=0, col_chunk_size=3),
        arb.routes.IpcParams(col_chunk=1, col_chunk_size=2),
        arb.routes.IpcParams(col_chunk=0, col_chunk_size=DUMMY_TABLE_COL_COUNT),
        arb.routes.IpcParams(col_chunk=1, col_chunk_size=DUMMY_TABLE_COL_COUNT // 2 + 1),
        # Limit both
        arb.routes.IpcParams(
            row_chunk=0,
            row_chunk_size=3,
            col_chunk=1,
            col_chunk_size=DUMMY_TABLE_COL_COUNT // 2 + 1,
        ),
        arb.routes.IpcParams(
            row_chunk=0,
            row_chunk_size=DUMMY_TABLE_ROW_COUNT,
            col_chunk=1,
            col_chunk_size=2,
        ),
        # Schema only
        arb.routes.IpcParams(
            row_chunk=0,
            row_chunk_size=0,
        ),
    ],
)
async def test_ipc_route_limit_row(
    jp_fetch: JpFetch,
    dummy_table: pa.Table,
    dummy_table_file: pathlib.Path,
    params: arb.routes.IpcParams,
) -> None:
    """Test fetching a file returns the limited rows and columns in IPC."""
    response = await jp_fetch(
        "arrow/stream",
        str(dummy_table_file),
        params={k: v for k, v in dataclasses.asdict(params).items() if v is not None},
    )

    assert response.code == 200
    assert response.headers["Content-Type"] == "application/vnd.apache.arrow.stream"
    payload = pa.ipc.open_stream(response.body).read_all()

    expected = dummy_table

    # Row slicing
    if (size := params.row_chunk_size) is not None and (cidx := params.row_chunk) is not None:
        expected_num_rows = min((size * (cidx + 1)), expected.num_rows) - (size * cidx)
        assert payload.num_rows == expected_num_rows
        expected = expected.slice(cidx * size, size)

    # Col slicing
    if (size := params.col_chunk_size) is not None and (cidx := params.col_chunk) is not None:
        expected_num_cols = min((size * (cidx + 1)), len(expected.schema)) - (size * cidx)
        assert len(payload.schema) == expected_num_cols
        col_names = expected.schema.names
        start = cidx * size
        end = start + size
        expected = expected.select(col_names[start:end])

    assert expected.cast(payload.schema) == payload


async def test_stats_route(jp_fetch: JpFetch, dummy_table: pa.Table, dummy_table_file: pathlib.Path) -> None:
    """Test fetching a file returns the correct metadata in Json."""
    response = await jp_fetch("arrow/stats/", str(dummy_table_file))

    assert response.code == 200
    assert response.headers["Content-Type"] == "application/json; charset=UTF-8"

    payload = json.loads(response.body)
    assert payload["num_cols"] == len(dummy_table.schema)
    assert payload["num_rows"] == dummy_table.num_rows
