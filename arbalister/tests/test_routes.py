import base64
import dataclasses
import json
import pathlib
import random
import string
from typing import Awaitable, Callable

import pyarrow as pa
import pytest
import tornado

import arbalister as arb
import arbalister.file_format as ff


@pytest.fixture(
    params=[
        (ff.FileFormat.Avro, arb.routes.NoReadParams()),
        (ff.FileFormat.Csv, arb.routes.NoReadParams()),
        (ff.FileFormat.Csv, arb.routes.CSVReadParams(delimiter=";")),
        (ff.FileFormat.Ipc, arb.routes.NoReadParams()),
        (ff.FileFormat.Orc, arb.routes.NoReadParams()),
        (ff.FileFormat.Parquet, arb.routes.NoReadParams()),
        (ff.FileFormat.Sqlite, arb.routes.NoReadParams()),
        (ff.FileFormat.Sqlite, arb.routes.SqliteReadParams(table_name="dummy_table_2")),
    ],
    ids=lambda f_p: f"{f_p[0].value}-{dataclasses.asdict(f_p[1])}",
    scope="module",
)
def file_format_and_params(request: pytest.FixtureRequest) -> tuple[ff.FileFormat, arb.routes.FileReadParams]:
    """Parametrize the file format and file parameters used in the tests.

    This is used to to build test cases with a give set of parameters since each file format may be tested
    with a different number of parameters.
    """
    out: tuple[ff.FileFormat, arb.routes.FileReadParams] = request.param
    return out


@pytest.fixture(scope="module")
def file_format(file_format_and_params: tuple[ff.FileFormat, arb.routes.FileReadParams]) -> ff.FileFormat:
    """Extract the the file format fixture value used in the tests."""
    return file_format_and_params[0]


@pytest.fixture(scope="module")
def file_params(
    file_format_and_params: tuple[ff.FileFormat, arb.routes.FileReadParams],
) -> arb.routes.FileReadParams:
    """Extract the the file parameters fixture value used in the tests."""
    return file_format_and_params[1]


@pytest.fixture(scope="module")
def dummy_table_1(num_rows: int = 10) -> pa.Table:
    """Generate a table with fake data."""
    data = {
        "lower": random.choices(string.ascii_lowercase, k=num_rows),
        "sequence": list(range(num_rows)),
        "upper": random.choices(string.ascii_uppercase, k=num_rows),
        "number": [random.random() for _ in range(num_rows)],
    }
    table = pa.table(data)
    return table


@pytest.fixture(scope="module")
def dummy_table_2(num_rows: int = 13) -> pa.Table:
    """Generate a table with different fake data."""
    data = {
        "id": list(range(num_rows)),
        "flag": [random.choice([True, False]) for _ in range(num_rows)],
        "letter": random.choices(string.ascii_letters, k=num_rows),
        "score": [random.randint(0, 100) for _ in range(num_rows)],
        "timestamp": [random.randint(1_600_000_000, 1_700_000_000) for _ in range(num_rows)],
    }
    table = pa.table(data)
    return table


@pytest.fixture(scope="module")
def full_table(file_params: ff.FileFormat, dummy_table_1: pa.Table, dummy_table_2: pa.Table) -> pa.Table:
    """Return the full table on which we are executed queries."""
    if isinstance(file_params, arb.routes.SqliteReadParams):
        return {
            "dummy_table_1": dummy_table_1,
            "dummy_table_2": dummy_table_2,
        }[file_params.table_name]
    return dummy_table_1


@pytest.fixture
def table_file(
    jp_root_dir: pathlib.Path,
    dummy_table_1: pa.Table,
    dummy_table_2: pa.Table,
    file_format: ff.FileFormat,
    file_params: arb.routes.FileReadParams,
) -> pathlib.Path:
    """Write the dummy table to file."""
    write_table = arb.arrow.get_table_writer(file_format)
    table_path = jp_root_dir / f"test.{str(file_format).lower()}"

    match file_format:
        case ff.FileFormat.Csv:
            write_table(dummy_table_1, table_path, delimiter=getattr(file_params, "delimiter", ","))
        case ff.FileFormat.Sqlite:
            write_table(dummy_table_1, table_path, table_name="dummy_table_1", mode="create_append")
            write_table(dummy_table_2, table_path, table_name="dummy_table_2", mode="create_append")
        case _:
            write_table(dummy_table_1, table_path)

    return table_path.relative_to(jp_root_dir)


JpFetch = Callable[..., Awaitable[tornado.httpclient.HTTPResponse]]


@pytest.fixture(
    params=[
        # No limits
        lambda table: arb.routes.IpcParams(),
        # Limit only number of rows
        lambda table: arb.routes.IpcParams(row_chunk=0, row_chunk_size=3),
        lambda table: arb.routes.IpcParams(row_chunk=1, row_chunk_size=2),
        lambda table: arb.routes.IpcParams(row_chunk=0, row_chunk_size=table.num_rows),
        lambda table: arb.routes.IpcParams(row_chunk=1, row_chunk_size=table.num_rows // 2 + 1),
        # Limit only number of cols
        lambda table: arb.routes.IpcParams(col_chunk=0, col_chunk_size=3),
        lambda table: arb.routes.IpcParams(col_chunk=1, col_chunk_size=2),
        lambda table: arb.routes.IpcParams(col_chunk=0, col_chunk_size=table.num_columns),
        lambda table: arb.routes.IpcParams(col_chunk=1, col_chunk_size=table.num_columns // 2 + 1),
        # Limit both
        lambda table: arb.routes.IpcParams(
            row_chunk=0,
            row_chunk_size=3,
            col_chunk=1,
            col_chunk_size=table.num_columns // 2 + 1,
        ),
        lambda table: arb.routes.IpcParams(
            row_chunk=0,
            row_chunk_size=table.num_rows,
            col_chunk=1,
            col_chunk_size=2,
        ),
        # Schema only
        lambda table: arb.routes.IpcParams(
            row_chunk=0,
            row_chunk_size=0,
        ),
    ]
)
def ipc_params(request: pytest.FixtureRequest, dummy_table_1: pa.Table) -> arb.routes.IpcParams:
    """Parameters used to select the IPC data in the response."""
    make_table: Callable[[pa.Table], arb.routes.IpcParams] = request.param
    return make_table(dummy_table_1)


async def test_ipc_route_limit(
    jp_fetch: JpFetch,
    full_table: pa.Table,
    table_file: pathlib.Path,
    ipc_params: arb.routes.IpcParams,
    file_params: arb.routes.SqliteReadParams,
    file_format: ff.FileFormat,
) -> None:
    """Test fetching a file returns the limited rows and columns in IPC."""
    response = await jp_fetch(
        "arrow/stream",
        str(table_file),
        params={
            k: v
            for k, v in {**dataclasses.asdict(ipc_params), **dataclasses.asdict(file_params)}.items()
            if v is not None
        },
    )

    assert response.code == 200
    assert response.headers["Content-Type"] == "application/vnd.apache.arrow.stream"
    payload = pa.ipc.open_stream(response.body).read_all()

    expected = full_table

    # Row slicing
    if (size := ipc_params.row_chunk_size) is not None and (cidx := ipc_params.row_chunk) is not None:
        expected_num_rows = min((size * (cidx + 1)), expected.num_rows) - (size * cidx)
        assert payload.num_rows == expected_num_rows
        expected = expected.slice(cidx * size, size)

    # Col slicing
    if (size := ipc_params.col_chunk_size) is not None and (cidx := ipc_params.col_chunk) is not None:
        expected_num_cols = min((size * (cidx + 1)), len(expected.schema)) - (size * cidx)
        assert len(payload.schema) == expected_num_cols
        col_names = expected.schema.names
        start = cidx * size
        end = start + size
        expected = expected.select(col_names[start:end])

    assert expected.cast(payload.schema) == payload


async def test_stats_route(
    jp_fetch: JpFetch,
    full_table: pa.Table,
    table_file: pathlib.Path,
    file_params: arb.routes.SqliteReadParams,
    file_format: ff.FileFormat,
) -> None:
    """Test fetching a file returns the correct metadata in Json."""
    response = await jp_fetch(
        "arrow/stats/",
        str(table_file),
        params={k: v for k, v in dataclasses.asdict(file_params).items() if v is not None},
    )

    assert response.code == 200
    assert response.headers["Content-Type"] == "application/json; charset=UTF-8"

    payload = json.loads(response.body)

    assert payload["num_cols"] == len(full_table.schema)
    assert payload["num_rows"] == full_table.num_rows
    assert payload["schema"]["mimetype"] == "application/vnd.apache.arrow.stream"
    assert payload["schema"]["encoding"] == "base64"
    table_64 = base64.b64decode(payload["schema"]["data"])
    table = pa.ipc.open_stream(table_64).read_all()
    assert table.num_rows == 0
    assert table.schema.names == full_table.schema.names


async def test_file_info_route_sqlite(
    jp_fetch: JpFetch,
    table_file: pathlib.Path,
    file_format: ff.FileFormat,
) -> None:
    """Test fetching file info for SQLite files returns table names."""
    response = await jp_fetch("file/info/", str(table_file))

    assert response.code == 200
    assert response.headers["Content-Type"] == "application/json; charset=UTF-8"

    payload = json.loads(response.body)

    if file_format == ff.FileFormat.Sqlite:
        assert payload["table_names"] is not None
        assert isinstance(payload["table_names"], list)
        assert "dummy_table_1" in payload["table_names"]
        assert "dummy_table_2" in payload["table_names"]
