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
        (ff.FileFormat.Avro, arb.routes.Empty()),
        (ff.FileFormat.Csv, arb.routes.Empty()),
        (ff.FileFormat.Csv, arb.routes.CsvReadOptions(delimiter=";")),
        (ff.FileFormat.Ipc, arb.routes.Empty()),
        (ff.FileFormat.Orc, arb.routes.Empty()),
        (ff.FileFormat.Parquet, arb.routes.Empty()),
        (ff.FileFormat.Sqlite, arb.routes.Empty()),
        (ff.FileFormat.Sqlite, arb.routes.SqliteReadOptions(table_name="dummy_table_2")),
    ],
    ids=lambda f_p: f"{f_p[0].value}-{dataclasses.asdict(f_p[1])}",
    scope="module",
)
def file_format_and_params(
    request: pytest.FixtureRequest,
) -> tuple[ff.FileFormat, arb.routes.FileReadOptions]:
    """Parametrize the file format and file parameters used in the tests.

    This is used to to build test cases with a give set of parameters since each file format may be tested
    with a different number of parameters.
    """
    out: tuple[ff.FileFormat, arb.routes.FileReadOptions] = request.param
    return out


@pytest.fixture(scope="module")
def file_format(file_format_and_params: tuple[ff.FileFormat, arb.routes.FileReadOptions]) -> ff.FileFormat:
    """Extract the the file format fixture value used in the tests."""
    return file_format_and_params[0]


@pytest.fixture(scope="module")
def file_params(
    file_format_and_params: tuple[ff.FileFormat, arb.routes.FileReadOptions],
) -> arb.routes.FileReadOptions:
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
    if isinstance(file_params, arb.routes.SqliteReadOptions):
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
    file_params: arb.routes.FileReadOptions,
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
        lambda table: arb.routes.IpcParams(start_row=0, end_row=3),
        lambda table: arb.routes.IpcParams(start_row=2, end_row=4),
        lambda table: arb.routes.IpcParams(start_row=0, end_row=table.num_rows),
        lambda table: arb.routes.IpcParams(start_row=table.num_rows // 2, end_row=table.num_rows),
        # Limit only number of cols
        lambda table: arb.routes.IpcParams(start_col=0, end_col=3),
        lambda table: arb.routes.IpcParams(start_col=2, end_col=4),
        lambda table: arb.routes.IpcParams(start_col=0, end_col=table.num_columns),
        lambda table: arb.routes.IpcParams(start_col=table.num_columns // 2, end_col=table.num_columns),
        # Limit both
        lambda table: arb.routes.IpcParams(
            start_row=0,
            end_row=3,
            start_col=table.num_columns // 2,
            end_col=table.num_columns,
        ),
        lambda table: arb.routes.IpcParams(
            start_row=0,
            end_row=table.num_rows,
            start_col=2,
            end_col=4,
        ),
        # Schema only
        lambda table: arb.routes.IpcParams(
            start_row=0,
            end_row=0,
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
    file_params: arb.routes.FileReadOptions,
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
    if (start_row := ipc_params.start_row) is not None and (end_row := ipc_params.end_row) is not None:
        expected_num_rows = min(end_row, expected.num_rows) - start_row
        assert payload.num_rows == expected_num_rows
        expected = expected.slice(start_row, end_row - start_row)

    # Col slicing
    if (start_col := ipc_params.start_col) is not None and (end_col := ipc_params.end_col) is not None:
        expected_num_cols = min(end_col, len(expected.schema)) - start_col
        assert len(payload.schema) == expected_num_cols
        col_names = expected.schema.names
        expected = expected.select(col_names[start_col:end_col])

    assert expected.cast(payload.schema) == payload


async def test_stats_route(
    jp_fetch: JpFetch,
    full_table: pa.Table,
    table_file: pathlib.Path,
    file_params: arb.routes.FileReadOptions,
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
    info = payload["info"]
    default_options = payload["default_options"]

    match file_format:
        case ff.FileFormat.Csv:
            assert isinstance(info["delimiters"], list)
            assert "," in info["delimiters"]
            assert default_options["delimiter"] == info["delimiters"][0]
        case ff.FileFormat.Sqlite:
            assert isinstance(info["table_names"], list)
            assert "dummy_table_1" in info["table_names"]
            assert "dummy_table_2" in info["table_names"]
            assert default_options["table_name"] == info["table_names"][0]
