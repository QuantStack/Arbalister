import base64
import dataclasses
import os
import pathlib

import datafusion as dn
import datafusion.functions as dnf
import jupyter_server.base.handlers
import jupyter_server.serverapp
import pyarrow as pa
import tornado
from jupyter_server.utils import url_path_join

from . import arrow as abw
from . import file_format as ff
from . import params as params


@dataclasses.dataclass(frozen=True, slots=True)
class Empty:
    """An empty data class."""


@dataclasses.dataclass(frozen=True, slots=True)
class SqliteOptions:
    """Query parameter for the Sqlite reader."""

    table_name: str | None = None


@dataclasses.dataclass(frozen=True, slots=True)
class CsvOptions:
    """Query parameter for the CSV reader."""

    delimiter: str | None = ","


FileOptions = SqliteOptions | CsvOptions | Empty


class BaseRouteHandler(jupyter_server.base.handlers.APIHandler):
    """A base handler to share common methods."""

    def initialize(self, context: dn.SessionContext) -> None:
        """Process custom constructor arguments."""
        super().initialize()
        self.context = context

    def data_file(self, path: str) -> pathlib.Path:
        """Return the file that is requested by the URL path."""
        root_dir = pathlib.Path(os.path.expanduser(self.settings["server_root_dir"])).resolve()
        return root_dir / path

    def dataframe(self, path: str) -> dn.DataFrame:
        """Return the DataFusion lazy DataFrame.

        Note: On some file type, the file is read eagerly when calling this method.
        """
        file = self.data_file(path)
        file_format = ff.FileFormat.from_filename(file)
        file_params = self.get_file_read_params(file_format)
        read_table = abw.get_table_reader(format=file_format)
        return read_table(self.context, file, **dataclasses.asdict(file_params))

    def get_query_params_as[T](self, dataclass_type: type[T]) -> T:
        """Extract query parameters into a dataclass type."""
        return params.build_dataclass(dataclass_type, self.get_query_argument)

    def get_file_read_params(self, file_format: ff.FileFormat) -> FileOptions:
        """Read the parameters associated with the relevant file format."""
        match file_format:
            case ff.FileFormat.Sqlite:
                return self.get_query_params_as(SqliteOptions)
            case ff.FileFormat.Csv:
                return self.get_query_params_as(CsvOptions)
        return Empty()


@dataclasses.dataclass(frozen=True, slots=True)
class IpcParams:
    """Query parameter for IPC data."""

    row_chunk_size: int | None = None
    row_chunk: int | None = None
    col_chunk_size: int | None = None
    col_chunk: int | None = None


class IpcRouteHandler(BaseRouteHandler):
    """An handler to get file in IPC."""

    @tornado.web.authenticated
    async def get(self, path: str) -> None:
        """HTTP GET return an IPC file."""
        params = self.get_query_params_as(IpcParams)

        self.set_header("Content-Type", "application/vnd.apache.arrow.stream")

        df: dn.DataFrame = self.dataframe(path)

        if params.row_chunk_size is not None and params.row_chunk is not None:
            offset: int = params.row_chunk * params.row_chunk_size
            df = df.limit(count=params.row_chunk_size, offset=offset)

        if params.col_chunk_size is not None and params.col_chunk is not None:
            col_names = df.schema().names
            start: int = params.col_chunk * params.col_chunk_size
            end: int = start + params.col_chunk_size
            df = df.select(*col_names[start:end])

        table: pa.Table = df.to_arrow_table()

        # TODO can we write directly to socket and send chunks
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)

        buf: pa.Buffer = sink.getvalue()
        self.write(buf.to_pybytes())  # FIXME to_pybytes copies memory

        await self.flush()


@dataclasses.dataclass(frozen=True, slots=True)
class SchemaInfo:
    """Schema information as a zero-row IPC stream."""

    data: str
    mimetype: str = "application/vnd.apache.arrow.stream"
    encoding: str = "base64"


@dataclasses.dataclass(frozen=True, slots=True)
class StatsResponse:
    """File statistics returned in the stats route."""

    schema: SchemaInfo
    num_rows: int = 0
    num_cols: int = 0


class StatsRouteHandler(BaseRouteHandler):
    """An handler to get file in IPC."""

    @tornado.web.authenticated
    async def get(self, path: str) -> None:
        """HTTP GET return statistics."""
        df = self.dataframe(path)

        # FIXME this is not optimal for ORC/CSV where we can read_metadata, but it is not read
        # via DataFusion.
        schema = df.schema()
        try:
            num_rows = df.count()
        # Workaround issue in Avro files df.count() not working
        except Exception as e:
            if len(schema.names) == 0:
                num_rows = 0
            # No dedicated exception type coming from DataFusion
            if str(e).startswith("DataFusion"):
                first_col: str = schema.names[0]
                batches = df.aggregate([], [dnf.count(dn.col(first_col))]).collect()
                num_rows = batches[0].column(0)[0].as_py()

        # Create a zero-row IPC stream with the table schema
        zero_row_table = df.limit(0, 0).to_arrow_table()

        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, schema) as writer:
            writer.write_table(zero_row_table)

        buf: pa.Buffer = sink.getvalue()
        schema_64 = base64.b64encode(buf.to_pybytes()).decode("utf-8")

        response = StatsResponse(
            num_cols=len(schema),
            num_rows=num_rows,
            schema=SchemaInfo(data=schema_64),
        )
        await self.finish(dataclasses.asdict(response))


@dataclasses.dataclass(frozen=True, slots=True)
class SqliteFileInfo:
    """Sqlite specific information about a file."""

    table_names: list[str]


@dataclasses.dataclass(frozen=True, slots=True)
class CsvFileInfo:
    """Csv specific information about a file."""

    delimiters: list[str] = dataclasses.field(default_factory=lambda: [",", ";", "\\t", "|", "#"])


FileInfo = SqliteFileInfo


@dataclasses.dataclass(frozen=True, slots=True)
class FileInfoResponse[I, P]:
    """File-specific information and defaults returned in the file info route."""

    info: I
    read_params: P


CsvFileInfoResponse = FileInfoResponse[CsvFileInfo, CsvOptions]
SqliteFileInfoResponse = FileInfoResponse[SqliteFileInfo, SqliteOptions]

NoFileInfoResponse = FileInfoResponse[Empty, Empty]


class FileInfoRouteHandler(BaseRouteHandler):
    """A handler to get file-specific information."""

    @tornado.web.authenticated
    async def get(self, path: str) -> None:
        """HTTP GET return file-specific information."""
        file = self.data_file(path)
        file_format = ff.FileFormat.from_filename(file)

        match file_format:
            case ff.FileFormat.Csv:
                info = CsvFileInfo()
                csv_response = CsvFileInfoResponse(
                    info=info,
                    read_params=CsvOptions(delimiter=info.delimiters[0]),
                )
                await self.finish(dataclasses.asdict(csv_response))
            case ff.FileFormat.Sqlite:
                from . import adbc

                table_names = adbc.SqliteDataFrame.get_table_names(file)

                sqlite_response = SqliteFileInfoResponse(
                    info=SqliteFileInfo(table_names=table_names),
                    read_params=SqliteOptions(table_name=table_names[0]),
                )
                await self.finish(dataclasses.asdict(sqlite_response))
            case _:
                no_response = NoFileInfoResponse(info=Empty(), read_params=Empty())
                await self.finish(dataclasses.asdict(no_response))


def make_datafusion_config() -> dn.SessionConfig:
    """Return the datafusion config."""
    config = (
        dn.SessionConfig()
        # Must use a single partition otherwise limit parallelism will return arbitrary rows
        .with_target_partitions(1)
        # String views do not get written properly to IPC
        .set("datafusion.execution.parquet.schema_force_view_types", "false")
    )

    return config


def setup_route_handlers(web_app: jupyter_server.serverapp.ServerWebApplication) -> None:
    """Jupyter server setup entry point."""
    host_pattern = ".*$"
    base_url = web_app.settings["base_url"]

    context = dn.SessionContext(make_datafusion_config())

    handlers = [
        (url_path_join(base_url, r"arrow/stream/([^?]*)"), IpcRouteHandler, {"context": context}),
        (url_path_join(base_url, r"arrow/stats/([^?]*)"), StatsRouteHandler, {"context": context}),
        (url_path_join(base_url, r"file/info/([^?]*)"), FileInfoRouteHandler, {"context": context}),
    ]

    web_app.add_handlers(host_pattern, handlers)  # type: ignore[no-untyped-call]
