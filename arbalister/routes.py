import dataclasses
import os
import pathlib

import datafusion as dtfn
import datafusion.functions as fn
import jupyter_server.base.handlers
import jupyter_server.serverapp
import pyarrow as pa
import tornado
from jupyter_server.utils import url_path_join

from . import arrow as abw
from . import params as params


class BaseRouteHandler(jupyter_server.base.handlers.APIHandler):
    """A base handler to share common methods."""

    def initialize(self, context: dtfn.SessionContext) -> None:
        """Process custom constructor arguments."""
        super().initialize()
        self.context = context

    def make(self) -> dtfn.SessionConfig:
        """Return the datafusion config."""
        config = dtfn.SessionConfig()
        # String views do not get written properly to IPC
        config.set("datafusion.execution.parquet.schema_force_view_types", "false")
        return config

    def data_file(self, path: str) -> pathlib.Path:
        """Return the file that is requested by the URL path."""
        root_dir = pathlib.Path(os.path.expanduser(self.settings["server_root_dir"])).resolve()
        return root_dir / path

    def dataframe(self, path: str) -> dtfn.DataFrame:
        """Return the DataFusion lazy DataFrame.

        Note: On some file type, the file is read eagerly when calling this method.
        """
        file = self.data_file(path)
        read_table = abw.get_table_reader(format=abw.FileFormat.from_filename(file))
        return read_table(self.context, file)

    def get_query_params_as[T](self, dataclass_type: type[T]) -> T:
        """Extract query parameters into a dataclass type."""
        return params.build_dataclass(dataclass_type, self.get_query_argument)


@dataclasses.dataclass(frozen=True, slots=True)
class IpcParams:
    """Query parameter for IPC data."""

    per_chunk: int | None = None
    chunk: int | None = None


class IpcRouteHandler(BaseRouteHandler):
    """An handler to get file in IPC."""

    @tornado.web.authenticated
    async def get(self, path: str) -> None:
        """HTTP GET return an IPC file."""
        params = self.get_query_params_as(IpcParams)

        self.set_header("Content-Type", "application/vnd.apache.arrow.stream")

        df: dtfn.DataFrame = self.dataframe(path)

        if params.per_chunk is not None and params.chunk is not None:
            offset: int = params.chunk * params.per_chunk
            df = df.limit(count=params.per_chunk, offset=offset)

        table: pa.Table = df.to_arrow_table()

        # TODO can we write directly to socket and send chunks
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)

        buf: pa.Buffer = sink.getvalue()
        self.write(buf.to_pybytes())  # FIXME to_pybytes copies memory

        await self.flush()


@dataclasses.dataclass(frozen=True, slots=True)
class StatsResponse:
    """File statistics returned in the stats route."""

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
                batches = df.aggregate([], [fn.count(dtfn.col(first_col))]).collect()
                num_rows = batches[0].column(0)[0].as_py()

        response = StatsResponse(num_cols=len(schema), num_rows=num_rows)
        await self.finish(dataclasses.asdict(response))


def make_datafusion_config() -> dtfn.SessionConfig:
    """Return the datafusion config."""
    config = dtfn.SessionConfig()
    # String views do not get written properly to IPC
    config.set("datafusion.execution.parquet.schema_force_view_types", "false")
    return config


def setup_route_handlers(web_app: jupyter_server.serverapp.ServerWebApplication) -> None:
    """Jupyter server setup entry point."""
    host_pattern = ".*$"
    base_url = web_app.settings["base_url"]

    context = dtfn.SessionContext(make_datafusion_config())

    handlers = [
        (url_path_join(base_url, r"arrow/stream/([^?]*)"), IpcRouteHandler, {"context": context}),
        (url_path_join(base_url, r"arrow/stats/([^?]*)"), StatsRouteHandler, {"context": context}),
    ]

    web_app.add_handlers(host_pattern, handlers)  # type: ignore[no-untyped-call]
