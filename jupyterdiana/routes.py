import pathlib

import jupyter_server.base.handlers
import jupyter_server.serverapp
import pyarrow as pa
import tornado
from jupyter_server.utils import url_path_join

from . import arrow as ja


class IpcRouteHandler(jupyter_server.base.handlers.APIHandler):
    """An handler to get file in IPC."""

    @tornado.web.authenticated
    async def get(self, path: str) -> None:
        """HTTP GET return an IPC file."""
        root_dir = pathlib.Path(self.settings["server_root_dir"])
        file = root_dir / path

        # TODO consider stream
        self.set_header("Content-Type", "application/vnd.apache.arrow.file")

        read_table = ja.get_table_reader(format=ja.FileFormat.from_filename(file))
        table: pa.Table = read_table(file)

        # TODO can we write directly to socket and send chunks
        sink = pa.BufferOutputStream()
        with pa.ipc.new_file(sink, table.schema) as writer:
            writer.write_table(table)

        buf: pa.Buffer = sink.getvalue()
        self.write(buf.to_pybytes())

        await self.flush()


def setup_route_handlers(web_app: jupyter_server.serverapp.ServerWebApplication) -> None:
    """Jupyter server setup entry point."""
    host_pattern = ".*$"
    base_url = web_app.settings["base_url"]

    arrow_route_pattern = url_path_join(base_url, "jupyterdiana/ipc/(.*)")
    handlers = [(arrow_route_pattern, IpcRouteHandler)]

    web_app.add_handlers(host_pattern, handlers)  # type: ignore[no-untyped-call]
