import enum
import pathlib
from typing import Any, Callable, Self

import datafusion as dn
import pyarrow as pa


class FileFormat(enum.StrEnum):
    """Known file format that we can read into an Arrow format.

    Todo:
    - ADBC (Sqlite/Postgres)

    """

    Avro = "avro"
    Csv = "csv"
    Ipc = "ipc"
    Orc = "orc"
    Parquet = "parquet"

    @classmethod
    def from_filename(cls, file: pathlib.Path | str) -> Self:
        """Get the file format from a filename extension."""
        file_type = pathlib.Path(file).suffix.removeprefix(".").strip().lower()

        # Match again their default value
        if ft := next((ft for ft in FileFormat if str(ft) == file_type), None):
            return ft
        # Match other known values
        match file_type:
            case "ipc" | "feather":
                return cls.Ipc
        raise ValueError(f"Unknown file type {file_type}")


ReadCallable = Callable[..., dn.DataFrame]


def _arrow_to_avro_type(field: pa.Field) -> str | dict[str, Any]:
    t = field.type
    if pa.types.is_integer(t):
        return "long" if t.bit_width > 32 else "int"
    if pa.types.is_floating(t):
        return "double" if t.bit_width > 32 else "float"
    if pa.types.is_boolean(t):
        return "boolean"
    if pa.types.is_string(t):
        return "string"
    if pa.types.is_binary(t) or pa.types.is_large_binary(t):
        return "bytes"
    if pa.types.is_timestamp(t):
        return "long"
    if pa.types.is_list(t) or pa.types.is_large_list(t):
        item = _arrow_to_avro_type(pa.field("item", t.value_type))
        return {"type": "array", "items": item}
    # fallback
    return "string"


def _write_avro(
    table: pa.Table, path: str | pathlib.Path, name: str = "Record", namespace: str = "ns"
) -> None:
    # Avro writing is an optional dependency not added by default as it is only necessary during testing
    import json

    import avro.schema
    from avro.datafile import DataFileWriter
    from avro.io import DatumWriter

    schema = {
        "type": "record",
        "name": name,
        "namespace": namespace,
        "fields": [{"name": f.name, "type": _arrow_to_avro_type(f)} for f in table.schema],
    }
    schema_parsed = avro.schema.parse(json.dumps(schema))
    recs = table.to_pylist()
    with open(path, "wb") as f:
        writer = DataFileWriter(f, DatumWriter(), schema_parsed)
        for rec in recs:
            writer.append(rec)
        writer.close()


def get_table_reader(format: FileFormat) -> ReadCallable:
    """Get the datafusion reader factory function for the given format."""
    # TODO: datafusion >= 50.0
    #  def read(ctx: dtfn.SessionContext, path: str | pathlib.Path, *args, **kwargs) -> dtfn.DataFrame:
    #      ds = pads.dataset(source=path, format=format.value)
    #      return ctx.read_table(ds, *args, **kwargs)
    out: ReadCallable
    match format:
        case FileFormat.Avro:
            out = dn.SessionContext.read_avro
        case FileFormat.Csv:
            out = dn.SessionContext.read_csv
        case FileFormat.Parquet:
            out = dn.SessionContext.read_parquet
        case FileFormat.Ipc:
            import pyarrow.feather

            def read_ipc(
                ctx: dn.SessionContext, path: str | pathlib.Path, **kwargs: dict[str, Any]
            ) -> dn.DataFrame:
                #  table = pyarrow.feather.read_table(path, {**{"memory_map": True}, **kwargs})
                table = pyarrow.feather.read_table(path, **kwargs)
                return ctx.from_arrow(table)

            out = read_ipc
        case FileFormat.Orc:
            # Watch for https://github.com/datafusion-contrib/datafusion-orc
            # Evolution for native datafusion reader
            import pyarrow.orc

            def read_orc(
                ctx: dn.SessionContext, path: str | pathlib.Path, **kwargs: dict[str, Any]
            ) -> dn.DataFrame:
                table = pyarrow.orc.read_table(path, **kwargs)
                return ctx.from_arrow(table)

            out = read_orc

    return out


WriteCallable = Callable[..., None]


def get_table_writer(format: FileFormat) -> WriteCallable:
    """Get the arrow writer factory function for the given format."""
    out: WriteCallable
    match format:
        case FileFormat.Avro:
            out = _write_avro
        case FileFormat.Csv:
            import pyarrow.csv

            out = pyarrow.csv.write_csv
        case FileFormat.Parquet:
            import pyarrow.parquet

            out = pyarrow.parquet.write_table
        case FileFormat.Ipc:
            import pyarrow.feather

            out = pyarrow.feather.write_feather
        case FileFormat.Orc:
            import pyarrow.orc

            out = pyarrow.orc.write_table
    return out
