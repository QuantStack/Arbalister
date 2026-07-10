import codecs
import importlib.util
import pathlib
from pathlib import Path
from typing import Any, Callable

import datafusion as dn
import pyarrow as pa

from . import file_format as ff

ReadCallable = Callable[..., dn.DataFrame]

# Optional third-party modules required to read each format. Formats not listed here
# rely only on the core dependencies and are always readable.
_READER_REQUIREMENTS: dict[ff.FileFormat, tuple[str, ...]] = {
    ff.FileFormat.Sqlite: ("adbc_driver_manager", "adbc_driver_sqlite"),
}


def missing_requirements(format: ff.FileFormat) -> list[str]:
    """Return the modules required to read the format that are not installed."""
    return [
        module for module in _READER_REQUIREMENTS.get(format, ()) if importlib.util.find_spec(module) is None
    ]


def _read_csv(
    ctx: dn.SessionContext, path: str | pathlib.Path, delimiter: str, **kwargs: dict[str, Any]
) -> dn.DataFrame:
    if len(delimiter) > 1:
        delimiter = codecs.decode(delimiter, "unicode_escape")
    return ctx.read_csv(path, delimiter=delimiter, **kwargs)  # type: ignore[arg-type]


def _read_ipc(ctx: dn.SessionContext, path: str | pathlib.Path, **kwargs: dict[str, Any]) -> dn.DataFrame:
    import pyarrow.feather

    #  table = pyarrow.feather.read_table(path, {**{"memory_map": True}, **kwargs})
    table = pyarrow.feather.read_table(path, **kwargs)  # type: ignore[no-untyped-call]
    return ctx.from_arrow(table)


def _read_orc(ctx: dn.SessionContext, path: str | pathlib.Path, **kwargs: dict[str, Any]) -> dn.DataFrame:
    # Watch for https://github.com/datafusion-contrib/datafusion-orc
    # Evolution for native datafusion reader
    import pyarrow.orc

    table = pyarrow.orc.read_table(path, **kwargs)  # type: ignore[no-untyped-call]
    return ctx.from_arrow(table)


def get_table_reader(format: ff.FileFormat) -> ReadCallable:
    """Get the datafusion reader factory function for the given format."""
    # TODO: datafusion >= 50.0
    #  def read(ctx: dtfn.SessionContext, path: str | pathlib.Path, *args, **kwargs) -> dtfn.DataFrame:
    #      ds = pads.dataset(source=path, format=format.value)
    #      return ctx.read_table(ds, *args, **kwargs)
    out: ReadCallable
    match format:
        case ff.FileFormat.Avro:
            out = dn.SessionContext.read_avro
        case ff.FileFormat.Csv:
            out = _read_csv
        case ff.FileFormat.Parquet:
            out = dn.SessionContext.read_parquet
        case ff.FileFormat.Ipc:
            out = _read_ipc
        case ff.FileFormat.Orc:
            out = _read_orc
        case ff.FileFormat.Sqlite:
            from . import adbc as adbc

            # FIXME: For now we just pretend SqliteDataFrame is a datafusion DataFrame
            # Either we integrate it properly into Datafusion, or we create a DataFrame as a
            # typing.protocol.
            out = adbc.SqliteDataFrame.read_sqlite  # type: ignore[assignment]

    return out


WriteCallable = Callable[..., None]


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


def get_table_writer(format: ff.FileFormat) -> WriteCallable:
    """Get the arrow writer factory function for the given format."""
    out: WriteCallable
    match format:
        case ff.FileFormat.Avro:
            out = _write_avro
        case ff.FileFormat.Csv:
            import pyarrow.csv

            def write_csv(
                data: pa.Table,
                output_file: str | pathlib.Path,
                memory_pool: pa.MemoryPool | None = None,
                **kwargs: dict[str, Any],
            ) -> None:
                pyarrow.csv.write_csv(  # type: ignore[attr-defined]
                    data=data,
                    output_file=str(output_file),
                    memory_pool=memory_pool,
                    write_options=pyarrow.csv.WriteOptions(**kwargs),  # type: ignore[attr-defined]
                )

            out = write_csv
        case ff.FileFormat.Parquet:
            import pyarrow.parquet

            out = pyarrow.parquet.write_table
        case ff.FileFormat.Ipc:
            import pyarrow.feather

            out = pyarrow.feather.write_feather
        case ff.FileFormat.Orc:
            import pyarrow.orc

            out = pyarrow.orc.write_table
        case ff.FileFormat.Sqlite:
            from . import adbc as adbc

            out = adbc.write_sqlite
    return out

def get_parquet_column_stats(path:str | Path)->list[dict[str, Any]]:
    """Get parquet column stats."""
    import pyarrow.parquet as pq

    if isinstance(path, Path):
        path = str(path)

    file = pq.ParquetFile(path)
    metadata = file.metadata
    schema = file.schema
    columns_stats = []
    for i, field in enumerate(schema):
        min_val = None
        max_val = None
        null_count = 0

        for row_group_index in range(metadata.num_row_groups):
            row = metadata.row_group(row_group_index)
            col_chunk = row.column(i)
            stats = col_chunk.statistics
            if stats:
                if min_val is None or stats.min < min_val:
                    min_val = stats.min
                if max_val is None or stats.max < max_val:
                    max_val = stats.max
                null_count +=stats.null_count if stats.null_count is not None else 0
            columns_stats.append({
                "name":field.name,
                "min":min_val,
                "max": max_val,
                "null_count": null_count
            })
    return columns_stats
