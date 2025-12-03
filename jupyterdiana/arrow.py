import enum
import pathlib
from typing import Callable, Self

import pyarrow as pa


class FileFormat(enum.Enum):
    """Known file format that we can read into an Arrow format.

    Todo:
    - Avro
    - ADBC (Sqlite/Postgres)

    """

    Csv = enum.auto()
    Parquet = enum.auto()
    Ipc = enum.auto()
    Orc = enum.auto()

    @classmethod
    def from_filename(cls, file: pathlib.Path | str) -> Self:
        """Get the file format from a filename extension."""
        file_type = pathlib.Path(file).suffix.removeprefix(".").strip().lower()
        match file_type:
            case "csv":
                return cls.Csv
            case "parquet":
                return cls.Parquet
            case "ipc" | "feather" | "arrow":
                return cls.Ipc
            case "orc":
                return cls.Orc
            case _:
                raise ValueError(f"Unknown file type {file_type}")


ReadCallable = Callable[..., pa.Table]


def get_table_reader(format: FileFormat) -> ReadCallable:
    """Get the arrow reader factory function for the given format."""
    out: ReadCallable
    match format:
        case FileFormat.Csv:
            import pyarrow.csv

            out = pyarrow.csv.read_csv
        case FileFormat.Parquet:
            import pyarrow.parquet

            out = pyarrow.parquet.read_table
        case FileFormat.Ipc:
            import pyarrow.feather

            out = pyarrow.feather.read_table
        case FileFormat.Orc:
            import pyarrow.orc

            out = pyarrow.orc.read_table
    return out


WriteCallable = Callable[..., None]


def get_table_writer(format: FileFormat) -> WriteCallable:
    """Get the arrow writer factory function for the given format."""
    out: WriteCallable
    match format:
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
