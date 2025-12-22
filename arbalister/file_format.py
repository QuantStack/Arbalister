import enum
import pathlib
from typing import Self


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
    Sqlite = "sqlite"

    @classmethod
    def from_filename(cls, file: pathlib.Path | str) -> Self:
        """Get the file format from a filename extension."""
        file_type = pathlib.Path(file).suffix.removeprefix(".").strip().lower()

        # Match again their default value
        if ft := next((ft for ft in FileFormat if str(ft) == file_type), None):
            return ft
        # Match other known values
        match file_type:
            case "ipc" | "feather" | "arrow":
                return cls.Ipc
            case "sqlite3" | "db" | "db3" | "s3db" | "sl3":
                return cls.Sqlite
        raise ValueError(f"Unknown file type {file_type}")
