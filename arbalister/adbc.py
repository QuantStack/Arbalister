import dataclasses
import pathlib
from typing import Any, Literal, Self

import adbc_driver_sqlite.dbapi as adbc_sqlite
import pyarrow as pa


def write_sqlite(
    table: pa.Table,
    path: str | pathlib.Path,
    table_name: str = "Table",
    catalog_name: str | None = None,
    mode: Literal["append", "create", "replace", "create_append"] = "create",
) -> None:
    """Write a table as an Sqlite file."""
    with adbc_sqlite.connect(str(path)) as connection:
        with connection.cursor() as cursor:
            cursor.adbc_ingest(table_name=table_name, data=table, mode=mode, catalog_name=catalog_name)
        connection.commit()


@dataclasses.dataclass
class SqliteDataFrame:
    """A DataFrame plan on a Sqlite file.

    This plan is made to resemble a Apache Datafusion DataFrame despite some performances penalty.
    This is because in the Rust Datafusion implementation of as a Sqlite contrib which we would
    like to eventually use so this class is only filling in temporarily.
    """

    _path: str
    _table_name: str
    _schema: pa.Schema
    _num_rows: int
    _limit: int | None = None
    _offset: int | None = None
    _select: list[str] | None = None

    @staticmethod
    def get_table_names(path: pathlib.Path | str) -> list[str]:
        """Get the list of table names in a SQLite database."""
        with adbc_sqlite.connect(str(path)) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                return [row for (row,) in cursor.fetchall()]

    @classmethod
    def read_sqlite(cls, context: Any, path: pathlib.Path | str, table_name: str | None = None) -> Self:
        """Read an Sqlite file metadata and start a new DataFrame plan."""
        tables = cls.get_table_names(path)

        if table_name is None and len(tables) > 0:
            table_name = tables[0]
        if table_name not in tables or table_name is None:
            raise ValueError(f"Invalid table name {table_name}")

        with adbc_sqlite.connect(str(path)) as connection:
            with connection.cursor() as cursor:
                cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
                num_rows = cursor.fetchone()[0]  # type: ignore[index]

            schema = connection.adbc_get_table_schema(table_name)

            return cls(_path=str(path), _table_name=table_name, _schema=schema, _num_rows=num_rows)

    def schema(self) -> pa.Schema:
        """Return the :py:class:`pyarrow.Schema` of this DataFrame."""
        return self._schema

    def limit(self, count: int, offset: int = 0) -> Self:
        """Return a new DataFrame with a limited number of rows."""
        return dataclasses.replace(self, _limit=count, _offset=offset)

    def select(self, *columns: str) -> Self:
        """Project arbitrary expressions into a new `DataFrame`."""
        return dataclasses.replace(self, _select=list(columns))

    def count(self) -> int:
        """Return the total number of rows in this DataFrame."""
        return self._num_rows

    def to_arrow_table(self) -> pa.Table:
        """Execute the DataFrame and convert it into an Arrow Table."""
        limit = (
            f"LIMIT {self._limit} OFFSET {self._offset}"
            if self._limit is not None and self._offset is not None
            else ""
        )
        columns = self._select if self._select is not None else ["*"]

        with adbc_sqlite.connect(self._path) as connection:
            with connection.cursor() as cursor:
                cursor.execute(f'SELECT {",".join(columns)} FROM "{self._table_name}" {limit}')
                return cursor.fetch_arrow_table()
