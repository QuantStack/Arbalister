import argparse
import pathlib
import random

import datafusion as dn
import datafusion.functions as dnf
import faker
import pyarrow as pa
import pyarrow.parquet as paq

import arbalister.arrow as aa
from arbalister import file_format as ff

MAX_FAKER_ROWS = 100_000


def _widen_field(field: pa.Field) -> pa.Field:
    return pa.field(field.name, pa.large_string()) if pa.types.is_string(field.type) else field


def widen(table: pa.Table) -> pa.Table:
    """Adapt Arrow schema for large files."""
    return table.cast(pa.schema([_widen_field(f) for f in table.schema]))


def generate_table(num_rows: int) -> pa.Table:
    """Generate a table with fake data."""
    if num_rows > MAX_FAKER_ROWS:
        table = widen(generate_table(MAX_FAKER_ROWS))
        n_repeat = num_rows // MAX_FAKER_ROWS
        large_table = pa.concat_tables([table] * n_repeat, promote_options="default")
        return large_table.slice(0, num_rows)

    gen = faker.Faker()
    data = {
        "name": [gen.name() for _ in range(num_rows)],
        "address": [gen.address().replace("\n", ", ") for _ in range(num_rows)],
        "age": [gen.random_number(digits=2) for _ in range(num_rows)],
        "id": [gen.uuid4() for _ in range(num_rows)],
    }
    return pa.table(data)


def _generate_coordinate_table_slice(
    row_start: int, row_end: int, num_cols: int, ctx: dn.SessionContext
) -> pa.Table:
    row_idx: pa.Array = pa.array(range(row_start, row_end), type=pa.int64())
    table = pa.table({"row": row_idx})
    table = (
        ctx.from_arrow(table)
        .with_columns(
            *[
                dnf.concat(
                    dn.lit("("),  # type: ignore[no-untyped-call]
                    dnf.col("row"),
                    dn.lit(f", {j})"),  # type: ignore[no-untyped-call]
                ).alias(f"col_{j}")
                for j in range(num_cols)
            ]
        )
        .drop("row")
        .to_arrow_table()
    )
    return widen(table)


def _sink_coordinate_table(
    num_rows: int,
    num_cols: int,
    writer: paq.ParquetWriter,
    ctx: dn.SessionContext,
    chunk_size: int = 100_000,
) -> None:
    for row_start in range(0, num_rows, chunk_size):
        print(f"Generating {row_start}", flush=True)
        row_end = min(row_start + chunk_size, num_rows)
        table = _generate_coordinate_table_slice(row_start, row_end, num_cols=num_cols, ctx=ctx)
        writer.write(table)


def sink_coordinate_table(
    num_rows: int, num_cols: int, path: pathlib.Path, chunk_size: int = 1_000_000
) -> None:
    """Write iteratively a table where each cell has its coordinates."""
    assert ff.FileFormat.from_filename(path) == ff.FileFormat.Parquet
    print("Initializing session context", flush=True)
    ctx = dn.SessionContext()
    print("Generating schema", flush=True)
    schema = _generate_coordinate_table_slice(0, 1, num_cols=num_cols, ctx=ctx).schema
    writer = paq.ParquetWriter(path, schema)
    _sink_coordinate_table(num_rows=num_rows, num_cols=num_cols, writer=writer, ctx=ctx)
    writer.close()


def configure_command_single(cmd: argparse.ArgumentParser) -> argparse.ArgumentParser:
    """Configure single subcommand CLI options."""
    cmd.add_argument("--output-file", "-o", type=pathlib.Path, required=True, help="Output file path")
    cmd.add_argument(
        "--output-type",
        "-t",
        choices=[t.name.lower() for t in ff.FileFormat],
        default=None,
        help="Output file type",
    )
    cmd.add_argument("--num-rows", type=int, default=1000, help="Number of rows to generate")
    return cmd


def configure_command_batch(cmd: argparse.ArgumentParser) -> argparse.ArgumentParser:
    """Configure batch subcommand CLI options."""
    cmd.add_argument(
        "--output-file", "-o", type=pathlib.Path, action="append", help="Output file path", default=[]
    )
    cmd.add_argument("--num-rows", type=int, default=1000, help="Number of rows to generate")
    return cmd


def configure_command_coordinate(cmd: argparse.ArgumentParser) -> argparse.ArgumentParser:
    """Configure coordinate subcommand CLI options."""
    cmd.add_argument(
        "--output-file", "-o", type=pathlib.Path, required=True, help="Output file path, must be parquet."
    )
    cmd.add_argument("--num-rows", type=int, default=1_000_000, help="Number of rows to generate")
    cmd.add_argument("--num-cols", type=int, default=1000, help="Number of rows to generate")
    return cmd


def configure_argparse() -> argparse.ArgumentParser:
    """Configure CLI options."""
    parser = argparse.ArgumentParser(description="Generate a table and write to file.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    cmd_one = subparsers.add_parser("single", help="Generate a single table and write to file.")
    configure_command_single(cmd_one)

    cmd_batch = subparsers.add_parser("batch", help="Generate a multiple tables with the same data.")
    configure_command_batch(cmd_batch)

    cmd_batch = subparsers.add_parser("coordinate", help="Generate a coordinate table file.")
    configure_command_coordinate(cmd_batch)

    return parser


def shuffle_table(table: pa.Table, seed: int | None = None) -> pa.Table:
    """Shuffle the rows and columns of a table."""
    rnd = random.Random(seed)
    row_indices = pa.array(rnd.sample(range(table.num_rows), table.num_rows), type=pa.int64())
    col_order = rnd.sample(table.column_names, len(table.column_names))
    return table.select(col_order).take(row_indices)


def save_table(table: pa.Table, path: pathlib.Path, file_type: ff.FileFormat) -> None:
    """Save a table to file with the given file type."""
    path.parent.mkdir(exist_ok=True, parents=True)
    write_table = aa.get_table_writer(file_type)
    write_table(table, str(path))


def main() -> None:
    """Generate data file."""
    parser = configure_argparse()
    args = parser.parse_args()

    table = generate_table(args.num_rows)

    match args.command:
        case "single":
            ft = next((t for t in ff.FileFormat if t.name.lower() == args.output_type), None)
            if ft is None:
                ft = ff.FileFormat.from_filename(args.output_file)
            save_table(shuffle_table(table), args.output_file, ft)
        case "batch":
            for p in args.output_file:
                ft = ff.FileFormat.from_filename(p)
                save_table(shuffle_table(table), p, ft)
        case "coordinate":
            sink_coordinate_table(num_rows=args.num_rows, num_cols=args.num_cols, path=args.output_file)


if __name__ == "__main__":
    main()
