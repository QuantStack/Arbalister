import argparse
import pathlib
import random

import faker
import pyarrow as pa

import arbalister.arrow as aa

MAX_FAKER_ROWS = 100_000


def widen(field: pa.Field) -> pa.Field:
    """Adapt Arrow schema for large files."""
    return pa.field(field.name, pa.large_string()) if pa.types.is_string(field.type) else field


def generate_table(num_rows: int) -> pa.Table:
    """Generate a table with fake data."""
    if num_rows > MAX_FAKER_ROWS:
        table = generate_table(MAX_FAKER_ROWS)
        widened = table.cast(pa.schema([widen(f) for f in table.schema]))
        n_repeat = num_rows // MAX_FAKER_ROWS
        large_table = pa.concat_tables([widened] * n_repeat, promote_options="default")
        return large_table.slice(0, num_rows)

    gen = faker.Faker()
    data = {
        "name": [gen.name() for _ in range(num_rows)],
        "address": [gen.address().replace("\n", ", ") for _ in range(num_rows)],
        "age": [gen.random_number(digits=2) for _ in range(num_rows)],
        "id": [gen.uuid4() for _ in range(num_rows)],
    }
    return pa.table(data)


def configure_command_single(cmd: argparse.ArgumentParser) -> argparse.ArgumentParser:
    """Configure single subcommand CLI options."""
    cmd.add_argument("--output-file", "-o", type=pathlib.Path, required=True, help="Output file path")
    cmd.add_argument(
        "--output-type",
        "-t",
        choices=[t.name.lower() for t in aa.FileFormat],
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


def configure_argparse() -> argparse.ArgumentParser:
    """Configure CLI options."""
    parser = argparse.ArgumentParser(description="Generate a table and write to file.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    cmd_one = subparsers.add_parser("single", help="Generate a single table and write to file.")
    configure_command_single(cmd_one)

    cmd_batch = subparsers.add_parser("batch", help="Generate a multiple tables with the same data.")
    configure_command_batch(cmd_batch)

    return parser


def shuffle_table(table: pa.Table, seed: int | None = None) -> pa.Table:
    """Shuffle the rows and columns of a table."""
    rnd = random.Random(seed)
    row_indices = pa.array(rnd.sample(range(table.num_rows), table.num_rows), type=pa.int64())
    col_order = rnd.sample(table.column_names, len(table.column_names))
    return table.select(col_order).take(row_indices)


def save_table(table: pa.Table, path: pathlib.Path, file_type: aa.FileFormat) -> None:
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
            ft = next((t for t in aa.FileFormat if t.name.lower() == args.output_type), None)
            if ft is None:
                ft = aa.FileFormat.from_filename(args.output_file)
            save_table(shuffle_table(table), args.output_file, ft)
        case "batch":
            for p in args.output_file:
                ft = aa.FileFormat.from_filename(p)
                save_table(shuffle_table(table), p, ft)


if __name__ == "__main__":
    main()
