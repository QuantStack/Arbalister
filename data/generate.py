import argparse
import pathlib

import faker
import pyarrow

import jupyterdiana.arrow as ja


def generate_table(num_rows: int) -> pyarrow.Table:
    """Generate a table with fake data."""
    gen = faker.Faker()
    data = {
        "name": [gen.name() for _ in range(num_rows)],
        "address": [gen.address() for _ in range(num_rows)],
        "age": [gen.random_number(digits=2) for _ in range(num_rows)],
        "id": [gen.uuid4() for _ in range(num_rows)],
    }
    return pyarrow.table(data)


def configure_argparse() -> argparse.ArgumentParser:
    """Configure CLI options."""
    parser = argparse.ArgumentParser(description="Generate a table and write to file.")
    parser.add_argument("--output-file", type=pathlib.Path, required=True, help="Output file path")
    parser.add_argument(
        "--output-type", choices=["csv", "parquet", None], default=None, help="Output file type"
    )
    parser.add_argument("--num-rows", type=int, default=1000, help="Number of rows to generate")
    return parser


def main() -> None:
    """Generate data file."""
    parser = configure_argparse()
    args = parser.parse_args()

    table = generate_table(args.num_rows)
    args.output_file.parent.mkdir(exist_ok=True, parents=True)

    if args.output_type is None:
        args.output_type = args.output_file.suffix.removeprefix(".")

    write_table = ja.get_table_writer(ja.FileFormat.from_filename(args.output_file))
    write_table(table, str(args.output_file))


if __name__ == "__main__":
    main()
