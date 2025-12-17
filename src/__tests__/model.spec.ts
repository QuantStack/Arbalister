import "jest-canvas-mock";

import { tableFromArrays } from "apache-arrow";
import type * as Arrow from "apache-arrow";

import { ArrowModel } from "../model";
import { fetchStats, fetchTable } from "../requests";
import type * as Req from "../requests";

const MOCK_TABLE = tableFromArrays({
  id: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
  name: ["Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Hank", "Ivy", "Jack"],
  age: [25, 30, 35, 40, 45, 50, 55, 60, 65, 70],
  city: ["Paris", "London", "Ur", "Turin", "Rome", "Tokyo", "Boston", "Sydney", "Lima", "Cairo"],
  score: [85, 90, 78, 92, 88, 76, 95, 81, 89, 93],
});

async function fetchStatsMocked(_params: Req.StatsOptions): Promise<Req.StatsResponse> {
  return {
    num_rows: MOCK_TABLE.numRows,
    num_cols: MOCK_TABLE.numCols,
  };
}

async function fetchTableMocked(params: Req.TableOptions): Promise<Arrow.Table> {
  let table: Arrow.Table = MOCK_TABLE;

  if (params.row_chunk !== undefined && params.row_chunk_size !== undefined) {
    const start = params.row_chunk * params.row_chunk_size;
    const end = start + params.row_chunk_size;
    table = table.slice(start, end);
  }

  if (params.col_chunk !== undefined && params.col_chunk_size !== undefined) {
    const colNames = table.schema.fields.map((field) => field.name);
    const start = params.col_chunk * params.col_chunk_size;
    const end = start + params.col_chunk_size;
    const selectedCols = colNames.slice(start, end);
    table = table.select(selectedCols);
  }

  return table;
}

jest.mock("../requests", () => ({
  fetchTable: jest.fn(),
  fetchStats: jest.fn(),
}));

describe("ArrowModel", () => {
  (fetchTable as jest.Mock).mockImplementation(fetchTableMocked);
  (fetchStats as jest.Mock).mockImplementation(fetchStatsMocked);

  const model = new ArrowModel({ path: "test/path.parquet" }, {});

  it("should initialize data", async () => {
    await model.ready;

    expect(fetchStats).toHaveBeenCalledTimes(1);
    // One for schema and once for data
    expect(fetchTable).toHaveBeenCalledTimes(2);

    expect(model.schema).toEqual(MOCK_TABLE.schema);
    expect(model.columnCount("body")).toEqual(MOCK_TABLE.numCols);
    expect(model.columnCount("row-header")).toEqual(1);
    expect(model.rowCount("body")).toEqual(MOCK_TABLE.numRows);
    expect(model.rowCount("column-header")).toEqual(1);

    // First chunk is initialized
    expect(model.data("body", 0, 0)).toEqual(MOCK_TABLE.getChildAt(0)?.get(0).toString());
  });
});
