import "jest-canvas-mock";

import { tableFromArrays } from "apache-arrow";
import type * as Arrow from "apache-arrow";

import { ArrowModel } from "../model";
import { fetchStats, fetchTable } from "../requests";
import type { FileInfo, FileReadOptions } from "../file-options";
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
    schema: MOCK_TABLE.schema,
  };
}

// Store pending resolvers to allow manual control of async operations
let pendingResolvers: Array<() => void> = [];

async function fetchTableMocked(params: Req.TableOptions): Promise<Arrow.Table> {
  let table: Arrow.Table = MOCK_TABLE;

  if (params.start_row !== undefined && params.end_row !== undefined) {
    table = table.slice(params.start_row, params.end_row);
  }

  if (params.start_col !== undefined && params.end_col !== undefined) {
    const colNames = table.schema.fields.map((field) => field.name);
    const selectedCols = colNames.slice(params.start_col, params.end_col);
    table = table.select(selectedCols);
  }

  // All fetches are manually resolvable
  return new Promise((resolve) => {
    pendingResolvers.push(() => resolve(table));
  });
}

// Flush all pending microtasks and yield to the event loop
function flushMicrotasks(): Promise<void> {
  return new Promise((resolve) => {
    // Use setImmediate if available (Node.js), otherwise setTimeout
    if (typeof setImmediate === "function") {
      setImmediate(resolve);
    } else {
      setTimeout(resolve, 0);
    }
  });
}

// Helper to resolve all pending fetches and wait for them to complete
async function resolveAllPendingFetches(): Promise<void> {
  // Flush microtasks to ensure fetchStats has completed and fetchTable has been called
  await Promise.resolve();

  // Micro tasks may make more requests to fetch
  while (pendingResolvers.length > 0) {
    const resolvers = [...pendingResolvers];
    pendingResolvers = [];

    // Start/resolve all pending promises
    for (const resolve of resolvers) {
      resolve();
    }

    // Flush all microtasks and yield to event loop to allow promise chains to complete
    // The model wraps fetch promises in .then() for emitChangedChunk and prefetching
    await flushMicrotasks();
  }
}

jest.mock("../requests", () => ({
  fetchTable: jest.fn(),
  fetchStats: jest.fn(),
}));

describe("ArrowModel", () => {
  (fetchTable as jest.Mock).mockImplementation(fetchTableMocked);
  (fetchStats as jest.Mock).mockImplementation(fetchStatsMocked);

  let model: ArrowModel;

  beforeEach(() => {
    // Clear pending resolvers before each test for isolation
    pendingResolvers = [];
  });

  it("should initialize data", async () => {
    model = new ArrowModel({ path: "test/path.parquet" }, {} as FileReadOptions, {} as FileInfo);
    await resolveAllPendingFetches();
    await model.ready;

    expect(fetchStats).toHaveBeenCalledTimes(1);
    // Schema comes from fetchStats, so fetchTable is only called once for data
    expect(fetchTable).toHaveBeenCalledTimes(1);

    expect(model.schema).toEqual(MOCK_TABLE.schema);
    expect(model.columnCount("body")).toEqual(MOCK_TABLE.numCols);
    expect(model.columnCount("row-header")).toEqual(1);
    expect(model.rowCount("body")).toEqual(MOCK_TABLE.numRows);
    expect(model.rowCount("column-header")).toEqual(1);

    // First chunk is initialized
    expect(model.data("body", 0, 0)).toEqual(MOCK_TABLE.getChildAt(0)?.get(0).toString());
  });

  it("should reinitialize when fileOptions is set", async () => {
    const model2 = new ArrowModel({ path: "test/data.csv" }, {} as FileReadOptions, {} as FileInfo);
    await resolveAllPendingFetches();
    await model2.ready;

    const initialStatsCallCount = (fetchStats as jest.Mock).mock.calls.length;
    const initialTableCallCount = (fetchTable as jest.Mock).mock.calls.length;

    model2.fileReadOptions = { delimiter: ";" } as FileReadOptions;
    await resolveAllPendingFetches();
    await model2.ready;

    expect(fetchStats).toHaveBeenCalledTimes(initialStatsCallCount + 1);
    expect(fetchTable).toHaveBeenCalledTimes(initialTableCallCount + 1);
  });

  describe("Cell region data retrieval", () => {
    it("should return correct data for column-header region", async () => {
      model = new ArrowModel({ path: "test/path.parquet" }, {} as FileReadOptions, {} as FileInfo);
      await resolveAllPendingFetches();
      await model.ready;

      const col0Header = model.data("column-header", 0, 0);
      const col1Header = model.data("column-header", 0, 1);
      const col2Header = model.data("column-header", 0, 2);

      // Build expected headers from schema
      const field0 = MOCK_TABLE.schema.fields[0];
      const field1 = MOCK_TABLE.schema.fields[1];
      const field2 = MOCK_TABLE.schema.fields[2];
      const expectedCol0Header = `${field0.name} (${field0.type}${field0.nullable ? " | null" : ""})`;
      const expectedCol1Header = `${field1.name} (${field1.type}${field1.nullable ? " | null" : ""})`;
      const expectedCol2Header = `${field2.name} (${field2.type}${field2.nullable ? " | null" : ""})`;

      expect(col0Header).toBe(expectedCol0Header);
      expect(col1Header).toBe(expectedCol1Header);
      expect(col2Header).toBe(expectedCol2Header);
    });

    it("should return correct data for row-header region", async () => {
      model = new ArrowModel({ path: "test/path.parquet" }, {} as FileReadOptions, {} as FileInfo);
      await resolveAllPendingFetches();
      await model.ready;

      const testRows = [0, 5, 9];
      for (const row of testRows) {
        expect(model.data("row-header", row, 0)).toBe(row.toString());
      }
    });
  });

  describe("Chunked data loading", () => {
    const loadingRepr = "loading";
    let model: ArrowModel;

    beforeEach(async () => {
      model = new ArrowModel(
        {
          path: "test/chunked.parquet",
          loadingRepr,
          rowChunkSize: 2,
          colChunkSize: 2,
          prefetchFactors: {
            rowPrefetchFactor: 1,
            colPrefetchFactor: 1,
          },
        },
        {} as FileReadOptions,
        {} as FileInfo,
      );
      await resolveAllPendingFetches();
      await model.ready;
    });

    it("should prefetch on data access", async () => {
      let tableCallCount = (fetchTable as jest.Mock).mock.calls.length;
      // First chunk (rows [0, 2[, cols [0, 1[) - loaded during init
      expect(model.data("body", 0, 0)).toBe("1");
      expect(model.data("body", 1, 1)).toBe("Bob");

      // Resolve prefetches
      await resolveAllPendingFetches();
      // Two prefetch calls were made
      expect(fetchTable).toHaveBeenCalledTimes(tableCallCount + 2);

      tableCallCount = (fetchTable as jest.Mock).mock.calls.length;
      // Accessing data from next row chunk [2, 4[ is ready by prefetch
      const expectedRow3Col0 = MOCK_TABLE.getChildAt(0)?.get(3).toString();
      expect(model.data("body", 3, 0)).toBe(expectedRow3Col0);
      // Accessing data from next col chunk [2, 4[ is ready by prefetch
      const expectedRow0Col3 = MOCK_TABLE.getChildAt(3)?.get(0).toString();
      expect(model.data("body", 0, 3)).toBe(expectedRow0Col3);

      // Resolve prefetches
      await resolveAllPendingFetches();
      // It triggers four prefetches (two in each chunk) but one is the same
      expect(fetchTable).toHaveBeenCalledTimes(tableCallCount + 3);

      tableCallCount = (fetchTable as jest.Mock).mock.calls.length;

      // Accessing data from further row chunk [4, 6[ is ready
      const expectedRow5Col1 = MOCK_TABLE.getChildAt(0)?.get(5).toString();
      expect(model.data("body", 5, 0)).toBe(expectedRow5Col1);
      // Accessing data from further col chunk [4, 5[ is not ready
      const expectedRow1Col4 = MOCK_TABLE.getChildAt(4)?.get(0).toString();
      expect(model.data("body", 0, 4)).toBe(expectedRow1Col4);
    });

    it("should load unavailable data in two steps", async () => {
      const tableCallCount = (fetchTable as jest.Mock).mock.calls.length;

      // Accessing data from next row chunk [2, 4[ is not ready
      expect(model.data("body", 3, 0)).toBe(loadingRepr);

      // Resolve prefetches
      await resolveAllPendingFetches();
      // One immediate call has been made (no prefetch)
      expect(fetchTable).toHaveBeenCalledTimes(tableCallCount + 1);

      // Accessing data from next row chunk [2, 4[ is now ready
      const expectedRow3Col0 = MOCK_TABLE.getChildAt(0)?.get(3).toString();
      expect(model.data("body", 3, 0)).toBe(expectedRow3Col0);
    });

    it("should handle last partial chunk correctly", async () => {
      // Trigger and wait for last row chunk [8,10[ - this is a full chunk (2 rows)
      model.data("body", 8, 0);
      await resolveAllPendingFetches();

      const expectedRow8Col0 = MOCK_TABLE.getChildAt(0)?.get(8).toString();
      const expectedRow9Col0 = MOCK_TABLE.getChildAt(0)?.get(9).toString();
      expect(model.data("body", 8, 0)).toBe(expectedRow8Col0);
      expect(model.data("body", 9, 0)).toBe(expectedRow9Col0);

      // Trigger and wait for last column chunk [4,5[ - this is a partial chunk (only 1 col)
      model.data("body", 0, 4);
      await resolveAllPendingFetches();

      const expectedRow0Col4 = MOCK_TABLE.getChildAt(4)?.get(0).toString();
      const expectedRow1Col4 = MOCK_TABLE.getChildAt(4)?.get(1).toString();
      expect(model.data("body", 0, 4)).toBe(expectedRow0Col4);
      expect(model.data("body", 1, 4)).toBe(expectedRow1Col4);
    });
  });

  describe("Null representation", () => {
    it("should use custom null representation", async () => {
      // Create table with null values
      const tableWithNull = tableFromArrays({
        id: [1, null, 3],
        name: ["Alice", "Bob", null],
      });

      (fetchStats as jest.Mock).mockImplementationOnce(async () => ({
        num_rows: tableWithNull.numRows,
        num_cols: tableWithNull.numCols,
        schema: tableWithNull.schema,
      }));

      (fetchTable as jest.Mock).mockImplementationOnce(async () => tableWithNull);

      const nullRepr = "N/A";
      const modelCustom = new ArrowModel(
        { path: "test/null.parquet", nullRepr },
        {} as FileReadOptions,
        {} as FileInfo,
      );
      await resolveAllPendingFetches();
      await modelCustom.ready;

      // Null values should use nullRepr
      expect(modelCustom.data("body", 1, 0)).toBe(nullRepr);
      expect(modelCustom.data("body", 2, 1)).toBe(nullRepr);
    });
  });

  describe("Edge cases", () => {
    it("should handle single row table", async () => {
      const expectedIdValue = 1;
      const expectedNameValue = "Alice";
      const singleRowTable = tableFromArrays({
        id: [expectedIdValue],
        name: [expectedNameValue],
      });

      (fetchStats as jest.Mock).mockImplementationOnce(async () => ({
        num_rows: 1,
        num_cols: 2,
        schema: singleRowTable.schema,
      }));

      (fetchTable as jest.Mock).mockImplementationOnce(async () => singleRowTable);

      const singleRowModel = new ArrowModel(
        { path: "test/single-row.parquet" },
        {} as FileReadOptions,
        {} as FileInfo,
      );
      await resolveAllPendingFetches();
      await singleRowModel.ready;

      expect(singleRowModel.rowCount("body")).toBe(1);
      expect(singleRowModel.data("body", 0, 0)).toBe(expectedIdValue.toString());
      expect(singleRowModel.data("body", 0, 1)).toBe(expectedNameValue);
    });

    it("should handle single column table", async () => {
      const expectedValues = [1, 2, 3];
      const singleColTable = tableFromArrays({
        id: expectedValues,
      });

      (fetchStats as jest.Mock).mockImplementationOnce(async () => ({
        num_rows: 3,
        num_cols: 1,
        schema: singleColTable.schema,
      }));

      (fetchTable as jest.Mock).mockImplementationOnce(async () => singleColTable);

      const singleColModel = new ArrowModel(
        { path: "test/single-col.parquet" },
        {} as FileReadOptions,
        {} as FileInfo,
      );
      await resolveAllPendingFetches();
      await singleColModel.ready;

      expect(singleColModel.columnCount("body")).toBe(1);
      expect(singleColModel.data("body", 0, 0)).toBe(expectedValues[0].toString());
      expect(singleColModel.data("body", 1, 0)).toBe(expectedValues[1].toString());
      expect(singleColModel.data("body", 2, 0)).toBe(expectedValues[2].toString());
    });
  });

  describe("FileInfo and FileReadOptions accessors", () => {
    it("should provide readonly access to fileInfo", async () => {
      const fileInfo: FileInfo = { delimiters: [",", ";"] };
      const modelWithInfo = new ArrowModel(
        { path: "test/info.csv" },
        {} as FileReadOptions,
        fileInfo,
      );

      expect(modelWithInfo.fileInfo).toBe(fileInfo);
    });

    it("should provide readonly access to fileReadOptions", async () => {
      const fileOptions: FileReadOptions = { delimiter: "," };
      const modelWithOptions = new ArrowModel(
        { path: "test/options.csv" },
        fileOptions,
        {} as FileInfo,
      );

      expect(modelWithOptions.fileReadOptions).toEqual(fileOptions);
    });
  });
});
