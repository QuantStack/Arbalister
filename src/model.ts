import { DataModel } from "@lumino/datagrid";

import type * as Arrow from "apache-arrow";

import { PairMap } from "./collection";
import { fetchStats, fetchTable } from "./requests";

const CHUNK_ROW_COUNT = 1024;
const CHUNK_COL_COUNT = 128;
const LOADING_REPR = "";

export class ArrowModel extends DataModel {
  constructor(path: string) {
    super();
    this._path = path;
    this._ready = this.initialize();
  }

  protected async initialize(): Promise<void> {
    const [stats, chunk0] = await Promise.all([
      fetchStats({ path: this._path }),
      this.fetchChunk([0, 0]),
    ]);

    this._chunks.set([0, 0], chunk0);
    this._numCols = stats.num_cols;
    this._numRows = stats.num_rows;
  }

  get ready(): Promise<void> {
    return this._ready;
  }

  private get schema(): Arrow.Schema {
    if (!this._chunks.has([0, 0])) {
      throw new Error("First chunk is null or undefined");
    }
    const chunk = this._chunks.get([0, 0])!;
    if (chunk instanceof Promise) {
      throw new Error("schema is not an Arrow.Table");
    }
    return chunk.schema;
  }

  columnCount(region: DataModel.ColumnRegion): number {
    if (region === "body") {
      return this._numCols;
    }
    return 1;
  }

  rowCount(region: DataModel.RowRegion): number {
    if (region === "body") {
      return this._numRows;
    }
    return 1;
  }

  data(region: DataModel.CellRegion, row: number, column: number): string {
    switch (region) {
      case "body":
        return this.dataBody(row, column);
      case "column-header":
        return this.schema.names[column].toString();
      case "row-header":
        return row.toString();
      case "corner-header":
        return "";
      default:
        throw "unreachable";
    }
  }

  private dataBody(row: number, col: number): string {
    const row_chunk: number = Math.floor(row / CHUNK_ROW_COUNT);
    const col_chunk: number = Math.floor(col / CHUNK_COL_COUNT);
    const chunk_idx: [number, number] = [row_chunk, col_chunk];

    if (this._chunks.has(chunk_idx)) {
      const chunk = this._chunks.get(chunk_idx)!;
      if (chunk instanceof Promise) {
        // Wait for Promise to complete and mark data as modified
        return LOADING_REPR;
      }
      // We have data
      const row_idx_in_chunk = row % CHUNK_ROW_COUNT;
      const col_idx_in_chunk = col % CHUNK_COL_COUNT;
      return chunk.getChildAt(col_idx_in_chunk)?.get(row_idx_in_chunk).toString();
    }

    // Fetch data, however we cannot await it due to the interface required by the DataGrid.
    // Instead, we fire the request, and notify of change upon completion.
    const promise = this.fetchChunk(chunk_idx).then((table) => {
      this._chunks.set(chunk_idx, table);
      this.emitChanged({
        type: "cells-changed",
        region: "body",
        row: row_chunk * CHUNK_ROW_COUNT,
        rowSpan: CHUNK_ROW_COUNT,
        column: col_chunk * CHUNK_COL_COUNT,
        columnSpan: CHUNK_COL_COUNT,
      });
    });
    this._chunks.set([row_chunk, col_chunk], promise);

    return LOADING_REPR;
  }

  private async fetchChunk(chunk_idx: [number, number]) {
    const [row_chunk, col_chunk] = chunk_idx;
    return await fetchTable({
      path: this._path,
      row_chunk_size: CHUNK_ROW_COUNT,
      row_chunk: row_chunk,
      col_chunk_size: CHUNK_COL_COUNT,
      col_chunk: col_chunk,
    });
  }

  private _numRows: number = 0;
  private _numCols: number = 0;
  private _path: string;
  private _chunks: PairMap<number, number, Arrow.Table | Promise<void>> = new PairMap();
  private _ready: Promise<void>;
}
