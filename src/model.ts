import { DataModel } from "@lumino/datagrid";

import type * as Arrow from "apache-arrow";

import { fetchStats, fetchTable } from "./requests";

const CHUNK_ROW_COUNT = 1024;
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
      this.fetchChunk(0),
    ]);

    this._chunks[0] = chunk0;
    this._numCols = stats.num_cols;
    this._numRows = stats.num_rows;
  }

  get ready(): Promise<void> {
    return this._ready;
  }

  private get schema(): Arrow.Schema {
    if (!this._chunks[0]) {
      throw new Error("First chunk is null or undefined");
    }
    const chunk = this._chunks[0];
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

  private dataBody(row: number, column: number): string {
    const chunk_idx: number = Math.floor(row / CHUNK_ROW_COUNT);

    if (chunk_idx in this._chunks) {
      const chunk = this._chunks[chunk_idx];
      if (chunk instanceof Promise) {
        // Wait for Promise to complete and mark data as modified
        return LOADING_REPR;
      }
      // We have data
      const chunk_row_idx = row % CHUNK_ROW_COUNT;
      return chunk.getChildAt(column)?.get(chunk_row_idx).toString();
    }

    // Fetch data, however we cannot await it due to the interface required by the DataGrid.
    // Instead, we fire the request, and notify of change upon completion.
    this._chunks[chunk_idx] = this.fetchChunk(chunk_idx).then((table) => {
      this._chunks[chunk_idx] = table;
      this.emitChanged({
        type: "cells-changed",
        region: "body",
        row: chunk_idx * CHUNK_ROW_COUNT,
        rowSpan: CHUNK_ROW_COUNT,
        column: 0,
        columnSpan: this._numCols,
      });
    });

    return LOADING_REPR;
  }

  private async fetchChunk(chunk_idx: number) {
    return await fetchTable({ path: this._path, per_chunk: CHUNK_ROW_COUNT, chunk: chunk_idx });
  }

  private _numRows: number = 0;
  private _numCols: number = 0;
  private _path: string;
  private _chunks: { [key: number]: Arrow.Table | Promise<void> } = {};
  private _ready: Promise<void>;
}
