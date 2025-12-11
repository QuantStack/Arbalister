import { DataModel } from "@lumino/datagrid";

import type * as Arrow from "apache-arrow";

import { PairMap } from "./collection";
import { fetchStats, fetchTable } from "./requests";

export namespace ArrowModel {
  export interface IOptions {
    path: string;
    rowChunkSize?: number;
    colChunkSize?: number;
    loadingRepr?: string;
  }
}

export class ArrowModel extends DataModel {
  constructor(options: ArrowModel.IOptions) {
    super();

    this._path = options.path;
    this._rowChunkSize = options.rowChunkSize ?? 512;
    this._colChunkSize = options.colChunkSize ?? 24;
    this._loadingRepr = options.loadingRepr ?? "";

    this._ready = this.initialize();
  }

  protected async initialize(): Promise<void> {
    const [schema, stats, chunk00] = await Promise.all([
      this.fetchSchema(),
      fetchStats({ path: this._path }),
      this.fetchChunk([0, 0]),
    ]);

    this._schema = schema;
    this._chunks.set([0, 0], chunk00);
    this._numCols = stats.num_cols;
    this._numRows = stats.num_rows;
  }

  get ready(): Promise<void> {
    return this._ready;
  }

  private get schema(): Arrow.Schema {
    return this._schema;
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
    const row_chunk: number = Math.floor(row / this._rowChunkSize);
    const col_chunk: number = Math.floor(col / this._colChunkSize);
    const chunk_idx: [number, number] = [row_chunk, col_chunk];

    if (this._chunks.has(chunk_idx)) {
      const chunk = this._chunks.get(chunk_idx)!;
      if (chunk instanceof Promise) {
        // Wait for Promise to complete and mark data as modified
        return this._loadingRepr;
      }
      // We have data
      const row_idx_in_chunk = row % this._rowChunkSize;
      const col_idx_in_chunk = col % this._colChunkSize;
      return chunk.getChildAt(col_idx_in_chunk)?.get(row_idx_in_chunk).toString();
    }

    // Fetch data, however we cannot await it due to the interface required by the DataGrid.
    // Instead, we fire the request, and notify of change upon completion.
    const promise = this.fetchChunk(chunk_idx).then((table) => {
      this._chunks.set(chunk_idx, table);
      this.emitChanged({
        type: "cells-changed",
        region: "body",
        row: row_chunk * this._rowChunkSize,
        rowSpan: this._rowChunkSize,
        column: col_chunk * this._colChunkSize,
        columnSpan: this._colChunkSize,
      });
    });
    this._chunks.set([row_chunk, col_chunk], promise);

    return this._loadingRepr;
  }

  private async fetchChunk(chunk_idx: [number, number]) {
    const [row_chunk, col_chunk] = chunk_idx;
    return await fetchTable({
      path: this._path,
      row_chunk_size: this._rowChunkSize,
      row_chunk: row_chunk,
      col_chunk_size: this._colChunkSize,
      col_chunk: col_chunk,
    });
  }

  private async fetchSchema() {
    const table = await fetchTable({
      path: this._path,
      row_chunk_size: 0,
      row_chunk: 0,
    });
    return table.schema;
  }

  private _path: string;
  private _rowChunkSize: number;
  private _colChunkSize: number;
  private _loadingRepr: string;

  private _numRows: number = 0;
  private _numCols: number = 0;
  private _schema!: Arrow.Schema;
  private _chunks: PairMap<number, number, Arrow.Table | Promise<void>> = new PairMap();
  private _ready: Promise<void>;
}
