import { DataModel } from "@lumino/datagrid";

import type * as Arrow from "apache-arrow";

import { PairMap } from "./collection";
import { fetchFileInfo, fetchStats, fetchTable } from "./requests";
import type { FileInfo, FileReadOptions } from "./file-options";

export namespace ArrowModel {
  export interface LoadingOptions {
    path: string;
    rowChunkSize?: number;
    colChunkSize?: number;
    loadingRepr?: string;
    nullRepr?: string;
  }
}

export class ArrowModel extends DataModel {
  static async fromRemoteFileInfo(loadingOptions: ArrowModel.LoadingOptions) {
    const { info: fileInfo, default_options: fileOptions } = await fetchFileInfo({
      path: loadingOptions.path,
    });
    return new ArrowModel(loadingOptions, fileOptions, fileInfo);
  }

  constructor(
    loadingOptions: ArrowModel.LoadingOptions,
    fileOptions: FileReadOptions,
    fileInfo: FileInfo,
  ) {
    super();

    this._loadingParams = {
      rowChunkSize: 512,
      colChunkSize: 24,
      loadingRepr: "",
      nullRepr: "",
      ...loadingOptions,
    };
    this._fileOptions = fileOptions;
    this._fileInfo = fileInfo;

    this._ready = this.initialize();
  }

  protected async initialize(): Promise<void> {
    const [stats, chunk00] = await Promise.all([
      fetchStats({ path: this._loadingParams.path, ...this._fileOptions }),
      this.fetchChunk([0, 0]),
    ]);

    this._schema = stats.schema;
    this._numCols = stats.num_cols;
    this._numRows = stats.num_rows;
    this._chunks = new PairMap();
    this._chunks.set([0, 0], chunk00);
  }

  get fileInfo(): Readonly<FileInfo> {
    return this._fileInfo;
  }

  get fileReadOptions(): Readonly<FileReadOptions> {
    return this._fileOptions;
  }

  set fileReadOptions(fileOptions: FileReadOptions) {
    this._fileOptions = fileOptions;
    this._ready = this.initialize().then(() => {
      this.emitChanged({ type: "model-reset" });
    });
  }

  get ready(): Promise<void> {
    return this._ready;
  }

  get schema(): Arrow.Schema {
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
    const chunkIdx = this.chunkIdx(row, col);

    if (this._chunks.has(chunkIdx)) {
      const chunk = this._chunks.get(chunkIdx)!;
      if (chunk instanceof Promise) {
        // Wait for Promise to complete and mark data as modified
        return this._loadingParams.loadingRepr;
      }

      // We have data
      const row_idx_in_chunk = row % this._loadingParams.rowChunkSize;
      const col_idx_in_chunk = col % this._loadingParams.colChunkSize;
      const val = chunk.getChildAt(col_idx_in_chunk)?.get(row_idx_in_chunk);
      const out = val?.toString() || this._loadingParams.nullRepr;

      // Prefetch next chunks only once we have data for the current chunk.
      // We chain the Promise because this can be considered a low priority operation so we want
      // to reduce load on the server
      const [rowChunk, colChunk] = chunkIdx;
      this.prefetchChunkIfNeeded([rowChunk + 1, colChunk]).then((_) => {
        this.prefetchChunkIfNeeded([rowChunk, colChunk + 1]);
      });

      return out;
    }

    // Fetch data, however we cannot await it due to the interface required by the DataGrid.
    // Instead, we fire the request, and notify of change upon completion.
    const promise = this.fetchChunk(chunkIdx).then((table) => {
      this._chunks.set(chunkIdx, table);
      this.emitChangedChunk(chunkIdx);
    });
    this._chunks.set(chunkIdx, promise);

    return this._loadingParams.loadingRepr;
  }

  private async fetchChunk(chunkIdx: [number, number]) {
    const [rowChunk, colChunk] = chunkIdx;
    return await fetchTable({
      path: this._loadingParams.path,
      row_chunk_size: this._loadingParams.rowChunkSize,
      row_chunk: rowChunk,
      col_chunk_size: this._loadingParams.colChunkSize,
      col_chunk: colChunk,
      ...this._fileOptions,
    });
  }

  private emitChangedChunk(chunkIdx: [number, number]) {
    const [rowChunk, colChunk] = chunkIdx;

    // We must ensure the range is within the bounds
    const rowStart = rowChunk * this._loadingParams.rowChunkSize;
    const rowEnd = Math.min(rowStart + this._loadingParams.rowChunkSize, this._numRows);
    const colStart = colChunk * this._loadingParams.colChunkSize;
    const colEnd = Math.min(colStart + this._loadingParams.colChunkSize, this._numCols);

    this.emitChanged({
      type: "cells-changed",
      region: "body",
      row: rowStart,
      rowSpan: rowEnd - rowStart,
      column: colStart,
      columnSpan: colEnd - colStart,
    });
  }

  private async prefetchChunkIfNeeded(chunkIdx: [number, number]) {
    if (this._chunks.has(chunkIdx) || !this.chunkIsValid(chunkIdx)) {
      return;
    }

    const promise = this.fetchChunk(chunkIdx).then((table) => {
      this._chunks.set(chunkIdx, table);
    });
    this._chunks.set(chunkIdx, promise);
  }

  private chunkIdx(row: number, col: number): [number, number] {
    return [
      Math.floor(row / this._loadingParams.rowChunkSize),
      Math.floor(col / this._loadingParams.colChunkSize),
    ];
  }

  private chunkIsValid(chunkIdx: [number, number]): boolean {
    const [rowChunk, colChunk] = chunkIdx;
    const [max_rowChunk, max_colChunk] = this.chunkIdx(this._numRows - 1, this._numCols - 1);
    return rowChunk >= 0 && rowChunk <= max_rowChunk && colChunk >= 0 && colChunk <= max_colChunk;
  }

  private readonly _loadingParams: Required<ArrowModel.LoadingOptions>;
  private readonly _fileInfo: FileInfo;
  private _fileOptions: FileReadOptions;

  private _numRows: number = 0;
  private _numCols: number = 0;
  private _schema!: Arrow.Schema;
  private _chunks: PairMap<number, number, Arrow.Table | Promise<void>> = new PairMap();
  private _ready: Promise<void>;
}
