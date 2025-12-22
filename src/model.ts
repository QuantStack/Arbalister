import { DataModel } from "@lumino/datagrid";

import type * as Arrow from "apache-arrow";

import { PairMap } from "./collection";
import { fetchFileInfo, fetchStats, fetchTable } from "./requests";
import type { FileInfo, FileOptions } from "./file_options";

export namespace ArrowModel {
  export interface LoadingOptions {
    path: string;
    rowChunkSize?: number;
    colChunkSize?: number;
    loadingRepr?: string;
  }
}

export class ArrowModel extends DataModel {
  static async fromRemoteFileInfo(loadingOptions: ArrowModel.LoadingOptions) {
    const { info: fileInfo, read_params: fileOptions } = await fetchFileInfo({
      path: loadingOptions.path,
    });
    return new ArrowModel(loadingOptions, fileOptions, fileInfo);
  }

  constructor(
    loadingOptions: ArrowModel.LoadingOptions,
    fileOptions: FileOptions,
    fileInfo: FileInfo,
  ) {
    super();

    this._loadingParams = {
      rowChunkSize: 512,
      colChunkSize: 24,
      loadingRepr: "",
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

  get fileOptions(): Readonly<FileOptions> {
    return this._fileOptions;
  }

  set fileOptions(fileOptions: FileOptions) {
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
    const chunk_idx = this.chunkIdx(row, col);

    if (this._chunks.has(chunk_idx)) {
      const chunk = this._chunks.get(chunk_idx)!;
      if (chunk instanceof Promise) {
        // Wait for Promise to complete and mark data as modified
        return this._loadingParams.loadingRepr;
      }

      // We have data
      const row_idx_in_chunk = row % this._loadingParams.rowChunkSize;
      const col_idx_in_chunk = col % this._loadingParams.colChunkSize;
      const out = chunk.getChildAt(col_idx_in_chunk)?.get(row_idx_in_chunk).toString();

      // Prefetch next chunks only once we have data for the current chunk.
      // We chain the Promise because this can be considered a low priority operation so we want
      // to reduce load on the server
      const [row_chunk, col_chunk] = chunk_idx;
      this.prefetchChunkIfNeeded([row_chunk + 1, col_chunk]).then((_) => {
        this.prefetchChunkIfNeeded([row_chunk, col_chunk + 1]);
      });

      return out;
    }

    // Fetch data, however we cannot await it due to the interface required by the DataGrid.
    // Instead, we fire the request, and notify of change upon completion.
    const promise = this.fetchChunk(chunk_idx).then((table) => {
      this._chunks.set(chunk_idx, table);
      this.emitChangedChunk(chunk_idx);
    });
    this._chunks.set(chunk_idx, promise);

    return this._loadingParams.loadingRepr;
  }

  private async fetchChunk(chunk_idx: [number, number]) {
    const [row_chunk, col_chunk] = chunk_idx;
    return await fetchTable({
      path: this._loadingParams.path,
      row_chunk_size: this._loadingParams.rowChunkSize,
      row_chunk: row_chunk,
      col_chunk_size: this._loadingParams.colChunkSize,
      col_chunk: col_chunk,
      ...this._fileOptions,
    });
  }

  private emitChangedChunk(chunk_idx: [number, number]) {
    const [row_chunk, col_chunk] = chunk_idx;
    this.emitChanged({
      type: "cells-changed",
      region: "body",
      row: row_chunk * this._loadingParams.rowChunkSize,
      rowSpan: this._loadingParams.rowChunkSize,
      column: col_chunk * this._loadingParams.colChunkSize,
      columnSpan: this._loadingParams.colChunkSize,
    });
  }

  private async prefetchChunkIfNeeded(chunk_idx: [number, number]) {
    if (this._chunks.has(chunk_idx) || !this.chunkIsValid(chunk_idx)) {
      return;
    }

    const promise = this.fetchChunk(chunk_idx).then((table) => {
      this._chunks.set(chunk_idx, table);
    });
    this._chunks.set(chunk_idx, promise);
  }

  private chunkIdx(row: number, col: number): [number, number] {
    return [
      Math.floor(row / this._loadingParams.rowChunkSize),
      Math.floor(col / this._loadingParams.colChunkSize),
    ];
  }

  private chunkIsValid(chunk_idx: [number, number]): boolean {
    const [row_chunk, col_chunk] = chunk_idx;
    const [max_row_chunk, max_col_chunk] = this.chunkIdx(this._numRows - 1, this._numCols - 1);
    return (
      row_chunk >= 0 && row_chunk <= max_row_chunk && col_chunk >= 0 && col_chunk <= max_col_chunk
    );
  }

  private readonly _loadingParams: Required<ArrowModel.LoadingOptions>;
  private readonly _fileInfo: FileInfo;
  private _fileOptions: FileOptions;

  private _numRows: number = 0;
  private _numCols: number = 0;
  private _schema!: Arrow.Schema;
  private _chunks: PairMap<number, number, Arrow.Table | Promise<void>> = new PairMap();
  private _ready: Promise<void>;
}
