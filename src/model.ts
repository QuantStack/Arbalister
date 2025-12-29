import { DataModel } from "@lumino/datagrid";

import type * as Arrow from "apache-arrow";

import { PairMap } from "./collection";
import { fetchFileInfo, fetchStats, fetchTable } from "./requests";
import type { FileInfo, FileReadOptions } from "./file-options";

export namespace ArrowModel {
  export interface PrefetchFactors {
    rowPrefetchFactor?: number;
    colPrefetchFactor?: number;
  }

  export interface LoadingOptions {
    path: string;
    rowChunkSize?: number;
    colChunkSize?: number;
    loadingRepr?: string;
    nullRepr?: string;
    prefetchFactors?: PrefetchFactors;
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
      rowChunkSize: 256,
      colChunkSize: 24,
      loadingRepr: "",
      nullRepr: "",
      ...loadingOptions,
      prefetchFactors: {
        rowPrefetchFactor: 16,
        colPrefetchFactor: 16,
        ...loadingOptions.prefetchFactors,
      },
    };
    this._fileOptions = fileOptions;
    this._fileInfo = fileInfo;

    this._ready = this.initialize();
  }

  protected async initialize(): Promise<void> {
    const stats = await fetchStats({ path: this._loadingParams.path, ...this._fileOptions });
    this._schema = stats.schema;
    this._numCols = stats.num_cols;
    this._numRows = stats.num_rows;
    this._chunks = new ChunkMap({
      rowChunkSize: this._loadingParams.rowChunkSize,
      numRows: this._numRows,
      colChunkSize: this._loadingParams.colChunkSize,
      numCols: this._numCols,
    });

    const chunkIdx00 = this._chunks.getChunkIdx({ rowIdx: 0, colIdx: 0 });
    await this.fetchThenStoreChunk(chunkIdx00);
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
      case "column-header": {
        // This is to showcase that we can put additional information in the column header but it
        // does not look good. HuggingFace dataset has some good inspiration.
        const field = this.schema.fields[column];
        return `${field.name} (${field.type}${field.nullable ? " | null" : ""})`;
      }
      case "row-header":
        return row.toString();
      case "corner-header":
        return "";
      default:
        throw "unreachable";
    }
  }

  private dataBody(row: number, col: number): string {
    const chunkIdx = this._chunks.getChunkIdx({ rowIdx: row, colIdx: col });

    if (this._chunks.has(chunkIdx)) {
      const chunk = this._chunks.get(chunkIdx)!;
      if (chunk.type === "pending") {
        // Wait for Promise to complete, and let it will mark data as modified.
        // If it was created through a prefetch, it does emit a change so we add it.
        if (chunk.reason === "prefetch") {
          const promise = chunk.promise.then((_) => this.emitChangedChunk(chunkIdx));
          this.storeChunkData(chunkIdx, ChunkMap.makePendingChunk({ promise, reason: "query" }));
        }
        return this._loadingParams.loadingRepr;
      }

      // We have data
      const rowIdxInChunk = row - chunk.startRow;
      const colIdxInChunk = col - chunk.startCol;
      const val = chunk.data.getChildAt(colIdxInChunk)?.get(rowIdxInChunk);
      const out = val?.toString() || this._loadingParams.nullRepr;

      // Prefetch next chunks only once we have the current data to prioritize current view
      this.prefetchAsNeededForChunk(chunkIdx);

      return out;
    }

    // Fetch data, however we cannot await it due to the interface required by the DataGrid.
    // Instead, we fire the request, and notify of change upon completion.
    const promise = this.fetchThenStoreChunk(chunkIdx).then((_) => this.emitChangedChunk(chunkIdx));
    this.storeChunkData(chunkIdx, ChunkMap.makePendingChunk({ promise, reason: "query" }));

    return this._loadingParams.loadingRepr;
  }

  private async fetchThenStoreChunk(
    chunkIdx: ChunkMap.ChunkIdx,
    factors: Required<ArrowModel.PrefetchFactors> = { rowPrefetchFactor: 1, colPrefetchFactor: 1 },
  ): Promise<void> {
    const { chunkRowIdx, chunkColIdx } = chunkIdx;

    const startRow = chunkRowIdx * this._loadingParams.rowChunkSize;
    const endRow = Math.min(
      startRow + this._loadingParams.rowChunkSize * factors.rowPrefetchFactor,
      this._numRows,
    );
    const startCol = chunkColIdx * this._loadingParams.colChunkSize;
    const endCol = Math.min(
      startCol + this._loadingParams.colChunkSize * factors.colPrefetchFactor,
      this._numCols,
    );

    const table = await fetchTable({
      path: this._loadingParams.path,
      start_row: startRow,
      end_row: endRow,
      start_col: startCol,
      end_col: endCol,
      ...this._fileOptions,
    });
    const chunk: ChunkMap.Chunk = ChunkMap.makeChunk({
      data: table,
      startRow,
      startCol,
    });

    this.storeChunkData(chunkIdx, chunk, factors);
  }

  private storeChunkData(
    chunkIdx: ChunkMap.ChunkIdx,
    data: ChunkMap.ChunkData,
    factors: Required<ArrowModel.PrefetchFactors> = { rowPrefetchFactor: 1, colPrefetchFactor: 1 },
  ) {
    const { chunkRowIdx, chunkColIdx } = chunkIdx;
    // If a mulitpliying factor is used, we store the chunk in multiple places so that it can
    // be found in constant time.
    // Since the chunk stores its start row/col it does not matter on which row the chunk starts
    // as long as it contains the expected data.
    for (let r = 0; r < factors.rowPrefetchFactor; r++) {
      for (let c = 0; c < factors.colPrefetchFactor; c++) {
        this._chunks.set({ chunkRowIdx: chunkRowIdx + r, chunkColIdx: chunkColIdx + c }, data);
      }
    }
  }

  private emitChangedChunk(chunkIdx: ChunkMap.ChunkIdx) {
    const { chunkRowIdx, chunkColIdx } = chunkIdx;

    // We must ensure the range is within the bounds
    const rowStart = chunkRowIdx * this._loadingParams.rowChunkSize;
    const rowEnd = Math.min(rowStart + this._loadingParams.rowChunkSize, this._numRows);
    const colStart = chunkColIdx * this._loadingParams.colChunkSize;
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

  /**
   * Prefetch next chunks if available.
   *
   * We chain the Promise because this can be considered a low priority operation so we want
   * to reduce load on the server.
   */
  private prefetchAsNeededForChunk(chunkIdx: ChunkMap.ChunkIdx) {
    const { chunkRowIdx, chunkColIdx } = chunkIdx;

    let promise = Promise.resolve();

    // This chunk uses the row prefetching multiplier and runs first as we estimate
    // scrolling horizontally is more likely.
    const nextRowsChunkIdx: ChunkMap.ChunkIdx = { chunkRowIdx: chunkRowIdx + 1, chunkColIdx };
    if (!this._chunks.has(nextRowsChunkIdx) && this._chunks.chunkIsValid(nextRowsChunkIdx)) {
      const rowFactors = {
        rowPrefetchFactor: this._loadingParams.prefetchFactors.rowPrefetchFactor,
        colPrefetchFactor: 1,
      };
      promise = promise.then((_) => this.fetchThenStoreChunk(nextRowsChunkIdx, rowFactors));
      this.storeChunkData(
        nextRowsChunkIdx,
        ChunkMap.makePendingChunk({ promise, reason: "prefetch" }),
        rowFactors,
      );
    }

    // This chunk uses the column prefetching multiplier and waits for the previous to complete
    // before running to reduce load on the server.
    const nextColsChunkIdx: ChunkMap.ChunkIdx = { chunkRowIdx, chunkColIdx: chunkColIdx + 1 };
    if (!this._chunks.has(nextColsChunkIdx) && this._chunks.chunkIsValid(nextColsChunkIdx)) {
      const colFactors = {
        rowPrefetchFactor: 1,
        colPrefetchFactor: this._loadingParams.prefetchFactors.colPrefetchFactor,
      };
      promise = promise.then((_) => this.fetchThenStoreChunk(nextColsChunkIdx, colFactors));
      this.storeChunkData(
        nextColsChunkIdx,
        ChunkMap.makePendingChunk({ promise, reason: "prefetch" }),
        colFactors,
      );
    }
  }

  private readonly _loadingParams: DeepRequired<ArrowModel.LoadingOptions>;
  private readonly _fileInfo: FileInfo;
  private _fileOptions: FileReadOptions;

  private _numRows: number = 0;
  private _numCols: number = 0;
  private _schema!: Arrow.Schema;
  private _chunks!: ChunkMap;
  private _ready: Promise<void>;
}

class ChunkMap {
  constructor(parameters: ChunkMap.Parameters) {
    this._parameters = parameters;
  }

  getChunkIdx(cellIdx: ChunkMap.CellIdx): ChunkMap.ChunkIdx {
    return {
      chunkRowIdx: Math.floor(cellIdx.rowIdx / this._parameters.rowChunkSize),
      chunkColIdx: Math.floor(cellIdx.colIdx / this._parameters.colChunkSize),
    };
  }

  chunkIsValid(chunkIdx: ChunkMap.ChunkIdx): boolean {
    const { chunkRowIdx, chunkColIdx } = chunkIdx;
    const { chunkRowIdx: maxChunkRowIdx, chunkColIdx: maxChunkColIdx } = this.getChunkIdx({
      rowIdx: this._parameters.numRows - 1,
      colIdx: this._parameters.numCols - 1,
    });
    return (
      chunkRowIdx >= 0 &&
      chunkRowIdx <= maxChunkRowIdx &&
      chunkColIdx >= 0 &&
      chunkColIdx <= maxChunkColIdx
    );
  }

  set(chunkIdx: ChunkMap.ChunkIdx, value: ChunkMap.ChunkData): this {
    this._map.set(ChunkMap._chunkIdxToKey(chunkIdx), value);
    return this;
  }

  get(chunkIdx: ChunkMap.ChunkIdx): ChunkMap.ChunkData | undefined {
    return this._map.get(ChunkMap._chunkIdxToKey(chunkIdx));
  }

  clear(): void {
    this._map.clear();
  }

  delete(chunkIdx: ChunkMap.ChunkIdx): boolean {
    return this._map.delete(ChunkMap._chunkIdxToKey(chunkIdx));
  }

  has(chunkIdx: ChunkMap.ChunkIdx): boolean {
    return this._map.has(ChunkMap._chunkIdxToKey(chunkIdx));
  }

  get size(): number {
    return this._map.size;
  }

  forEach(
    callbackfn: (value: ChunkMap.ChunkData, key: ChunkMap.ChunkIdx, map: ChunkMap) => void,
    // biome-ignore lint/suspicious/noExplicitAny: This is in the Map signature
    thisArg?: any,
  ): void {
    this._map.forEach((value, key) => {
      callbackfn.call(thisArg, value, ChunkMap._keyToChunkIdx(key), this);
    });
  }

  private static _chunkIdxToKey(chunkIdx: ChunkMap.ChunkIdx): [number, number] {
    return [chunkIdx.chunkRowIdx, chunkIdx.chunkColIdx];
  }

  private static _keyToChunkIdx(key: [number, number]): ChunkMap.ChunkIdx {
    return { chunkRowIdx: key[0], chunkColIdx: key[1] };
  }

  private _map = new PairMap<number, number, ChunkMap.ChunkData>();
  private _parameters: Required<ChunkMap.Parameters>;
}

namespace ChunkMap {
  export type Parameters = {
    rowChunkSize: number;
    numRows: number;
    colChunkSize: number;
    numCols: number;
  };

  export type ChunkIdx = {
    chunkRowIdx: number;
    chunkColIdx: number;
  };

  export type CellIdx = {
    rowIdx: number;
    colIdx: number;
  };

  export type Chunk = {
    data: Arrow.Table;
    startRow: number;
    startCol: number;
    readonly type: "chunk";
  };

  export function makeChunk(chunk: Omit<Chunk, "type">): Chunk {
    return {
      ...chunk,
      type: "chunk",
    };
  }

  export type PendingChunk = {
    promise: Promise<void>;
    reason: "query" | "prefetch";
    readonly type: "pending";
  };

  export function makePendingChunk(chunk: Omit<PendingChunk, "type">): PendingChunk {
    return {
      ...chunk,
      type: "pending",
    };
  }

  export type ChunkData = Chunk | PendingChunk;
}

type DeepRequired<T> = T extends object ? { [K in keyof T]-?: DeepRequired<T[K]> } : T;
