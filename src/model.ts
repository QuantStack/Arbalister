import { DataModel } from "@lumino/datagrid";

import { tableFromIPC } from "apache-arrow";
import type * as Arrow from "apache-arrow";

export class ArrowModel extends DataModel {
  constructor(path: string) {
    super();
    this._path = path;
    this._ready = this.initialize();
  }

  protected async initialize(): Promise<void> {
    // Works with IPC stream and file
    this._table = await tableFromIPC(fetch(`/arrow/stream/${this._path}`));
  }

  get ready(): Promise<void> {
    return this._ready;
  }

  private get table(): Arrow.Table {
    return this._table!;
  }

  columnCount(region: DataModel.ColumnRegion): number {
    if (region === "body") {
      return this.table.numCols;
    }
    return 1;
  }

  rowCount(region: DataModel.RowRegion): number {
    if (region === "body") {
      return this.table.numRows;
    }
    return 1;
  }

  data(region: DataModel.CellRegion, row: number, column: number): string {
    switch (region) {
      case "body":
        return this.table.getChildAt(column)?.get(row).toString();
      case "column-header":
        return this.table.schema.names[column].toString();
      case "row-header":
        return row.toString();
      case "corner-header":
        return "";
      default:
        throw "unreachable";
    }
  }

  private _path: string;
  private _table: Arrow.Table | undefined = undefined;
  private _ready: Promise<void>;
}
