import { DataModel } from "@lumino/datagrid";

import { tableFromIPC } from "apache-arrow";
import type * as Arrow from "apache-arrow";

export class ArrowModel extends DataModel {
  static async fetch(path: string): Promise<ArrowModel> {
    // Works with IPC stream and file
    const table = await tableFromIPC(fetch(`/arrow/stream/${path}`));
    return new ArrowModel(table);
  }

  constructor(table: Arrow.Table) {
    super();
    this.data_ = table;
  }

  columnCount(region: DataModel.ColumnRegion): number {
    if (region === "body") {
      return this.data_.numCols;
    }
    return 1;
  }

  rowCount(region: DataModel.RowRegion): number {
    if (region === "body") {
      return this.data_.numRows;
    }
    return 1;
  }

  data(region: DataModel.CellRegion, row: number, column: number): string {
    switch (region) {
      case "body":
        return this.data_.getChildAt(column)?.get(row).toString();
      case "column-header":
        return this.data_.schema.names[column].toString();
      case "row-header":
        return row.toString();
      case "corner-header":
        return "";
      default:
        throw "unreachable";
    }
  }

  private data_: Arrow.Table;
}
