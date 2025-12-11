import { tableFromIPC } from "apache-arrow";
import type * as Arrow from "apache-arrow";

export interface StatsParams {
  readonly path: string;
}

export interface StatsResponse {
  readonly num_rows: number;
  readonly num_cols: number;
}

export async function fetchStats(params: StatsParams): Promise<StatsResponse> {
  const response = await fetch(`/arrow/stats/${params.path}`);
  const data = await response.json();
  return data;
}

export interface TableParams {
  readonly path: string;
  readonly row_chunk_size?: number;
  readonly row_chunk?: number;
  readonly col_chunk_size?: number;
  readonly col_chunk?: number;
}

export async function fetchTable(params: TableParams): Promise<Arrow.Table> {
  const query: string[] = [];
  if (params.row_chunk_size !== undefined) {
    query.push(`row_chunk_size=${encodeURIComponent(params.row_chunk_size)}`);
  }
  if (params.row_chunk !== undefined) {
    query.push(`row_chunk=${encodeURIComponent(params.row_chunk)}`);
  }
  if (params.col_chunk_size !== undefined) {
    query.push(`col_chunk_size=${encodeURIComponent(params.col_chunk_size)}`);
  }
  if (params.col_chunk !== undefined) {
    query.push(`col_chunk=${encodeURIComponent(params.col_chunk)}`);
  }
  const queryString = query.length ? `?${query.join("&")}` : "";
  const url = `/arrow/stream/${params.path}${queryString}`;
  return await tableFromIPC(fetch(url));
}
