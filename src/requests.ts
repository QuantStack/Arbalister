import { tableFromIPC } from "apache-arrow";
import type * as Arrow from "apache-arrow";

export interface StatsOptions {
  path: string;
}

export interface StatsResponse {
  num_rows: number;
  num_cols: number;
}

export async function fetchStats(params: Readonly<StatsOptions>): Promise<StatsResponse> {
  const response = await fetch(`/arrow/stats/${params.path}`);
  const data = await response.json();
  return data;
}

export interface TableOptions {
  path: string;
  row_chunk_size?: number;
  row_chunk?: number;
  col_chunk_size?: number;
  col_chunk?: number;
}

export async function fetchTable(params: Readonly<TableOptions>): Promise<Arrow.Table> {
  const queryKeys = ["row_chunk_size", "row_chunk", "col_chunk_size", "col_chunk"] as const;

  const query: string[] = [];
  for (const key of queryKeys) {
    const value = params[key];
    if (value !== undefined) {
      query.push(`${key}=${encodeURIComponent(value)}`);
    }
  }

  const queryString = query.length ? `?${query.join("&")}` : "";
  const url = `/arrow/stream/${params.path}${queryString}`;
  return await tableFromIPC(fetch(url));
}
