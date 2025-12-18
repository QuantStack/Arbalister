import { tableFromIPC } from "apache-arrow";
import type * as Arrow from "apache-arrow";

import type { FileOptions } from "./file_options";

export interface StatsOptions {
  path: string;
}

export interface StatsResponse {
  num_rows: number;
  num_cols: number;
}

/**
 * Transform a union into a union where every member is optionally present.
 */
type OptionalizeUnion<T> = {
  [K in T extends unknown ? keyof T : never]?: T extends Record<K, infer V> ? V : never;
};

export async function fetchStats(
  params: Readonly<StatsOptions & FileOptions>,
): Promise<StatsResponse> {
  const queryKeys = ["path", "delimiter"] as const;

  const query = new URLSearchParams();

  for (const key of queryKeys) {
    const value = (params as Readonly<TableOptions> & OptionalizeUnion<FileOptions>)[key];
    if (value !== undefined && value != null) {
      query.set(key, value.toString());
    }
  }

  const response = await fetch(`/arrow/stats/${params.path}?${query.toString()}`);
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

export async function fetchTable(
  params: Readonly<TableOptions & FileOptions>,
): Promise<Arrow.Table> {
  const queryKeys = [
    "row_chunk_size",
    "row_chunk",
    "col_chunk_size",
    "col_chunk",
    "delimiter",
  ] as const;

  const query = new URLSearchParams();

  for (const key of queryKeys) {
    const value = (params as Readonly<TableOptions> & OptionalizeUnion<FileOptions>)[key];
    if (value !== undefined && value != null) {
      query.set(key, value.toString());
    }
  }

  const url = `/arrow/stream/${params.path}?${query.toString()}`;
  return await tableFromIPC(fetch(url));
}
