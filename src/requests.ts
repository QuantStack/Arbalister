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
  readonly per_chunk?: number;
  readonly chunk?: number;
}

export async function fetchTable(params: TableParams): Promise<Arrow.Table> {
  const query: string[] = [];
  if (params.per_chunk !== undefined) {
    query.push(`per_chunk=${encodeURIComponent(params.per_chunk)}`);
  }
  if (params.chunk !== undefined) {
    query.push(`chunk=${encodeURIComponent(params.chunk)}`);
  }
  const queryString = query.length ? `?${query.join("&")}` : "";
  const url = `/arrow/stream/${params.path}${queryString}`;
  return await tableFromIPC(fetch(url));
}
