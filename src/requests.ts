import { tableFromIPC } from "apache-arrow";
import type * as Arrow from "apache-arrow";

import type { FileInfo, FileOptions } from "./file_options";

export interface FileInfoOptions {
  path: string;
}

export interface FileInfoResponse {
  info: FileInfo;
  read_params: FileOptions;
}

export async function fetchFileInfo(params: Readonly<FileInfoOptions>): Promise<FileInfoResponse> {
  const response = await fetch(`/file/info/${params.path}`);
  const data: FileInfoResponse = await response.json();
  return data;
}

export interface StatsOptions {
  path: string;
}

interface SchemaInfo {
  data: string;
  mimetype: string;
  encoding: string;
}

interface StatsResponseRaw {
  num_rows: number;
  num_cols: number;
  schema: SchemaInfo;
}

export interface StatsResponse {
  num_rows: number;
  num_cols: number;
  schema: Arrow.Schema;
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
  const queryKeys = ["path", "delimiter", "table_name"] as const;
  const queryKeyMap: Record<string, string> = {
    tableName: "table_name",
  };

  const query = new URLSearchParams();

  for (const key of queryKeys) {
    const value = (params as Readonly<TableOptions> & OptionalizeUnion<FileOptions>)[key];
    if (value !== undefined && value != null) {
      const queryKey = queryKeyMap[key] || key;
      query.set(queryKey, value.toString());
    }
  }

  const response = await fetch(`/arrow/stats/${params.path}?${query.toString()}`);
  const data: StatsResponseRaw = await response.json();

  // Validate encoding and content type
  if (data.schema.encoding !== "base64") {
    throw new Error(`Unexpected schema encoding: ${data.schema.encoding}, expected "base64"`);
  }

  if (data.schema.mimetype !== "application/vnd.apache.arrow.stream") {
    throw new Error(
      `Unexpected schema mimetype: ${data.schema.mimetype}, expected "application/vnd.apache.arrow.stream"`,
    );
  }

  // Decode base64 data
  const binaryString = atob(data.schema.data);
  const bytes = new Uint8Array(binaryString.length);
  for (let i = 0; i < binaryString.length; i++) {
    bytes[i] = binaryString.charCodeAt(i);
  }

  // Parse Arrow IPC stream to extract schema
  const table = tableFromIPC(bytes);
  const schema = table.schema;

  return {
    num_rows: data.num_rows,
    num_cols: data.num_cols,
    schema,
  };
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
    "table_name",
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
