export interface SqliteOptions {
  table_name?: string;
}

export interface CsvOptions {
  delimiter?: string;
}

export type FileOptions = SqliteOptions | CsvOptions;
