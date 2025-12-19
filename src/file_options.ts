export interface CsvOptions {
  delimiter?: string;
}

export const DEFAULT_CSV_OPTIONS: Required<CsvOptions> = {
  delimiter: ",",
};

export interface SqliteOptions {
  table_name?: string;
}

export const DEFAULT_SQLITE_OPTIONS: Required<SqliteOptions> = {
  table_name: "sqlite_master",
};

export type FileOptions = CsvOptions | SqliteOptions;
