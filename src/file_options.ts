export interface CsvOptions {
  delimiter?: string;
}

export const DEFAULT_CSV_OPTIONS: Required<CsvOptions> = {
  delimiter: ",",
};

export interface SqliteOptions {
  tableName?: string;
}

export const DEFAULT_SQLITE_OPTIONS: Required<SqliteOptions> = {
  tableName: "sqlite_master",
};

export type FileOptions = CsvOptions | SqliteOptions;
