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
  table_name: "Table",
};

export type FileOptions = CsvOptions | SqliteOptions;

export interface SqliteFileInfo {
  table_names: string[];
}

export interface CsvFileInfo {
  delimiters: string[];
}

export type FileInfo = SqliteFileInfo | CsvFileInfo;
