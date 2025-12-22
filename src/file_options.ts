export interface CsvOptions {
  delimiter: string;
}

export interface SqliteOptions {
  table_name: string;
}

export type FileOptions = CsvOptions | SqliteOptions;

export interface SqliteFileInfo {
  table_names: string[];
}

export interface CsvFileInfo {
  delimiters: string[];
}

export type FileInfo = SqliteFileInfo | CsvFileInfo;
