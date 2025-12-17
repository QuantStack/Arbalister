export interface SqliteOptions {
  table_name?: string;
}

export interface CsvOptions {
  delimiter?: string;
}

export const DEFAULT_CSV_OPTIONS: Required<CsvOptions> = {
  delimiter: ",",
};

export type FileOptions = SqliteOptions | CsvOptions;
