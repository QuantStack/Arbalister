export interface CsvOptions {
  delimiter?: string;
}

export const DEFAULT_CSV_OPTIONS: Required<CsvOptions> = {
  delimiter: ",",
};

export type FileOptions = CsvOptions;
