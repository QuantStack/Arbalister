import { FileType } from "./file-types";

export interface CsvReadOptions {
  delimiter: string;
}

export interface SqliteReadOptions {
  table_name: string;
}

export interface CsvFileInfo {
  delimiters: string[];
}

export interface SqliteFileInfo {
  table_names: string[];
}

/**
 * Central registry mapping FileType to its related types.
 * This ensures type safety when working with file-type-specific data.
 */
interface FileTypeRegistry {
  [FileType.Csv]: {
    readOptions: CsvReadOptions;
    info: CsvFileInfo;
  };
  [FileType.Sqlite]: {
    readOptions: SqliteReadOptions;
    info: SqliteFileInfo;
  };
}

/**
 * Extract the options type for a specific FileType.
 */
export type FileReadOptionsFor<T extends FileType> = T extends keyof FileTypeRegistry
  ? FileTypeRegistry[T]["readOptions"]
  : never;

/**
 * Extract the info type for a specific FileType.
 */
export type FileInfoFor<T extends FileType> = T extends keyof FileTypeRegistry
  ? FileTypeRegistry[T]["info"]
  : never;

/**
 * Union of all possible file options.
 */
export type FileReadOptions = FileTypeRegistry[keyof FileTypeRegistry]["readOptions"];

/**
 * Union of all possible file info.
 */
export type FileInfo = FileTypeRegistry[keyof FileTypeRegistry]["info"];
