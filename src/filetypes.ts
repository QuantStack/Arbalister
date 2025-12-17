import type { DocumentRegistry } from "@jupyterlab/docregistry";
import type { Contents } from "@jupyterlab/services";

export type ContentType = Contents.ContentType;

export const CONTENT_TYPE_AVRO: ContentType = "apache-avro";
export const CONTENT_TYPE_PARQUET: ContentType = "apache-parquet";
export const CONTENT_TYPE_IPC: ContentType = "apache-arrow-ipc-avro";
export const CONTENT_TYPE_ORC: ContentType = "apache-orc";
export const CONTENT_TYPE_SQLITE: ContentType = "sqlite";

export function addAvroFileType(
  docRegistry: DocumentRegistry,
  options: Partial<DocumentRegistry.IFileType> = {},
): DocumentRegistry.IFileType {
  docRegistry.addFileType({
    ...options,
    name: CONTENT_TYPE_AVRO,
    displayName: "Avro",
    mimeTypes: ["application/avro-binary"],
    extensions: [".avro"],
    contentType: "file",
    fileFormat: "base64",
  });
  return docRegistry.getFileType(CONTENT_TYPE_AVRO)!;
}

export function addParquetFileType(
  docRegistry: DocumentRegistry,
  options: Partial<DocumentRegistry.IFileType> = {},
): DocumentRegistry.IFileType {
  docRegistry.addFileType({
    ...options,
    name: CONTENT_TYPE_PARQUET,
    displayName: "Parquet",
    mimeTypes: ["application/vnd.apache.parquet"],
    extensions: [".parquet"],
    contentType: "file",
    fileFormat: "base64",
  });
  return docRegistry.getFileType(CONTENT_TYPE_PARQUET)!;
}

export function addIpcFileType(
  docRegistry: DocumentRegistry,
  options: Partial<DocumentRegistry.IFileType> = {},
): DocumentRegistry.IFileType {
  docRegistry.addFileType({
    ...options,
    name: CONTENT_TYPE_IPC,
    displayName: "Arrow IPC",
    mimeTypes: ["application/vnd.apache.arrow.file"],
    extensions: [".ipc", ".feather", ".arrow"],
    contentType: "file",
    fileFormat: "base64",
  });
  return docRegistry.getFileType(CONTENT_TYPE_IPC)!;
}

export function addOrcFileType(
  docRegistry: DocumentRegistry,
  options: Partial<DocumentRegistry.IFileType> = {},
): DocumentRegistry.IFileType {
  docRegistry.addFileType({
    ...options,
    name: CONTENT_TYPE_ORC,
    displayName: "Arrow ORC",
    mimeTypes: ["application/octet-stream"],
    extensions: [".orc"],
    contentType: "file",
    fileFormat: "base64",
  });
  return docRegistry.getFileType(CONTENT_TYPE_ORC)!;
}

export function addSqliteFileType(
  docRegistry: DocumentRegistry,
  options: Partial<DocumentRegistry.IFileType> = {},
): DocumentRegistry.IFileType {
  docRegistry.addFileType({
    ...options,
    name: CONTENT_TYPE_SQLITE,
    displayName: "SQLite",
    mimeTypes: ["application/vnd.sqlite3"],
    extensions: [".sqlite", ".sqlite3", ".db", ".db3", ".s3db", ".sl3"],
    contentType: "file",
    fileFormat: "base64",
  });
  return docRegistry.getFileType(CONTENT_TYPE_SQLITE)!;
}
