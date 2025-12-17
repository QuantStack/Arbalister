import type { DocumentRegistry } from "@jupyterlab/docregistry";
import type { Contents } from "@jupyterlab/services";

export type ContentType = Contents.ContentType;

export namespace ContentType {
  export const Avro: ContentType = "apache-avro";
  export const Csv: ContentType = "csv";
  export const Ipc: ContentType = "apache-arrow-ipc-avro";
  export const Orc: ContentType = "apache-orc";
  export const Parquet: ContentType = "apache-parquet";
  export const Sqlite: ContentType = "sqlite";
}

export function ensureCsvFileType(docRegistry: DocumentRegistry): DocumentRegistry.IFileType {
  const ft = docRegistry.getFileType(ContentType.Csv);
  if (ft) {
    return ft;
  }
  docRegistry.addFileType({
    name: ContentType.Csv,
    displayName: "CSV",
    mimeTypes: ["text/csv"],
    extensions: [".csv"],
    contentType: "file",
  });
  return docRegistry.getFileType(ContentType.Csv)!;
}

export function addAvroFileType(
  docRegistry: DocumentRegistry,
  options: Partial<DocumentRegistry.IFileType> = {},
): DocumentRegistry.IFileType {
  docRegistry.addFileType({
    ...options,
    name: ContentType.Avro,
    displayName: "Avro",
    mimeTypes: ["application/avro-binary"],
    extensions: [".avro"],
    contentType: "file",
    fileFormat: "base64",
  });
  return docRegistry.getFileType(ContentType.Avro)!;
}

export function addParquetFileType(
  docRegistry: DocumentRegistry,
  options: Partial<DocumentRegistry.IFileType> = {},
): DocumentRegistry.IFileType {
  docRegistry.addFileType({
    ...options,
    name: ContentType.Parquet,
    displayName: "Parquet",
    mimeTypes: ["application/vnd.apache.parquet"],
    extensions: [".parquet"],
    contentType: "file",
    fileFormat: "base64",
  });
  return docRegistry.getFileType(ContentType.Parquet)!;
}

export function addIpcFileType(
  docRegistry: DocumentRegistry,
  options: Partial<DocumentRegistry.IFileType> = {},
): DocumentRegistry.IFileType {
  docRegistry.addFileType({
    ...options,
    name: ContentType.Ipc,
    displayName: "Arrow IPC",
    mimeTypes: ["application/vnd.apache.arrow.file"],
    extensions: [".ipc", ".feather", ".arrow"],
    contentType: "file",
    fileFormat: "base64",
  });
  return docRegistry.getFileType(ContentType.Ipc)!;
}

export function addOrcFileType(
  docRegistry: DocumentRegistry,
  options: Partial<DocumentRegistry.IFileType> = {},
): DocumentRegistry.IFileType {
  docRegistry.addFileType({
    ...options,
    name: ContentType.Orc,
    displayName: "Arrow ORC",
    mimeTypes: ["application/octet-stream"],
    extensions: [".orc"],
    contentType: "file",
    fileFormat: "base64",
  });
  return docRegistry.getFileType(ContentType.Orc)!;
}

export function addSqliteFileType(
  docRegistry: DocumentRegistry,
  options: Partial<DocumentRegistry.IFileType> = {},
): DocumentRegistry.IFileType {
  docRegistry.addFileType({
    ...options,
    name: ContentType.Sqlite,
    displayName: "SQLite",
    mimeTypes: ["application/vnd.sqlite3"],
    extensions: [".sqlite", ".sqlite3", ".db", ".db3", ".s3db", ".sl3"],
    contentType: "file",
    fileFormat: "base64",
  });
  return docRegistry.getFileType(ContentType.Sqlite)!;
}
