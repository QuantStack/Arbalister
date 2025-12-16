import type { DocumentRegistry } from "@jupyterlab/docregistry";

export function addAvroFileType(
  docRegistry: DocumentRegistry,
  options: Partial<DocumentRegistry.IFileType> = {},
): DocumentRegistry.IFileType {
  const name = "apache-avro";
  docRegistry.addFileType({
    ...options,
    name,
    displayName: "Avro",
    mimeTypes: ["application/avro-binary"],
    extensions: [".avro"],
    contentType: "file",
    fileFormat: "base64",
  });
  return docRegistry.getFileType(name)!;
}

export function addParquetFileType(
  docRegistry: DocumentRegistry,
  options: Partial<DocumentRegistry.IFileType> = {},
): DocumentRegistry.IFileType {
  const name = "apache-parquet";
  docRegistry.addFileType({
    ...options,
    name,
    displayName: "Parquet",
    mimeTypes: ["application/vnd.apache.parquet"],
    extensions: [".parquet"],
    contentType: "file",
    fileFormat: "base64",
  });
  return docRegistry.getFileType(name)!;
}

export function addIpcFileType(
  docRegistry: DocumentRegistry,
  options: Partial<DocumentRegistry.IFileType> = {},
): DocumentRegistry.IFileType {
  const name = "apache-arrow-ipc";
  docRegistry.addFileType({
    ...options,
    name,
    displayName: "Arrow IPC",
    mimeTypes: ["application/vnd.apache.arrow.file"],
    extensions: [".ipc", ".feather", ".arrow"],
    contentType: "file",
    fileFormat: "base64",
  });
  return docRegistry.getFileType(name)!;
}

export function addOrcFileType(
  docRegistry: DocumentRegistry,
  options: Partial<DocumentRegistry.IFileType> = {},
): DocumentRegistry.IFileType {
  const name = "apache-orc";
  docRegistry.addFileType({
    ...options,
    name,
    displayName: "Arrow ORC",
    mimeTypes: ["application/octet-stream"],
    extensions: [".orc"],
    contentType: "file",
    fileFormat: "base64",
  });
  return docRegistry.getFileType(name)!;
}

export function addSqliteFileType(
  docRegistry: DocumentRegistry,
  options: Partial<DocumentRegistry.IFileType> = {},
): DocumentRegistry.IFileType {
  const name = "sqlite";
  docRegistry.addFileType({
    ...options,
    name,
    displayName: "SQLite",
    mimeTypes: ["application/vnd.sqlite3"],
    extensions: [".sqlite", ".sqlite3", ".db", ".db3", ".s3db", ".sl3"],
    contentType: "file",
    fileFormat: "base64",
  });
  return docRegistry.getFileType(name)!;
}
