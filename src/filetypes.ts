import type { DocumentRegistry } from "@jupyterlab/docregistry";

export enum FileType {
  Avro = "apache-avro",
  Csv = "csv",
  Ipc = "apache-arrow-ipc-avro",
  Orc = "apache-orc",
  Parquet = "apache-parquet",
  Sqlite = "sqlite",
}

export function ensureCsvFileType(docRegistry: DocumentRegistry): DocumentRegistry.IFileType {
  const ft = docRegistry.getFileType(FileType.Csv);
  if (ft) {
    return ft;
  }
  docRegistry.addFileType({
    name: FileType.Csv,
    displayName: "CSV",
    mimeTypes: ["text/csv"],
    extensions: [".csv"],
    contentType: "file",
  });
  return docRegistry.getFileType(FileType.Csv)!;
}

export function addAvroFileType(
  docRegistry: DocumentRegistry,
  options: Partial<DocumentRegistry.IFileType> = {},
): DocumentRegistry.IFileType {
  docRegistry.addFileType({
    ...options,
    name: FileType.Avro,
    displayName: "Avro",
    mimeTypes: ["application/avro-binary"],
    extensions: [".avro"],
    contentType: "file",
    fileFormat: "base64",
  });
  return docRegistry.getFileType(FileType.Avro)!;
}

export function addParquetFileType(
  docRegistry: DocumentRegistry,
  options: Partial<DocumentRegistry.IFileType> = {},
): DocumentRegistry.IFileType {
  docRegistry.addFileType({
    ...options,
    name: FileType.Parquet,
    displayName: "Parquet",
    mimeTypes: ["application/vnd.apache.parquet"],
    extensions: [".parquet"],
    contentType: "file",
    fileFormat: "base64",
  });
  return docRegistry.getFileType(FileType.Parquet)!;
}

export function addIpcFileType(
  docRegistry: DocumentRegistry,
  options: Partial<DocumentRegistry.IFileType> = {},
): DocumentRegistry.IFileType {
  docRegistry.addFileType({
    ...options,
    name: FileType.Ipc,
    displayName: "Arrow IPC",
    mimeTypes: ["application/vnd.apache.arrow.file"],
    extensions: [".ipc", ".feather", ".arrow"],
    contentType: "file",
    fileFormat: "base64",
  });
  return docRegistry.getFileType(FileType.Ipc)!;
}

export function addOrcFileType(
  docRegistry: DocumentRegistry,
  options: Partial<DocumentRegistry.IFileType> = {},
): DocumentRegistry.IFileType {
  docRegistry.addFileType({
    ...options,
    name: FileType.Orc,
    displayName: "Arrow ORC",
    mimeTypes: ["application/octet-stream"],
    extensions: [".orc"],
    contentType: "file",
    fileFormat: "base64",
  });
  return docRegistry.getFileType(FileType.Orc)!;
}

export function addSqliteFileType(
  docRegistry: DocumentRegistry,
  options: Partial<DocumentRegistry.IFileType> = {},
): DocumentRegistry.IFileType {
  docRegistry.addFileType({
    ...options,
    name: FileType.Sqlite,
    displayName: "SQLite",
    mimeTypes: ["application/vnd.sqlite3"],
    extensions: [".sqlite", ".sqlite3", ".db", ".db3", ".s3db", ".sl3"],
    contentType: "file",
    fileFormat: "base64",
  });
  return docRegistry.getFileType(FileType.Sqlite)!;
}
