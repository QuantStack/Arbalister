import { LabIcon } from "@jupyterlab/ui-components";
import type { DocumentRegistry } from "@jupyterlab/docregistry";

import arrowIpcSvg from "../style/icons/arrow.svg";
import arrowIpcDarkSvg from "../style/icons/arrow_dark.svg";
import avroSvg from "../style/icons/avro.svg";
import orcLightSvg from "../style/icons/orc.svg";
import orcDarkSvg from "../style/icons/orc_dark.svg";
import parquetSvgLight from "../style/icons/parquet.svg";
import parquetSvgDark from "../style/icons/parquet_dark.svg";
import sqliteSvgLight from "../style/icons/sqlite.svg";
import sqliteSvgDark from "../style/icons/sqlite_dark.svg";

export enum FileType {
  Avro = "apache-avro",
  Csv = "csv",
  Ipc = "apache-arrow-ipc-avro",
  Orc = "apache-orc",
  Parquet = "apache-parquet",
  Sqlite = "sqlite",
}

export namespace FileType {
  export function all(): FileType[] {
    return Object.values(FileType).filter((v): v is FileType => typeof v === "string");
  }
}

function _getIconSvg(fileType: FileType, isLight: boolean): string {
  switch (fileType) {
    case FileType.Parquet:
      return isLight ? parquetSvgLight : parquetSvgDark;
    case FileType.Ipc:
      return isLight ? arrowIpcSvg : arrowIpcDarkSvg;
    case FileType.Orc:
      return isLight ? orcLightSvg : orcDarkSvg;
    case FileType.Avro:
      return avroSvg;
    case FileType.Sqlite:
      return isLight ? sqliteSvgLight : sqliteSvgDark;
    case FileType.Csv:
      throw new Error(`CSV file type does not have an icon`);
    default:
      throw new Error(`Unknown file type: ${fileType}`);
  }
}

function _makeIcon(fileType: FileType, isLight: boolean): LabIcon {
  return new LabIcon({
    name: `arbalister:${fileType}`,
    svgstr: _getIconSvg(fileType, isLight),
  });
}

function _updateIcon(icon: LabIcon, fileType: FileType, isLight: boolean) {
  icon.svgstr = _getIconSvg(fileType, isLight);
}

export function addCsvFileType(
  docRegistry: DocumentRegistry,
  options: Partial<DocumentRegistry.IFileType> = {},
): DocumentRegistry.IFileType {
  docRegistry.addFileType({
    ...options,
    name: FileType.Csv,
    displayName: "CSV",
    mimeTypes: ["text/csv"],
    extensions: [".csv"],
    contentType: "file",
    fileFormat: "text",
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

export function ensureFileType(
  docRegistry: DocumentRegistry,
  fileType: FileType,
  isLight: boolean,
): DocumentRegistry.IFileType {
  const ft = docRegistry.getFileType(fileType);
  if (ft) {
    return ft;
  }
  switch (fileType) {
    case FileType.Avro:
      return addAvroFileType(docRegistry, { icon: _makeIcon(FileType.Avro, isLight) });
    case FileType.Parquet:
      return addParquetFileType(docRegistry, { icon: _makeIcon(FileType.Parquet, isLight) });
    case FileType.Ipc:
      return addIpcFileType(docRegistry, { icon: _makeIcon(FileType.Ipc, isLight) });
    case FileType.Orc:
      return addOrcFileType(docRegistry, { icon: _makeIcon(FileType.Orc, isLight) });
    case FileType.Sqlite:
      return addSqliteFileType(docRegistry, { icon: _makeIcon(FileType.Sqlite, isLight) });
    case FileType.Csv:
      return addCsvFileType(docRegistry);
    default:
      throw new Error(`Unknown file type: ${fileType}`);
  }
}

export function updateIcon(
  docRegistry: DocumentRegistry,
  fileType: FileType,
  isLight: boolean,
): void {
  const ft = docRegistry.getFileType(fileType);
  // We most likely we did not set the Csv file type
  if (ft?.name === FileType.Csv) {
    return;
  }
  if (ft?.icon) {
    _updateIcon(ft?.icon, fileType, isLight);
  }
}
