import { ILayoutRestorer } from "@jupyterlab/application";
import { WidgetTracker } from "@jupyterlab/apputils";
import { IDefaultDrive } from "@jupyterlab/services";
import { ITranslator } from "@jupyterlab/translation";
import type { JupyterFrontEnd, JupyterFrontEndPlugin } from "@jupyterlab/application";
import type { DocumentRegistry, IDocumentWidget } from "@jupyterlab/docregistry";
import type * as services from "@jupyterlab/services";
import type { Contents } from "@jupyterlab/services";

import { ArrowGridViewerFactory } from "./widget";
import type { ArrowGridViewer } from "./widget";

export namespace NoOpContentProvider {
  export interface IOptions {
    currentDrive: services.Contents.IDrive;
  }
}

export class NoOpContentProvider implements services.IContentProvider {
  constructor(options: NoOpContentProvider.IOptions) {
    this._currentDrive = options.currentDrive;
  }

  async get(
    localPath: string,
    options?: services.Contents.IFetchOptions,
  ): Promise<services.Contents.IModel> {
    // Not calling get() with options.contentProviderId otherwise it's an infinite loop.
    // Not requesting content since the DataModel will do it.
    return this._currentDrive.get(localPath, {
      ...options,
      contentProviderId: undefined,
      content: false,
    });
  }

  async save(
    localPath: string,
    options: Partial<services.Contents.IModel> & services.Contents.IContentProvisionOptions = {},
  ): Promise<services.Contents.IModel> {
    return this._currentDrive.save(localPath, {
      ...options,
      contentProviderId: undefined,
    });
  }

  private _currentDrive: services.Contents.IDrive;
}

const NOOP_CONTENT_PROVIDER_ID = "noop-provider";

const arrowGrid: JupyterFrontEndPlugin<void> = {
  activate: activateArrowGrid,
  id: "@jupyterdiana/arrowgridviewer-extension:arrowgrid",
  description: "Adds viewer for file that can be read into Arrow format.",
  requires: [ITranslator, IDefaultDrive],
  optional: [ILayoutRestorer],
  autoStart: true,
};

function ensureCsvFileType(
  docRegistry: DocumentRegistry,
): DocumentRegistry.IFileType {
  const name = "csv";
  const ft =  docRegistry.getFileType(name)!;
  if(ft){
    return ft;
  }
  docRegistry.addFileType({
    name,
    displayName: "CSV",
    mimeTypes: ["text/csv"],
    extensions: [".csv"],
    contentType: "file",
  });
  return docRegistry.getFileType(name)!;
}

function addParquetFileType(
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

function addIpcFileType(
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

function addOrcFileType(
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

function activateArrowGrid(
  app: JupyterFrontEnd,
  translator: ITranslator,
  defaultDrive: Contents.IDrive,
  restorer: ILayoutRestorer | null,
): void {
  const factory_arrow = "ArrowTable";

  const trans = translator.load("jupyterlab");

  // Register the NoOp content provider once
  const registry = defaultDrive.contentProviderRegistry;
  if (registry) {
    const noOpContentProvider = new NoOpContentProvider({
      currentDrive: defaultDrive,
    });
    registry.register(NOOP_CONTENT_PROVIDER_ID, noOpContentProvider);
  }

  const csv_ft = ensureCsvFileType(app.docRegistry);
  const prq_ft = addParquetFileType(app.docRegistry, { icon: csv_ft?.icon });
  const ipc_ft = addIpcFileType(app.docRegistry, { icon: csv_ft?.icon });
  const orc_ft = addOrcFileType(app.docRegistry, { icon: csv_ft?.icon });

  const factory = new ArrowGridViewerFactory({
    name: factory_arrow,
    label: trans.__("Arrow Dataframe Viewer"),
    fileTypes: [csv_ft.name, prq_ft.name, ipc_ft.name, orc_ft.name],
    defaultFor: [csv_ft.name, prq_ft.name, ipc_ft.name, orc_ft.name],
    readOnly: true,
    translator,
    contentProviderId: NOOP_CONTENT_PROVIDER_ID,
  });
  const tracker = new WidgetTracker<IDocumentWidget<ArrowGridViewer>>({
    namespace: "arrowviewer",
  });

  if (restorer) {
    void restorer.restore(tracker, {
      command: "docmanager:open",
      args: (widget) => ({ path: widget.context.path, factory: factory_arrow }),
      name: (widget) => widget.context.path,
    });
  }

  app.docRegistry.addWidgetFactory(factory);

  factory.widgetCreated.connect(async (_sender, widget) => {
    // Track the widget.
    void tracker.add(widget);
    // Notify the widget tracker if restore data needs to update.
    widget.context.pathChanged.connect(() => {
      void tracker.save(widget);
    });

    if (csv_ft) {
      widget.title.icon = csv_ft.icon;
      widget.title.iconClass = csv_ft.iconClass!;
      widget.title.iconLabel = csv_ft.iconLabel!;
    }

    await widget.content.ready;
  });
}

export default arrowGrid;
