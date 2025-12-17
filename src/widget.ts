import { Dialog, showDialog } from "@jupyterlab/apputils";
import { ABCWidgetFactory, DocumentWidget } from "@jupyterlab/docregistry";
import { PromiseDelegate } from "@lumino/coreutils";
import { BasicKeyHandler, BasicMouseHandler, DataGrid, TextRenderer } from "@lumino/datagrid";
import { Panel } from "@lumino/widgets";
import type { DocumentRegistry, IDocumentWidget } from "@jupyterlab/docregistry";
import type * as DataGridModule from "@lumino/datagrid";

import { DEFAULT_CSV_OPTIONS } from "./file_options";
import { FileType } from "./filetypes";
import { ArrowModel } from "./model";
import { CsvToolbar } from "./toolbar";
import type { FileOptions } from "./file_options";

export namespace ArrowGridViewer {
  export interface Options {
    path: string;
  }
}

export class ArrowGridViewer extends Panel {
  constructor(options: ArrowGridViewer.Options, fileOptions: FileOptions) {
    super();
    this._options = options;
    this._fileOptions = fileOptions;

    this.addClass("arrow-viewer");

    this._grid = new DataGrid({
      defaultSizes: {
        rowHeight: 24,
        columnWidth: 144,
        rowHeaderWidth: 64,
        columnHeaderHeight: 36,
      },
    });
    this._grid.addClass("arrow-grid-viewer");
    this._grid.headerVisibility = "all";
    this._grid.keyHandler = new BasicKeyHandler();
    this._grid.mouseHandler = new BasicMouseHandler();
    this.addWidget(this._grid);
    this._ready = this.initialize();
  }

  get ready(): Promise<void> {
    return this._ready;
  }

  get revealed(): Promise<void> {
    return this._revealed.promise;
  }

  get path(): string {
    return this._options.path;
  }

  get fileOptions(): Readonly<FileOptions> {
    return this._fileOptions;
  }

  set fileOptions(fileOptions: FileOptions) {
    this._fileOptions = fileOptions;
    this._updateGrid();
  }

  updateFileOptions(fileOptionsUpdate: Partial<FileOptions>) {
    this.fileOptions = {
      ...this.fileOptions,
      ...fileOptionsUpdate,
    };
  }

  /**
   * The style used by the data grid.
   */
  get style(): DataGridModule.DataGrid.Style {
    return this._grid.style;
  }

  set style(value: DataGridModule.DataGrid.Style) {
    this._grid.style = { ...this._defaultStyle, ...value };
  }

  /**
   * The config used to create text renderer.
   */
  set rendererConfig(rendererConfig: ITextRenderConfig) {
    this._baseRenderer = rendererConfig;
    void this._updateRenderer();
  }

  protected async initialize(): Promise<void> {
    this._defaultStyle = DataGrid.defaultStyle;
    await this._updateGrid();
    this._revealed.resolve(undefined);
  }

  private async _updateGrid() {
    try {
      const model = new ArrowModel({ path: this.path }, this.fileOptions);
      await model.ready;
      this._grid.dataModel = model;
    } catch (error) {
      const trans = Dialog.translator.load("jupyterlab");
      const buttons = [
        Dialog.cancelButton({ label: trans.__("Close") }),
        Dialog.okButton({ label: trans.__("Retry") }),
      ];
      const confirm = await showDialog({
        title: "Failed to initialized ArrowGridViewer",
        body: typeof error === "string" ? error : (error as Error).message,
        buttons,
      });
      const shouldRetry = confirm.button.accept;

      if (shouldRetry) {
        await this._updateGrid();
      }
    }
  }

  private async _updateRenderer(): Promise<void> {
    if (this._baseRenderer === null) {
      return;
    }
    const rendererConfig = this._baseRenderer;
    const renderer = new TextRenderer({
      textColor: rendererConfig.textColor,
      horizontalAlignment: rendererConfig.horizontalAlignment,
    });

    this._grid.cellRenderers.update({
      body: renderer,
      "column-header": renderer,
      "corner-header": renderer,
      "row-header": renderer,
    });
  }

  private _options: ArrowGridViewer.Options;
  private _fileOptions: FileOptions;
  private _grid: DataGridModule.DataGrid;
  private _revealed = new PromiseDelegate<void>();
  private _ready: Promise<void>;
  private _baseRenderer: ITextRenderConfig | null = null;
  private _defaultStyle: typeof DataGridModule.DataGrid.defaultStyle | undefined;
}

export namespace ArrowGridDocumentWidget {
  export interface IOptions extends Omit<DocumentWidget.IOptions<ArrowGridViewer>, "content"> {
    content?: ArrowGridViewer;
  }
}

export class ArrowGridDocumentWidget extends DocumentWidget<ArrowGridViewer> {
  constructor(options: ArrowGridDocumentWidget.IOptions, fileOptions: FileOptions) {
    let { content, context, reveal, ...other } = options;
    content = content || ArrowGridDocumentWidget._createContent(context, fileOptions);
    reveal = Promise.all([reveal, content.revealed, context.ready]);
    super({ content, context, reveal, ...other });
    this.addClass("arrow-viewer-base");
  }

  private static _createContent(
    context: DocumentRegistry.IContext<DocumentRegistry.IModel>,
    fileOptions: FileOptions,
  ): ArrowGridViewer {
    return new ArrowGridViewer({ path: context.path }, fileOptions);
  }
}

export class ArrowGridViewerFactory extends ABCWidgetFactory<IDocumentWidget<ArrowGridViewer>> {
  constructor(
    options: DocumentRegistry.IWidgetFactoryOptions<IDocumentWidget<ArrowGridViewer>>,
    docRegistry: DocumentRegistry,
  ) {
    super(options);
    this._docRegistry = docRegistry;
  }

  protected createNewWidget(context: DocumentRegistry.Context): IDocumentWidget<ArrowGridViewer> {
    const translator = this.translator;
    const ft = this.fileType(context.path);

    let fileOption: FileOptions = {};
    if (ft?.name === FileType.Csv) {
      fileOption = DEFAULT_CSV_OPTIONS;
    }
    const widget = new ArrowGridDocumentWidget({ context, translator }, fileOption);
    this.updateIcon(widget);
    return widget;
  }

  /**
   * Default factory for toolbar items to be added after the widget is created.
   */
  protected defaultToolbarFactory(
    widget: IDocumentWidget<ArrowGridViewer>,
  ): DocumentRegistry.IToolbarItem[] {
    const ft = this.fileType(widget.context.path);
    if (ft?.name === FileType.Csv) {
      return [
        {
          name: "arbalister:csv-toolbar",
          widget: new CsvToolbar(
            {
              gridViewer: widget.content,
              translator: this.translator,
            },
            DEFAULT_CSV_OPTIONS,
          ),
        },
      ];
    }
    return [];
  }

  updateIcon(widget: IDocumentWidget<ArrowGridViewer>) {
    const ft = this.fileType(widget.context.path);
    if (ft !== undefined) {
      widget.title.icon = ft.icon;
      if (ft.iconClass) {
        widget.title.iconClass = ft.iconClass;
      }
      if (ft.iconLabel) {
        widget.title.iconLabel = ft.iconLabel;
      }
    }
  }

  private fileType(path: string): DocumentRegistry.IFileType | undefined {
    const fileTypes = this._docRegistry
      .getFileTypesForPath(path)
      .filter((ft) => Object.values(FileType).includes(ft.name as FileType));
    if (fileTypes.length === 1) {
      return fileTypes[0];
    }
    return undefined;
  }

  private _docRegistry: DocumentRegistry;
}

export interface ITextRenderConfig {
  /**
   * default text color
   */
  textColor: string;

  /**
   * horizontalAlignment of the text
   */
  horizontalAlignment: DataGridModule.TextRenderer.HorizontalAlignment;
}
