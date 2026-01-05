import { Dialog, showDialog } from "@jupyterlab/apputils";
import { ABCWidgetFactory, DocumentWidget } from "@jupyterlab/docregistry";
import { PromiseDelegate } from "@lumino/coreutils";
import {
  BasicKeyHandler,
  BasicMouseHandler,
  BasicSelectionModel,
  DataGrid,
  TextRenderer,
} from "@lumino/datagrid";
import { Debouncer } from "@lumino/polling";
import { Panel } from "@lumino/widgets";
import type { DocumentRegistry, IDocumentWidget } from "@jupyterlab/docregistry";
import type * as DataGridModule from "@lumino/datagrid";
import type { IMessageHandler, Message } from "@lumino/messaging";

import { FileType } from "./file-types";
import { ArrowModel } from "./model";
import { createToolbar } from "./toolbar";
import type { FileInfo, FileReadOptions } from "./file-options";

/**
 * DataGrid that intercepts scroll-request messages and debounce them on large changes.
 */
class DebouncedDataGrid extends DataGrid {
  constructor(options: DataGrid.IOptions & { scrollThreshold: number; debounceDelay: number }) {
    super(options);

    this._scrollThresholdY = options.scrollThreshold;
    this._scrollThresholdX = options.scrollThreshold;

    this._scrollDebouncer = new Debouncer((handler: IMessageHandler, msg: Message) => {
      super.messageHook(handler, msg);
    }, options.debounceDelay);
  }

  setScrollThresholds(thresholdY: number, thresholdX: number): void {
    this._scrollThresholdY = thresholdY;
    this._scrollThresholdX = thresholdX;
  }

  messageHook(handler: IMessageHandler, msg: Message): boolean {
    if (handler === this.viewport && msg.type === "scroll-request") {
      // Calculate the percentage change in vertical scroll position
      const scrollChangeY = Math.abs(this.scrollY - this._previousScrollY);
      const scrollChangePercentY = this.maxScrollY > 0 ? scrollChangeY / this.maxScrollY : 0;

      // Calculate the percentage change in horizontal scroll position
      const scrollChangeX = Math.abs(this.scrollX - this._previousScrollX);
      const scrollChangePercentX = this.maxScrollX > 0 ? scrollChangeX / this.maxScrollX : 0;

      // Check if either direction exceeds its threshold
      const shouldDebounceY = scrollChangePercentY > this._scrollThresholdY;
      const shouldDebounceX = scrollChangePercentX > this._scrollThresholdX;

      if (shouldDebounceY || shouldDebounceX) {
        // Large scroll change - debounce it
        void this._scrollDebouncer.invoke(handler, msg);
        this._previousScrollY = this.scrollY;
        this._previousScrollX = this.scrollX;
        // Don't process immediately, return false to stop propagation
        return false;
      } else {
        // Small scroll change - process immediately
        this._previousScrollY = this.scrollY;
        this._previousScrollX = this.scrollX;
        return super.messageHook(handler, msg);
      }
    }

    return super.messageHook(handler, msg);
  }

  dispose(): void {
    this._scrollDebouncer.dispose();
    super.dispose();
  }

  private _scrollDebouncer: Debouncer<void>;
  private _previousScrollY: number = 0;
  private _previousScrollX: number = 0;
  private _scrollThresholdY: number;
  private _scrollThresholdX: number;
}

export namespace ArrowGridViewer {
  export interface Options extends ArrowModel.LoadingOptions {
    debounceDelay?: number;
  }
}

export class ArrowGridViewer extends Panel {
  constructor(options: ArrowGridViewer.Options) {
    super();
    this._options = options;

    this.addClass("arrow-viewer");

    this._defaultStyle = DataGrid.defaultStyle;

    // Start with a conservative default threshold that will be updated when model is loaded
    const defaultScrollThreshold = 0.01;
    const debounceDelay = options.debounceDelay ?? 300;

    this._grid = new DebouncedDataGrid({
      defaultSizes: {
        rowHeight: 24,
        columnWidth: 144,
        rowHeaderWidth: 64,
        columnHeaderHeight: 36,
      },
      scrollThreshold: defaultScrollThreshold,
      debounceDelay,
    });
    this._grid.addClass("arrow-grid-viewer");
    this._grid.headerVisibility = "all";
    this._grid.keyHandler = new BasicKeyHandler();
    this._grid.mouseHandler = new BasicMouseHandler();
    this._grid.copyConfig = {
      separator: "\t",
      format: DataGrid.copyFormatGeneric,
      headers: "all",
      warningThreshold: 1e6,
    };

    this.addWidget(this._grid);

    this._ready = this.initialize();
  }

  get ready(): Promise<void> {
    return this._ready.then(() => this.dataModel.ready);
  }

  get revealed(): Promise<void> {
    return this._revealed.promise;
  }

  get path(): string {
    return this._options.path;
  }

  private get dataModel(): ArrowModel {
    return this._grid.dataModel as ArrowModel;
  }

  get fileInfo(): Readonly<FileInfo> {
    return this.dataModel.fileInfo;
  }

  get fileReadOptions(): Readonly<FileReadOptions> {
    return this.dataModel.fileReadOptions;
  }

  set fileReadOptions(fileOptions: FileReadOptions) {
    this.dataModel.fileReadOptions = fileOptions;
  }

  updateFileReadOptions(fileOptionsUpdate: Partial<FileReadOptions>) {
    this.fileReadOptions = {
      ...this.fileReadOptions,
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
      const dataModel = await ArrowModel.fromRemoteFileInfo(this._options);
      await dataModel.ready;
      this._grid.dataModel = dataModel;
      this._grid.selectionModel = new BasicSelectionModel({ dataModel });

      // Calculate scroll debounce thresholds based on chunk sizes
      const rowChunkSize = this._options.rowChunkSize ?? 256;
      const colChunkSize = this._options.colChunkSize ?? 24;
      const numRows = dataModel.numRows;
      const numCols = dataModel.numCols;
      const scrollThresholdY = numRows > 0 ? rowChunkSize / numRows : 0.01;
      const scrollThresholdX = numCols > 0 ? colChunkSize / numCols : 0.01;
      (this._grid as DebouncedDataGrid).setScrollThresholds(scrollThresholdY, scrollThresholdX);
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
  constructor(options: ArrowGridDocumentWidget.IOptions) {
    let { content, context, reveal, ...other } = options;
    content = content || ArrowGridDocumentWidget._createContent(context.path);
    reveal = Promise.all([reveal, content.ready, content.revealed, context.ready]);
    super({ content, context, reveal, ...other });
    this.addClass("arrow-viewer-base");
  }

  private static _createContent(path: string): ArrowGridViewer {
    return new ArrowGridViewer({ path });
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
    const widget = new ArrowGridDocumentWidget({ context, translator });
    this.updateIcon(widget);
    widget.content.ready.then(() => {
      this.makeToolbarItems(widget.content).forEach(({ widget: toolbarItem, name }) => {
        widget.toolbar.addItem(name, toolbarItem);
      });
    });
    return widget;
  }

  protected makeToolbarItems(gridViewer: ArrowGridViewer): DocumentRegistry.IToolbarItem[] {
    const ft = this.fileType(gridViewer.path);
    if (!ft) {
      return [];
    }

    const toolbar = createToolbar(
      ft.name as FileType,
      { gridViewer, translator: this.translator },
      gridViewer.fileReadOptions,
      gridViewer.fileInfo,
    );

    return toolbar ? [{ name: `arbalister:${ft.name}-toolbar`, widget: toolbar }] : [];
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
    const knowFileTypes = FileType.all();
    const fileTypes = this._docRegistry
      .getFileTypesForPath(path)
      .filter((ft) => knowFileTypes.includes(ft.name as FileType));
    if (fileTypes.length >= 1) {
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
