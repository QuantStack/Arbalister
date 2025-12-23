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
import { ConflatableMessage, MessageLoop } from "@lumino/messaging";
import { Debouncer } from "@lumino/polling";
import { Panel } from "@lumino/widgets";
import type { DocumentRegistry, IDocumentWidget } from "@jupyterlab/docregistry";
import type * as DataGridModule from "@lumino/datagrid";
import type { ISignal } from "@lumino/signaling";
import type { ScrollBar } from "@lumino/widgets";

import { FileType } from "./file-types";
import { ArrowModel } from "./model";
import { createToolbar } from "./toolbar";
import type { FileInfo, FileReadOptions } from "./file-options";

/* grid: DataGrid instance */

function installDebouncedScrollBarHook(grid: DataGrid, delay = 100) {
  // Access the internal vertical scrollbar and its thumbMoved signal
  // biome-ignore lint/suspicious/noExplicitAny: Hacking into private property
  const vScrollBar = (grid as any)._vScrollBar as ScrollBar;
  // biome-ignore lint/suspicious/noExplicitAny: Hacking into private property
  const thumbMoved = (vScrollBar as any).thumbMoved as ISignal<ScrollBar, void>;

  // Get the original handler method from the grid
  // biome-ignore lint/suspicious/noExplicitAny: Hacking into private property
  const originalHandler = (grid as any)._onThumbMoved as (sender: ScrollBar) => void;

  // Disconnect the original handler
  thumbMoved.disconnect(originalHandler, grid);

  // Create a debouncer that posts the scroll request after the delay
  // The debouncer ensures the last event is always processed
  const debouncer = new Debouncer(() => {
    MessageLoop.postMessage(grid.viewport, new ConflatableMessage("scroll-request"));
  }, delay);

  // Handler that invokes the debouncer on each thumb move
  const debouncedHandler = () => {
    void debouncer.invoke();
  };

  // Connect our debounced handler
  thumbMoved.connect(debouncedHandler);

  // Return cleanup function
  return () => {
    // Disconnect our handler
    thumbMoved.disconnect(debouncedHandler);
    // Reconnect the original handler
    thumbMoved.connect(originalHandler, grid);
    // Dispose the debouncer
    debouncer.dispose();
  };
}

export namespace ArrowGridViewer {
  export interface Options {
    path: string;
  }
}

export class ArrowGridViewer extends Panel {
  constructor(options: ArrowGridViewer.Options) {
    super();
    this._options = options;

    this.addClass("arrow-viewer");

    this._defaultStyle = DataGrid.defaultStyle;
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
    this._grid.copyConfig = {
      separator: "\t",
      format: DataGrid.copyFormatGeneric,
      headers: "all",
      warningThreshold: 1e6,
    };

    this.addWidget(this._grid);

    installDebouncedScrollBarHook(this._grid, 100);

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
      const dataModel = await ArrowModel.fromRemoteFileInfo({ path: this.path });
      await dataModel.ready;
      this._grid.dataModel = dataModel;
      this._grid.selectionModel = new BasicSelectionModel({ dataModel });
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
