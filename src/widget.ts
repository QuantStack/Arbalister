import { ABCWidgetFactory, DocumentWidget } from "@jupyterlab/docregistry";
import { PromiseDelegate } from "@lumino/coreutils";
import { BasicKeyHandler, BasicMouseHandler, DataGrid, TextRenderer } from "@lumino/datagrid";
import { Panel } from "@lumino/widgets";
import type { DocumentRegistry, IDocumentWidget } from "@jupyterlab/docregistry";
import type * as DataGridModule from "@lumino/datagrid";

import { ArrowModel } from "./model";

export namespace ArrowGridViewer {
  export interface IOptions {
    path: string;
  }
}

export class ArrowGridViewer extends Panel {
  constructor(options: ArrowGridViewer.IOptions) {
    super();
    this._options = options;

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
    const model = new ArrowModel({ path: this.path });
    await model.ready;
    this._grid.dataModel = model;
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

  private _options: ArrowGridViewer.IOptions;
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
    content = content || ArrowGridDocumentWidget._createContent(context);
    reveal = Promise.all([reveal, content.revealed, context.ready]);
    super({ content, context, reveal, ...other });
    this.addClass("arrow-viewer-base");
  }

  private static _createContent(
    context: DocumentRegistry.IContext<DocumentRegistry.IModel>,
  ): ArrowGridViewer {
    return new ArrowGridViewer({ path: context.path });
  }
}

export class ArrowGridViewerFactory extends ABCWidgetFactory<IDocumentWidget<ArrowGridViewer>> {
  protected createNewWidget(context: DocumentRegistry.Context): IDocumentWidget<ArrowGridViewer> {
    const translator = this.translator;
    return new ArrowGridDocumentWidget({ context, translator });
  }
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
