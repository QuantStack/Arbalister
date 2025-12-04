import { ABCWidgetFactory, DocumentWidget } from "@jupyterlab/docregistry";
import { PromiseDelegate } from "@lumino/coreutils";
import { DataGrid } from "@lumino/datagrid";
import { Panel } from "@lumino/widgets";
import type { DocumentRegistry, IDocumentWidget } from "@jupyterlab/docregistry";
import type * as DataGridModule from "@lumino/datagrid";

import { ArrowModel } from "./model";

export namespace ArrowGridViewer {
  export interface IOptions {
    context: DocumentRegistry.Context;
  }
}

export class ArrowGridViewer extends Panel {
  constructor(options: ArrowGridViewer.IOptions) {
    super();
    this._context = options.context;
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
    this.addWidget(this._grid);
    this._ready = this.initialize();
  }

  get ready(): Promise<void> {
    return this._ready;
  }

  get revealed(): Promise<void> {
    return this._revealed.promise;
  }

  protected async initialize(): Promise<void> {
    await this._context.ready;
    await this._updateGrid();
    this._revealed.resolve(undefined);
  }

  private async _updateGrid() {
    this._grid.dataModel = await ArrowModel.fetch("data/gen/test.parquet");
  }

  private _context: DocumentRegistry.Context;
  private _grid: DataGridModule.DataGrid;
  private _revealed = new PromiseDelegate<void>();
  private _ready: Promise<void>;
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
    reveal = Promise.all([reveal, content.revealed]);
    super({ content, context, reveal, ...other });
    this.addClass("arrow-viewer-base");
  }

  private static _createContent(
    context: DocumentRegistry.IContext<DocumentRegistry.IModel>,
  ): ArrowGridViewer {
    return new ArrowGridViewer({ context });
  }
}

export class ArrowGridViewerFactory extends ABCWidgetFactory<IDocumentWidget<ArrowGridViewer>> {
  protected createNewWidget(context: DocumentRegistry.Context): IDocumentWidget<ArrowGridViewer> {
    const translator = this.translator;
    return new ArrowGridDocumentWidget({ context, translator });
  }
}
