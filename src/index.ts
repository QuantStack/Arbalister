import { ILayoutRestorer } from "@jupyterlab/application";
import { ICommandPalette, MainAreaWidget, WidgetTracker } from "@jupyterlab/apputils";
import { DataGrid, DataModel } from "@lumino/datagrid";
import { Panel } from "@lumino/widgets";
import type { JupyterFrontEnd, JupyterFrontEndPlugin } from "@jupyterlab/application";
import type * as DataGridModule from "@lumino/datagrid";

export class ArrowModel extends DataModel {
  columnCount(region: DataModel.ColumnRegion): number {
    if (region === "body") {
      return 3_000;
    }
    return 1;
  }

  rowCount(region: DataModel.RowRegion): number {
    if (region === "body") {
      return 3_000_000;
    }
    return 1;
  }

  data(region: DataModel.CellRegion, row: number, column: number): string {
    switch (region) {
      case "body":
        return `cell(${row}, ${column})`;
      case "column-header":
        return `column ${column}`;
      case "row-header":
        return row.toString();
      case "corner-header":
        return "";
      default:
        throw "unreachable";
    }
  }
}

const ARROW_GRID_CSS = "arrow-grid-viewer";
const ARROW_VIEWER_CSS = "arrow-viewer";

class ArrowGridViewer extends Panel {
  constructor() {
    super();
    this.addClass(ARROW_VIEWER_CSS);

    this._grid = new DataGrid({
      defaultSizes: {
        rowHeight: 24,
        columnWidth: 144,
        rowHeaderWidth: 64,
        columnHeaderHeight: 36,
      },
    });
    this._grid.addClass(ARROW_GRID_CSS);
    this.addWidget(this._grid);
    this.updateGrid();
  }

  updateGrid() {
    this._grid.dataModel = new ArrowModel();
  }

  private _grid: DataGridModule.DataGrid;
}

function activate(
  app: JupyterFrontEnd,
  palette: ICommandPalette,
  restorer: ILayoutRestorer | null,
) {
  console.log("JupyterLab extension diana is activated!");

  // Declare a widget variable
  let widget: MainAreaWidget<ArrowGridViewer>;

  // Add an application command
  const command: string = "diana:open";
  app.commands.addCommand(command, {
    label: "Open dataframe viewer",
    execute: () => {
      if (!widget || widget.isDisposed) {
        const content = new ArrowGridViewer();
        widget = new MainAreaWidget({ content });
        widget.addClass("diana-base");
        widget.id = "diana";
        widget.title.label = "Dataframe viewer";
        widget.title.closable = true;
      }
      if (!tracker.has(widget)) {
        // Track the state of the widget for later restoration
        tracker.add(widget);
      }
      if (!widget.isAttached) {
        app.shell.add(widget, "main");
      }

      // Activate the widget
      app.shell.activateById(widget.id);
    },
  });

  // Add the command to the palette.
  palette.addItem({ command, category: "Tutorial" });

  // Track and restore the widget state
  const tracker = new WidgetTracker<MainAreaWidget<ArrowGridViewer>>({
    namespace: "diana",
  });
  if (restorer) {
    restorer.restore(tracker, {
      command,
      name: () => "diana",
    });
  }
}

/**
 * Initialization data for the jupyterdiana extension.
 */
const plugin: JupyterFrontEndPlugin<void> = {
  id: "jupyterdiana:plugin",
  description: "Arrow viewer for Jupyter",
  requires: [ICommandPalette],
  optional: [ILayoutRestorer],
  autoStart: true,
  activate: activate,
};

export default plugin;
