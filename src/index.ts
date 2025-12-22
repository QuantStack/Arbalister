import { ILayoutRestorer } from "@jupyterlab/application";
import { IThemeManager, showErrorMessage, WidgetTracker } from "@jupyterlab/apputils";
import { IDefaultDrive } from "@jupyterlab/services";
import { ITranslator } from "@jupyterlab/translation";
import type { JupyterFrontEnd, JupyterFrontEndPlugin } from "@jupyterlab/application";
import type { IDocumentWidget } from "@jupyterlab/docregistry";
import type * as services from "@jupyterlab/services";
import type { Contents } from "@jupyterlab/services";
import type { DataGrid } from "@lumino/datagrid";

import { ensureFileType, FileType, updateIcon } from "./file-types";
import { ArrowGridViewerFactory } from "./widget";
import type { ArrowGridViewer, ITextRenderConfig } from "./widget";

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
      format: "base64",
      contentProviderId: undefined,
      content: false,
      // The hash is too time consuming on large files.
      // Does this prevent Jupyter from detecting file changes?
      hash: false,
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
  id: "@arbalister/arrowgridviewer-extension:arrowgrid",
  description: "Adds viewer for file that can be read into Arrow format.",
  requires: [ITranslator, IDefaultDrive],
  optional: [ILayoutRestorer, IThemeManager],
  autoStart: true,
};

function activateArrowGrid(
  app: JupyterFrontEnd,
  translator: ITranslator,
  defaultDrive: Contents.IDrive,
  restorer: ILayoutRestorer | null,
  themeManager: IThemeManager | null,
): void {
  console.log("Launching JupyterLab extension arbalister");

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

  const currentTheme = themeManager?.theme;
  let isLight = true;
  if (themeManager?.isLight) {
    isLight = currentTheme ? themeManager?.isLight(currentTheme as string) : true;
  }

  const fileTypes = FileType.all().map((ft) => ensureFileType(app.docRegistry, ft, isLight));
  const fileTypesNames = fileTypes.map((ft) => ft.name);

  const factory = new ArrowGridViewerFactory(
    {
      name: factory_arrow,
      label: trans.__("Arrow Dataframe Viewer"),
      fileTypes: fileTypesNames,
      defaultFor: fileTypesNames,
      readOnly: true,
      translator,
      contentProviderId: NOOP_CONTENT_PROVIDER_ID,
    },
    app.docRegistry,
  );
  const tracker = new WidgetTracker<IDocumentWidget<ArrowGridViewer>>({
    namespace: "arrowviewer",
  });
  let style: DataGrid.Style = Private.LIGHT_STYLE;
  let rendererConfig: ITextRenderConfig = Private.LIGHT_TEXT_CONFIG;

  if (restorer) {
    void restorer.restore(tracker, {
      command: "docmanager:open",
      args: (widget) => ({ path: widget.context.path, factory: factory_arrow }),
      name: (widget) => widget.context.path,
    });
  }

  app.docRegistry.addWidgetFactory(factory);

  factory.widgetCreated.connect(async (_sender, widget) => {
    try {
      // Track the widget.
      void tracker.add(widget);
      // Notify the widget tracker if restore data needs to update.
      widget.context.pathChanged.connect(() => {
        void tracker.save(widget);
      });

      await widget.content.ready;
      widget.content.style = style;
      widget.content.rendererConfig = rendererConfig;
      updateThemes();
      console.log("JupyterLab extension arbalister is activated!");
    } catch (error) {
      await showErrorMessage(
        trans.__("ArrowGridViewer widget initialization failed"),
        error as Error,
      );
    }
  });

  const updateThemes = (newTheme?: string | null) => {
    const themeName = newTheme ? (newTheme as string) : themeManager?.theme;
    const isLightNew = themeManager?.isLight(themeName as string) ?? true;
    style = isLightNew ? Private.LIGHT_STYLE : Private.DARK_STYLE;
    rendererConfig = isLightNew ? Private.LIGHT_TEXT_CONFIG : Private.DARK_TEXT_CONFIG;
    tracker.forEach(async (widget) => {
      await widget.content.ready;
      widget.content.style = style;
      widget.content.rendererConfig = rendererConfig;
    });

    // Update the file icons to match theme
    FileType.all().forEach((ft) => {
      updateIcon(app.docRegistry, ft, isLightNew);
    });
  };

  if (themeManager) {
    themeManager.themeChanged.connect((_, args) => {
      try {
        const newTheme = args.newValue;
        updateThemes(newTheme);
      } catch (error) {
        void showErrorMessage(
          trans.__("Failed to the viewer according to updated theme"),
          error as Error,
        );
      }
    });
  }
}

/**
 * A namespace for private data.
 */
namespace Private {
  /**
   * The light theme for the data grid.
   */
  export const LIGHT_STYLE: DataGrid.Style = {
    voidColor: "#F3F3F3",
    backgroundColor: "white",
    headerBackgroundColor: "#EEEEEE",
    gridLineColor: "rgba(20, 20, 20, 0.15)",
    headerGridLineColor: "rgba(20, 20, 20, 0.25)",
    rowBackgroundColor: (i) => (i % 2 === 0 ? "#F5F5F5" : "white"),
  };

  /**
   * The dark theme for the data grid.
   */
  export const DARK_STYLE: DataGrid.Style = {
    voidColor: "black",
    backgroundColor: "#111111",
    headerBackgroundColor: "#424242",
    gridLineColor: "rgba(235, 235, 235, 0.15)",
    headerGridLineColor: "rgba(235, 235, 235, 0.25)",
    rowBackgroundColor: (i) => (i % 2 === 0 ? "#212121" : "#111111"),
  };

  /**
   * The light config for the data grid renderer.
   */
  export const LIGHT_TEXT_CONFIG: ITextRenderConfig = {
    textColor: "#111111",
    horizontalAlignment: "left",
  };

  /**
   * The dark config for the data grid renderer.
   */
  export const DARK_TEXT_CONFIG: ITextRenderConfig = {
    textColor: "#F5F5F5",
    horizontalAlignment: "left",
  };
}

export default arrowGrid;
