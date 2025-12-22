// Copyright (c) Jupyter Development Team.
// Distributed under the terms of the Modified BSD License.

import { nullTranslator } from "@jupyterlab/translation";
import { Styling } from "@jupyterlab/ui-components";
import { Widget } from "@lumino/widgets";
import type { ITranslator } from "@jupyterlab/translation";
import type { Message } from "@lumino/messaging";

import { FileType } from "./filetypes";
import type {
  CsvFileInfo,
  CsvOptions,
  FileInfo,
  FileOptions,
  SqliteFileInfo,
  SqliteOptions,
} from "./file_options";
import type { ArrowGridViewer } from "./widget";

/**
 * Base toolbar class for file-specific options with a dropdown selector.
 */
abstract class DropdownToolbar extends Widget {
  constructor(gridViewer: ArrowGridViewer, node: HTMLElement) {
    super({ node });
    this._gridViewer = gridViewer;
    this.addClass("arrow-viewer-toolbar");
  }

  abstract get fileOptions(): FileOptions;

  get selectNode(): HTMLSelectElement {
    return this.node.getElementsByTagName("select")![0];
  }

  /**
   * Handle the DOM events for the widget.
   *
   * @param event - The DOM event sent to the widget.
   *
   * #### Notes
   * This method implements the DOM `EventListener` interface and is
   * called in response to events on the dock panel's node. It should
   * not be called directly by user code.
   */
  handleEvent(event: Event): void {
    switch (event.type) {
      case "change":
        this._gridViewer.updateFileOptions(this.fileOptions);
        break;
      default:
        break;
    }
  }

  protected onAfterAttach(_msg: Message): void {
    this.selectNode.addEventListener("change", this);
  }

  protected onBeforeDetach(_msg: Message): void {
    this.selectNode.removeEventListener("change", this);
  }

  protected _gridViewer: ArrowGridViewer;
}

export namespace CsvToolbar {
  export interface Options {
    gridViewer: ArrowGridViewer;
    translator?: ITranslator;
  }
}

export class CsvToolbar extends DropdownToolbar {
  constructor(options: CsvToolbar.Options, fileOptions: CsvOptions, fileInfo: CsvFileInfo) {
    super(
      options.gridViewer,
      Private.createDelimiterNode(fileOptions.delimiter, fileInfo.delimiters, options.translator),
    );
  }

  get fileOptions(): CsvOptions {
    return {
      delimiter: this.selectNode.value,
    };
  }
}

export namespace SqliteToolbar {
  export interface Options {
    gridViewer: ArrowGridViewer;
    translator?: ITranslator;
  }
}

export class SqliteToolbar extends DropdownToolbar {
  constructor(
    options: SqliteToolbar.Options,
    fileOptions: SqliteOptions,
    fileInfo: SqliteFileInfo,
  ) {
    super(
      options.gridViewer,
      Private.createTableNameNode(fileOptions.table_name, fileInfo.table_names, options.translator),
    );
  }

  get fileOptions(): SqliteOptions {
    return {
      table_name: this.selectNode.value,
    };
  }
}

/**
 * Common options for toolbar creation.
 */
export interface ToolbarOptions {
  gridViewer: ArrowGridViewer;
  translator?: ITranslator;
}

/**
 * Factory function to create the appropriate toolbar for a given file type.
 */
export function createToolbar(
  fileType: FileType,
  options: ToolbarOptions,
  fileOptions: FileOptions,
  fileInfo: FileInfo,
): Widget | null {
  switch (fileType) {
    case FileType.Csv:
      return new CsvToolbar(options, fileOptions as CsvOptions, fileInfo as CsvFileInfo);
    case FileType.Sqlite:
      return new SqliteToolbar(options, fileOptions as SqliteOptions, fileInfo as SqliteFileInfo);
    default:
      return null;
  }
}

namespace Private {
  /**
   * Create a labeled dropdown node with items.
   */
  function createLabeledDropdown(
    label: string,
    items: string[],
    selected: string,
    translator?: ITranslator,
  ): HTMLElement {
    translator = translator || nullTranslator;
    const trans = translator?.load("jupyterlab");
    const options: [string, string][] = items.map((item) => [item, item]);
    return createDropdownNode(trans.__(label), options, selected);
  }

  /**
   * Create the node for the delimiter switcher.
   */
  export function createDelimiterNode(
    selected: string,
    delimiters: string[],
    translator?: ITranslator,
  ): HTMLElement {
    return createLabeledDropdown("Delimiter: ", delimiters, selected, translator);
  }

  /**
   * Create the node for the table name switcher.
   */
  export function createTableNameNode(
    selected: string,
    table_names: string[],
    translator?: ITranslator,
  ): HTMLElement {
    return createLabeledDropdown("Table: ", table_names, selected, translator);
  }

  /**
   * Create a generic dropdown node with a label and options.
   */
  function createDropdownNode(
    labelText: string,
    options: Array<[string, string]>,
    selected: string,
  ): HTMLElement {
    const div = document.createElement("div");
    const label = document.createElement("span");
    const select = document.createElement("select");
    label.textContent = labelText;
    label.className = "toolbar-label";
    for (const [value, displayLabel] of options) {
      const option = document.createElement("option");
      option.value = value;
      option.textContent = displayLabel;
      if (value === selected) {
        option.selected = true;
      }
      select.appendChild(option);
    }
    div.appendChild(label);
    const node = Styling.wrapSelect(select);
    node.classList.add("toolbar-dropdown");
    div.appendChild(node);
    return div;
  }
}
