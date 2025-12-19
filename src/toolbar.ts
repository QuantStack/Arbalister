// Copyright (c) Jupyter Development Team.
// Distributed under the terms of the Modified BSD License.

import { nullTranslator } from "@jupyterlab/translation";
import { Styling } from "@jupyterlab/ui-components";
import { Widget } from "@lumino/widgets";
import type { ITranslator } from "@jupyterlab/translation";
import type { Message } from "@lumino/messaging";

import type { CsvOptions, FileOptions, SqliteOptions } from "./file_options";
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
  constructor(options: CsvToolbar.Options, fileOptions: Required<CsvOptions>) {
    super(
      options.gridViewer,
      Private.createDelimiterNode(fileOptions.delimiter, options.translator),
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
  constructor(options: SqliteToolbar.Options, fileOptions: Required<SqliteOptions>) {
    super(
      options.gridViewer,
      Private.createTableNameNode(fileOptions.tableName, options.translator),
    );
  }

  get fileOptions(): SqliteOptions {
    return {
      tableName: this.selectNode.value,
    };
  }
}

namespace Private {
  /**
   * Create the node for the delimiter switcher.
   */
  export function createDelimiterNode(selected: string, translator?: ITranslator): HTMLElement {
    translator = translator || nullTranslator;
    const trans = translator?.load("jupyterlab");

    // The supported parsing delimiters and labels.
    const delimiters: Array<[string, string]> = [
      [",", ","],
      [";", ";"],
      ["\\t", trans.__("tab")],
      ["|", trans.__("pipe")],
      ["#", trans.__("hash")],
    ];

    return createDropdownNode(trans.__("Delimiter: "), delimiters, selected);
  }

  /**
   * Create the node for the table name switcher.
   */
  export function createTableNameNode(selected: string, translator?: ITranslator): HTMLElement {
    translator = translator || nullTranslator;
    const trans = translator?.load("jupyterlab");

    // Placeholder table names that will be replaced when connected to the route
    const tableNames: Array<[string, string]> = [
      ["sqlite_master", "sqlite_master"],
      ["table1", "table1"],
      ["table2", "table2"],
    ];

    return createDropdownNode(trans.__("Table: "), tableNames, selected);
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
