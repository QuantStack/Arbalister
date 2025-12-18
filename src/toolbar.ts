// Copyright (c) Jupyter Development Team.
// Distributed under the terms of the Modified BSD License.

import { nullTranslator } from "@jupyterlab/translation";
import { Styling } from "@jupyterlab/ui-components";
import { Widget } from "@lumino/widgets";
import type { ITranslator } from "@jupyterlab/translation";
import type { Message } from "@lumino/messaging";

import type { CsvOptions } from "./file_options";
import type { ArrowGridViewer } from "./widget";

export namespace CsvToolbar {
  export interface Options {
    gridViewer: ArrowGridViewer;
    translator?: ITranslator;
  }
}

export class CsvToolbar extends Widget {
  constructor(options: CsvToolbar.Options, fileOptions: Required<CsvOptions>) {
    super({
      node: Private.createDelimiterNode(fileOptions.delimiter, options.translator),
    });
    this._gridViewer = options.gridViewer;
    this.addClass("arrow-viewer-toolbar");
  }

  get fileOptions(): CsvOptions {
    return {
      delimiter: this.delimiterNode.value,
    };
  }

  get delimiterNode(): HTMLSelectElement {
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
    this.delimiterNode.addEventListener("change", this);
  }

  protected onBeforeDetach(_msg: Message): void {
    this.delimiterNode.removeEventListener("change", this);
  }

  protected _gridViewer: ArrowGridViewer;
}

namespace Private {
  /**
   * Create the node for the delimiter switcher.
   */
  export function createDelimiterNode(selected: string, translator?: ITranslator): HTMLElement {
    translator = translator || nullTranslator;
    const trans = translator?.load("jupyterlab");

    // The supported parsing delimiters and labels.
    const delimiters = [
      [",", ","],
      [";", ";"],
      ["\\t", trans.__("tab")],
      ["|", trans.__("pipe")],
      ["#", trans.__("hash")],
    ];

    const div = document.createElement("div");
    const label = document.createElement("span");
    const select = document.createElement("select");
    label.textContent = trans.__("Delimiter: ");
    label.className = "toolbar-label";
    for (const [delimiter, label] of delimiters) {
      const option = document.createElement("option");
      option.value = delimiter;
      option.textContent = label;
      if (delimiter === selected) {
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
