import { Dialog, showDialog } from "@jupyterlab/apputils";
import { nullTranslator } from "@jupyterlab/translation";
import { Styling } from "@jupyterlab/ui-components";
import { Widget } from "@lumino/widgets";
import type { ITranslator } from "@jupyterlab/translation";
import type { Message } from "@lumino/messaging";

import { FileType } from "./file-types";
import type {
  CsvFileInfo,
  CsvReadOptions,
  FileInfoFor,
  FileReadOptionsFor,
  SqliteFileInfo,
  SqliteReadOptions,
} from "./file-options";
import type { ArrowGridViewer } from "./widget";

/**
 * Base toolbar class for a dropdown selector with error recovery.
 * Maintain a value synchronized with the UI and falls back to the previous value on error.
 */
abstract class DropdownToolbar extends Widget {
  constructor(labelName: string, options: Array<[string, string]>, selected: string) {
    const node = DropdownToolbar.createDropdownNode(labelName, options, selected);
    super({ node });
    this._currentValue = selected;
    this._labelName = labelName;
    this.addClass("arrow-viewer-toolbar");
  }

  /**
   * Create a generic dropdown node with a label and options.
   */
  protected static createDropdownNode(
    labelName: string,
    options: Array<[string, string]>,
    selected: string,
  ): HTMLElement {
    const div = document.createElement("div");
    const label = document.createElement("span");
    const select = document.createElement("select");
    label.textContent = `${labelName}: `;
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

  /**
   * Called when the dropdown value changes. Implement this to handle the change.
   * If this method throws an error, the dropdown will revert to the previous value.
   */
  protected abstract onChange(newValue: string): Promise<void>;

  protected get value(): string {
    return this.selectNode.value;
  }

  protected set value(val: string) {
    this.selectNode.value = val;
  }

  get labelName(): string {
    return this._labelName;
  }

  /**
   * Handle the DOM events for the widget.
   *
   * This method implements the DOM `EventListener` interface and is called in response to events
   * on the dock panel's node.
   * It should not be called directly by user code.
   *
   * This method is made async to handle the chain of event required to catch the exception and
   * show the dialog, but it will be fired and forgotten by the browser.
   *
   * @param event - The DOM event sent to the widget.
   */
  async handleEvent(event: Event): Promise<void> {
    switch (event.type) {
      case "change": {
        const previousValue = this._currentValue;
        const newValue = this.value;
        try {
          await this.onChange(newValue);
          this._currentValue = newValue;
        } catch (error) {
          // Reset the selector value
          this.value = previousValue;

          // Show a message to the user
          const trans = Dialog.translator.load("jupyterlab");
          const cancel = Dialog.cancelButton({ label: trans.__("Close") });
          await showDialog({
            title: trans.__(`Error changing the ${this.labelName.toLowerCase()} option`),
            body: typeof error === "string" ? error : (error as Error).message,
            buttons: [cancel],
          });
        }
        break;
      }
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

  private get selectNode(): HTMLSelectElement {
    return this.node.getElementsByTagName("select")![0];
  }

  private _currentValue: string;
  private _labelName: string;
}

export namespace CsvToolbar {
  export interface Options {
    gridViewer: ArrowGridViewer;
    translator?: ITranslator;
  }
}

export class CsvToolbar extends DropdownToolbar {
  constructor(options: CsvToolbar.Options, fileOptions: CsvReadOptions, fileInfo: CsvFileInfo) {
    const translator = options.translator || nullTranslator;
    const trans = translator.load("jupyterlab");
    const delimiterOptions: [string, string][] = fileInfo.delimiters.map((delim) => [delim, delim]);
    super(trans.__("Delimiter"), delimiterOptions, fileOptions.delimiter);
    this._gridViewer = options.gridViewer;
  }

  get fileOptions(): CsvReadOptions {
    return {
      delimiter: this.value,
    };
  }

  protected async onChange(newValue: string): Promise<void> {
    this._gridViewer.updateFileReadOptions({ delimiter: newValue });
    await this._gridViewer.ready;
  }

  private _gridViewer: ArrowGridViewer;
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
    fileOptions: SqliteReadOptions,
    fileInfo: SqliteFileInfo,
  ) {
    const translator = options.translator || nullTranslator;
    const trans = translator.load("jupyterlab");
    const tableOptions: [string, string][] = fileInfo.table_names.map((name) => [name, name]);
    super(trans.__("Table"), tableOptions, fileOptions.table_name);
    this._gridViewer = options.gridViewer;
  }

  get fileOptions(): SqliteReadOptions {
    return {
      table_name: this.value,
    };
  }

  protected async onChange(newValue: string): Promise<void> {
    this._gridViewer.updateFileReadOptions({ table_name: newValue });
    await this._gridViewer.ready;
  }

  private _gridViewer: ArrowGridViewer;
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
 * Type-safe overloads ensure the correct options and info types are used.
 */
export function createToolbar<T extends FileType>(
  fileType: T,
  options: ToolbarOptions,
  fileOptions: FileReadOptionsFor<T>,
  fileInfo: FileInfoFor<T>,
): Widget | null {
  switch (fileType) {
    case FileType.Csv:
      return new CsvToolbar(options, fileOptions as CsvReadOptions, fileInfo as CsvFileInfo);
    case FileType.Sqlite:
      return new SqliteToolbar(
        options,
        fileOptions as SqliteReadOptions,
        fileInfo as SqliteFileInfo,
      );
    default:
      return null;
  }
}
