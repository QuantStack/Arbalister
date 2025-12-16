import { showErrorMessage } from "@jupyterlab/apputils";

export const handleError = async (err: Error | string | unknown, title: string): Promise<void> => {
  await showErrorMessage(title, err as Error | string);
};
