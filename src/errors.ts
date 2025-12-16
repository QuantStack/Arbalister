import { showErrorMessage } from "@jupyterlab/apputils";

export const handleError = async (err: Error | string, title: string): Promise<void> => {
  await showErrorMessage(title, err);
};
