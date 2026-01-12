import { expect, galata, type IJupyterLabPageFixture, test } from "@jupyterlab/galata";

import path from "node:path";

async function openFile(page: IJupyterLabPageFixture, filename: string) {
  await page.goto();
  const tmpPath = "arbalister-viewer-tests";
  const target = `${tmpPath}/${filename}`;
  await page.notebook.openByPath(target);
  await page.notebook.activate(target);
  await page.waitForTimeout(10000);
}

async function checkFile(page: IJupyterLabPageFixture, filename: string, snapshotFile: string) {
  await openFile(page, filename);
  expect(await page.screenshot()).toMatchSnapshot({
    name: snapshotFile,
    maxDiffPixelRatio: 0.02,
  });
}

async function checkToolbar(page: IJupyterLabPageFixture) {
  const text = page.getByTestId(`toolbar-group-cols-rows`);
  await expect(text).toBeVisible();

  const rows = page.locator(".toolbar-group-cols-rows .toolbar-label:not(.cols)");
  await expect(rows).toHaveText("3 rows;");

  const cols = page.locator(".toolbar-group-cols-rows .toolbar-label.cols");
  await expect(cols).toHaveText("3 columns");
}

/**
 * Don't load JupyterLab webpage before running the tests.
 * This is required to ensure we capture all log messages.
 */
test.use({ autoGoto: false, tmpPath: "arbalister-viewer-tests" });

test("should emit an activation console message", async ({ page }) => {
  const logs: string[] = [];

  page.on("console", (message) => {
    logs.push(message.text());
  });

  await page.goto();

  expect(logs.filter((s) => s === "Launching JupyterLab extension arbalister")).toHaveLength(1);
});

test.describe
  .serial("Arbalister Viewer", () => {
    test.beforeAll(async ({ request, tmpPath }) => {
      const contents = galata.newContentsHelper(request);
      await contents.uploadFile(
        path.resolve(__dirname, `./test-files/test.csv`),
        `${tmpPath}/test.csv`,
      );

      await contents.uploadFile(
        path.resolve(__dirname, `./test-files/test.parquet`),
        `${tmpPath}/test.parquet`,
      );
    });

    test.afterAll(async ({ request, tmpPath }) => {
      const contents = galata.newContentsHelper(request);
      await contents.deleteDirectory(tmpPath);
    });

    test("open csv file and change a delimiter", async ({ page }) => {
      await checkFile(page, "test.csv", "arbalister_viewer_csv.png");
      await page.notebook.close(true);
    });

    test("open parquet file", async ({ page }) => {
      await checkFile(page, "test.parquet", "arbalister_viewer_parquet.png");
      await checkToolbar(page);
      await page.notebook.close(true);
    });
  });
