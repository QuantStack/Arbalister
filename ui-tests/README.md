# Integration Testing

This folder contains the integration tests of the extension.

They are defined using [Playwright](https://playwright.dev/docs/intro) test runner
and [Galata](https://github.com/jupyterlab/jupyterlab/tree/main/galata) helper.

The Playwright configuration is defined in [playwright.config.js](./playwright.config.js).

The JupyterLab server configuration to use for the integration test is defined
in [jupyter_server_test_config.py](./jupyter_server_test_config.py).

The default configuration will produce video for failing tests and an HTML report.

> There is a UI mode that you may like; see [that video](https://www.youtube.com/watch?v=jF0yA-JLQW0).

## Run the tests

You can run all tests through ``pixi``.
This will setup all required dependencies, including browsers downloads.
You can control the location of the download with the environment variable ``PLAYWRIGHT_BROWSERS_PATH``.

```sh
pixi run test-ui
```

Test results will be shown in the terminal.
In case of any test failures, the test report will be opened in your browser at the end of the
tests execution; see
[Playwright documentation](https://playwright.dev/docs/test-reporters#html-reporter)
for configuring that behavior.

## Update the tests snapshots

> All commands are assumed to be executed from the root directory

If you are comparing snapshots to validate your tests, you may need to update
the reference snapshots stored in the repository. To do that, you need to:

```sh
pixi run test-ui -u
```

> Some discrepancy may occurs between the snapshots generated on your computer and
> the one generated on the CI.
> To ease updating the snapshots on a PR, you can type `please update playwright snapshots` to
> trigger the update by a bot on the CI.
> Once the bot has computed new snapshots, it will commit them to the PR branch.

## Create tests

To create tests, the easiest way is to use the code generator tool of playwright:

Start the server with ``pixi``

```sh
pixi run jlpm-ui start
```

4. Execute the [Playwright code generator](https://playwright.dev/docs/codegen) in **another terminal**:

```sh
pixi run playwright codegen localhost:53729
```

## Debug tests

To debug tests, a good way is to use the inspector tool of playwright.
Execute the Playwright tests in [debug mode](https://playwright.dev/docs/debug):

```sh
pixi run test-ui --debug
```

## Upgrade Playwright and the browsers

To update the web browser versions, you must update the package `@playwright/test`:

```sh
pixi run jlpm-ui up "@playwright/test"
pixi run playwright install
```
