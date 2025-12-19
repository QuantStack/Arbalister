# Arbalister

[![Github Actions Status](https://github.com/QuantStack/Arbalister/workflows/Build/badge.svg)](https://github.com/QuantStack/Arbalister/actions/workflows/build.yml)

This viewer lets you double click on many file types supported in the Apache Arrow ecosystem
to automatically view it as tabular data (Csv, Parquet, Avro, Orc, Ipc).

For library authors, the server extension serves files in the Arrow IPC stream format.
It can be reused to provide other type of application specific viewers (*e.g.* as time series, ...).

This extension is composed two packages both called `arbalister`:
- A Python server extension available on PyPI;
- A Typescript client extension available on NPM.

## Requirements

- JupyterLab >= 4.5.0

## Install

To install the extension, execute:

```bash
pip install arbalister
```

## Uninstall

To remove the extension, execute:

```bash
pip uninstall arbalister
```

## Troubleshoot

If you are seeing the frontend extension, but it is not working, check
that the server extension is enabled:

```bash
jupyter server extension list
```

If the server extension is installed and enabled, but you are not seeing
the frontend extension, check the frontend extension is installed:

```bash
jupyter labextension list
```

## Contributing

### Development install

We use the [Pixi](https://prefix.dev/) Conda-compatible environment manager for development.
With this single tool, we can get most dependencies, including NodeJS and Python themselves.
Head to their site for installation instructions.

Run `pixi task list` for details on all available tasks.

Only the javascript packages need to be installed.
This is managed by the `jlpm` command, JupyterLab's pinned version of [yarn](https://yarnpkg.com/)
that is installed with JupyterLab in the Pixi file.
You may use `yarn` or `npm` in lieu of `jlpm` below.

To install the client-side extension for development, use:
```sh
pixi run install-dev
```

To run the extension in development mode in JupyterLab, run the following with pixi.
This will launch jupyter after making sure the extension is installed.
You can pass any other command line argument to forward them to JupyterLab.
```sh
pixi run serve-dev
```

You can watch the source directory and run JupyterLab at the same time in different terminals to
watch for changes in the extension's source and automatically rebuild the extension.
```sh
pixi run jlpm watch
```

With the watch command running, every saved change will immediately be built locally and available
in your running JupyterLab.
Refresh JupyterLab to load the change in your browser (you may need to wait several seconds for the
extension to be rebuilt).

By default, the `pixi run jlpm build` command generates the source maps for this extension to make
it easier to debug using the browser dev tools.
To also generate source maps for the JupyterLab core extensions, you can run the following command:

```sh
pixi run jupyter lab build --minimize=False
```

### Testing the extension

#### Server tests

This extension is using [Pytest](https://docs.pytest.org/) for Python code testing.
With pixi, simply run (pass any other command line argument to forward them to Pytest):

```sh
pixi run test-pytest
```

#### Frontend tests

This extension is using [Jest](https://jestjs.io/) for JavaScript code testing.
With pixi, simply run (pass any other command line argument to forward them to Jest):

```sh
pixi run test-jest
```

#### Integration tests

This extension uses [Playwright](https://playwright.dev/docs/intro) for the integration tests (aka
user level tests).
More precisely, the JupyterLab helper
[Galata](https://github.com/jupyterlab/jupyterlab/tree/master/galata) is used to handle testing the
extension in JupyterLab.

More information are provided within the [ui-tests](./ui-tests/README.md) README.

### Running code formatters and checks

To run all code formatters, use Pixi:

```sh
pixi run fmt
```

Similarly for the code checks:

```sh
pixi run check
```


### Packaging the extension

See [RELEASE](RELEASE.md)
