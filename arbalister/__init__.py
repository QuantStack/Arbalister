"""Jupyter server extension to serve content in Arrow format."""

try:
    from ._version import __version__
except ImportError:
    # Fallback when using the package in dev mode without installing
    # in editable mode with pip. It is highly recommended to install
    # the package from a stable release or in editable mode:
    # https://pip.pypa.io/en/stable/topics/local-project-installs/#editable-installs
    import warnings

    warnings.warn("Importing 'arbalister' outside a proper installation.", stacklevel=1)
    __version__ = "dev"

import jupyterlab.labapp

from . import arrow as arrow
from . import file_format as file_format
from . import params as params
from . import routes as routes


def _jupyter_labextension_paths() -> list[dict[str, str]]:
    return [{"src": "labextension", "dest": "arbalister"}]


def _jupyter_server_extension_points() -> list[dict[str, str]]:
    return [{"module": "arbalister"}]


def _load_jupyter_server_extension(server_app: jupyterlab.labapp.LabApp) -> None:
    """Register the API handler to receive HTTP requests from the frontend extension."""
    routes.setup_route_handlers(server_app.web_app)
    name = "arbalister"
    server_app.log.info(f"Registered {name} server extension")
