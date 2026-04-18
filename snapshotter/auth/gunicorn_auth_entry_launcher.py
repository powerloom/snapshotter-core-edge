"""
Gunicorn Auth Entry Launcher
This module sets up and launches a Gunicorn server for the authentication service.
It configures logging using the singleton logger, sets up workers, and initializes the application.

Set ``AUTH_HTTP_ENABLED=false`` (or ``0`` / ``no``) to skip starting the HTTP API. Use
``scripts/auth_registry.py`` for Redis registry changes instead.
"""
import logging
import os
import sys

from snapshotter.auth.conf import auth_settings
from snapshotter.auth.server_entry import app
from snapshotter.utils.default_logger import default_logger
from snapshotter.utils.gunicorn import StandaloneApplication
from snapshotter.utils.gunicorn import StubbedGunicornLogger

gunicorn_logger = default_logger.bind(module='AuthAPI')
# Configuration variables from environment
WORKERS = int(os.environ.get('GUNICORN_WORKERS', '5'))


class InterceptHandler(logging.Handler):
    """
    Custom logging handler to intercept standard logging and redirect to Loguru.

    This handler captures logs from the standard logging module and forwards them
    to Loguru, maintaining consistent logging across the application.
    """

    def emit(self, record):
        """
        Emit a log record.

        This method is called by the logging system to emit a log record. It translates
        the standard log record into a Loguru log entry.

        Args:
            record (logging.LogRecord): The log record to be emitted.
        """
        # Get corresponding Loguru level if it exists
        try:
            level = gunicorn_logger.level(record.levelname).name
        except ValueError:
            # If the level name is not recognized, use the numeric level
            level = record.levelno

        # Find the caller's frame to get accurate line numbers
        frame, depth = logging.currentframe(), 2
        while frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        # Log the message using Loguru with the appropriate context
        gunicorn_logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


def setup_logging():
    """
    Set up logging for the application.
    """

    # Intercept standard logging
    logging.basicConfig(handlers=[InterceptHandler()], level=0)

    # Intercept uvicorn and gunicorn logging
    for _logger in ['uvicorn', 'uvicorn.access', 'uvicorn.error', 'gunicorn', 'gunicorn.access', 'gunicorn.error']:
        logging.getLogger(_logger).handlers = [InterceptHandler()]


def _auth_http_enabled() -> bool:
    raw = os.environ.get("AUTH_HTTP_ENABLED", "true").strip().lower()
    return raw not in ("0", "false", "no", "off")


if __name__ == '__main__':
    if not _auth_http_enabled():
        print(
            "AUTH_HTTP_ENABLED is false: auth HTTP server not started. "
            "Use scripts/auth_registry.py against the auth registry Redis.",
            file=sys.stderr,
        )
        sys.exit(0)

    # Set up logging
    setup_logging()

    # Gunicorn server options
    options = {
        'bind': f'{auth_settings.bind.host}:{auth_settings.bind.port}',
        'workers': WORKERS,
        'accesslog': '-',  # Log to stdout
        'errorlog': '-',  # Log to stderr
        'worker_class': 'uvicorn.workers.UvicornWorker',
        'logger_class': StubbedGunicornLogger,
    }

    # Run the Gunicorn application
    StandaloneApplication(app, options).run()
