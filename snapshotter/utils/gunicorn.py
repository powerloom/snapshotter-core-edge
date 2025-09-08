import logging

from gunicorn.app.base import BaseApplication
from gunicorn.glogging import Logger

from snapshotter.utils.default_logger import default_logger

logger = default_logger.bind(module='Gunicorn')


class InterceptHandler(logging.Handler):
    """
    A logging handler that forwards standard logging records to the Loguru logger.

    This handler ensures that all logs from Gunicorn and its workers are routed through
    the Loguru-based default_logger, preserving formatting and context.
    """

    def emit(self, record):
        """
        Emit a log record by forwarding it to the Loguru logger.

        :param record: The log record to be emitted
        :type record: logging.LogRecord
        """
        try:
            # Map standard logging level to Loguru level name
            level = logger.level(record.levelname).name
        except Exception:
            level = record.levelno

        # Find the frame where the logging call was made, skipping logging internals
        frame, depth = logging.currentframe(), 2
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(
            level,
            record.getMessage(),
        )

class StubbedGunicornLogger(Logger):
    """
    A custom Gunicorn logger that routes Gunicorn logs to Loguru via InterceptHandler.

    This disables Gunicorn's default file logging and ensures all logs go through Loguru.
    """

    def setup(self, cfg):
        """
        Set up the logger to use InterceptHandler for both error and access logs.

        :param cfg: Gunicorn configuration object
        :type cfg: gunicorn.config.Config
        """
        handler = InterceptHandler()

        # Set up error logger
        self.error_logger = logging.getLogger('gunicorn.error')
        self.error_logger.handlers = []
        self.error_logger.propagate = False
        self.error_logger.addHandler(handler)
        self.error_logger.setLevel(logging.DEBUG)

        # Set up access logger
        self.access_logger = logging.getLogger('gunicorn.access')
        self.access_logger.handlers = []
        self.access_logger.propagate = False
        self.access_logger.addHandler(handler)
        self.access_logger.setLevel(logging.INFO)


class StandaloneApplication(BaseApplication):
    """
    A standalone Gunicorn application that can be run without a Gunicorn server.

    This class allows for programmatic configuration and running of a Gunicorn server
    with a given WSGI application.
    """

    def __init__(self, app, options=None):
        """
        Initialize the Gunicorn server with the given app and options.

        :param app: The WSGI application to run
        :type app: callable
        :param options: Optional dictionary of configuration options
        :type options: dict
        """
        self.options = options or {}
        self.application = app
        super().__init__()

    def load_config(self):
        """
        Load the configuration for the Gunicorn server.

        This function loads the configuration for the Gunicorn server from the options
        provided by the user. It sets the configuration values in the `cfg` object.
        """
        config = {
            key: value
            for key, value in self.options.items()
            if key in self.cfg.settings and value is not None
        }
        for key, value in config.items():
            self.cfg.set(key.lower(), value)

    def load(self):
        """
        Load the application and return it.

        :return: The WSGI application
        :rtype: callable
        """
        return self.application
