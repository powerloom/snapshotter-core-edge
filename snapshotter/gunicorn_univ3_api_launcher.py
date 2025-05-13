import os
import logging

from snapshotter.univ3_api import app
from snapshotter.settings.config import settings
from snapshotter.utils.gunicorn import StandaloneApplication
from snapshotter.utils.gunicorn import InterceptHandler

WORKERS = int(os.environ.get('GUNICORN_WORKERS', '5'))
JSON_LOGS = True if os.environ.get('JSON_LOGS', '0') == '1' else False

# Configure logging
logging.basicConfig(handlers=[InterceptHandler()], level=0)

# Intercept uvicorn and gunicorn logging
for _logger in ['uvicorn', 'uvicorn.access', 'uvicorn.error', 'gunicorn', 'gunicorn.access', 'gunicorn.error']:
    logging.getLogger(_logger).handlers = [InterceptHandler()]

if __name__ == '__main__':
    options = {
        'bind': f'{settings.core_api.host}:9003',
        'workers': WORKERS,
        'accesslog': '-',
        'errorlog': '-',
        'worker_class': 'uvicorn.workers.UvicornWorker',
    }

    StandaloneApplication(app, options).run()
